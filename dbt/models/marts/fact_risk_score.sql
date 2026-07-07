-- fact_risk_score : meme grain que fact_workout_session (1 ligne = 1 exercice
-- realise dans une seance), enrichi du calcul de risque DETERMINISTE demande
-- (pas de ML). Chaque facteur est une colonne VISIBLE du modele final : le
-- dashboard peut donc justifier "pourquoi" un score est eleve, pas de boite
-- noire. Toutes les valeurs de facteurs/bornes sont des choix de
-- modelisation documentes ici ET dans data/gold/GOLD_MODEL_DECISIONS.md.
--
-- Formule : risk_score = base_zone x charge_factor x volume_factor
--                         x recup_factor x duree_factor
-- puis normalisation lineaire sur 0-100 (bornes theoriques ci-dessous).

with fact_with_week as (
    select
        f.*,
        d.week_start_date
    from {{ ref('fact_workout_session') }} f
    left join {{ ref('dim_date') }} d
        on f.date_id = d.date_id
),

-- Agregation hebdomadaire par (utilisateur, zone musculaire, semaine) :
-- base de calcul pour charge_factor et volume_factor.
weekly_muscle_agg as (
    select
        user_id,
        muscle_id,
        week_start_date,
        sum(lifted_weight_kg) as weekly_load_kg,
        sum(total_reps) as weekly_volume
    from fact_with_week
    group by user_id, muscle_id, week_start_date
),

weekly_muscle_factors as (
    select
        user_id,
        muscle_id,
        week_start_date,
        weekly_load_kg,
        weekly_volume,
        lag(weekly_load_kg) over (
            partition by user_id, muscle_id order by week_start_date
        ) as previous_week_load_kg,
        -- Moyenne des semaines STRICTEMENT anterieures (pas la semaine
        -- courante) : evite toute fuite/look-ahead dans le calcul de la
        -- moyenne historique.
        avg(weekly_volume) over (
            partition by user_id, muscle_id order by week_start_date
            rows between unbounded preceding and 1 preceding
        ) as historical_avg_volume
    from weekly_muscle_agg
),

weekly_muscle_factors_computed as (
    select
        user_id,
        muscle_id,
        week_start_date,

        -- charge_factor : penalite x1.3 (valeur choisie, documentee) si la
        -- charge hebdomadaire sur cette zone augmente de plus de 10% vs la
        -- semaine precedente. 1.0 (neutre) si pas de semaine precedente.
        case
            when previous_week_load_kg is not null
             and previous_week_load_kg > 0
             and (weekly_load_kg - previous_week_load_kg) / previous_week_load_kg > 0.10
                then 1.3
            else 1.0
        end as charge_factor,

        -- volume_factor : ratio volume de la semaine / moyenne historique de
        -- cette zone pour cet utilisateur, borne a [0.5, 2.0] (evite qu'un
        -- ratio extreme, ex. debut d'historique, ne domine le score total).
        -- 1.0 (neutre) si aucun historique anterieur disponible.
        case
            when historical_avg_volume is null or historical_avg_volume = 0
                then 1.0
            else least(greatest(weekly_volume / historical_avg_volume, 0.5), 2.0)
        end as volume_factor
    from weekly_muscle_factors
),

-- Recuperation : la meme zone a-t-elle ete sollicitee il y a moins de 48h ?
-- (grain "seance" = jour calendaire, cf. stg_weight_training.sql)
muscle_session_dates as (
    select distinct user_id, muscle_id, session_date
    from fact_with_week
),

muscle_session_recup as (
    select
        user_id,
        muscle_id,
        session_date,
        lag(session_date) over (
            partition by user_id, muscle_id order by session_date
        ) as previous_session_date
    from muscle_session_dates
),

muscle_session_recup_computed as (
    select
        user_id,
        muscle_id,
        session_date,
        -- recup_factor : penalite x1.4 (valeur choisie, documentee) si moins
        -- de 48h se sont ecoulees depuis la derniere sollicitation de cette
        -- meme zone. 1.0 si pas de seance anterieure sur cette zone.
        case
            when previous_session_date is not null
             and (session_date - previous_session_date) * 24 < 48
                then 1.4
            else 1.0
        end as recup_factor
    from muscle_session_recup
),

-- Duree totale de la seance (tous exercices confondus ce jour-la), pour duree_factor.
session_duration as (
    select
        user_id,
        session_date,
        sum(duration_seconds) as session_total_duration_seconds
    from fact_with_week
    group by user_id, session_date
),

scored as (
    select
        f.workout_session_id,
        f.exercise_id,
        f.muscle_id,
        f.user_id,
        f.date_id,
        f.session_date,
        f.workout_name,
        f.sets,
        f.reps,
        f.total_reps,
        f.lifted_weight_kg,
        f.duration_seconds,

        mu.base_epidemiological_risk as base_zone,
        wmf.charge_factor,
        wmf.volume_factor,
        msr.recup_factor,
        -- duree_factor : penalite x1.2 (valeur choisie, documentee) si la
        -- seance complete (tous exercices) depasse 2h (7200s) au total.
        case when sd.session_total_duration_seconds > 7200 then 1.2 else 1.0 end as duree_factor

    from fact_with_week f
    left join {{ ref('dim_muscle') }} mu
        on f.muscle_id = mu.muscle_id
    left join weekly_muscle_factors_computed wmf
        on f.user_id = wmf.user_id
       and f.muscle_id = wmf.muscle_id
       and f.week_start_date = wmf.week_start_date
    left join muscle_session_recup_computed msr
        on f.user_id = msr.user_id
       and f.muscle_id = msr.muscle_id
       and f.session_date = msr.session_date
    left join session_duration sd
        on f.user_id = sd.user_id
       and f.session_date = sd.session_date
),

raw_scored as (
    select
        *,
        (base_zone * charge_factor * volume_factor * recup_factor * duree_factor) as raw_risk_score
    from scored
),

-- Normalisation lineaire 0-100. Bornes = variables dbt partagees avec
-- fact_risk_score_demo_synthetic.sql (dbt_project.yml : risk_score_min_raw /
-- risk_score_max_raw), pour eviter de dupliquer ces constantes.
--   min = 0.10 (base la plus faible, cf. dim_muscle) x 1.0 x 0.5 (plancher
--         volume_factor) x 1.0 x 1.0 = 0.05
--   max = 0.25 (base la plus elevee) x 1.3 x 2.0 (plafond volume_factor)
--         x 1.4 x 1.0 = 0.86
--
-- CORRECTIF (voir data/gold/GOLD_MODEL_DECISIONS.md, section "Correctif
-- duree_factor") : le plafond de duree_factor (1.2) est VOLONTAIREMENT
-- EXCLU du calcul de la borne max, contrairement a la version initiale
-- (qui utilisait 1.092 = ...x1.4x1.2). Raison : duration_seconds est fiable
-- a 0% sur ce dataset (quasi toutes les valeurs sont a 0, cf.
-- data/silver/CLEANING_LOG.md), donc duree_factor ne vaut JAMAIS 1.2 en
-- pratique -- il reste neutre (1.0) sur 100% des lignes reelles. Inclure ce
-- plafond theorique jamais atteignable dans le denominateur de
-- normalisation compressait artificiellement TOUS les scores reels vers le
-- bas, sans aucune justification metier. duree_factor reste neanmoins
-- implemente et actif ligne par ligne (il redeviendra pleinement effectif
-- si des donnees de duree fiables sont disponibles un jour) ; seule la
-- borne de normalisation a ete recalibree pour refleter honnetement ce que
-- CE dataset permet reellement d'observer.
normalized as (
    select
        *,
        round(
            least(greatest(
                100.0 * (raw_risk_score - {{ var('risk_score_min_raw') }})
                    / ({{ var('risk_score_max_raw') }} - {{ var('risk_score_min_raw') }})
            , 0), 100)::numeric
        , 2) as risk_score
    from raw_scored
)

select
    workout_session_id,
    exercise_id,
    muscle_id,
    user_id,
    date_id,
    session_date,
    workout_name,
    sets,
    reps,
    total_reps,
    lifted_weight_kg,
    duration_seconds,
    base_zone,
    charge_factor,
    volume_factor,
    recup_factor,
    duree_factor,
    round(raw_risk_score::numeric, 4) as raw_risk_score,
    risk_score,
    case
        when risk_score <= 33 then 'Faible'
        when risk_score <= 66 then 'Modere'
        else 'Eleve'
    end as risk_level
from normalized
