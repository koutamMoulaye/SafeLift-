-- fact_workout_session : une ligne = un exercice realise dans une seance.
-- Source UNIQUE = weight_training (voir decision d'architecture : 600k_fitness
-- n'est jamais une source de faits, seulement le catalogue dim_exercise).
--
-- Grain d'agregation retenu : (session_date, workout_name,
-- normalized_exercise_name) -- PAS exercise_name brut : deux variantes
-- textuelles d'un meme exercice (ex. casse differente) qui normalisent vers
-- la meme valeur doivent fusionner en une seule ligne, sans quoi elles
-- produiraient deux lignes distinctes partageant pourtant le meme
-- exercise_id apres jointure sur dim_exercise -- violation du grain,
-- detectee par tests/assert_fact_workout_session_grain_unique.sql.
-- Le Silver weight_training est au grain "1 ligne = 1 set" (set_order
-- distingue les sets d'un meme exercice). "sets" n'existe donc pas comme
-- colonne brute -- c'est un decompte obtenu PAR REGROUPEMENT (comme demande),
-- d'ou l'agregation ci-dessous :
--   - sets          = nombre de sets loggees pour cet exercice ce jour-la
--   - reps          = repetitions moyennes PAR SET (arrondi), format usuel
--                      "3x10" plutot qu'un total brut
--   - total_reps    = somme exacte des repetitions (colonne technique
--                      supplementaire, necessaire au calcul EXACT du
--                      volume_factor dans fact_risk_score -- sets x reps
--                      moyen introduirait une approximation)
--   - lifted_weight_kg = poids moyen souleve sur les sets de cet exercice
--                         ce jour-la ("charge de travail" representative)
--   - duration_seconds = somme des durees (rarement non-nul sur ce dataset,
--                         voir data/silver/CLEANING_LOG.md)
--
-- Jointures en LEFT JOIN (pas INNER) : dim_exercise couvre par construction
-- 100% des exercise_name de weight_training (catalogue + non-matches
-- ajoutes), donc aucun orphelin n'est attendu -- mais un LEFT JOIN + tests
-- not_null (schema.yml) font ECHOUER le pipeline explicitement si ce n'etait
-- plus le cas, plutot que de faire disparaitre silencieusement des lignes
-- via un INNER JOIN.

with sets_detail as (
    select
        session_date,
        workout_name,
        normalized_exercise_name,
        set_order,
        reps,
        lifted_weight_kg,
        duration_seconds
    from {{ ref('stg_weight_training') }}
),

exercise_session_agg as (
    select
        session_date,
        workout_name,
        normalized_exercise_name,
        count(*) as sets,
        round(avg(reps)::numeric) as reps,
        sum(reps) as total_reps,
        round(avg(lifted_weight_kg)::numeric, 2) as lifted_weight_kg,
        sum(duration_seconds) as duration_seconds
    from sets_detail
    group by session_date, workout_name, normalized_exercise_name
),

demo_user as (
    select user_id
    from {{ ref('dim_user') }}
    where is_weight_training_demo_user
)

select
    row_number() over (
        order by e.session_date, e.workout_name, e.normalized_exercise_name
    ) as workout_session_id,
    ex.exercise_id,
    mu.muscle_id,
    du.user_id,
    dt.date_id,
    e.session_date,
    e.workout_name,
    e.sets,
    e.reps,
    e.total_reps,
    e.lifted_weight_kg,
    e.duration_seconds
from exercise_session_agg e
left join {{ ref('dim_exercise') }} ex
    on e.normalized_exercise_name = ex.normalized_exercise_name
left join {{ ref('dim_muscle') }} mu
    on ex.muscle_group = mu.muscle_group
cross join demo_user du
left join {{ ref('dim_date') }} dt
    on e.session_date = dt.date_id
