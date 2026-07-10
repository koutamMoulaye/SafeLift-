-- Union des deux sources de seances de musculation, AVANT construction de
-- fact_workout_session (Jalon 2, sous-etape 3/5 -- voir
-- data/gold/GOLD_MODEL_DECISIONS.md section 11 pour la decision
-- d'architecture complete) :
--
--   1. weight_training (Kaggle, historique statique 2015-2018, reparti
--      entre PLUSIEURS "demo_users" depuis l'extension multi-profils
--      (2026-07-11) -- voir dim_user.sql et
--      dbt/seeds/demo_user_blocks_seed.csv). Grain SOURCE = 1 ligne par SET
--      (stg_weight_training) ; AGREGE ICI au grain (user_id, session_date,
--      workout_name, normalized_exercise_name) pour rejoindre le grain de
--      la 2e source.
--   2. realtime_user_sessions (saisies utilisateur temps reel via le
--      dashboard). DEJA au bon grain (1 ligne = 1 exercice complet, sets/reps
--      deja agreges cote formulaire) -- aucune agregation necessaire ici.
--
-- ⚠️ EXTENSION MULTI-PROFILS (2026-07-11) : user_id pour la source
-- weight_training est desormais REEL et NON-NULL, assigne par
-- demo_user_blocks_seed.csv (jointure sur session_date BETWEEN date_from
-- ET date_to) -- AVANT cette extension, user_id etait NULL ici et
-- rattache a un seul demo_user via un `cross join` dans
-- fact_workout_session.sql (limite a un seul profil, incompatible avec
-- plusieurs profils demo). Chaque bloc chronologique CONTIGU du dataset
-- (aucun melange de dates) est desormais rattache a un profil dim_user
-- REEL distinct -- HYPOTHESE DE DEMONSTRATION toujours assumee (aucune
-- cle de jointure reelle entre gym_members et weight_training), voir
-- data/gold/GOLD_MODEL_DECISIONS.md section 5 et dim_user.sql. user_id
-- reste REEL et OBLIGATOIRE pour la source realtime (l'API
-- POST /users/{user_id}/sessions exige un user_id existant, verifie avant
-- publication Kafka) -- inchange par cette extension.
--
-- IMPORTANT : dim_exercise.sql continue de lire stg_weight_training et
-- stg_600k_fitness_detailed DIRECTEMENT (pas ce modele unifie) -- choix
-- deliberer pour ne JAMAIS perturber le taux de matching deja verifie et
-- documente (38.3%->90.1%, voir GOLD_MODEL_DECISIONS.md section 2). Les 2
-- sources unifiees ici utilisent neanmoins les MEMES macros de
-- normalisation (normalize_exercise_name / normalize_exercise_base_name),
-- donc le join sur normalized_exercise_name dans fact_workout_session.sql
-- s'applique de facon identique aux deux -- voir aussi la limite assumee
-- dans GOLD_MODEL_DECISIONS.md section 11 (un exercice saisi en temps reel
-- qui ne matche AUCUN normalized_exercise_name deja connu de dim_exercise
-- resterait orphelin ; le formulaire du dashboard contourne ce risque en
-- restreignant le choix aux exercices deja pratiques par l'utilisateur,
-- cf. GET /users/{user_id}/exercises, deja utilise par le simulateur what-if).

with weight_training_per_set as (
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

-- Assignation du user_id reel par bloc chronologique (voir seed) : un
-- LEFT JOIN + test not_null en aval (schema.yml) fait ECHOUER le pipeline
-- explicitement si une session_date ne tombait dans AUCUN bloc, plutot que
-- de laisser passer silencieusement une ligne orpheline (les 5 blocs du
-- seed sont contigus et couvrent l'integralite des 570 jours de seance
-- reels, verifie a la creation du seed).
weight_training_with_user as (
    select
        b.user_id,
        w.session_date,
        w.workout_name,
        w.normalized_exercise_name,
        w.set_order,
        w.reps,
        w.lifted_weight_kg,
        w.duration_seconds
    from weight_training_per_set w
    left join {{ ref('demo_user_blocks_seed') }} b
        on w.session_date between b.date_from and b.date_to
),

weight_training_agg as (
    select
        user_id,
        session_date,
        workout_name,
        normalized_exercise_name,
        count(*) as sets,
        round(avg(reps)::numeric) as reps,
        sum(reps) as total_reps,
        round(avg(lifted_weight_kg)::numeric, 2) as lifted_weight_kg,
        sum(duration_seconds) as duration_seconds,
        'weight_training' as source_dataset
    from weight_training_with_user
    group by user_id, session_date, workout_name, normalized_exercise_name
),

-- ⚠️ BUG REEL trouve et corrige (2026-07-11, pendant l'extension
-- multi-profils, sans rapport avec elle) : `stg_realtime_user_sessions`
-- est deja au grain "1 ligne = 1 exercice complet", mais RIEN n'empechait
-- 2 soumissions du MEME exercice le MEME jour calendaire (ex: 2 tests
-- reels de "Bench Press (Barbell)" pour user_id=9 le 2026-07-10, faits a
-- des heures differentes lors de sessions de verification anterieures)
-- de produire 2 lignes distinctes partageant pourtant le meme grain
-- (user_id, session_date, workout_name, exercise_id) -- exactement le
-- meme type de violation que le bug deja documente pour weight_training
-- (voir plus haut dans ce fichier / GOLD_MODEL_DECISIONS.md), detecte ici
-- par tests/assert_fact_workout_session_grain_unique.sql. Corrige en
-- agregeant EGALEMENT cette source au meme grain, pondere par le nombre
-- de sets de chaque soumission (pas une simple moyenne non ponderee : une
-- soumission de 5 sets doit peser plus qu'une soumission de 2 sets dans
-- la charge/repetition moyenne resultante).
realtime_sessions as (
    select
        user_id,
        session_date,
        workout_name,
        normalized_exercise_name,
        sum(sets) as sets,
        round(sum(total_reps)::numeric / nullif(sum(sets), 0)) as reps,
        sum(total_reps) as total_reps,
        round((sum(lifted_weight_kg * sets)::numeric / nullif(sum(sets), 0)), 2) as lifted_weight_kg,
        sum(duration_seconds) as duration_seconds,
        'realtime_user_input' as source_dataset
    from {{ ref('stg_realtime_user_sessions') }}
    group by user_id, session_date, workout_name, normalized_exercise_name
)

select * from weight_training_agg
union all
select * from realtime_sessions
