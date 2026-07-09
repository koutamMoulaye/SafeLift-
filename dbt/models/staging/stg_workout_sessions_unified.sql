-- Union des deux sources de seances de musculation, AVANT construction de
-- fact_workout_session (Jalon 2, sous-etape 3/5 -- voir
-- data/gold/GOLD_MODEL_DECISIONS.md section 11 pour la decision
-- d'architecture complete) :
--
--   1. weight_training (Kaggle, historique statique 2015-2018, rattache au
--      "demo_user" par hypothese de demonstration -- voir dim_user.sql).
--      Grain SOURCE = 1 ligne par SET (stg_weight_training) ; AGREGE ICI au
--      grain (session_date, workout_name, normalized_exercise_name) pour
--      rejoindre le grain de la 2e source.
--   2. realtime_user_sessions (saisies utilisateur temps reel via le
--      dashboard). DEJA au bon grain (1 ligne = 1 exercice complet, sets/reps
--      deja agreges cote formulaire) -- aucune agregation necessaire ici.
--
-- user_id est NULL pour la source weight_training (aucun identifiant
-- utilisateur natif dans ce dataset) : rattache au demo_user par
-- fact_workout_session.sql via COALESCE -- MEME hypothese de demonstration
-- qu'avant cette sous-etape, seulement deplacee un cran plus tard dans le
-- pipeline (etait un `cross join demo_user` directement dans
-- fact_workout_session.sql, avant l'introduction d'une 2e source reelle).
-- user_id est REEL et OBLIGATOIRE pour la source realtime (l'API
-- POST /users/{user_id}/sessions exige un user_id existant, verifie avant
-- publication Kafka).
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

weight_training_agg as (
    select
        cast(null as integer) as user_id,
        session_date,
        workout_name,
        normalized_exercise_name,
        count(*) as sets,
        round(avg(reps)::numeric) as reps,
        sum(reps) as total_reps,
        round(avg(lifted_weight_kg)::numeric, 2) as lifted_weight_kg,
        sum(duration_seconds) as duration_seconds,
        'weight_training' as source_dataset
    from weight_training_per_set
    group by session_date, workout_name, normalized_exercise_name
),

realtime_sessions as (
    select
        user_id,
        session_date,
        workout_name,
        normalized_exercise_name,
        sets,
        reps,
        total_reps,
        lifted_weight_kg,
        duration_seconds,
        'realtime_user_input' as source_dataset
    from {{ ref('stg_realtime_user_sessions') }}
)

select * from weight_training_agg
union all
select * from realtime_sessions
