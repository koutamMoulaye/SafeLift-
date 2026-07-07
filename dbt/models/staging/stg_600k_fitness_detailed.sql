-- Staging : catalogue de reference des exercices (grain source = 1 exercice
-- prescrit dans un programme, 604 129 lignes attendues). Sert uniquement a
-- construire dim_exercise (via exercise_name deduplique) -- ce n'est PAS une
-- source de faits (voir decision d'architecture dans
-- data/gold/GOLD_MODEL_DECISIONS.md et airflow/dags/gold_dbt_run.py).
--
-- 600k_fitness_summary n'a pas de modele staging dedie : aucune colonne de
-- cette table (program_length_weeks, level_list, goal_list...) n'est requise
-- par le modele Gold demande (dim_exercise ne se base que sur exercise_name).

select
    exercise_name,
    {{ normalize_exercise_name('exercise_name') }} as normalized_exercise_name,
    {{ normalize_exercise_base_name('exercise_name') }} as normalized_exercise_base_name,
    equipment as program_equipment  -- attribut du PROGRAMME, pas de l'exercice (voir dim_exercise.sql)
from {{ source('raw', 'silver_600k_fitness_detailed') }}
where exercise_name is not null
