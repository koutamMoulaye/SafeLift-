-- Test singulier dbt : verifie l'absence de doublon sur le grain metier de
-- fact_workout_session, qui est (session_date, workout_name, exercise_name)
-- -- PAS workout_session_id, qui est un simple compteur row_number() donc
-- trivialement unique par construction et ne testerait rien d'utile.
-- Un test PASSE si cette requete ne renvoie AUCUNE ligne.

select
    session_date,
    workout_name,
    exercise_id,
    count(*) as duplicate_count
from {{ ref('fact_workout_session') }}
group by session_date, workout_name, exercise_id
having count(*) > 1
