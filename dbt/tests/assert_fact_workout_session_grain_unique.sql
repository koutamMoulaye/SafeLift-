-- Test singulier dbt : verifie l'absence de doublon sur le grain metier de
-- fact_workout_session, qui est (user_id, session_date, workout_name,
-- exercise_id) -- PAS workout_session_id, qui est un simple compteur
-- row_number() donc trivialement unique par construction et ne testerait
-- rien d'utile.
--
-- user_id AJOUTE au grain depuis le Jalon 2 sous-etape 3/5 (avant, tous les
-- faits appartenaient au meme demo_user, (session_date, workout_name,
-- exercise_id) suffisait ; desormais plusieurs utilisateurs reels peuvent
-- contribuer des seances via l'API temps reel, cf. fact_workout_session.sql).
--
-- Un test PASSE si cette requete ne renvoie AUCUNE ligne.

select
    user_id,
    session_date,
    workout_name,
    exercise_id,
    count(*) as duplicate_count
from {{ ref('fact_workout_session') }}
group by user_id, session_date, workout_name, exercise_id
having count(*) > 1
