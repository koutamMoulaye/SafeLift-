-- Staging : seances de musculation reellement loggees (grain = 1 exercice
-- realise dans une seance, 9 142 lignes attendues apres dedup Silver).
-- Unique source de fact_workout_session (voir decision d'architecture,
-- data/gold/GOLD_MODEL_DECISIONS.md : 600k_fitness N'EST PAS une source de
-- faits).
--
-- session_date : simplification retenue pour identifier une "seance" -- une
-- seance = tous les exercices partageant la meme date calendaire
-- (DATE(performed_at)). Le jeu de donnees source ne fournit pas d'identifiant
-- de seance explicite ; regrouper par jour calendaire est une approximation
-- raisonnable pour ce dataset (peu de personnes s'entrainent 2 fois le meme
-- jour), documentee comme hypothese de modelisation.

select
    workout_name,
    exercise_name,
    {{ normalize_exercise_name('exercise_name') }} as normalized_exercise_name,
    {{ normalize_exercise_base_name('exercise_name') }} as normalized_exercise_base_name,
    set_order,
    reps,
    distance,
    duration_seconds,
    lifted_weight_kg,
    performed_at,
    date(performed_at) as session_date
from {{ source('raw', 'silver_weight_training') }}
where exercise_name is not null
  and performed_at is not null
