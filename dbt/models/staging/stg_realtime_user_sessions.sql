-- Staging : seances saisies en temps reel via le dashboard (Jalon 2,
-- sous-etape 3/5). POST /users/{user_id}/sessions -> Kafka
-- safelift-user-inputs -> scripts/consume_user_inputs.py ->
-- raw.realtime_user_sessions (creee/alimentee par ce consumer, PAS par un
-- job Spark).
--
-- Contrairement a stg_weight_training (grain = 1 ligne par SET), cette
-- source est DEJA au grain "1 ligne = 1 exercice complet d'une seance" : le
-- formulaire capture directement sets/reps agreges (pas chaque repetition
-- individuellement). Voir stg_workout_sessions_unified.sql pour
-- l'unification des deux grains avant fact_workout_session.
--
-- Memes macros de normalisation que stg_weight_training
-- (normalize_exercise_name / normalize_exercise_base_name) : garantit que
-- le matching vers dim_exercise (base sur normalized_exercise_name)
-- s'applique de facon identique aux deux sources, sans code duplique.
--
-- workout_name : weight_training porte un vrai libelle de programme
-- (colonne source) ; les seances temps reel n'en ont pas de natif (le
-- formulaire ne demande pas de nom de programme) -- valeur constante
-- 'Séance temps réel', qui sert aussi a distinguer visuellement l'origine
-- d'une ligne dans le panneau de detail du dashboard.

select
    user_id,
    exercise_name,
    {{ normalize_exercise_name('exercise_name') }} as normalized_exercise_name,
    {{ normalize_exercise_base_name('exercise_name') }} as normalized_exercise_base_name,
    sets,
    reps,
    (sets * reps) as total_reps,
    lifted_weight_kg,
    duration_seconds,
    performed_at,
    date(performed_at) as session_date,
    'Séance temps réel' as workout_name
from {{ source('raw', 'realtime_user_sessions') }}
where exercise_name is not null
  and performed_at is not null
  and user_id is not null
