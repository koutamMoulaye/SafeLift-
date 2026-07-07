-- dim_user : profils d'utilisateurs de la plateforme (973 lignes attendues,
-- un profil = un user_id, source unique = gym_members).
--
-- Colonne is_weight_training_demo_user : marque LE SEUL profil auquel les
-- 9 142 lignes de fact_workout_session (issues de weight_training) seront
-- rattachees. C'est une HYPOTHESE DE DEMONSTRATION, pas une jointure de
-- donnees reelle : weight_training (journal personnel de seances sur ~3 ans)
-- ne contient AUCUN identifiant utilisateur, et aucune cle commune n'existe
-- entre les deux jeux de donnees sources. Voir
-- data/gold/GOLD_MODEL_DECISIONS.md pour la justification complete.
--
-- Critere de selection du profil (100% deterministe) :
--   1. experience_level le plus eleve (coherent avec un historique
--      d'entrainement de plusieurs annees, tel que celui de weight_training)
--   2. en cas d'egalite, workout_frequency_days_per_week le plus eleve
--   3. en dernier recours, user_id le plus petit (departage arbitraire mais stable)

with users as (
    select * from {{ ref('stg_gym_members') }}
),

demo_user_selection as (
    select user_id
    from users
    order by experience_level desc, workout_frequency_days_per_week desc, user_id asc
    limit 1
)

select
    u.user_id,
    u.age,
    u.gender,
    u.body_weight_kg,
    u.height_m,
    u.max_bpm,
    u.avg_bpm,
    u.resting_bpm,
    u.session_duration_hours,
    u.calories_burned,
    u.workout_type,
    u.fat_percentage,
    u.water_intake_liters,
    u.workout_frequency_days_per_week,
    u.experience_level,
    u.bmi,
    (u.user_id = d.user_id) as is_weight_training_demo_user
from users u
cross join demo_user_selection d
