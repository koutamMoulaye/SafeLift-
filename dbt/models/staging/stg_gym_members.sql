-- Staging : profils d'utilisateurs de la plateforme (973 lignes attendues).
-- Ajoute uniquement une cle de substitution user_id (aucune cle naturelle
-- fiable dans la source -- row_number sur un tri stable garantit un id
-- deterministe et reproductible d'un run dbt a l'autre, tant que le contenu
-- de la table source ne change pas).

select
    row_number() over (
        order by age, gender, body_weight_kg, height_m, experience_level
    ) as user_id,
    age,
    gender,
    body_weight_kg,
    height_m,
    max_bpm,
    avg_bpm,
    resting_bpm,
    session_duration_hours,
    calories_burned,
    workout_type,
    fat_percentage,
    water_intake_liters,
    workout_frequency_days_per_week,
    experience_level,
    bmi
from {{ source('raw', 'silver_gym_members') }}
