-- fact_nutrition_target : besoins nutritionnels cibles par utilisateur
-- (Jalon 3, sous-etape 1/6). Formules DETERMINISTES et STANDARD
-- (litterature sportive generaliste), PAS une recommandation medicale
-- personnalisee -- voir data/gold/GOLD_MODEL_DECISIONS.md section 13 pour
-- le detail complet des formules/hypotheses et le rappel du cadre ethique
-- du projet (SafeLift ne remplace ni un coach ni un medecin).
--
-- Grain : 1 ligne par utilisateur de dim_user (973 lignes attendues),
-- recalculee integralement a chaque run dbt -- PAS un historique date
-- comme fact_risk_score (aucune notion de "seance", juste un profil
-- courant).
--
-- Tous les facteurs intermediaires (bmr_kcal, activity_factor,
-- protein_g_per_kg_target) restent des colonnes VISIBLES du modele final,
-- meme philosophie "pas de boite noire" que fact_risk_score.sql.

with users as (
    select * from {{ ref('dim_user') }}
),

with_bmr as (
    select
        user_id,
        age,
        gender,
        body_weight_kg,
        height_m,
        workout_frequency_days_per_week,
        experience_level,

        -- BMR (Mifflin-St Jeor, 1990) -- formule standard la plus citee
        -- pour estimer le metabolisme de base a partir du poids/taille/
        -- age/sexe. Hauteur convertie m -> cm (x100). Les 2 seules valeurs
        -- de gender presentes dans gym_members sont "Male"/"Female"
        -- (verifie sur les 973 lignes) : aucune branche de repli, un
        -- gender inattendu remonterait un bmr_kcal NULL, detecte
        -- explicitement par le test not_null ci-dessous plutot que
        -- suppose silencieusement. Voir GOLD_MODEL_DECISIONS.md section 13
        -- pour la formule complete et sa source.
        case
            when gender = 'Male' then
                10 * body_weight_kg + 6.25 * (height_m * 100) - 5 * age + 5
            when gender = 'Female' then
                10 * body_weight_kg + 6.25 * (height_m * 100) - 5 * age - 161
        end as bmr_kcal
    from users
),

with_factors as (
    select
        *,
        -- Facteur d'activite (tables Harris-Benedict/Mifflin usuelles),
        -- deduit de workout_frequency_days_per_week (gym_members) --
        -- mapping documente en detail dans GOLD_MODEL_DECISIONS.md
        -- section 13. Bornes <=1 et >=6 defensives (jamais observees dans
        -- ce dataset, qui ne couvre que 2 a 5 jours/semaine), pour rester
        -- robuste si un profil futur en sortait.
        case
            when workout_frequency_days_per_week <= 1 then 1.2    -- sedentaire
            when workout_frequency_days_per_week = 2 then 1.375   -- legerement actif
            when workout_frequency_days_per_week = 3 then 1.55    -- moderement actif
            when workout_frequency_days_per_week = 4 then 1.725   -- actif
            when workout_frequency_days_per_week >= 5 then 1.9    -- tres actif
        end as activity_factor,

        -- Besoin proteique cible (g par kg de poids corporel), dans la
        -- fourchette 1.6-2.2 g/kg couramment citee pour les pratiquants de
        -- musculation/fitness -- deduit ici de experience_level (1/2/3,
        -- gym_members), documente en detail dans GOLD_MODEL_DECISIONS.md
        -- section 13.
        case
            when experience_level = 1 then 1.6
            when experience_level = 2 then 1.9
            when experience_level = 3 then 2.2
        end as protein_g_per_kg_target
    from with_bmr
)

select
    user_id,
    age,
    gender,
    body_weight_kg,
    height_m,
    workout_frequency_days_per_week,
    activity_factor,
    experience_level,
    protein_g_per_kg_target,
    round(bmr_kcal::numeric, 0) as bmr_kcal,
    round((bmr_kcal * activity_factor)::numeric, 0) as tdee_kcal,
    round((protein_g_per_kg_target * body_weight_kg)::numeric, 1) as protein_target_g_per_day
from with_factors
