-- Staging : aliments USDA FoodData Central ingeres via
-- airflow/dags/nutrition_ingestion.py -> spark/jobs/silver_usda_nutrition.py
-- (Jalon 3, sous-etape 1/6). Voir data/gold/GOLD_MODEL_DECISIONS.md
-- section 13 pour le detail de la collecte (mots-cles interroges,
-- filtrage dataType Foundation/SR Legacy, IDs de nutriments USDA utilises).

select
    fdc_id,
    food_name,
    food_category,
    data_type,
    kcal_per_100g,
    protein_g_per_100g,
    carbs_g_per_100g,
    fat_g_per_100g
from {{ source('raw', 'silver_usda_nutrition') }}
where food_name is not null
