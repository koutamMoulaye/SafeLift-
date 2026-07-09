-- dim_nutrition : catalogue d'aliments courants (USDA FoodData Central),
-- pour un contexte fitness/nutrition (sources de proteines/glucides,
-- legumes, fruits courants). Jalon 3, sous-etape 1/6. Toutes les valeurs
-- de macro-nutriments sont exprimees PAR 100G (convention native des
-- dataType Foundation/SR Legacy de l'API USDA, aucune conversion
-- appliquee). Voir data/gold/GOLD_MODEL_DECISIONS.md section 13 pour le
-- detail complet de la collecte.

select
    fdc_id,
    food_name,
    food_category,
    kcal_per_100g,
    protein_g_per_100g,
    carbs_g_per_100g,
    fat_g_per_100g
from {{ ref('stg_usda_nutrition') }}
