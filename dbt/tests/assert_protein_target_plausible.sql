-- Test singulier dbt : un test PASSE si cette requete ne renvoie AUCUNE ligne.
-- Verifie que protein_target_g_per_day (fact_nutrition_target) reste
-- coherent avec le poids de l'utilisateur : jamais negatif ou nul, et
-- jamais superieur a 4g/kg (protein_g_per_kg_target est borne a 1.6-2.2
-- dans le modele -- 4g/kg est une marge large au-dela de toute
-- recommandation serieuse meme pour un athlete de haut niveau, garde-fou
-- contre une regression de la formule, pas une borne medicale exacte).
-- Voir data/gold/GOLD_MODEL_DECISIONS.md section 13.

select *
from {{ ref('fact_nutrition_target') }}
where protein_target_g_per_day is null
   or protein_target_g_per_day <= 0
   or protein_target_g_per_day > 4 * body_weight_kg
