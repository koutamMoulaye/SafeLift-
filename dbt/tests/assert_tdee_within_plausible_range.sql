-- Test singulier dbt : un test PASSE si cette requete ne renvoie AUCUNE ligne.
-- Verifie que tdee_kcal (fact_nutrition_target) reste toujours defini et
-- dans une fourchette PHYSIOLOGIQUEMENT PLAUSIBLE (1000-6000 kcal/jour) --
-- pas une borne medicale exacte, mais un garde-fou simple pour detecter une
-- valeur absurde due a un bug de formule (ex. conversion d'unite oubliee,
-- facteur d'activite mal applique) plutot qu'un vrai profil extreme. Voir
-- data/gold/GOLD_MODEL_DECISIONS.md section 13.

select *
from {{ ref('fact_nutrition_target') }}
where tdee_kcal is null
   or tdee_kcal < 1000
   or tdee_kcal > 6000
