-- Test singulier dbt : un test PASSE si cette requete ne renvoie AUCUNE ligne.
-- Verifie que risk_score est toujours defini et strictement compris dans
-- l'intervalle [0, 100] (jamais null, jamais hors bornes), meme si la
-- normalisation dans fact_risk_score.sql comporte deja un GREATEST/LEAST --
-- ce test sert de garde-fou independant, pour detecter une regression si la
-- formule de normalisation est modifiee plus tard sans mettre a jour ce
-- test en meme temps.

select *
from {{ ref('fact_risk_score') }}
where risk_score is null
   or risk_score < 0
   or risk_score > 100
