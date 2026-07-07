-- dim_muscle : zones musculaires distinctes trouvees dans dim_exercise,
-- avec un score de risque epidemiologique de base par zone.
--
-- Valeurs shoulder=0.25, knee=0.20, lower_back=0.18 : ordres de grandeur
-- retenus pour ce projet (zones classiquement associees a un risque de
-- blessure plus eleve en musculation -- epaule/genou/lombaires), NON issus
-- d'une etude epidemiologique citee et verifiee. A traiter avec prudence
-- dans le rapport de certification (voir data/gold/GOLD_MODEL_DECISIONS.md).
--
-- Valeur par defaut 0.10 pour toute autre zone (chest, legs, abs, arms,
-- back, unknown) : AUCUNE SOURCE EPIDEMIOLOGIQUE IDENTIFIEE POUR CES ZONES
-- PRECISES DANS LE CADRE DE CE PROJET -- hypothese de modelisation, PAS une
-- donnee epidemiologique verifiee. Ce commentaire est volontairement
-- duplique dans GOLD_MODEL_DECISIONS.md pour qu'il ne passe pas inapercu.

with distinct_muscles as (
    select distinct muscle_group
    from {{ ref('dim_exercise') }}
)

select
    row_number() over (order by muscle_group) as muscle_id,
    muscle_group,
    case muscle_group
        when 'shoulder' then 0.25
        when 'knee' then 0.20
        when 'lower_back' then 0.18
        else 0.10  -- valeur par defaut, hypothese de modelisation (voir commentaire ci-dessus)
    end as base_epidemiological_risk
from distinct_muscles
