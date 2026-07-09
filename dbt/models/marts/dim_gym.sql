-- dim_gym : salles SafeLift (Jalon 2, sous-etape 1/5 -- streaming affluence).
--
-- ATTENTION : donnees 100% FICTIVES. Aucun dataset Kaggle d'affluence de
-- salle de sport en temps reel n'est disponible (perimetre du projet) ;
-- le cahier des charges du Jalon 2 a explicitement retenu un simulateur
-- custom (scripts/simulate_gym_occupancy.py) plutot qu'un dataset
-- synthetique externe. Ces 5 salles (nom, ville/quartier, capacite) sont
-- inventees pour ce projet -- voir data/gold/GOLD_MODEL_DECISIONS.md
-- section 9. Ne jamais les presenter comme des etablissements reels dans
-- le rapport de certification.
--
-- Alimentee par un seed dbt (dbt/seeds/dim_gym_seed.csv) plutot que par une
-- source Bronze/Silver : il n'y a aucune donnee source a transformer, le
-- seed EST directement la donnee de reference (meme pattern que
-- dbt/seeds/demo_synthetic_risk_scenarios.csv pour
-- fact_risk_score_demo_synthetic.sql).

select
    gym_id,
    gym_name,
    city,
    neighborhood,
    capacity_max
from {{ ref('dim_gym_seed') }}
