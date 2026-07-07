-- ============================================================================
-- ATTENTION : TABLE 100% SYNTHETIQUE / FICTIVE -- NE JAMAIS MELANGER AUX
-- STATISTIQUES REELLES NI UTILISER DANS UN CALCUL AGREGE SUR LES VRAIES
-- DONNEES.
-- ============================================================================
-- Contexte : le correctif du normalisation de risk_score (voir
-- fact_risk_score.sql et data/gold/GOLD_MODEL_DECISIONS.md) a suffi a faire
-- apparaitre des scores "Eleve" reels (26 lignes / 1.2% sur les vraies
-- donnees). Cette table synthetique N'A DONC PAS ete creee pour compenser
-- une distribution plate -- elle est fournie en anticipation du besoin du
-- futur dashboard (etape 5) de disposer d'exemples CANONIQUES et
-- GARANTIS pour illustrer chaque seuil (Faible/Modere/Eleve), independamment
-- de la rarete naturelle des cas reels extremes dans le jeu de donnees actuel.
--
-- Chaque scenario est une combinaison de facteurs choisie a la main
-- (dbt/seeds/demo_synthetic_risk_scenarios.csv) pour illustrer un point
-- precis de la formule -- ce ne sont PAS des seances reellement effectuees.
-- Le calcul de risk_score reutilise EXACTEMENT la meme formule et les memes
-- bornes de normalisation que fact_risk_score.sql (variables dbt
-- risk_score_min_raw/risk_score_max_raw, dbt_project.yml) pour garantir que
-- ces exemples restent coherents avec le vrai moteur de calcul.
--
-- is_synthetic_demo = true sur 100% des lignes de cette table (redondant
-- avec le nom de la table, mais permet un filtre explicite si jamais cette
-- table etait un jour UNIONee par erreur avec fact_risk_score : filtrer sur
-- is_synthetic_demo = false ferait immediatement disparaitre ces lignes).

with scenarios as (
    select * from {{ ref('demo_synthetic_risk_scenarios') }}
),

raw_scored as (
    select
        scenario_id,
        scenario_label,
        muscle_group,
        base_zone,
        charge_factor,
        volume_factor,
        recup_factor,
        duree_factor,
        notes,
        (base_zone * charge_factor * volume_factor * recup_factor * duree_factor) as raw_risk_score
    from scenarios
),

normalized as (
    select
        *,
        round(
            least(greatest(
                100.0 * (raw_risk_score - {{ var('risk_score_min_raw') }})
                    / ({{ var('risk_score_max_raw') }} - {{ var('risk_score_min_raw') }})
            , 0), 100)::numeric
        , 2) as risk_score
    from raw_scored
)

select
    scenario_id,
    scenario_label,
    muscle_group,
    base_zone,
    charge_factor,
    volume_factor,
    recup_factor,
    duree_factor,
    round(raw_risk_score::numeric, 4) as raw_risk_score,
    risk_score,
    case
        when risk_score <= 33 then 'Faible'
        when risk_score <= 66 then 'Modere'
        else 'Eleve'
    end as risk_level,
    notes,
    true as is_synthetic_demo
from normalized
