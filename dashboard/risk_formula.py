"""SafeLift — Feature A (simulateur what-if) : formule de risque partagee.

Extrait de dbt/models/marts/fact_risk_score.sql les CONSTANTES et la logique
de calcul (formule deterministe, pas de ML), pour permettre un calcul a la
volee cote API sur une hypothese NON enregistree en base.

**Duplication assumee, documentee explicitement (voir CLAUDE.md)** : dbt
calcule risk_score en BATCH sur l'historique reel (agregations SQL par
semaine/session) ; ce module calcule un score EQUIVALENT sur un scenario
hypothetique ponctuel (aucune notion de "semaine suivante"/"session
suivante" n'existe pour une hypothese). Les CONSTANTES (seuils, penalites,
bornes de normalisation, seuils de niveau) sont recopiees a l'identique.
Toute evolution future de dbt/models/marts/fact_risk_score.sql ou de
dbt/dbt_project.yml (vars risk_score_min_raw/risk_score_max_raw) DOIT etre
repercutee ici manuellement -- aucune synchronisation automatique.

base_zone n'est PAS duplique ici (ce n'est pas une constante de formule mais
une DONNEE par zone) : il est toujours lu en direct dans
gold.dim_muscle.base_epidemiological_risk par l'appelant, memes valeurs que
dbt/models/marts/dim_muscle.sql.
"""

from datetime import date

# --- Constantes dupliquees depuis dbt/models/marts/fact_risk_score.sql ---

CHARGE_INCREASE_THRESHOLD = 0.10  # +10% vs baseline -> penalite
CHARGE_FACTOR_PENALTY = 1.3
CHARGE_FACTOR_NEUTRAL = 1.0

VOLUME_FACTOR_MIN = 0.5
VOLUME_FACTOR_MAX = 2.0
VOLUME_FACTOR_NEUTRAL = 1.0

RECUP_THRESHOLD_HOURS = 48  # < 48h depuis la derniere sollicitation -> penalite
RECUP_FACTOR_PENALTY = 1.4
RECUP_FACTOR_NEUTRAL = 1.0

DUREE_THRESHOLD_SECONDS = 7200  # > 2h -> penalite
DUREE_FACTOR_PENALTY = 1.2
DUREE_FACTOR_NEUTRAL = 1.0

# Bornes de normalisation : dupliquees depuis dbt/dbt_project.yml (vars
# risk_score_min_raw / risk_score_max_raw), voir le commentaire "CORRECTIF
# duree_factor" dans fact_risk_score.sql pour la justification complete.
RISK_SCORE_MIN_RAW = 0.05
RISK_SCORE_MAX_RAW = 0.86

# Seuils de niveau : identiques au CASE final de fact_risk_score.sql.
RISK_LEVEL_FAIBLE_MAX = 33
RISK_LEVEL_MODERE_MAX = 66


def compute_charge_factor(hypothetical_kg: float, baseline_avg_kg: float | None) -> tuple[float, float | None]:
    """charge_factor hypothetique : compare la charge hypothetique a la
    MOYENNE reelle historique de l'utilisateur (pas a zero), contrairement a
    dbt qui compare la semaine courante a la semaine PRECEDENTE -- une
    hypothese ponctuelle n'a pas de "semaine precedente", la moyenne
    historique est l'equivalent le plus proche et le plus stable. Deviation
    documentee explicitement (voir CLAUDE.md).

    Renvoie (charge_factor, pourcentage_variation_ou_None).
    """
    if baseline_avg_kg is None or baseline_avg_kg <= 0:
        return CHARGE_FACTOR_NEUTRAL, None

    pct_change = (hypothetical_kg - baseline_avg_kg) / baseline_avg_kg
    factor = CHARGE_FACTOR_PENALTY if pct_change > CHARGE_INCREASE_THRESHOLD else CHARGE_FACTOR_NEUTRAL
    return factor, pct_change


def compute_volume_factor(hypothetical_volume: float, baseline_avg_volume: float | None) -> tuple[float, float | None]:
    """volume_factor hypothetique : ratio volume hypothetique (sets x reps) /
    moyenne historique reelle de cet exercice/zone, borne a [0.5, 2.0] --
    memes bornes que dbt (qui compare un volume HEBDOMADAIRE a la moyenne des
    semaines precedentes). Ici, sans notion de semaine pour une hypothese
    ponctuelle, la comparaison se fait directement contre la moyenne
    historique par occurrence (deviation documentee, memes bornes).

    Renvoie (volume_factor, ratio_ou_None).
    """
    if baseline_avg_volume is None or baseline_avg_volume <= 0:
        return VOLUME_FACTOR_NEUTRAL, None

    ratio = hypothetical_volume / baseline_avg_volume
    factor = min(max(ratio, VOLUME_FACTOR_MIN), VOLUME_FACTOR_MAX)
    return factor, ratio


def compute_recup_factor(last_session_date: date | None, reference_date: date) -> tuple[float, int | None]:
    """recup_factor hypothetique : compare `reference_date` (date "aujourd'hui"
    de l'hypothese) a la VRAIE derniere date de sollicitation de cette zone
    par cet utilisateur, deja en base (gold.fact_workout_session) -- rien
    n'est invente ici, conformement a la consigne.

    Limite honnete du dataset (documentee en CLAUDE.md/PROGRESS.md) : la
    derniere seance reelle de weight_training remonte a 2018-09-29, tres loin
    dans le passe par rapport a la date reelle d'utilisation de l'API -- ce
    facteur sera donc quasi toujours neutre (1.0) en pratique avec
    `reference_date = date.today()`, sauf si une seance reelle a ete loguee
    dans les 48h precedant l'appel.

    Renvoie (recup_factor, heures_ecoulees_ou_None).
    """
    if last_session_date is None:
        return RECUP_FACTOR_NEUTRAL, None

    hours_elapsed = (reference_date - last_session_date).days * 24
    factor = RECUP_FACTOR_PENALTY if hours_elapsed < RECUP_THRESHOLD_HOURS else RECUP_FACTOR_NEUTRAL
    return factor, hours_elapsed


def compute_duree_factor(hypothetical_duration_seconds: float) -> float:
    """duree_factor hypothetique : identique a dbt (le "session_total_duration_seconds"
    de dbt correspond, pour ce simulateur, a la duree hypothetique fournie --
    l'hypothese porte sur UN exercice unique traite comme la seance complete)."""
    return DUREE_FACTOR_PENALTY if hypothetical_duration_seconds > DUREE_THRESHOLD_SECONDS else DUREE_FACTOR_NEUTRAL


def risk_level_from_score(risk_score: float) -> str:
    """Meme CASE que fact_risk_score.sql : <=33 Faible, <=66 Modere, sinon Eleve."""
    if risk_score <= RISK_LEVEL_FAIBLE_MAX:
        return "Faible"
    if risk_score <= RISK_LEVEL_MODERE_MAX:
        return "Modere"
    return "Eleve"


def compute_risk_score(
    base_zone: float,
    charge_factor: float,
    volume_factor: float,
    recup_factor: float,
    duree_factor: float,
) -> tuple[float, float, str]:
    """Formule complete : base_zone x charge_factor x volume_factor x
    recup_factor x duree_factor, puis normalisation lineaire 0-100 sur les
    memes bornes que dbt.

    Renvoie (raw_risk_score, risk_score_normalise, risk_level).
    """
    raw_risk_score = base_zone * charge_factor * volume_factor * recup_factor * duree_factor

    normalized = 100.0 * (raw_risk_score - RISK_SCORE_MIN_RAW) / (RISK_SCORE_MAX_RAW - RISK_SCORE_MIN_RAW)
    risk_score = round(min(max(normalized, 0), 100), 2)

    return raw_risk_score, risk_score, risk_level_from_score(risk_score)
