"""SafeLift -- Scoring batch du modele ML bonus (Jalon 3, sous-etape 5/6).

Charge le modele entraine en sous-etape 4/6 (data/ml/model.pkl) et produit
une prediction du risk_score de la SEMAINE SUIVANTE, pour chaque
(user_id, muscle_group) qui possede au moins une semaine d'historique reel
dans gold.fact_risk_score.

REUTILISE la logique de calcul de features de scripts/prepare_ml_features.py
(fetch_weekly_aggregates, build_features) et l'imputation de
scripts/train_risk_trend_model.py (impute_lag_nulls, NUMERIC_FEATURES,
CATEGORICAL_FEATURES) -- ces fonctions/constantes sont IMPORTEES, jamais
recopiees ici, pour garantir que le scoring utilise EXACTEMENT les memes
features que l'entrainement (toute derive entre les deux serait un bug
silencieux classique en ML : "training/serving skew").

⚠️ Limite structurelle, documentee et assumee (voir data/ml/ML_DATA_PREP.md
et ML_TRAINING_RESULTS.md) : ce script ne peut produire une prediction que
pour les (user_id, muscle_group) DEJA presents dans gold.fact_risk_score.
Concretement, a ce stade du projet, cela signifie UNIQUEMENT user_id=9 (le
seul utilisateur avec un historique de seances reel) -- les 972 autres
profils gold.dim_user n'ont AUCUNE ligne fact_risk_score, ils n'apparaissent
donc jamais dans fetch_weekly_aggregates() et RIEN n'est ecrit pour eux.
Aucune extrapolation sur un historique vide n'est tentee.

Table cible : gold.ml_risk_prediction, creee directement par ce script
(psycopg2, meme pattern que spark/jobs/stream_gym_occupancy.py pour
gold.gym_occupancy_live) -- PAS geree par dbt, car la source de cette table
est un modele ML externe, pas une transformation SQL des donnees Gold
existantes. Rafraichissement complet (TRUNCATE puis reinsertion) a chaque
execution : ce script maintient un ETAT COURANT ("meilleure prediction
disponible maintenant"), pas un historique de predictions passees a
preserver -- meme philosophie que gold.gym_occupancy_live.
"""

import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

import joblib
import psycopg2

# Les modules reutilises (prepare_ml_features, train_risk_trend_model) vivent
# dans le meme repertoire que ce script -- ajoute explicitement au PYTHONPATH
# pour un import direct, sans creer de package Python formel (coherent avec
# le reste de scripts/, aucun __init__.py present).
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPTS_DIR)

from prepare_ml_features import build_features, fetch_weekly_aggregates  # noqa: E402
from train_risk_trend_model import (  # noqa: E402
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
    impute_lag_nulls,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("score_risk_trend")

MODEL_PATH = Path("/opt/airflow/data/ml/model.pkl")

DB_CONFIG = dict(
    host=os.environ.get("DBT_POSTGRES_HOST", "app-postgres"),
    port=os.environ.get("DBT_POSTGRES_PORT", "5432"),
    dbname=os.environ.get("DBT_POSTGRES_DB", "safelift_dwh"),
    user=os.environ.get("DBT_POSTGRES_USER", "safelift_app"),
    password=os.environ.get("DBT_POSTGRES_PASSWORD", "change_me_app"),
)

TARGET_TABLE = "gold.ml_risk_prediction"


def ensure_target_table_exists() -> None:
    """Cree gold.ml_risk_prediction si absente. Cle primaire (user_id,
    muscle_group) : une seule prediction "courante" par zone et par
    utilisateur, jamais un historique de predictions accumule (voir
    docstring du module)."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                    user_id INTEGER NOT NULL,
                    muscle_group TEXT NOT NULL,
                    week_predicted_for DATE NOT NULL,
                    predicted_risk_score DOUBLE PRECISION NOT NULL,
                    based_on_week DATE NOT NULL,
                    model_version TEXT NOT NULL,
                    model_trained_at TIMESTAMPTZ NOT NULL,
                    scored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (user_id, muscle_group)
                )
                """
            )
        logger.info("Table %s prete (creee si absente).", TARGET_TABLE)
    finally:
        conn.close()


def latest_rows_per_zone(features_df):
    """Pour chaque (user_id, muscle_group), ne garde que la ligne de la
    semaine la PLUS RECENTE deja observee -- c'est a partir de cette semaine
    que l'on predit la suivante (pas encore observee). Le lag_1/2/3 de cette
    ligne regarde donc toujours strictement le passe par rapport a la
    semaine predite -- coherence stricte avec les garanties anti-fuite de
    prepare_ml_features.py (voir data/ml/ML_DATA_PREP.md section 7)."""
    idx = features_df.groupby(["user_id", "muscle_group"])["week_start_date"].idxmax()
    return features_df.loc[idx].reset_index(drop=True)


def main() -> None:
    if not MODEL_PATH.exists():
        raise RuntimeError(
            f"Modele introuvable a {MODEL_PATH} -- executer d'abord "
            "scripts/train_risk_trend_model.py (Jalon 3, sous-etape 4/6) "
            "avant de lancer le scoring."
        )

    bundle = joblib.load(MODEL_PATH)
    pipeline = bundle["pipeline"]
    metadata = bundle["metadata"]
    model_name = metadata["model_name"]
    logger.info(
        "Modele charge : %s (entraine le %s, RMSE test=%.4f, MAE test=%.4f).",
        model_name,
        metadata["trained_at_utc"],
        metadata["metrics_test_set"][model_name]["rmse"],
        metadata["metrics_test_set"][model_name]["mae"],
    )

    logger.info(
        "Recalcul des features (reutilise prepare_ml_features.fetch_weekly_aggregates/build_features)..."
    )
    weekly_df = fetch_weekly_aggregates()
    if weekly_df.empty:
        logger.warning(
            "gold.fact_risk_score est vide -- aucune prediction a produire "
            "(rien ecrit dans %s).",
            TARGET_TABLE,
        )
        return

    features_df = build_features(weekly_df)
    scoring_rows = latest_rows_per_zone(features_df)
    scoring_rows = impute_lag_nulls(scoring_rows)

    n_users = scoring_rows["user_id"].nunique()
    logger.info(
        "%d ligne(s) a scorer, pour %d utilisateur(s) distinct(s) -- "
        "uniquement les (user_id, muscle_group) avec au moins une semaine "
        "d'historique reel dans gold.fact_risk_score. Aucune extrapolation "
        "n'est faite pour un utilisateur sans historique : il n'apparait "
        "simplement jamais dans ce jeu de lignes.",
        len(scoring_rows),
        n_users,
    )

    feature_cols = CATEGORICAL_FEATURES + NUMERIC_FEATURES
    predictions = pipeline.predict(scoring_rows[feature_cols])

    ensure_target_table_exists()

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                # Rafraichissement complet : ce script recalcule TOUJOURS
                # l'etat le plus a jour a partir de gold.fact_risk_score,
                # jamais un historique de predictions passees a preserver
                # (voir docstring du module) -- TRUNCATE puis reinsertion
                # complete dans la MEME transaction (rien n'est jamais lu
                # dans un etat intermediaire "table vide").
                cur.execute(f"TRUNCATE TABLE {TARGET_TABLE}")
                for row, predicted_score in zip(scoring_rows.itertuples(), predictions):
                    week_predicted_for = (row.week_start_date + timedelta(weeks=1)).date()
                    cur.execute(
                        f"""
                        INSERT INTO {TARGET_TABLE}
                            (user_id, muscle_group, week_predicted_for, predicted_risk_score,
                             based_on_week, model_version, model_trained_at, scored_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                        """,
                        (
                            int(row.user_id),
                            row.muscle_group,
                            week_predicted_for,
                            float(predicted_score),
                            row.week_start_date.date(),
                            model_name,
                            metadata["trained_at_utc"],
                        ),
                    )
        logger.info("%d prediction(s) ecrite(s) dans %s.", len(scoring_rows), TARGET_TABLE)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
