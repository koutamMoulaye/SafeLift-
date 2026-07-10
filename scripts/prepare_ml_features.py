"""SafeLift — Preparation des donnees ML (Jalon 3, sous-etape 3/6).

Bloc 4 deja valide (GeoPort Intelligence) : ce ML est un BONUS qui raffine
risk_score, PAS une exigence de certification.

OBJECTIF (deja tranche) : predire le risk_score de la SEMAINE SUIVANTE par
zone musculaire, a partir de l'historique des semaines precedentes -- PAS
reapprendre la formule deterministe actuelle de fact_risk_score.sql (ce
serait une fuite de donnees inutile : le modele n'a rien a apprendre s'il
peut juste recopier une formule connue).

Grain : (user_id, muscle_group, semaine ISO -- lundi de la semaine,
`gold.dim_date.week_start_date`, MEME convention que celle deja utilisee en
interne par fact_risk_score.sql pour calculer charge_factor/volume_factor).

AUCUNE fuite temporelle : chaque feature "lag" est recherchee a la semaine
calendaire EXACTE (semaine courante - N semaines) -- si cette semaine
precise n'a aucune donnee (aucune seance ce jour-la pour cette zone), la
feature est NULL, JAMAIS interpolee ni remplacee par la derniere valeur
observee plus loin dans le passe (ce qui reviendrait a pretendre connaitre
une regularite qui n'existe pas). Meme logique, en sens inverse, pour la
cible (semaine courante + 1 semaine).

Ce script NE FAIT AUCUN CALCUL DE RISQUE : il lit uniquement
gold.fact_risk_score (deja calculee par dbt, sous-etape Gold du Jalon 1) et
agrege/decale dans le temps. Sortie : fichiers Parquet dans data/ml/, voir
data/ml/ML_DATA_PREP.md pour le detail complet des chiffres reels obtenus.
"""

import os
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

DB_CONFIG = {
    "host": os.environ.get("DBT_POSTGRES_HOST", "app-postgres"),
    "port": int(os.environ.get("DBT_POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("DBT_POSTGRES_DB", "safelift_dwh"),
    "user": os.environ.get("DBT_POSTGRES_USER", "safelift_app"),
    "password": os.environ.get("DBT_POSTGRES_PASSWORD", "change_me_app"),
}

# Chemin de sortie : coherent avec data/bronze|silver|gold deja presents
# dans le projet -- data/ml/ pour les artefacts specifiques au Bloc 4 bonus,
# jamais melanges avec le data lake medaillon (Bronze/Silver/Gold).
OUTPUT_DIR = Path("/opt/airflow/data/ml")

# Proportion des SEMAINES DISTINCTES les plus recentes du jeu de donnees
# LABELISE (avec cible connue) reservee au test -- decision de cette
# sous-etape, documentee ici et dans ML_DATA_PREP.md. 20% est un choix
# standard pour un split temporel ; avec un volume de donnees potentiellement
# tres petit (mono-utilisateur, voir docstring du module), la valeur EXACTE
# de coupure et le compte de lignes de chaque cote sont recalcules et
# affiches a chaque execution -- jamais suppose a l'avance.
TEST_SIZE_RATIO = 0.20

WEEKLY_AGG_QUERY = """
    select
        fr.user_id,
        mu.muscle_group,
        dt.week_start_date,
        avg(fr.risk_score) as risk_score_avg,
        avg(fr.charge_factor) as charge_factor_avg,
        avg(fr.volume_factor) as volume_factor_avg,
        avg(fr.recup_factor) as recup_factor_avg,
        avg(fr.duree_factor) as duree_factor_avg,
        count(*) as session_count
    from gold.fact_risk_score fr
    join gold.dim_muscle mu on fr.muscle_id = mu.muscle_id
    join gold.dim_date dt on fr.date_id = dt.date_id
    group by fr.user_id, mu.muscle_group, dt.week_start_date
    order by fr.user_id, mu.muscle_group, dt.week_start_date
"""


def fetch_weekly_aggregates() -> pd.DataFrame:
    """Agrege gold.fact_risk_score par (user_id, muscle_group, semaine ISO).

    GROUP BY sur des lignes reellement existantes : par construction, AUCUNE
    combinaison (user, zone, semaine) sans seance reelle ne peut apparaitre
    ici -- pas besoin de filtre explicite pour "ne garder que les semaines
    avec au moins une seance", c'est deja garanti par la nature d'un GROUP BY
    (une semaine sans ligne source ne produit tout simplement aucun groupe).

    Curseur psycopg2 brut (pas pandas.read_sql) -- coherent avec
    scripts/fuzzy_match_exercises.py, evite aussi l'avertissement pandas
    "only supports SQLAlchemy connectable" (psycopg2 direct n'est pas
    officiellement teste par pandas.read_sql, purement cosmetique mais
    evite le bruit dans les logs).
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(WEEKLY_AGG_QUERY)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    df = pd.DataFrame(rows, columns=columns)
    df["week_start_date"] = pd.to_datetime(df["week_start_date"])
    return df


def get_value_at_offset(df: pd.DataFrame, weeks_offset: int, value_col: str) -> pd.Series:
    """Pour chaque ligne (user_id, muscle_group, week_start_date), renvoie la
    valeur de `value_col` a la semaine EXACTE (week_start_date + weeks_offset
    semaines), meme user/zone -- ou NaN si cette semaine precise n'existe pas
    dans les donnees (aucune interpolation, aucun repli sur une semaine plus
    ancienne/recente).

    weeks_offset negatif -> lag (semaine passee).
    weeks_offset positif -> cible (semaine future).
    """
    lookup = df.set_index(["user_id", "muscle_group", "week_start_date"])[value_col]
    offset = pd.Timedelta(weeks=weeks_offset)

    keys = pd.MultiIndex.from_arrays(
        [df["user_id"], df["muscle_group"], df["week_start_date"] + offset]
    )
    return pd.Series(lookup.reindex(keys).to_numpy(), index=df.index)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Construit les features (lags, tendance) et la cible (semaine suivante).

    AUCUNE fuite temporelle : chaque lag_N_risk_score ne regarde QUE le passe
    (weeks_offset negatif) au moment de la semaine courante -- verifie
    explicitement ligne par ligne dans les tests manuels de ce script (voir
    data/ml/ML_DATA_PREP.md, section verification anti-fuite).
    """
    df = df.sort_values(["user_id", "muscle_group", "week_start_date"]).reset_index(drop=True)

    df["lag_1_risk_score"] = get_value_at_offset(df, -1, "risk_score_avg")
    df["lag_2_risk_score"] = get_value_at_offset(df, -2, "risk_score_avg")
    df["lag_3_risk_score"] = get_value_at_offset(df, -3, "risk_score_avg")

    # Tendance = variation vs la semaine precedente EXACTE -- NaN si lag_1
    # est lui-meme NaN (propagation naturelle de pandas, pas de valeur
    # inventee pour "masquer" une semaine manquante).
    df["trend_vs_previous_week"] = df["risk_score_avg"] - df["lag_1_risk_score"]

    # Cible : risk_score de la semaine SUIVANTE (ce qu'on veut predire).
    # C'est la SEULE colonne qui regarde le futur -- exclue de tout
    # entrainement en tant que FEATURE (elle EST le label), jamais utilisee
    # pour calculer une autre feature de la semaine courante.
    df["target_next_week_risk_score"] = get_value_at_offset(df, 1, "risk_score_avg")

    return df


def temporal_train_test_split(labeled_df: pd.DataFrame, test_size_ratio: float):
    """Split TEMPOREL (pas aleatoire) : les semaines distinctes les plus
    recentes vont en test, tout ce qui precede va en train. Coupure calculee
    sur les SEMAINES DISTINCTES du jeu labelise (pas sur le nombre de lignes
    brutes, qui pourrait etre deforme si une zone a beaucoup plus de lignes
    qu'une autre) -- documente et journalise a chaque execution.
    """
    distinct_weeks = sorted(labeled_df["week_start_date"].unique())
    n_weeks = len(distinct_weeks)
    n_test_weeks = max(1, round(n_weeks * test_size_ratio))
    cutoff_date = distinct_weeks[-n_test_weeks]

    train_df = labeled_df[labeled_df["week_start_date"] < cutoff_date].copy()
    test_df = labeled_df[labeled_df["week_start_date"] >= cutoff_date].copy()

    return train_df, test_df, cutoff_date, n_weeks, n_test_weeks


def main() -> None:
    print("[prepare_ml_features] Lecture de gold.fact_risk_score et agregation hebdomadaire...")
    weekly_df = fetch_weekly_aggregates()
    print(
        f"[prepare_ml_features] {len(weekly_df)} lignes (user, zone, semaine) agregees "
        f"depuis gold.fact_risk_score."
    )
    print(
        f"[prepare_ml_features] Utilisateurs distincts : {weekly_df['user_id'].nunique()} -- "
        f"Zones distinctes : {weekly_df['muscle_group'].nunique()} -- "
        f"Plage de semaines : {weekly_df['week_start_date'].min().date()} -> "
        f"{weekly_df['week_start_date'].max().date()}"
    )

    features_df = build_features(weekly_df)

    n_with_lag1 = features_df["lag_1_risk_score"].notna().sum()
    n_with_target = features_df["target_next_week_risk_score"].notna().sum()
    print(
        f"[prepare_ml_features] Lignes avec lag_1 disponible : {n_with_lag1}/{len(features_df)} -- "
        f"Lignes avec cible (semaine suivante) disponible : {n_with_target}/{len(features_df)}"
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    full_path = OUTPUT_DIR / "weekly_features_full.parquet"
    features_df.to_parquet(full_path, engine="pyarrow", index=False)
    print(f"[prepare_ml_features] Table complete (avec NaN) ecrite -> {full_path}")

    # Jeu LABELISE : uniquement les lignes ou la cible est connue -- une
    # ligne sans cible ne peut ni entrainer, ni evaluer un modele supervise
    # (rien a comparer a la prediction). Ce sont naturellement les toutes
    # dernieres semaines observees par zone (pas encore de "semaine
    # suivante" connue) -- PAS un filtre arbitraire.
    labeled_df = features_df[features_df["target_next_week_risk_score"].notna()].copy()
    print(
        f"[prepare_ml_features] Jeu labelise (cible connue) : {len(labeled_df)} lignes "
        f"sur {len(features_df)} ({len(features_df) - len(labeled_df)} exclues, pas de semaine suivante connue)."
    )

    train_df, test_df, cutoff_date, n_weeks, n_test_weeks = temporal_train_test_split(
        labeled_df, TEST_SIZE_RATIO
    )
    print(
        f"[prepare_ml_features] Split temporel : {n_weeks} semaines distinctes labelisees, "
        f"{n_test_weeks} en test (>= {cutoff_date.date()}), {n_weeks - n_test_weeks} en train."
    )
    print(f"[prepare_ml_features] Train : {len(train_df)} lignes -- Test : {len(test_df)} lignes")

    train_path = OUTPUT_DIR / "train.parquet"
    test_path = OUTPUT_DIR / "test.parquet"
    train_df.to_parquet(train_path, engine="pyarrow", index=False)
    test_df.to_parquet(test_path, engine="pyarrow", index=False)
    print(f"[prepare_ml_features] Train ecrit -> {train_path}")
    print(f"[prepare_ml_features] Test ecrit -> {test_path}")


if __name__ == "__main__":
    main()
