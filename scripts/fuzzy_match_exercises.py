"""SafeLift — Etape 3 du pipeline de matching dim_exercise : fuzzy matching.

Contexte : dbt-postgres ne supporte pas les "modeles Python" dbt (reserves a
Snowflake/Databricks/BigQuery). Le fuzzy matching (rapidfuzz) doit donc
s'executer HORS dbt, dans ce script autonome, qui ecrit son resultat dans
une table Postgres (raw.fuzzy_exercise_matches) que dbt relit ensuite comme
une source normale.

Ce script tourne APRES un premier `dbt run --select staging` (il lit les
vues staging.stg_*) et AVANT le `dbt run` complet (qui construit dim_exercise
en s'appuyant sur ses resultats). Voir airflow/dags/gold_dbt_run.py pour
l'orchestration complete.

Methode : pour chaque normalized_exercise_base_name distinct de
weight_training, on calcule le meilleur candidat du catalogue
600k_fitness_detailed via rapidfuzz.process.extractOne (scorer
token_sort_ratio, insensible a l'ordre des mots — ex. "seated shoulder
press" et "shoulder press seated" obtiennent le meme score). Le score BRUT
est toujours enregistre, MEME EN DESSOUS DU SEUIL retenu (85%, voir
data/gold/GOLD_MODEL_DECISIONS.md) : c'est dbt (dim_exercise.sql) qui
applique le seuil au moment de decider d'utiliser ou non le resultat, ce qui
garde toute la table auditable (on peut inspecter les scores meme rejetes).
"""

import os

import psycopg2
from rapidfuzz import fuzz, process

FUZZY_MATCH_THRESHOLD = 85  # documente et justifie dans GOLD_MODEL_DECISIONS.md

DB_CONFIG = {
    "host": os.environ.get("DBT_POSTGRES_HOST", "app-postgres"),
    "port": int(os.environ.get("DBT_POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("DBT_POSTGRES_DB", "safelift_dwh"),
    "user": os.environ.get("DBT_POSTGRES_USER", "safelift_app"),
    "password": os.environ.get("DBT_POSTGRES_PASSWORD", "change_me_app"),
}


def fetch_distinct_base_names(cur, table: str) -> list[str]:
    """Renvoie la liste des normalized_exercise_base_name distincts et non vides d'une table staging."""
    cur.execute(
        f"select distinct normalized_exercise_base_name from staging.{table} "
        "where normalized_exercise_base_name is not null and normalized_exercise_base_name != ''"
    )
    return [row[0] for row in cur.fetchall()]


def fetch_representative_names(cur) -> dict[str, str]:
    """Pour chaque normalized_exercise_base_name du catalogue, renvoie un exercise_name representatif.

    Plusieurs exercise_name originaux (variantes d'equipement) peuvent partager
    le meme normalized_exercise_base_name : on retient le plus frequent comme
    libelle d'affichage (deterministe : egalite departagee alphabetiquement).
    """
    cur.execute(
        """
        select normalized_exercise_base_name, exercise_name
        from (
            select
                normalized_exercise_base_name,
                exercise_name,
                count(*) as occurrence_count,
                row_number() over (
                    partition by normalized_exercise_base_name
                    order by count(*) desc, exercise_name asc
                ) as rn
            from staging.stg_600k_fitness_detailed
            where normalized_exercise_base_name is not null and normalized_exercise_base_name != ''
            group by normalized_exercise_base_name, exercise_name
        ) ranked
        where rn = 1
        """
    )
    return dict(cur.fetchall())


def ensure_result_table(cur) -> None:
    cur.execute("create schema if not exists raw")
    cur.execute("drop table if exists raw.fuzzy_exercise_matches")
    cur.execute(
        """
        create table raw.fuzzy_exercise_matches (
            weight_training_normalized_base_name text primary key,
            matched_catalog_normalized_base_name text,
            matched_catalog_exercise_name text,
            similarity_score real
        )
        """
    )


def main() -> None:
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    catalog_representative_names = fetch_representative_names(cur)
    catalog_base_names = list(catalog_representative_names.keys())
    weight_training_base_names = fetch_distinct_base_names(cur, "stg_weight_training")

    ensure_result_table(cur)

    rows_to_insert = []
    for base_name in weight_training_base_names:
        best_match = process.extractOne(base_name, catalog_base_names, scorer=fuzz.token_sort_ratio)
        if best_match is None:
            continue
        matched_base_name, score, _ = best_match
        rows_to_insert.append(
            (
                base_name,
                matched_base_name,
                catalog_representative_names[matched_base_name],
                score,
            )
        )

    cur.executemany(
        """
        insert into raw.fuzzy_exercise_matches
            (weight_training_normalized_base_name, matched_catalog_normalized_base_name,
             matched_catalog_exercise_name, similarity_score)
        values (%s, %s, %s, %s)
        """,
        rows_to_insert,
    )

    above_threshold = sum(1 for row in rows_to_insert if row[3] >= FUZZY_MATCH_THRESHOLD)
    print(
        f"[fuzzy_match_exercises] {len(rows_to_insert)} candidats calcules, "
        f"{above_threshold} au-dessus du seuil {FUZZY_MATCH_THRESHOLD}% "
        f"-> raw.fuzzy_exercise_matches"
    )

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
