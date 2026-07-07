"""SafeLift — Etape 6/6, sous-etape 2/6 : export Gold (Postgres) -> Parquet -> S3.

Contexte : les 7 tables du modele en etoile dbt vivent dans
`app-postgres` (schema `gold`), pas en Parquet (voir CLAUDE.md : "Gold : dbt
opere sur Postgres ... pas directement sur les Parquet Silver"). Ce script
exporte chaque table vers un fichier Parquet local (`data/gold/<table>/`,
repertoire deja gitignore comme le reste de data/gold/*) puis l'upload vers
`s3://safelift-datalake-<account_id>/gold/<table>/`, structure attendue par
les tables externes Glue/Athena declarees dans terraform/athena.tf.

Le schema Parquet de chaque table est declare EXPLICITEMENT ci-dessous
(`GOLD_TABLES`), avec les memes noms/types que les colonnes `columns { }` de
terraform/athena.tf (eux-memes recuperes par introspection reelle de
`information_schema.columns` sur `app-postgres`, schema `gold`,
2026-07-06) : un schema Parquet invente ou laisse a l'inference pandas/pyarrow
risquerait de diverger silencieusement du schema Athena (ex. `numeric`
Postgres lu comme `Decimal` par psycopg2, jamais compatible tel quel avec un
`pa.array` type `double`).

Ce script est autonome (pas encore de DAG Airflow) : il est prevu pour etre
execute manuellement depuis le host, dans le venv dedie `.venv-aws/`
(voir scripts/requirements_aws.txt), PAS dans un conteneur Docker -- il se
connecte donc a app-postgres via le port EXPOSE sur l'hote
(APP_POSTGRES_PORT_EXPOSED, cf. .env), pas le port interne du reseau Docker.
L'orchestration Airflow de ce transfert (si besoin) sera une sous-etape
ulterieure explicitement demandee.

Gouvernance RGPD (etape 6/6, sous-etape 4/6) : cette export S3/Athena est la
COUCHE DE RESTITUTION EXTERNE (voir scripts/pseudonymize.py et CLAUDE.md pour
la justification complete du choix d'architecture). Chaque table qui contient
`user_id` (dim_user, fact_workout_session, fact_risk_score) voit cette colonne
remplacee par `user_pseudo_id` (HMAC-SHA256, cle PSEUDONYMIZATION_KEY) AVANT
l'ecriture Parquet -- le `user_id` reel ne quitte jamais l'environnement
Postgres local. fact_risk_score_demo_synthetic n'a pas de user_id (donnees
100% synthetiques) : non concernee.
"""

import os
import sys
from decimal import Decimal
from pathlib import Path

import boto3
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

from pseudonymize import load_pseudonymization_key, pseudonymize_user_id

REPO_ROOT = Path(__file__).resolve().parent.parent

# Tables et colonne source a pseudonymiser avant export : la colonne d'origine
# (cle) est retiree du Parquet exporte, remplacee par la colonne de valeur
# (nom d'export, type). Aucune autre colonne n'est affectee.
PSEUDONYMIZED_COLUMNS: dict[str, tuple[str, str, pa.DataType]] = {
    "dim_user": ("user_id", "user_pseudo_id", pa.string()),
    "fact_workout_session": ("user_id", "user_pseudo_id", pa.string()),
    "fact_risk_score": ("user_id", "user_pseudo_id", pa.string()),
}
BUCKET_NAME = "safelift-datalake-097115946702"  # doit rester aligne avec terraform/variables.tf
AWS_PROFILE = "awslearnerlab"
AWS_REGION = "us-east-1"  # aucune region par defaut sur ce compte lab, voir AWS_LAB_CONSTRAINTS.md

# Schema SOURCE (colonnes reelles de gold.<table> sur Postgres, utilise pour
# construire le SELECT). Ordre et types alignes sur terraform/athena.tf, SAUF
# pour les 3 tables listees dans PSEUDONYMIZED_COLUMNS ci-dessus : le schema
# EXPORTE (Parquet + Athena) substitue user_id (bigint) par user_pseudo_id
# (string) -- voir export_table_to_parquet().
# (bigint -> int64, int -> int32, string -> string, boolean -> bool_,
# double -> float64, date -> date32).
GOLD_TABLES: dict[str, list[tuple[str, pa.DataType]]] = {
    "fact_workout_session": [
        ("workout_session_id", pa.int64()),
        ("exercise_id", pa.int64()),
        ("muscle_id", pa.int64()),
        ("user_id", pa.int64()),
        ("date_id", pa.date32()),
        ("session_date", pa.date32()),
        ("workout_name", pa.string()),
        ("sets", pa.int64()),
        ("reps", pa.float64()),
        ("total_reps", pa.float64()),
        ("lifted_weight_kg", pa.float64()),
        ("duration_seconds", pa.float64()),
    ],
    "fact_risk_score": [
        ("workout_session_id", pa.int64()),
        ("exercise_id", pa.int64()),
        ("muscle_id", pa.int64()),
        ("user_id", pa.int64()),
        ("date_id", pa.date32()),
        ("session_date", pa.date32()),
        ("workout_name", pa.string()),
        ("sets", pa.int64()),
        ("reps", pa.float64()),
        ("total_reps", pa.float64()),
        ("lifted_weight_kg", pa.float64()),
        ("duration_seconds", pa.float64()),
        ("base_zone", pa.float64()),
        ("charge_factor", pa.float64()),
        ("volume_factor", pa.float64()),
        ("recup_factor", pa.float64()),
        ("duree_factor", pa.float64()),
        ("raw_risk_score", pa.float64()),
        ("risk_score", pa.float64()),
        ("risk_level", pa.string()),
    ],
    "dim_exercise": [
        ("exercise_id", pa.int64()),
        ("exercise_name", pa.string()),
        ("normalized_exercise_name", pa.string()),
        ("muscle_group", pa.string()),
        ("equipment", pa.string()),
        ("is_matched", pa.bool_()),
        ("match_stage", pa.string()),
        ("source", pa.string()),
    ],
    "dim_muscle": [
        ("muscle_id", pa.int64()),
        ("muscle_group", pa.string()),
        ("base_epidemiological_risk", pa.float64()),
    ],
    "dim_user": [
        ("user_id", pa.int64()),
        ("age", pa.int64()),
        ("gender", pa.string()),
        ("body_weight_kg", pa.float64()),
        ("height_m", pa.float64()),
        ("max_bpm", pa.int64()),
        ("avg_bpm", pa.int64()),
        ("resting_bpm", pa.int64()),
        ("session_duration_hours", pa.float64()),
        ("calories_burned", pa.float64()),
        ("workout_type", pa.string()),
        ("fat_percentage", pa.float64()),
        ("water_intake_liters", pa.float64()),
        ("workout_frequency_days_per_week", pa.int64()),
        ("experience_level", pa.int64()),
        ("bmi", pa.float64()),
        ("is_weight_training_demo_user", pa.bool_()),
    ],
    "dim_date": [
        ("date_id", pa.date32()),
        ("date_day", pa.date32()),
        ("day_of_month", pa.int32()),
        ("day_of_week", pa.int32()),
        ("week_of_year", pa.int32()),
        ("month", pa.int32()),
        ("year", pa.int32()),
        ("week_start_date", pa.date32()),
    ],
    "fact_risk_score_demo_synthetic": [
        ("scenario_id", pa.int32()),
        ("scenario_label", pa.string()),
        ("muscle_group", pa.string()),
        ("base_zone", pa.float64()),
        ("charge_factor", pa.float64()),
        ("volume_factor", pa.float64()),
        ("recup_factor", pa.float64()),
        ("duree_factor", pa.float64()),
        ("raw_risk_score", pa.float64()),
        ("risk_score", pa.float64()),
        ("risk_level", pa.string()),
        ("notes", pa.string()),
        ("is_synthetic_demo", pa.bool_()),
    ],
}


def load_env_file(path: Path) -> dict[str, str]:
    """Parseur minimal de fichier .env (KEY=VALUE), sans dependance externe."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def get_db_config() -> dict:
    env_file = load_env_file(REPO_ROOT / ".env")

    def env(key: str, default: str) -> str:
        return os.environ.get(key, env_file.get(key, default))

    return {
        # Ce script tourne HORS Docker (host) : host="localhost" + le port
        # EXPOSE sur l'hote, pas "app-postgres"/5432 (reseau Docker interne).
        "host": "localhost",
        "port": int(env("APP_POSTGRES_PORT_EXPOSED", "15432")),
        "dbname": env("APP_POSTGRES_DB", "safelift_dwh"),
        "user": env("APP_POSTGRES_USER", "safelift_app"),
        "password": env("APP_POSTGRES_PASSWORD", "change_me_app"),
    }


def to_arrow_value(value, pa_type: pa.DataType):
    """Convertit une valeur psycopg2 (Decimal notamment) vers un type compatible pyarrow."""
    if value is None:
        return None
    if pa.types.is_floating(pa_type) and isinstance(value, Decimal):
        return float(value)
    return value


def export_table_to_parquet(
    conn, table_name: str, schema: list[tuple[str, pa.DataType]], pseudonymization_key: str
) -> tuple[Path, int]:
    columns = [col for col, _ in schema]
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(columns)} FROM gold.{table_name}")
        rows = cur.fetchall()

    row_count = len(rows)
    columns_data = list(zip(*rows)) if rows else [[] for _ in columns]

    # Pseudonymisation : la colonne source (ex. user_id) est retiree du
    # Parquet exporte, remplacee par sa version pseudonymisee sous un nouveau
    # nom (ex. user_pseudo_id) -- voir PSEUDONYMIZED_COLUMNS et
    # scripts/pseudonymize.py.
    pseudo_spec = PSEUDONYMIZED_COLUMNS.get(table_name)
    export_names = list(columns)
    export_types = [pa_type for _, pa_type in schema]
    export_values: list[list] = [list(col) for col in columns_data]

    if pseudo_spec is not None:
        source_col, output_col, output_type = pseudo_spec
        idx = columns.index(source_col)
        export_values[idx] = [
            None if v is None else pseudonymize_user_id(v, pseudonymization_key)
            for v in columns_data[idx]
        ]
        export_names[idx] = output_col
        export_types[idx] = output_type

    arrays = [
        pa.array(
            [to_arrow_value(v, export_types[i]) for v in export_values[i]],
            type=export_types[i],
        )
        for i in range(len(export_names))
    ]
    arrow_table = pa.Table.from_arrays(arrays, names=export_names)

    out_dir = REPO_ROOT / "data" / "gold" / table_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{table_name}.parquet"
    pq.write_table(arrow_table, out_path)

    return out_path, row_count


def upload_to_s3(s3_client, local_path: Path, table_name: str) -> str:
    key = f"gold/{table_name}/{local_path.name}"
    s3_client.upload_file(str(local_path), BUCKET_NAME, key)
    return key


def main() -> None:
    pseudonymization_key = load_pseudonymization_key()
    db_config = get_db_config()
    conn = psycopg2.connect(**db_config)

    session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    s3_client = session.client("s3")

    print(f"[upload_gold_to_s3] Export Postgres ({db_config['host']}:{db_config['port']}/{db_config['dbname']}) -> S3 (s3://{BUCKET_NAME}/gold/)")

    total_rows = 0
    try:
        for table_name, schema in GOLD_TABLES.items():
            local_path, row_count = export_table_to_parquet(conn, table_name, schema, pseudonymization_key)
            s3_key = upload_to_s3(s3_client, local_path, table_name)
            total_rows += row_count
            pseudo_note = " (user_id pseudonymise -> user_pseudo_id)" if table_name in PSEUDONYMIZED_COLUMNS else ""
            print(f"  - gold.{table_name}: {row_count} lignes -> {local_path} -> s3://{BUCKET_NAME}/{s3_key}{pseudo_note}")
    finally:
        conn.close()

    print(f"[upload_gold_to_s3] Termine : {len(GOLD_TABLES)} tables, {total_rows} lignes au total.")


if __name__ == "__main__":
    sys.exit(main())
