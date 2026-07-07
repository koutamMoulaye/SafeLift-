"""SafeLift — Ingestion Bronze.

Lit les CSV bruts telecharges depuis Kaggle (data/bronze/raw/) et les ecrit
tels quels (aucun nettoyage, aucun renommage, aucune jointure) en Parquet dans
data/bronze/{dataset_name}/ingestion_date=<ds>/, avec ajout de trois colonnes
de metadonnees d'ingestion : ingestion_timestamp, source_file, source_dataset.

Une table Bronze = un fichier CSV source. Les deux fichiers du dataset
"600k_fitness" (program_summary.csv et programs_detailed_boostcamp_kaggle.csv)
ne sont pas fusionnes : ce sont deux grains differents (un programme vs le
detail exercice/semaine/jour), les fusionner necessiterait une jointure, hors
perimetre du Bronze. Voir data/bronze/SCHEMA_NOTES.md pour le detail des
schemas sources.

Idempotence : chaque tache ecrit dans la partition "ingestion_date={{ ds }}"
(date logique du DAG run). La partition est entierement supprimee puis
reecrite a chaque execution : relancer le DAG pour la meme date logique
n'accumule donc pas de doublons.

Une fois les 4 tables ingerees avec succes, ce DAG declenche automatiquement
le DAG silver_transformation (TriggerDagRunOperator) — voir
airflow/dags/silver_transformation.py pour la justification de ce choix
plutot qu'un ExternalTaskSensor.
"""

import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# --- Chemins (montes depuis ./data sur l'hote vers /opt/airflow/data dans le conteneur) ---
DATA_ROOT = Path("/opt/airflow/data")
RAW_DIR = DATA_ROOT / "bronze" / "raw"
BRONZE_DIR = DATA_ROOT / "bronze"

# --- Sources : (nom de la table Bronze, chemin relatif du CSV sous RAW_DIR) ---
SOURCES = [
    ("600k_fitness_summary", "600k_fitness/program_summary.csv"),
    ("600k_fitness_detailed", "600k_fitness/programs_detailed_boostcamp_kaggle.csv"),
    ("gym_members", "gym_members/gym_members_exercise_tracking.csv"),
    ("weight_training", "weight_training/weightlifting_721_workouts.csv"),
]


def ingest_csv_to_bronze(dataset_name: str, csv_relpath: str, ds: str, **_context) -> None:
    """Lit un CSV brut et l'ecrit en Parquet partitionne, avec metadonnees d'ingestion."""
    source_path = RAW_DIR / csv_relpath
    if not source_path.exists():
        raise FileNotFoundError(f"Fichier source introuvable : {source_path}")

    dataframe = pd.read_csv(source_path)
    row_count = len(dataframe)

    # Metadonnees d'ingestion — ajoutees telles quelles, aucune autre colonne modifiee
    dataframe["ingestion_timestamp"] = datetime.utcnow().isoformat()
    dataframe["source_file"] = csv_relpath
    dataframe["source_dataset"] = dataset_name

    partition_dir = BRONZE_DIR / dataset_name / f"ingestion_date={ds}"

    # Idempotence : on repart d'une partition vide avant d'ecrire, pour que
    # relancer le DAG sur la meme date logique remplace (et non accumule).
    if partition_dir.exists():
        shutil.rmtree(partition_dir)
    partition_dir.mkdir(parents=True, exist_ok=True)

    output_path = partition_dir / f"{dataset_name}.parquet"
    dataframe.to_parquet(output_path, engine="pyarrow", index=False)

    print(
        f"[bronze_ingestion] {dataset_name} : {row_count} lignes lues depuis "
        f"{source_path} -> {output_path}"
    )


default_args = {
    "owner": "safelift",
    "retries": 0,
}

with DAG(
    dag_id="bronze_ingestion",
    description="Ingestion brute des datasets Kaggle (CSV -> Parquet) dans le data lake Bronze",
    default_args=default_args,
    schedule=None,  # declenchement manuel pour cette etape
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion", "safelift"],
) as dag:
    ingestion_tasks = [
        PythonOperator(
            task_id=f"ingest_{dataset_name}",
            python_callable=ingest_csv_to_bronze,
            op_kwargs={"dataset_name": dataset_name, "csv_relpath": csv_relpath},
        )
        for dataset_name, csv_relpath in SOURCES
    ]

    trigger_silver_transformation = TriggerDagRunOperator(
        task_id="trigger_silver_transformation",
        trigger_dag_id="silver_transformation",
    )

    ingestion_tasks >> trigger_silver_transformation
