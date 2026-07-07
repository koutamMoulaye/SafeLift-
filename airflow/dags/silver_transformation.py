"""SafeLift — Transformation Silver.

Soumet au cluster Spark standalone (spark://spark-master:7077) les 4 jobs de
nettoyage definis dans spark/jobs/silver_*.py, qui lisent chacun la derniere
partition Bronze d'une table et ecrivent une vue Silver nettoyee dans
data/silver/{table}/ (pas de partitionnement par date : Silver est une vue
cumulative, reecrite entierement a chaque run — voir spark/jobs/silver_common.py
et data/silver/CLEANING_LOG.md pour le detail des regles de nettoyage).

Dependance vis-a-vis de bronze_ingestion : ce DAG est declenche automatiquement
par une task TriggerDagRunOperator ajoutee en fin de bronze_ingestion.py, une
fois les 4 tasks d'ingestion terminees avec succes. Ce mecanisme (plutot qu'un
ExternalTaskSensor) a ete retenu car les deux DAGs sont a declenchement manuel
(schedule=None) : un ExternalTaskSensor exigerait de faire correspondre les
dates logiques des deux DAG runs (execution_date/execution_delta), ce qui est
fragile pour des declenchements manuels independants. Un trigger explicite en
fin de DAG amont est plus simple et plus robuste dans ce contexte.

Les 4 jobs sont independants (une table Silver = une table Bronze nettoyee,
aucune jointure a ce stade) et peuvent donc s'executer en parallele.

Une fois les 4 jobs Silver termines avec succes, ce DAG declenche
automatiquement le DAG gold_dbt_run (TriggerDagRunOperator, meme mecanisme
que bronze_ingestion -> silver_transformation).
"""

from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

SPARK_JOBS_DIR = "/opt/airflow/spark_jobs"
SPARK_MASTER_URL = "spark://spark-master:7077"

# --- Jobs Silver a soumettre : (nom du job, nom du fichier .py) ---
SILVER_JOBS = [
    ("silver_600k_fitness_summary", "silver_600k_fitness_summary.py"),
    ("silver_600k_fitness_detailed", "silver_600k_fitness_detailed.py"),
    ("silver_gym_members", "silver_gym_members.py"),
    ("silver_weight_training", "silver_weight_training.py"),
]

# spark.driver.host=$(hostname -i) : necessaire en mode client dans un
# environnement Docker Compose — sans cela, le driver Spark (qui tourne dans
# le conteneur Airflow ayant soumis le job) peut s'annoncer aux executeurs
# (conteneur spark-worker) avec une adresse non joignable. On utilise l'IP du
# conteneur sur le reseau docker plutot qu'un nom d'hote, pour rester correct
# quel que soit le conteneur Airflow qui execute la task (scheduler avec
# LocalExecutor).
# spark.hadoop.fs.permissions.umask-mode=000 : le driver (conteneur airflow-*,
# uid 50000) et les executeurs (conteneur spark-worker, uid 185 "spark") n'ont
# pas le meme UID. Sans ce reglage, les repertoires crees par le driver lors de
# l'ecriture (ex. data/silver/{table}/_temporary/...) heritent d'un umask
# proprietaire-seul (755), que les executeurs ne peuvent alors plus ecrire.
# Ce reglage force Hadoop (utilise en interne par Spark pour l'ecriture
# fichier) a creer repertoires/fichiers en 777, quel que soit l'UID.
SPARK_SUBMIT_TEMPLATE = (
    "spark-submit "
    f"--master {SPARK_MASTER_URL} "
    "--deploy-mode client "
    "--conf spark.driver.host=$(hostname -i) "
    "--conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.hadoop.fs.permissions.umask-mode=000 "
    f"{SPARK_JOBS_DIR}/{{job_file}}"
)

default_args = {
    "owner": "safelift",
    "retries": 0,
}

with DAG(
    dag_id="silver_transformation",
    description="Nettoyage et normalisation Bronze -> Silver via Spark",
    default_args=default_args,
    schedule=None,  # declenche par bronze_ingestion (TriggerDagRunOperator)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["silver", "transformation", "safelift"],
) as dag:
    silver_tasks = [
        BashOperator(
            task_id=job_name,
            bash_command=SPARK_SUBMIT_TEMPLATE.format(job_file=job_file),
        )
        for job_name, job_file in SILVER_JOBS
    ]

    trigger_gold_dbt_run = TriggerDagRunOperator(
        task_id="trigger_gold_dbt_run",
        trigger_dag_id="gold_dbt_run",
    )

    silver_tasks >> trigger_gold_dbt_run
