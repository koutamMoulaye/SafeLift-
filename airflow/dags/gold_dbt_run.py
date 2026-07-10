"""SafeLift — Gold : construction du modele en etoile via dbt.

Pipeline en 5 etapes :
1. `load_silver_to_postgres` : charge les 4 tables Silver dans Postgres
   (schema `raw`, via un job Spark/JDBC).
2. `dbt_seed` : charge les seeds (mapping manuel exercice->muscle_group,
   scenarios synthetiques de demo).
3. `dbt_run_staging` : construit UNIQUEMENT les vues staging (dont les
   colonnes de normalisation exercise_name necessaires au fuzzy matching).
4. `fuzzy_match_exercises` : script Python (rapidfuzz) qui complete le
   matching exercise_name entre weight_training et le catalogue
   600k_fitness_detailed, hors dbt (dbt-postgres ne supporte pas les
   modeles Python). Ecrit raw.fuzzy_exercise_matches, relu par dim_exercise.sql.
   Doit s'executer APRES les vues staging (etape 3) et AVANT le run complet
   (etape 5).
5. `dbt_run` (run complet, staging + marts) puis `dbt_test`.

Pipeline de matching exercise_name (dim_exercise.sql), 4 etapes en cascade :
strict -> base-name (equipement retire) -> fuzzy (rapidfuzz, seuil 85%) ->
mapping manuel. Taux de matching et detail complet : voir
data/gold/GOLD_MODEL_DECISIONS.md.

Echec explicite : BashOperator echoue par defaut des qu'une commande retourne
un code de sortie non nul. `dbt test` retourne un code non nul des qu'au
moins un test echoue -> la task ET le DAG run sont marques FAILED, aucune
donnee invalide n'est donc presentee comme valide en aval (dashboard/API).
Aucun rattrapage silencieux n'est tente.

dbt tourne dans un venv Python ENTIEREMENT ISOLE de l'environnement Airflow
(/opt/dbt_venv, cf. airflow/Dockerfile) : dbt-core et Airflow ont des
contraintes de version conflictuelles sur des dependances communes
(click, jinja2...), les installer dans le meme environnement est une source
classique de rupture. dbt est donc toujours invoque via son chemin complet.
Le fuzzy matching (rapidfuzz), lui, tourne dans l'environnement Airflow
normal (scripts/fuzzy_match_exercises.py), pas dans le venv dbt.

Dependance vis-a-vis de silver_transformation : declenchee automatiquement
via TriggerDagRunOperator en fin de silver_transformation.py, meme mecanisme
et meme justification que bronze_ingestion -> silver_transformation (les deux
DAGs sont a declenchement manuel, un ExternalTaskSensor serait fragile ici).

Declenche a son tour ml_scoring (Jalon 3, sous-etape 5/6) en fin de dbt_test,
meme mecanisme -- le scoring ML bonus reste ainsi a jour avec chaque
recalcul de Gold (y compris ceux declenches par une seance temps reel,
Jalon 2 sous-etape 3/5), sans mecanisme de synchronisation supplementaire.
Si dbt_test echoue, ml_scoring n'est PAS declenche (trigger_rule par defaut
= all_success).
"""

import os
from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

SPARK_JOBS_DIR = "/opt/airflow/spark_jobs"
SPARK_MASTER_URL = "spark://spark-master:7077"
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/opt/dbt_venv/bin/dbt"
SCRIPTS_DIR = "/opt/airflow/scripts"

# Meme mecanisme que silver_transformation.py (IP du conteneur + umask 000)
# pour le mode client Spark en environnement Docker Compose -- voir les
# commentaires detailles dans silver_transformation.py.
# --packages telecharge le driver JDBC Postgres depuis Maven Central au
# demarrage du driver Spark (pas de jar embarque dans l'image : evite d'avoir
# a maintenir une image Spark personnalisee juste pour ce driver).
LOAD_SILVER_TO_POSTGRES_CMD = (
    "spark-submit "
    f"--master {SPARK_MASTER_URL} "
    "--deploy-mode client "
    "--conf spark.driver.host=$(hostname -i) "
    "--conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.hadoop.fs.permissions.umask-mode=000 "
    "--packages org.postgresql:postgresql:42.7.4 "
    f"{SPARK_JOBS_DIR}/load_silver_to_postgres.py"
)

# Variables consommees par dbt/profiles.yml via env_var(...) ET par
# scripts/fuzzy_match_exercises.py (memes noms). Memes valeurs que le service
# app-postgres (docker-compose.yml / .env), dupliquees explicitement ici car
# l'environnement du conteneur Airflow ne partage pas automatiquement le
# fichier .env de l'hote.
DBT_CONNECTION_ENV = {
    "DBT_POSTGRES_HOST": "app-postgres",
    "DBT_POSTGRES_PORT": "5432",
    "DBT_POSTGRES_USER": "safelift_app",
    "DBT_POSTGRES_PASSWORD": "change_me_app",
    "DBT_POSTGRES_DB": "safelift_dwh",
}
# BashOperator.env REMPLACE l'environnement du sous-processus au lieu de le
# completer (comportement documente d'Airflow) : on fusionne donc
# explicitement avec os.environ pour ne pas perdre PATH/HOME/JAVA_HOME/etc.
TASK_ENV = {**os.environ, **DBT_CONNECTION_ENV}

DBT_SEED_CMD = f"{DBT_BIN} seed --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
DBT_RUN_STAGING_CMD = (
    f"{DBT_BIN} run --select staging --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
)
DBT_RUN_CMD = f"{DBT_BIN} run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
DBT_TEST_CMD = f"{DBT_BIN} test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
FUZZY_MATCH_CMD = f"python3 {SCRIPTS_DIR}/fuzzy_match_exercises.py"

default_args = {
    "owner": "safelift",
    "retries": 0,
}

with DAG(
    dag_id="gold_dbt_run",
    description="Chargement Silver->Postgres puis construction du modele en etoile Gold via dbt",
    default_args=default_args,
    schedule=None,  # declenche par silver_transformation
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "dbt", "safelift"],
) as dag:
    load_silver_to_postgres = BashOperator(
        task_id="load_silver_to_postgres",
        bash_command=LOAD_SILVER_TO_POSTGRES_CMD,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=DBT_SEED_CMD,
        env=TASK_ENV,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=DBT_RUN_STAGING_CMD,
        env=TASK_ENV,
    )

    fuzzy_match_exercises = BashOperator(
        task_id="fuzzy_match_exercises",
        bash_command=FUZZY_MATCH_CMD,
        env=TASK_ENV,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=DBT_RUN_CMD,
        env=TASK_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_TEST_CMD,
        env=TASK_ENV,
    )

    trigger_ml_scoring = TriggerDagRunOperator(
        task_id="trigger_ml_scoring",
        trigger_dag_id="ml_scoring",
    )

    load_silver_to_postgres >> dbt_seed >> dbt_run_staging >> fuzzy_match_exercises >> dbt_run >> dbt_test >> trigger_ml_scoring
