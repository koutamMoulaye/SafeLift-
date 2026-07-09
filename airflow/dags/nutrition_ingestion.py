"""SafeLift — Ingestion nutrition (Jalon 3, sous-etape 1/6).

Appelle l'API USDA FoodData Central (endpoint /foods/search) pour recuperer
un jeu RAISONNABLE d'aliments courants pertinents pour un contexte
fitness/nutrition (sources de proteines, glucides, legumes, fruits) et les
depose en Bronze, puis enchaine Silver -> chargement Postgres -> dbt (memes
etapes que le pipeline Kaggle existant, voir bronze_ingestion.py /
silver_transformation.py / gold_dbt_run.py).

Perimetre volontairement borne a environ 50-100 aliments distincts (une
trentaine de mots-cles de recherche interroges, quelques resultats par
mot-cle) -- PAS une aspiration complete des ~300k entrees USDA, hors sujet
et inutilement lent pour ce projet.

DAG SELF-CONTAINED, independant de bronze_ingestion/silver_transformation/
gold_dbt_run (domaine different -- nutrition, pas Kaggle fitness) :
- ingest_usda_nutrition (PythonOperator) : appel API -> Bronze
- silver_usda_nutrition (BashOperator, spark-submit) : nettoyage -> Silver
- load_usda_nutrition_to_postgres (BashOperator, spark-submit) : reutilise
  spark/jobs/load_silver_to_postgres.py (etendu avec la table
  "usda_nutrition") -- charge aussi les 4 tables Kaggle existantes au
  passage (idempotent, overwrite+truncate, cf. ce script), leger surcout
  accepte pour rester coherent avec le mecanisme deja en place plutot que
  dupliquer un script de chargement dedie.
- dbt_run_nutrition / dbt_test_nutrition (BashOperator) : `dbt run`/`test`
  SCOPES a stg_usda_nutrition + dim_nutrition + fact_nutrition_target
  UNIQUEMENT (pas tout gold_dbt_run) -- fact_nutrition_target ne depend que
  de dim_user (deja construite), inutile de retraiter tout le pipeline
  Kaggle (matching d'exercices, etc.) pour une ingestion nutrition.

Cle API : USDA_API_KEY (variable d'environnement, voir docker-compose.yml
x-airflow-common-env), JAMAIS en dur dans le code, JAMAIS loggee meme
partiellement (voir _redact_secret()).

Gestion des erreurs : retry avec backoff sur rate limit (HTTP 429) et
erreurs reseau transitoires (MAX_RETRIES tentatives), puis echec EXPLICITE
de la task (aucune donnee partielle silencieusement presentee comme
complete) si l'API reste indisponible.

Idempotence : meme pattern que bronze_ingestion.py -- partition
ingestion_date={{ ds }} entierement supprimee puis reecrite a chaque run.
"""

import logging
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# --- Chemins (memes conventions que bronze_ingestion.py) ---
DATA_ROOT = Path("/opt/airflow/data")
BRONZE_DIR = DATA_ROOT / "bronze"
DATASET_NAME = "usda_nutrition"

SPARK_JOBS_DIR = "/opt/airflow/spark_jobs"
SPARK_MASTER_URL = "spark://spark-master:7077"
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/opt/dbt_venv/bin/dbt"

# --- API USDA FoodData Central ---
USDA_API_BASE_URL = "https://api.nal.usda.gov/fdc/v1"
USDA_SEARCH_ENDPOINT = f"{USDA_API_BASE_URL}/foods/search"

# Mots-cles de recherche : couverture volontairement large mais bornee
# (proteines, feculents/glucides, legumes, fruits, matieres grasses,
# produits laitiers) -- objectif 50-100 aliments distincts au total, pas
# une couverture exhaustive du catalogue USDA.
FOOD_KEYWORDS = [
    # Proteines
    "chicken breast", "salmon", "tuna", "egg", "greek yogurt",
    "tofu", "lentils", "black beans", "shrimp", "turkey breast",
    "cottage cheese", "beef",
    # Feculents / glucides
    "brown rice", "oats", "sweet potato", "quinoa", "whole wheat bread",
    "pasta", "potato",
    # Legumes
    "broccoli", "spinach", "carrot", "tomato", "bell pepper", "cucumber",
    # Fruits
    "banana", "apple", "orange", "blueberries", "avocado", "strawberries",
    # Matieres grasses / divers
    "almonds", "peanut butter", "olive oil", "milk", "cheddar cheese",
]

# dataType restreint a Foundation + SR Legacy : jeux de donnees de reference
# USDA (aliments "bruts"/peu transformes, valeurs nutritionnelles fiables et
# systematiquement exprimees par 100g) -- exclut volontairement "Branded"
# (produits industriels de marque, tres nombreux et bruyants pour une
# recherche par mot-cle generique) et "Survey (FNDDS)". Voir
# data/gold/GOLD_MODEL_DECISIONS.md section 13.
USDA_DATA_TYPES = "Foundation,SR Legacy"
RESULTS_PER_KEYWORD = 4

# ID des nutriments USDA (identifiants stables du referentiel FoodData
# Central, verifies sur des reponses reelles de l'API) :
#   1008 = Energy (kcal), 1003 = Protein (g), 1004 = Total lipid/fat (g),
#   1005 = Carbohydrate, by difference (g). Tous exprimes par 100g pour les
#   dataType Foundation/SR Legacy interroges ici.
NUTRIENT_IDS = {
    "kcal": 1008,
    "protein_g": 1003,
    "fat_g": 1004,
    "carbs_g": 1005,
}

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
PAUSE_BETWEEN_KEYWORDS_SECONDS = 0.3


def _redact_secret(text: str, secret: str) -> str:
    """Retire TOUTE occurrence de la cle API d'une chaine avant de la logger.

    Applique systematiquement a tout message d'erreur/exception avant tout
    logger.warning/error -- meme un message d'exception "brut" (str(exc))
    peut contenir l'URL complete avec la cle API en parametre de requete.
    """
    if secret:
        return text.replace(secret, "***REDACTED***")
    return text


def _search_food(keyword: str, api_key: str) -> list:
    """Interroge /foods/search pour un mot-cle, avec retry sur rate limit/erreur reseau."""
    params = {
        "query": keyword,
        "pageSize": RESULTS_PER_KEYWORD,
        "dataType": USDA_DATA_TYPES,
        "api_key": api_key,
    }
    last_error_message = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(USDA_SEARCH_ENDPOINT, params=params, timeout=15)
            if response.status_code == 429:
                logger.warning(
                    "Rate limit USDA API atteint pour '%s' (tentative %d/%d) -- nouvelle tentative dans %ds.",
                    keyword, attempt, MAX_RETRIES, RETRY_BACKOFF_SECONDS,
                )
                time.sleep(RETRY_BACKOFF_SECONDS)
                continue
            response.raise_for_status()
            return response.json().get("foods", [])
        except requests.exceptions.RequestException as exc:
            last_error_message = _redact_secret(str(exc), api_key)
            logger.warning(
                "Erreur API USDA pour '%s' (tentative %d/%d) : %s",
                keyword, attempt, MAX_RETRIES, last_error_message,
            )
            time.sleep(RETRY_BACKOFF_SECONDS)

    # Echec EXPLICITE apres epuisement des tentatives -- jamais de donnee
    # partielle silencieusement presentee comme complete.
    raise RuntimeError(
        f"Echec de l'appel USDA API pour le mot-cle '{keyword}' apres {MAX_RETRIES} "
        f"tentatives. Derniere erreur : {last_error_message}"
    )


def _extract_nutrient_value(food_nutrients: list, nutrient_id: int):
    for nutrient in food_nutrients:
        if nutrient.get("nutrientId") == nutrient_id:
            return nutrient.get("value")
    return None


def ingest_usda_nutrition(ds: str, **_context) -> None:
    """Interroge l'API USDA pour chaque mot-cle, deduplique par fdcId, ecrit en Bronze."""
    api_key = os.environ.get("USDA_API_KEY")
    if not api_key:
        raise RuntimeError(
            "USDA_API_KEY n'est pas definie dans l'environnement -- "
            "voir .env / docker-compose.yml (x-airflow-common-env)."
        )

    logger.info(
        "Debut de l'ingestion USDA FoodData Central -- %d mot(s)-cle(s) a interroger.",
        len(FOOD_KEYWORDS),
    )

    rows = []
    seen_fdc_ids = set()
    skipped_incomplete = 0

    for keyword in FOOD_KEYWORDS:
        foods = _search_food(keyword, api_key)
        for food in foods:
            fdc_id = food.get("fdcId")
            if fdc_id is None or fdc_id in seen_fdc_ids:
                continue

            nutrients = food.get("foodNutrients", [])
            kcal = _extract_nutrient_value(nutrients, NUTRIENT_IDS["kcal"])
            protein = _extract_nutrient_value(nutrients, NUTRIENT_IDS["protein_g"])
            fat = _extract_nutrient_value(nutrients, NUTRIENT_IDS["fat_g"])
            carbs = _extract_nutrient_value(nutrients, NUTRIENT_IDS["carbs_g"])

            # Aliment ignore si un macro-nutriment central manque (pas
            # d'imputation inventee) -- reste rare sur Foundation/SR Legacy,
            # mais jamais suppose silencieusement.
            if kcal is None or protein is None or fat is None or carbs is None:
                skipped_incomplete += 1
                continue

            seen_fdc_ids.add(fdc_id)
            rows.append(
                {
                    "fdc_id": fdc_id,
                    "food_name": food.get("description"),
                    "food_category": food.get("foodCategory"),
                    "data_type": food.get("dataType"),
                    "kcal_per_100g": kcal,
                    "protein_g_per_100g": protein,
                    "carbs_g_per_100g": carbs,
                    "fat_g_per_100g": fat,
                    "search_keyword": keyword,
                }
            )

        time.sleep(PAUSE_BETWEEN_KEYWORDS_SECONDS)

    if not rows:
        raise RuntimeError(
            "Aucun aliment recupere depuis l'API USDA -- ingestion annulee, "
            "rien ecrit en Bronze (echec explicite plutot qu'une table vide silencieuse)."
        )

    dataframe = pd.DataFrame(rows)
    dataframe["ingestion_timestamp"] = datetime.utcnow().isoformat()
    dataframe["source_file"] = "usda_fooddata_central_api/foods/search"
    dataframe["source_dataset"] = DATASET_NAME

    partition_dir = BRONZE_DIR / DATASET_NAME / f"ingestion_date={ds}"
    if partition_dir.exists():
        shutil.rmtree(partition_dir)
    partition_dir.mkdir(parents=True, exist_ok=True)

    output_path = partition_dir / f"{DATASET_NAME}.parquet"
    dataframe.to_parquet(output_path, engine="pyarrow", index=False)

    logger.info(
        "[nutrition_ingestion] %d aliments distincts ecrits dans %s "
        "(%d ignores pour macro-nutriment manquant).",
        len(dataframe), output_path, skipped_incomplete,
    )


# Meme trame que silver_transformation.py/gold_dbt_run.py (mode client,
# adresse IP du conteneur pour que les executeurs joignent le driver).
SPARK_SUBMIT_SILVER_CMD = (
    "spark-submit "
    f"--master {SPARK_MASTER_URL} "
    "--deploy-mode client "
    "--conf spark.driver.host=$(hostname -i) "
    "--conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.hadoop.fs.permissions.umask-mode=000 "
    f"{SPARK_JOBS_DIR}/silver_usda_nutrition.py"
)

SPARK_SUBMIT_LOAD_CMD = (
    "spark-submit "
    f"--master {SPARK_MASTER_URL} "
    "--deploy-mode client "
    "--conf spark.driver.host=$(hostname -i) "
    "--conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.hadoop.fs.permissions.umask-mode=000 "
    "--packages org.postgresql:postgresql:42.7.4 "
    f"{SPARK_JOBS_DIR}/load_silver_to_postgres.py"
)

# Variables dbt (memes noms/valeurs que gold_dbt_run.py, dupliquees ici
# volontairement : l'environnement du conteneur Airflow ne partage pas
# automatiquement le fichier .env de l'hote, cf. commentaire identique dans
# gold_dbt_run.py).
DBT_CONNECTION_ENV = {
    "DBT_POSTGRES_HOST": "app-postgres",
    "DBT_POSTGRES_PORT": "5432",
    "DBT_POSTGRES_USER": "safelift_app",
    "DBT_POSTGRES_PASSWORD": "change_me_app",
    "DBT_POSTGRES_DB": "safelift_dwh",
}
TASK_ENV = {**os.environ, **DBT_CONNECTION_ENV}

# --select scope volontairement restreint : fact_nutrition_target ne
# depend que de dim_user (deja construite par le pipeline Kaggle) --
# inutile de relancer tout gold_dbt_run (matching d'exercices, etc.) pour
# une simple ingestion nutrition.
DBT_SELECT_SCOPE = "stg_usda_nutrition dim_nutrition fact_nutrition_target"
DBT_RUN_CMD = f"{DBT_BIN} run --select {DBT_SELECT_SCOPE} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
DBT_TEST_CMD = f"{DBT_BIN} test --select {DBT_SELECT_SCOPE} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"

default_args = {
    "owner": "safelift",
    "retries": 0,
}

with DAG(
    dag_id="nutrition_ingestion",
    description="Ingestion nutrition USDA FoodData Central -> Bronze -> Silver -> Gold (dim_nutrition, fact_nutrition_target)",
    default_args=default_args,
    schedule=None,  # declenchement manuel pour cette sous-etape
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nutrition", "jalon3", "safelift"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_usda_nutrition",
        python_callable=ingest_usda_nutrition,
    )

    silver_task = BashOperator(
        task_id="silver_usda_nutrition",
        bash_command=SPARK_SUBMIT_SILVER_CMD,
    )

    load_task = BashOperator(
        task_id="load_usda_nutrition_to_postgres",
        bash_command=SPARK_SUBMIT_LOAD_CMD,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run_nutrition",
        bash_command=DBT_RUN_CMD,
        env=TASK_ENV,
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test_nutrition",
        bash_command=DBT_TEST_CMD,
        env=TASK_ENV,
    )

    ingest_task >> silver_task >> load_task >> dbt_run_task >> dbt_test_task
