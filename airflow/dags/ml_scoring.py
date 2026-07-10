"""SafeLift -- DAG de scoring ML bonus (Jalon 3, sous-etape 5/6).

Produit une prediction du risk_score de la semaine suivante par
(user_id, muscle_group), via le modele entraine en sous-etape 4/6
(data/ml/model.pkl), et l'ecrit dans gold.ml_risk_prediction
(scripts/score_risk_trend.py -- voir ce fichier pour le detail complet du
calcul et des garanties anti-fuite reutilisees de prepare_ml_features.py).

Declenchement : APRES gold_dbt_run (TriggerDagRunOperator ajoute en fin de
gold_dbt_run.py, apres dbt_test) -- meme mecanisme d'orchestration deja en
place pour la cascade bronze_ingestion -> silver_transformation ->
gold_dbt_run. Consequence assumee : ce DAG se declenche donc aussi
automatiquement apres qu'une seance temps reel (Jalon 2, sous-etape 3/5)
ait declenche un recalcul complet de gold_dbt_run -- la prediction ML reste
ainsi a jour avec les memes donnees fraiches que le risk_score deterministe,
sans mecanisme de synchronisation supplementaire a maintenir. Si dbt_test
echoue dans gold_dbt_run, ce DAG n'est PAS declenche (trigger_rule par
defaut = all_success sur la task de declenchement) : aucune prediction n'est
jamais calculee sur des donnees Gold potentiellement invalides.

Perimetre volontairement borne : SCORING uniquement, jamais de
reentrainement automatique ici -- scripts/train_risk_trend_model.py reste
une action manuelle explicite (sous-etape 4/6), un nouveau modele ne doit
jamais etre entraine silencieusement en arriere-plan (voir CLAUDE.md :
"toute evolution future vers du ML devra etre une etape explicitement
identifiee, pas une modification silencieuse").
"""

import os
from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator

SCRIPTS_DIR = "/opt/airflow/scripts"

# Memes noms/valeurs que gold_dbt_run.py/nutrition_ingestion.py -- dupliques
# volontairement ici (l'environnement du conteneur Airflow ne partage pas
# automatiquement le fichier .env de l'hote, meme raisonnement documente
# dans les DAGs existants). Ce DAG n'utilise pas dbt lui-meme, mais
# scripts/score_risk_trend.py se connecte a app-postgres via les memes
# variables DBT_POSTGRES_* que scripts/prepare_ml_features.py (reutilise).
DB_CONNECTION_ENV = {
    "DBT_POSTGRES_HOST": "app-postgres",
    "DBT_POSTGRES_PORT": "5432",
    "DBT_POSTGRES_USER": "safelift_app",
    "DBT_POSTGRES_PASSWORD": "change_me_app",
    "DBT_POSTGRES_DB": "safelift_dwh",
}
# BashOperator.env REMPLACE l'environnement du sous-processus (comportement
# documente d'Airflow) : fusion explicite avec os.environ pour ne pas perdre
# PATH/HOME/etc. -- meme pattern que gold_dbt_run.py/nutrition_ingestion.py.
TASK_ENV = {**os.environ, **DB_CONNECTION_ENV}

SCORE_RISK_TREND_CMD = f"python3 {SCRIPTS_DIR}/score_risk_trend.py"

default_args = {
    "owner": "safelift",
    "retries": 0,
}

with DAG(
    dag_id="ml_scoring",
    description="Scoring batch du modele ML bonus (prediction risk_score semaine suivante) -> gold.ml_risk_prediction",
    default_args=default_args,
    schedule=None,  # declenche par gold_dbt_run (TriggerDagRunOperator)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ml", "jalon3", "safelift"],
) as dag:
    score_risk_trend = BashOperator(
        task_id="score_risk_trend",
        bash_command=SCORE_RISK_TREND_CMD,
        env=TASK_ENV,
    )
