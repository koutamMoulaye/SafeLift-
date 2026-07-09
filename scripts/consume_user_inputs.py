"""SafeLift — Consumer des saisies utilisateur temps reel (Jalon 2,
sous-etape 3/5).

Processus long-courant (PAS un DAG Airflow, meme famille de service que
scripts/simulate_gym_occupancy.py) qui :
1. Lit en continu le topic Kafka `safelift-user-inputs` (publie par
   `POST /users/{user_id}/sessions` dans dashboard/main.py -- l'API ne
   write JAMAIS directement en base, le decouplage evenementiel passe
   entierement par Kafka, c'est le point architectural demontre ici).
2. Valide le schema de chaque message (types + valeurs positives) --
   message malforme : logue et ignore, JAMAIS de crash du consumer (meme
   philosophie que spark/jobs/stream_gym_occupancy.py).
3. Insere la seance validee dans `raw.realtime_user_sessions` (table creee
   par ce script au demarrage, PAS geree par dbt).
4. Declenche un run dbt CIBLE... en pratique le DAG `gold_dbt_run` EXISTANT
   dans son integralite (pas de run partiel par utilisateur) via l'API REST
   d'Airflow, avec `conf={"user_id": ...}` transmis uniquement a des fins de
   tracabilite/logging dans l'UI Airflow.

DECISION D'ARCHITECTURE DEJA ACTEE (zero duplication de logique metier) :
ce consumer NE RECALCULE PAS le risk_score lui-meme -- la SEULE formule de
calcul reste celle de dbt (dbt/models/marts/fact_risk_score.sql). Ce script
se contente de faire entrer la donnee dans le pipeline (raw.realtime_user_sessions
-> stg_realtime_user_sessions -> stg_workout_sessions_unified ->
fact_workout_session -> fact_risk_score, voir
data/gold/GOLD_MODEL_DECISIONS.md section 11) puis de declencher le run qui
applique cette formule.

Run dbt PARTIEL par utilisateur juge trop complexe a isoler proprement pour
cette sous-etape et donc PAS implemente (documente, pas simplement omis) :
fact_risk_score.sql calcule des facteurs par FENETRE GLISSANTE (charge_factor/
volume_factor comparent chaque semaine a la semaine precedente et a la
moyenne historique, cf. `lag()`/`avg() over (...)`) -- un `dbt run --select
fact_risk_score, where user_id=X` n'existe pas nativement en dbt (le
`--select` filtre des MODELES, pas des lignes), et materialiser un filtre
par utilisateur necessiterait soit un modele incremental (refonte hors
perimetre de cette sous-etape), soit de recalculer quand meme TOUTE la
fenetre pour rester correct. Le run complet (`gold_dbt_run`) reste donc la
seule option a la fois simple et garantie correcte -- acceptable pour le
volume de donnees de ce projet (dbt run complet mesure en quelques secondes,
voir PROGRESS_JALON2.md pour le delai reel observe de bout en bout).

Gestion d'erreur robuste (explicitement demandee, jamais de perte
silencieuse) :
- Kafka injoignable au demarrage : le Consumer confluent-kafka retente en
  interne (comportement natif de la librairie), logue au fil des poll().
- Postgres injoignable au moment d'inserer une seance validee : logue en
  ERROR (stack complete), message NON commit (le prochain poll() le
  represente -- `enable.auto.commit=True`, l'offset de ce message n'a pas
  encore ete avance) -- pas de perte, juste un retard.
- API Airflow injoignable/erreur HTTP apres une insertion REUSSIE : logue
  en ERROR avec un message explicite ("la seance est bien en base, recalcul
  a declencher manuellement") -- la seance n'est PAS perdue (deja en base),
  seul le recalcul automatique echoue, ce qui est bien la consequence la
  moins grave possible dans ce cas de figure.
"""

import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import psycopg2
import requests
from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("consume_user_inputs")

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_USER_INPUTS_TOPIC"]

# Relu par le healthcheck Docker (voir docker-compose.yml, service
# user-inputs-consumer) -- reecrit a chaque iteration de la boucle de poll,
# que des messages soient recus ou non (contrairement a
# spark/jobs/stream_gym_occupancy.py, qui ne l'ecrit qu'apres un micro-batch
# reussi : ici il n'y a pas de notion de micro-batch, poll() est deja
# frequent par construction, ecrire a chaque iteration est suffisant et
# simple).
HEARTBEAT_PATH = "/tmp/user_inputs_consumer_heartbeat"

PG_CONN_PARAMS = dict(
    host=os.environ["APP_POSTGRES_HOST"],
    port=os.environ["APP_POSTGRES_PORT"],
    dbname=os.environ["APP_POSTGRES_DB"],
    user=os.environ["APP_POSTGRES_USER"],
    password=os.environ["APP_POSTGRES_PASSWORD"],
)

AIRFLOW_API_BASE_URL = os.environ["AIRFLOW_API_BASE_URL"]
AIRFLOW_ADMIN_USERNAME = os.environ["AIRFLOW_ADMIN_USERNAME"]
AIRFLOW_ADMIN_PASSWORD = os.environ["AIRFLOW_ADMIN_PASSWORD"]
GOLD_DBT_RUN_DAG_ID = "gold_dbt_run"

# Schema EXPLICITE du message attendu (memes champs que UserSessionInput
# dans dashboard/main.py, plus user_id ajoute par l'API avant publication).
REQUIRED_FIELDS = {
    "user_id": int,
    "exercise_name": str,
    "lifted_weight_kg": (int, float),
    "reps": int,
    "sets": int,
}

_stop_requested = False


def _handle_stop_signal(signum, _frame):
    global _stop_requested
    logger.info("Signal %s recu -- arret propre demande.", signum)
    _stop_requested = True


def ensure_target_table_exists() -> None:
    """Cree raw.realtime_user_sessions si absente, via psycopg2 (pas de JVM
    ici -- ce script n'utilise pas Spark, mais meme raisonnement de fond que
    load_silver_to_postgres.py/stream_gym_occupancy.py : DDL simple, source
    de verite du schema directement dans le code qui l'alimente).
    """
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw.realtime_user_sessions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    exercise_name TEXT NOT NULL,
                    lifted_weight_kg DOUBLE PRECISION NOT NULL,
                    reps INTEGER NOT NULL,
                    sets INTEGER NOT NULL,
                    duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                    performed_at TIMESTAMPTZ NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
        logger.info("Table raw.realtime_user_sessions prete (creee si absente).")
    finally:
        conn.close()


def validate_message(data) -> str | None:
    """Renvoie un message d'erreur (str) si le message est invalide, None si valide.

    Volontairement permissif sur les champs EXTRA (ex. submitted_at, envoye
    par l'API mais pas requis ici) -- seuls les champs REQUIS sont verifies,
    strictement.
    """
    if not isinstance(data, dict):
        return "le message racine n'est pas un objet JSON"

    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in data:
            return f"champ requis manquant : {field}"
        if isinstance(data[field], bool) or not isinstance(data[field], expected_type):
            return f"champ {field} de type invalide (recu {type(data[field]).__name__})"

    if data["lifted_weight_kg"] <= 0 or data["reps"] <= 0 or data["sets"] <= 0:
        return "lifted_weight_kg, reps et sets doivent etre strictement positifs"

    return None


def insert_session(data: dict) -> None:
    performed_at = data.get("submitted_at") or datetime.now(timezone.utc).isoformat()
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw.realtime_user_sessions
                        (user_id, exercise_name, lifted_weight_kg, reps, sets, duration_seconds, performed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        data["user_id"],
                        data["exercise_name"],
                        float(data["lifted_weight_kg"]),
                        data["reps"],
                        data["sets"],
                        float(data.get("duration_seconds") or 0),
                        performed_at,
                    ),
                )
    finally:
        conn.close()


def trigger_gold_dbt_run(user_id: int) -> None:
    """Declenche le DAG gold_dbt_run (run COMPLET, pas partiel -- voir
    docstring du module) via l'API REST Airflow.

    `conf` transmis uniquement pour la tracabilite dans l'UI Airflow (visible
    sur la page du DAG run) -- gold_dbt_run.py ne lit pas cette conf, le run
    recalcule tout le schema gold comme d'habitude.
    """
    dag_run_id = f"realtime_input_user{user_id}_{uuid.uuid4().hex[:8]}"
    url = f"{AIRFLOW_API_BASE_URL}/api/v1/dags/{GOLD_DBT_RUN_DAG_ID}/dagRuns"
    payload = {
        "dag_run_id": dag_run_id,
        "conf": {"triggered_by": "realtime_user_input", "user_id": user_id},
    }
    try:
        resp = requests.post(
            url,
            json=payload,
            auth=(AIRFLOW_ADMIN_USERNAME, AIRFLOW_ADMIN_PASSWORD),
            timeout=10,
        )
        if resp.status_code in (200, 201):
            logger.info(
                "DAG %s declenche (dag_run_id=%s) pour user_id=%s.",
                GOLD_DBT_RUN_DAG_ID, dag_run_id, user_id,
            )
        else:
            logger.error(
                "Echec du declenchement de %s (HTTP %s) : %s -- la seance est bien en base "
                "(raw.realtime_user_sessions), mais le recalcul devra etre declenche manuellement "
                "(airflow dags trigger %s).",
                GOLD_DBT_RUN_DAG_ID, resp.status_code, resp.text[:500], GOLD_DBT_RUN_DAG_ID,
            )
    except requests.exceptions.RequestException as exc:
        logger.error(
            "Airflow injoignable pour declencher %s (%s) -- la seance est bien en base "
            "(raw.realtime_user_sessions), recalcul a declencher manuellement.",
            GOLD_DBT_RUN_DAG_ID, exc,
        )


def _write_heartbeat() -> None:
    try:
        with open(HEARTBEAT_PATH, "w") as f:
            f.write(str(time.time()))
    except OSError as exc:
        logger.warning("Impossible d'ecrire le heartbeat (%s).", exc)


def main() -> None:
    signal.signal(signal.SIGINT, _handle_stop_signal)
    signal.signal(signal.SIGTERM, _handle_stop_signal)

    logger.info(
        "Demarrage du consumer de saisies utilisateur -- topic=%s bootstrap=%s",
        KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS,
    )

    ensure_target_table_exists()

    # auto.offset.reset=earliest (PAS latest, contrairement au simulateur
    # d'affluence -- voir data/gold/GOLD_MODEL_DECISIONS.md section 11) :
    # une seance utilisateur est une DONNEE IMPORTANTE qui ne doit jamais
    # etre perdue silencieusement, contrairement a un etat d'affluence
    # ephemere. "earliest" ne joue en pratique QUE lors du tout premier
    # demarrage de ce groupe de consumers (aucun offset commit encore) --
    # les redemarrages suivants reprennent depuis le dernier offset commit
    # (persiste par Kafka), pas depuis le debut du topic a chaque fois.
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "safelift-user-inputs-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    logger.info("Consumer pret, en attente de messages sur %s...", KAFKA_TOPIC)

    while not _stop_requested:
        msg = consumer.poll(1.0)
        _write_heartbeat()

        if msg is None:
            continue

        if msg.error():
            logger.error("Erreur Kafka lors du poll : %s", msg.error())
            continue

        raw_value = msg.value()
        try:
            data = json.loads(raw_value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning(
                "Message JSON malforme ignore (%s) : %r -- stream poursuivi.",
                exc, raw_value[:200] if raw_value else raw_value,
            )
            continue

        error = validate_message(data)
        if error:
            logger.warning("Message invalide ignore (%s) : %r -- stream poursuivi.", error, data)
            continue

        try:
            insert_session(data)
        except Exception:
            # Jamais de crash silencieux : log complet, message NON marque
            # traite (enable.auto.commit periodique -- l'offset de CE
            # message precis peut etre re-livre au prochain redemarrage si
            # le commit n'a pas encore eu lieu, comportement "au moins une
            # fois" assume et documente, cf. docstring du module).
            logger.exception(
                "Echec d'insertion en base pour user_id=%s -- message ignore pour ce cycle.",
                data.get("user_id"),
            )
            continue

        logger.info(
            "Seance inseree : user_id=%s, %s, %skg x %s series x %s reps.",
            data["user_id"], data["exercise_name"], data["lifted_weight_kg"], data["sets"], data["reps"],
        )

        trigger_gold_dbt_run(data["user_id"])

    logger.info("Arret demande -- fermeture propre du consumer (commit final des offsets).")
    consumer.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
