"""SafeLift - Simulateur d'affluence (Jalon 2, sous-etape 1/5).

Processus long-courant (PAS un DAG Airflow, PAS un batch planifie) qui
publie en continu, toutes les 5 a 10 secondes (configurable), un message
JSON d'occupation pour chacune des salles de `gold.dim_gym` sur le topic
Kafka `safelift-gym-occupancy`.

Format du message (decision deja actee : le consumer de la sous-etape
suivante sera en Spark Structured Streaming, JSON simple, un message = un
evenement d'affluence a un instant T pour une salle donnee) :

    {
        "gym_id": 1,
        "timestamp": "2026-07-09T18:03:41.512+00:00",
        "current_occupancy": 87,
        "capacity": 140
    }

IMPORTANT : `current_occupancy` suit un pattern FICTIF mais credible (pas
une vraie donnee d'affluence mesuree) -- voir `peak_ratio()` ci-dessous et
data/gold/GOLD_MODEL_DECISIONS.md section 9. `gold.dim_gym` elle-meme
contient 5 salles 100% inventees (aucun dataset Kaggle d'affluence de salle
de sport disponible).

Bibliotheque Kafka : `confluent-kafka`, choisie pour rester coherente avec
`apache-airflow-providers-apache-kafka` (deja utilisee cote Airflow, voir
airflow/requirements.txt), qui s'appuie lui-meme sur `confluent-kafka`
(bindings officiels librdkafka) -- aucune autre librairie Kafka Python
n'etait deja utilisee dans le projet.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("gym_simulator")

# Fichier de heartbeat relu par le healthcheck Docker (voir docker-compose.yml,
# service gym-simulator) : reecrit a chaque cycle reussi, permet de detecter
# un simulateur bloque (ex. producteur Kafka qui ne repond plus) sans avoir
# de port HTTP a exposer pour ce service.
HEARTBEAT_PATH = "/tmp/gym_simulator_heartbeat"

# Vitesse de convergence de l'occupation courante vers sa cible horaire a
# chaque cycle (0 = n'evolue jamais, 1 = saute instantanement a la cible).
# Une valeur intermediaire produit une trajectoire lissee (comme une vraie
# affluence qui monte/descend progressivement) plutot qu'un bruit i.i.d. a
# chaque message, ce qui est le point explicitement demande pour ce
# simulateur.
SMOOTHING = 0.35

# Bruit aleatoire ajoute a chaque cycle, exprime en fraction de la capacite
# max de la salle (ecart-type d'une loi normale centree en 0).
NOISE_STD_RATIO = 0.05

# Etat partage entre le thread principal et les handlers de signal : demande
# d'arret propre (Ctrl+C / docker stop -> SIGTERM).
_stop_requested = False


def _handle_stop_signal(signum, _frame):
    global _stop_requested
    logger.info("Signal %s recu -- arret propre demande (fin du cycle en cours).", signum)
    _stop_requested = True


def peak_ratio(dt: datetime) -> float:
    """Ratio d'occupation "cible" (0.0 a 1.0 de la capacite max) pour un instant donne.

    Pattern FICTIF mais credible, PAS une vraie donnee d'affluence mesuree
    (voir data/gold/GOLD_MODEL_DECISIONS.md section 9) :
    - En semaine (lundi a vendredi) : deux pics (avant/apres le travail)
      7h-9h et 18h-21h (ratio 0.75), affluence moderee le reste de la
      journee 9h-18h et 21h-23h (ratio 0.35), quasi vide la nuit 23h-7h
      (ratio 0.05).
    - Le week-end (samedi/dimanche) : pas de double pic matin/soir (pas de
      trajet domicile-travail) -- un plateau plus etale en journee, 10h-19h
      (ratio 0.45), quasi vide le reste du temps (ratio 0.05).
    """
    hour = dt.hour
    is_weekend = dt.weekday() >= 5  # 5 = samedi, 6 = dimanche

    if is_weekend:
        return 0.45 if 10 <= hour < 19 else 0.05

    if 7 <= hour < 9 or 18 <= hour < 21:
        return 0.75
    if 9 <= hour < 18 or 21 <= hour < 23:
        return 0.35
    return 0.05


def next_occupancy(current: float, capacity: int, dt: datetime) -> int:
    """Fait evoluer l'occupation courante d'un cran vers sa cible horaire, avec bruit."""
    target = peak_ratio(dt) * capacity
    noise = random.gauss(0, NOISE_STD_RATIO * capacity)
    new_value = current + (target - current) * SMOOTHING + noise
    return int(round(min(max(new_value, 0), capacity)))


def load_gyms_with_retry(max_attempts: int = 10, delay_seconds: float = 5.0):
    """Charge (gym_id, gym_name, capacity_max) depuis gold.dim_gym, avec retry.

    Retry necessaire car `gold.dim_gym` est creee par dbt (voir
    dbt/models/marts/dim_gym.sql), pas par ce script : au tout premier
    demarrage de la stack, il est possible que ce conteneur demarre avant
    que `dbt seed && dbt run --select dim_gym` ait ete execute au moins une
    fois. Erreur fatale (exit 1) seulement apres epuisement des tentatives.
    """
    conn_params = dict(
        host=os.environ["APP_POSTGRES_HOST"],
        port=os.environ["APP_POSTGRES_PORT"],
        dbname=os.environ["APP_POSTGRES_DB"],
        user=os.environ["APP_POSTGRES_USER"],
        password=os.environ["APP_POSTGRES_PASSWORD"],
    )

    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(**conn_params)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT gym_id, gym_name, capacity_max FROM gold.dim_gym ORDER BY gym_id"
                    )
                    rows = cur.fetchall()
            finally:
                conn.close()

            if not rows:
                raise RuntimeError("gold.dim_gym existe mais est vide")

            gyms = [
                {"gym_id": r[0], "gym_name": r[1], "capacity_max": r[2]}
                for r in rows
            ]
            logger.info("%d salle(s) chargee(s) depuis gold.dim_gym : %s",
                        len(gyms), ", ".join(g["gym_name"] for g in gyms))
            return gyms
        except Exception as exc:
            logger.warning(
                "Tentative %d/%d de lecture de gold.dim_gym echouee (%s). "
                "Nouvelle tentative dans %ss -- table probablement pas encore creee par dbt "
                "(voir PROGRESS_JALON2.md pour la commande dbt seed/run a lancer).",
                attempt, max_attempts, exc, delay_seconds,
            )
            time.sleep(delay_seconds)

    logger.error("Impossible de lire gold.dim_gym apres %d tentatives. Arret du simulateur.",
                 max_attempts)
    sys.exit(1)


def _delivery_callback(err, msg):
    """Callback confluent-kafka : log clair a chaque succes/echec, jamais silencieux."""
    if err is not None:
        logger.error("Echec de livraison Kafka (key=%s) : %s", msg.key(), err)
    else:
        logger.info(
            "Message livre sur %s [partition %d, offset %d] : %s",
            msg.topic(), msg.partition(), msg.offset(), msg.value().decode("utf-8"),
        )


def _write_heartbeat():
    try:
        with open(HEARTBEAT_PATH, "w") as f:
            f.write(str(time.time()))
    except OSError as exc:
        # Le heartbeat n'est qu'un signal pour le healthcheck Docker : une
        # erreur d'ecriture ne doit jamais interrompre la publication Kafka.
        logger.warning("Impossible d'ecrire le heartbeat (%s) -- healthcheck potentiellement affecte.", exc)


def _interruptible_sleep(duration_seconds: float, step_seconds: float = 0.5):
    """time.sleep() decoupe en petits pas pour reagir vite a un signal d'arret."""
    slept = 0.0
    while slept < duration_seconds and not _stop_requested:
        time.sleep(min(step_seconds, duration_seconds - slept))
        slept += step_seconds


def main():
    signal.signal(signal.SIGINT, _handle_stop_signal)
    signal.signal(signal.SIGTERM, _handle_stop_signal)

    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    topic = os.environ["KAFKA_GYM_OCCUPANCY_TOPIC"]
    interval_min = float(os.environ.get("GYM_SIMULATOR_INTERVAL_MIN_SECONDS", "5"))
    interval_max = float(os.environ.get("GYM_SIMULATOR_INTERVAL_MAX_SECONDS", "10"))

    logger.info(
        "Demarrage du simulateur d'affluence -- topic=%s bootstrap=%s intervalle=[%s,%s]s",
        topic, bootstrap_servers, interval_min, interval_max,
    )

    gyms = load_gyms_with_retry()

    # Etat initial : chaque salle demarre directement a son ratio "cible" de
    # l'instant present (pas a 0), pour eviter une fausse periode de "montee
    # en charge" a chaque redemarrage du conteneur (ex. restart: unless-stopped).
    now = datetime.now()
    occupancy_state = {g["gym_id"]: peak_ratio(now) * g["capacity_max"] for g in gyms}

    producer = Producer({"bootstrap.servers": bootstrap_servers})

    cycle = 0
    while not _stop_requested:
        cycle += 1
        now = datetime.now()

        for gym in gyms:
            gym_id = gym["gym_id"]
            capacity = gym["capacity_max"]
            new_occupancy = next_occupancy(occupancy_state[gym_id], capacity, now)
            occupancy_state[gym_id] = new_occupancy

            message = {
                "gym_id": gym_id,
                "timestamp": now.astimezone(timezone.utc).isoformat(),
                "current_occupancy": new_occupancy,
                "capacity": capacity,
            }

            try:
                producer.produce(
                    topic,
                    key=str(gym_id).encode("utf-8"),
                    value=json.dumps(message).encode("utf-8"),
                    callback=_delivery_callback,
                )
            except BufferError:
                logger.warning(
                    "File d'attente locale du producteur pleine -- poll() force avant de continuer."
                )
                producer.poll(1)
            except Exception:
                # Jamais de crash silencieux : on logge la stack complete et on
                # poursuit avec les autres salles plutot que d'arreter tout le
                # simulateur pour un seul message en echec.
                logger.exception(
                    "Erreur inattendue lors de la publication pour gym_id=%s -- message ignore.",
                    gym_id,
                )

        producer.poll(0)
        producer.flush(timeout=5)
        _write_heartbeat()

        if cycle % 6 == 0:
            logger.info("Cycle %d termine (%d salle(s) publiee(s)).", cycle, len(gyms))

        _interruptible_sleep(random.uniform(interval_min, interval_max))

    logger.info("Arret demande -- flush final du producteur Kafka...")
    producer.flush(timeout=10)
    logger.info("Simulateur arrete proprement.")
    sys.exit(0)


if __name__ == "__main__":
    main()
