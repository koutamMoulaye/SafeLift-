"""SafeLift — Consumer Spark Structured Streaming : etat courant d'affluence
(Jalon 2, sous-etape 2/5).

Lit en continu le topic Kafka `safelift-gym-occupancy` (produit par
scripts/simulate_gym_occupancy.py, format JSON : gym_id, timestamp,
current_occupancy, capacity) et maintient l'ETAT COURANT (dernier message
connu par salle) dans la table Postgres `gold.gym_occupancy_live` — PAS un
historique/une fenetre temporelle a ce stade (decision deja actee : approche
simple, une ligne par salle, ecrasee a chaque nouveau message la concernant).

Choix structurants (voir aussi data/gold/GOLD_MODEL_DECISIONS.md section 10
pour le detail complet et chiffre) :

- `startingOffsets=latest` : au (re)demarrage du job, on ignore l'historique
  deja publie sur le topic et on ne consomme que les nouveaux messages. Ce
  job maintient un ETAT COURANT (pas un historique a preserver) : rejouer
  tout l'historique a chaque redemarrage n'apporterait rien (les vieux
  messages seraient de toute facon ecrases par les plus recents au premier
  micro-batch les concernant pour chaque salle), et retarderait inutilement
  la fraicheur de la table pendant tout le rattrapage. Consequence assumee :
  les messages publies pendant que ce job est arrete (redemarrage,
  deploiement) sont definitivement perdus — acceptable ici puisqu'aucune
  valeur n'est tiree de connaitre l'occupation exacte a un instant passe non
  observe (contrairement a un historique, hors perimetre de cette
  sous-etape).
- Upsert manuel via `INSERT ... ON CONFLICT (gym_id) DO UPDATE` (psycopg2,
  PAS le writer JDBC de Spark, qui ne supporte nativement que
  append/overwrite/ignore/errorifexists — aucune notion d'upsert) : chaque
  micro-batch est minuscule (au plus une ligne par salle de dim_gym, donc
  3 a 5 lignes), collecte cote driver (`.collect()`, foreachBatch s'execute
  de toute facon sur le driver) puis ecrite en une seule transaction
  psycopg2. Prefere a un DELETE puis INSERT explicites (option initialement
  envisagee) : `ON CONFLICT DO UPDATE` est une primitive Postgres atomique
  unique, alors que DELETE+INSERT necessiterait 2 instructions distinctes
  dans la meme transaction pour offrir la meme garantie — strictement
  equivalent en resultat final, plus simple a lire et a maintenir.
- Aucun operateur stateful (pas d'aggregation/watermark/join) : le
  checkpoint Spark ne contient donc que les offsets Kafka consommes et le
  log de commit du sink foreachBatch, geres uniquement par le driver.
  Contrairement a silver_transformation.py (ecriture Parquet partagee entre
  driver et executeurs, necessitant `spark.hadoop.fs.permissions.umask-mode=000`),
  aucun repertoire partage entre ce conteneur et spark-worker n'est requis
  ici : le volume de checkpoint est monte UNIQUEMENT sur le conteneur
  driver (spark-streaming-gym, voir docker-compose.yml).
- Erreurs de parsing JSON : gerees via `from_json` avec un schema Spark
  EXPLICITE (pas d'inference automatique). `from_json` est en mode
  PERMISSIVE par defaut et ne leve JAMAIS d'exception — un message malforme
  produit un struct entierement null plutot que de faire planter le job.
  Chaque micro-batch filtre explicitement les lignes invalides (champs
  requis null) avant l'upsert, avec un simple log d'avertissement (compte
  de messages ignores) — le stream continue sans interruption.
"""

import logging
import os
import time

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("stream_gym_occupancy")

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_GYM_OCCUPANCY_TOPIC"]

# Volume Docker dedie (spark_streaming_checkpoints, voir docker-compose.yml),
# monte UNIQUEMENT sur ce conteneur -- voir docstring du module.
CHECKPOINT_LOCATION = "/tmp/spark-checkpoints/gym_occupancy"

# Relu par le healthcheck Docker du service spark-streaming-gym -- meme
# pattern que scripts/simulate_gym_occupancy.py (pas de port HTTP expose par
# ce job, un fichier de heartbeat est le signal le plus simple).
HEARTBEAT_PATH = "/tmp/spark_streaming_heartbeat"

PG_CONN_PARAMS = dict(
    host=os.environ["APP_POSTGRES_HOST"],
    port=os.environ["APP_POSTGRES_PORT"],
    dbname=os.environ["APP_POSTGRES_DB"],
    user=os.environ["APP_POSTGRES_USER"],
    password=os.environ["APP_POSTGRES_PASSWORD"],
)

TARGET_TABLE = "gold.gym_occupancy_live"

# Schema EXPLICITE du message JSON (meme format que
# scripts/simulate_gym_occupancy.py) -- pas d'inference automatique.
MESSAGE_SCHEMA = StructType(
    [
        StructField("gym_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("current_occupancy", IntegerType(), True),
        StructField("capacity", IntegerType(), True),
    ]
)

# Seuils de charge_category -- voir data/gold/GOLD_MODEL_DECISIONS.md
# section 10 pour la justification complete. Choisis pour rester dans
# l'esprit du reste du projet (seuils simples, documentes, aucune boite
# noire) : Faible sous 40% (salle confortable), Moderee de 40% a 70% (salle
# qui se remplit mais reste utilisable), Elevee au-dessus de 70% (seuil
# couramment retenu comme "quasi-sature" pour un lieu recevant du public --
# file d'attente probable sur les equipements les plus demandes).
CHARGE_LOW_THRESHOLD = 0.40
CHARGE_HIGH_THRESHOLD = 0.70


def ensure_target_table_exists() -> None:
    """Cree gold.gym_occupancy_live si absente, via psycopg2 (pas de JVM).

    Meme raisonnement que ensure_raw_schema_exists() dans
    spark/jobs/load_silver_to_postgres.py : le driver JDBC Postgres ajoute
    par --packages vit dans un classloader Spark isole (MutableURLClassLoader),
    invisible d'un DriverManager JDBC "nu" utilise via py4j. psycopg2 evite
    completement ce probleme puisqu'il ne passe jamais par la JVM.
    """
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS gold.gym_occupancy_live (
                    gym_id INTEGER PRIMARY KEY,
                    last_message_timestamp TIMESTAMPTZ NOT NULL,
                    current_occupancy INTEGER NOT NULL,
                    capacity INTEGER NOT NULL,
                    occupancy_rate DOUBLE PRECISION NOT NULL,
                    charge_category TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
        logger.info("Table %s prete (creee si absente).", TARGET_TABLE)
    finally:
        conn.close()


def _write_heartbeat() -> None:
    try:
        with open(HEARTBEAT_PATH, "w") as f:
            f.write(str(time.time()))
    except OSError as exc:
        logger.warning("Impossible d'ecrire le heartbeat (%s) -- healthcheck potentiellement affecte.", exc)


def upsert_batch(batch_df, batch_id: int) -> None:
    """Sink foreachBatch : upsert manuel (INSERT ... ON CONFLICT DO UPDATE).

    S'execute sur le DRIVER (comportement documente de foreachBatch dans
    Spark Structured Streaming). Chaque micro-batch est petit (au plus une
    ligne par salle de dim_gym) : un .collect() est donc largement
    suffisant, pas besoin de repasser par un ecrivain JDBC distribue.
    """
    total = batch_df.count()
    if total == 0:
        return

    valid_df = batch_df.filter(
        col("gym_id").isNotNull()
        & col("current_occupancy").isNotNull()
        & col("capacity").isNotNull()
        & col("parsed_timestamp").isNotNull()
        & col("occupancy_rate").isNotNull()
        & col("charge_category").isNotNull()
    )
    valid_rows = valid_df.collect()
    invalid_count = total - len(valid_rows)

    if invalid_count > 0:
        logger.warning(
            "Batch %d : %d message(s) JSON malforme(s) ou incomplet(s) ignore(s) sur %d -- stream poursuivi.",
            batch_id, invalid_count, total,
        )

    if not valid_rows:
        return

    try:
        conn = psycopg2.connect(**PG_CONN_PARAMS)
    except Exception:
        # Jamais de crash silencieux : une Postgres momentanement injoignable
        # ne doit pas tuer tout le stream -- le prochain micro-batch (quelques
        # secondes plus tard) retentera naturellement une ecriture a jour.
        logger.exception("Batch %d : connexion a Postgres impossible -- micro-batch ignore.", batch_id)
        return

    try:
        with conn:
            with conn.cursor() as cur:
                for row in valid_rows:
                    cur.execute(
                        """
                        INSERT INTO gold.gym_occupancy_live
                            (gym_id, last_message_timestamp, current_occupancy, capacity,
                             occupancy_rate, charge_category, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (gym_id) DO UPDATE SET
                            last_message_timestamp = EXCLUDED.last_message_timestamp,
                            current_occupancy = EXCLUDED.current_occupancy,
                            capacity = EXCLUDED.capacity,
                            occupancy_rate = EXCLUDED.occupancy_rate,
                            charge_category = EXCLUDED.charge_category,
                            updated_at = now()
                        """,
                        (
                            row.gym_id,
                            row.parsed_timestamp,
                            row.current_occupancy,
                            row.capacity,
                            row.occupancy_rate,
                            row.charge_category,
                        ),
                    )
        logger.info(
            "Batch %d : %d salle(s) mise(s) a jour dans %s.",
            batch_id, len(valid_rows), TARGET_TABLE,
        )
        _write_heartbeat()
    except Exception:
        logger.exception("Batch %d : erreur lors de l'upsert Postgres -- micro-batch ignore.", batch_id)
    finally:
        conn.close()


def main() -> None:
    logger.info(
        "Demarrage du consumer streaming -- topic=%s bootstrap=%s -> %s",
        KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS, TARGET_TABLE,
    )

    ensure_target_table_exists()

    spark = SparkSession.builder.appName("stream_gym_occupancy").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        # startingOffsets=latest : voir docstring du module pour la
        # justification complete (etat courant, pas d'historique a preserver).
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_stream.select(
            from_json(col("value").cast("string"), MESSAGE_SCHEMA).alias("data")
        )
        .select(
            col("data.gym_id").alias("gym_id"),
            col("data.current_occupancy").alias("current_occupancy"),
            col("data.capacity").alias("capacity"),
            to_timestamp(col("data.timestamp")).alias("parsed_timestamp"),
        )
        .withColumn(
            "occupancy_rate",
            when(
                col("capacity").isNotNull() & (col("capacity") > 0),
                col("current_occupancy") / col("capacity"),
            ),
        )
        .withColumn(
            "charge_category",
            when(col("occupancy_rate").isNull(), None)
            .when(col("occupancy_rate") < CHARGE_LOW_THRESHOLD, "Faible")
            .when(col("occupancy_rate") <= CHARGE_HIGH_THRESHOLD, "Moderee")
            .otherwise("Elevee"),
        )
    )

    query = (
        parsed.writeStream.foreachBatch(upsert_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="5 seconds")
        .start()
    )

    logger.info("Stream demarre (checkpoint=%s). En attente de messages...", CHECKPOINT_LOCATION)
    query.awaitTermination()


if __name__ == "__main__":
    main()
