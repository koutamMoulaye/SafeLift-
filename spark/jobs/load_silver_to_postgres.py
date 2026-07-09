"""SafeLift — Chargement Silver -> Postgres (schema raw), pour alimenter dbt.

dbt (adaptateur dbt-postgres) opere sur une base SQL, pas directement sur des
fichiers Parquet. Ce job Spark relit les tables Silver (data/silver/*) et
les ecrit telles quelles dans le schema `raw` de la base applicative
(app-postgres), via JDBC. dbt part ensuite de ces tables `raw.*` comme
sources pour construire le modele en etoile (staging -> marts).

Reutilise pour la nutrition (Jalon 3, sous-etape 1/6, table
"usda_nutrition") -- meme mecanisme que pour les 4 tables Kaggle
d'origine, invoque a la fois par gold_dbt_run.py (pipeline Kaggle) et par
nutrition_ingestion.py (pipeline nutrition, domaine independant) : chaque
run recharge TOUTES les tables de TABLES ci-dessous, y compris celles hors
du domaine du DAG appelant -- leger surcout (quelques secondes par table)
accepte pour ne pas dupliquer ce script.

Mode d'ecriture : "overwrite" (table recreee a chaque run). Coherent avec
Silver, qui est deja une vue cumulative recalculee entierement a chaque
execution (pas de logique incrementale a ce stade du projet).

Colonnes exclues du chargement : `level_list`/`goal_list` (600k_fitness_*),
de type array<string> en Parquet. Le connecteur JDBC Spark->Postgres ne les
porte pas de maniere fiable dans toutes les configurations, et ces colonnes
ne sont pas necessaires au modele Gold demande (dim_exercise/dim_muscle ne
se basent que sur exercise_name). Exclues explicitement plutot que
serialisees silencieusement — voir data/gold/GOLD_MODEL_DECISIONS.md.
"""

import psycopg2
from pyspark.sql import SparkSession

SILVER_ROOT = "/opt/data/silver"
RAW_SCHEMA = "raw"

# --- Tables Silver a charger : (nom silver, colonnes a exclure du chargement) ---
TABLES = {
    "600k_fitness_summary": ["level_list", "goal_list"],
    "600k_fitness_detailed": ["level_list", "goal_list"],
    "gym_members": [],
    "weight_training": [],
    "usda_nutrition": [],
}

JDBC_URL = "jdbc:postgresql://app-postgres:5432/safelift_dwh"
JDBC_PROPERTIES = {
    "user": "safelift_app",
    "password": "change_me_app",
    "driver": "org.postgresql.Driver",
}


def ensure_raw_schema_exists() -> None:
    """Cree le schema "raw" via psycopg2 (Python pur, pas de JVM).

    Une connexion JDBC brute via spark._jvm.java.sql.DriverManager a ete
    tentee en premier lieu, mais le jar du driver Postgres ajoute par
    --packages vit dans un classloader Spark isole (MutableURLClassLoader),
    invisible du DriverManager JDBC "nu" utilise via py4j (erreurs
    "No suitable driver" puis ClassNotFoundException meme apres
    Class.forName). psycopg2 evite completement ce probleme puisqu'il ne
    passe jamais par la JVM.
    """
    conn = psycopg2.connect(
        host="app-postgres",
        port=5432,
        dbname="safelift_dwh",
        user=JDBC_PROPERTIES["user"],
        password=JDBC_PROPERTIES["password"],
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
    conn.close()


def main() -> None:
    spark = SparkSession.builder.appName("load_silver_to_postgres").getOrCreate()

    ensure_raw_schema_exists()

    for table_name, excluded_columns in TABLES.items():
        df = spark.read.parquet(f"{SILVER_ROOT}/{table_name}")
        if excluded_columns:
            df = df.drop(*excluded_columns)

        row_count = df.count()
        target_table = f"{RAW_SCHEMA}.silver_{table_name}"

        # truncate=true : TRUNCATE + INSERT plutot que DROP TABLE + CREATE.
        # Necessaire car les modeles staging dbt sont materialises en VUES
        # (dbt_project.yml) qui referencent directement raw.silver_* : un
        # DROP TABLE echoue des la 2e execution avec "cannot drop table ...
        # because other objects depend on it". TRUNCATE preserve l'objet
        # table (donc les vues qui en dependent) tant que le schema de
        # colonnes ne change pas d'un run a l'autre -- ce qui est le cas ici
        # (schema Silver stable, fixe par les jobs spark/jobs/silver_*.py).
        (
            df.write.mode("overwrite")
            .option("truncate", "true")
            .jdbc(url=JDBC_URL, table=target_table, properties=JDBC_PROPERTIES)
        )

        print(f"[load_silver_to_postgres] {table_name} : {row_count} lignes -> {target_table}")

    spark.stop()


if __name__ == "__main__":
    main()
