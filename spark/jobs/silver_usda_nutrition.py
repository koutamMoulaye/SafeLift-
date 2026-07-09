"""SafeLift — Job Silver : usda_nutrition (Jalon 3, sous-etape 1/6).

Nettoyage applique :
- Deduplication sur fdc_id : l'ingestion Bronze deduplique deja par mot-cle
  de recherche (un meme aliment peut remonter pour plusieurs mots-cles
  proches, ex. "chicken breast" et un autre mot-cle related), mais cette
  dedup est refaite ici cote Silver par securite/idempotence si la logique
  d'ingestion venait a changer.
- food_name : trim (pas de renommage de colonnes necessaire, deja en
  snake_case depuis l'ingestion Python).
- Aucune conversion d'unite : l'API USDA fournit deja les valeurs par 100g
  pour les dataType Foundation/SR Legacy interroges (voir
  airflow/dags/nutrition_ingestion.py et
  data/gold/GOLD_MODEL_DECISIONS.md section 13).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver_common import add_silver_lineage, read_latest_bronze, write_silver

DATASET_NAME = "usda_nutrition"


def main() -> None:
    spark = SparkSession.builder.appName("silver_usda_nutrition").getOrCreate()

    df = read_latest_bronze(spark, DATASET_NAME)
    row_count_before = df.count()

    df = df.dropDuplicates(["fdc_id"])
    df = df.withColumn("food_name", F.trim(F.col("food_name")))

    df = add_silver_lineage(df)

    row_count_after = df.count()
    print(f"[silver_usda_nutrition] lignes avant={row_count_before} apres={row_count_after}")

    write_silver(df, DATASET_NAME)
    spark.stop()


if __name__ == "__main__":
    main()
