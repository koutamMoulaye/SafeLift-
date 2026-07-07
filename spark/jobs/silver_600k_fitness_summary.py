"""SafeLift — Job Silver : 600k_fitness_summary.

Nettoyage applique (voir data/silver/CLEANING_LOG.md pour le detail chiffre) :
- level/goal : chaines representant des listes Python (ex. "['A', 'B']")
  parsees en vraies colonnes array<string> (level_list/goal_list) via
  ast.literal_eval (silver_common.parse_stringified_list_column). Choix
  motive par la volonte de garder un grain "1 ligne = 1 programme" (un
  explode multiplierait les lignes, notamment en produit croise level x
  goal), et par l'expressivite d'un type array natif Parquet/Spark par
  rapport a des colonnes one-hot qui figeraient a l'avance un vocabulaire de
  valeurs.
- program_length -> program_length_weeks : hypothese "semaines" confirmee
  empiriquement (correspond a max(week) du grain detaille pour 99.5% des
  titres communs — voir CLEANING_LOG.md pour le detail du controle).
- created/last_edit -> created_at/last_edited_at : cast en timestamp reel.
- Aucun doublon detecte en Bronze : pas de deduplication necessaire.
- Valeurs nulles (description, equipment, program_length, created, last_edit :
  toutes <0.2% en Bronze) laissees telles quelles, pas d'imputation sans
  justification metier.
- Pas de jointure : cette table reste independante.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver_common import add_silver_lineage, parse_stringified_list_column, read_latest_bronze, write_silver

DATASET_NAME = "600k_fitness_summary"


def main() -> None:
    spark = SparkSession.builder.appName("silver_600k_fitness_summary").getOrCreate()

    df = read_latest_bronze(spark, DATASET_NAME)
    row_count_before = df.count()

    df = parse_stringified_list_column(df, "level", "level_list")
    df = parse_stringified_list_column(df, "goal", "goal_list")

    df = df.withColumnRenamed("program_length", "program_length_weeks")

    df = df.withColumn("created_at", F.to_timestamp("created", "yyyy-MM-dd HH:mm:ss")).drop("created")
    df = df.withColumn("last_edited_at", F.to_timestamp("last_edit", "yyyy-MM-dd HH:mm:ss")).drop("last_edit")

    df = add_silver_lineage(df)

    row_count_after = df.count()
    print(f"[silver_600k_fitness_summary] lignes avant={row_count_before} apres={row_count_after}")

    write_silver(df, DATASET_NAME)
    spark.stop()


if __name__ == "__main__":
    main()
