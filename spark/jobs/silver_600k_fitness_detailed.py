"""SafeLift — Job Silver : 600k_fitness_detailed.

Nettoyage applique (voir data/silver/CLEANING_LOG.md pour le detail chiffre) :
- Suppression des lignes strictement dupliquees (904 constatees en Bronze sur
  605033 -> comptage exact reaffiche dans les logs de cette execution).
- level/goal parses en array<string> (level_list/goal_list), meme logique que
  silver_600k_fitness_summary.py (voir ce fichier pour la justification).
- program_length -> program_length_weeks (meme justification que
  600k_fitness_summary : cf. CLEANING_LOG.md).
- reps : ~4.3% de valeurs negatives constatees en Bronze (25967/605033),
  semantique incertaine (aucune metadonnee source ne documente une convention
  de signe). Decision retenue : NULLIFIER ces valeurs plutot que les rendre
  positives par valeur absolue, et ajouter un flag booleen reps_anomaly_flag
  sur les lignes concernees. Raisonnement : la valeur absolue presenterait
  une donnee fabriquee (ex. -180 -> 180 repetitions, physiologiquement peu
  plausible pour les exercices de mobilite ou l'anomalie est concentree)
  comme si elle etait fiable, sans confirmation possible. Nullifier + flaguer
  est plus honnete pour un usage en aval (Gold / calcul de risk_score) : la
  donnee absente reste visible et exclue des agregations par defaut, au lieu
  d'etre silencieusement biaisee par une hypothese non verifiee.
- week/day/number_of_exercises/sets : cast en entier (stockees en float en
  Bronze, sans valeur manquante sur cette table).
- created/last_edit -> created_at/last_edited_at : cast en timestamp reel.
- Pas de jointure : cette table reste independante.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver_common import add_silver_lineage, parse_stringified_list_column, read_latest_bronze, write_silver

DATASET_NAME = "600k_fitness_detailed"


def main() -> None:
    spark = SparkSession.builder.appName("silver_600k_fitness_detailed").getOrCreate()

    df = read_latest_bronze(spark, DATASET_NAME)
    row_count_before = df.count()

    business_columns = [c for c in df.columns if c not in ("ingestion_timestamp", "source_file", "source_dataset")]
    duplicate_count = row_count_before - df.dropDuplicates(business_columns).count()
    df = df.dropDuplicates(business_columns)
    row_count_after_dedup = df.count()

    df = parse_stringified_list_column(df, "level", "level_list")
    df = parse_stringified_list_column(df, "goal", "goal_list")

    df = df.withColumnRenamed("program_length", "program_length_weeks")

    for col_name in ("week", "day", "number_of_exercises", "sets"):
        df = df.withColumn(col_name, F.col(col_name).cast("int"))

    reps_anomaly_count = df.filter(F.col("reps") < 0).count()
    df = df.withColumn("reps_anomaly_flag", F.col("reps") < 0)
    df = df.withColumn("reps", F.when(F.col("reps") < 0, None).otherwise(F.col("reps")).cast("int"))

    df = df.withColumn("created_at", F.to_timestamp("created", "yyyy-MM-dd HH:mm:ss")).drop("created")
    df = df.withColumn("last_edited_at", F.to_timestamp("last_edit", "yyyy-MM-dd HH:mm:ss")).drop("last_edit")

    df = add_silver_lineage(df)

    row_count_after = df.count()
    print(
        f"[silver_600k_fitness_detailed] lignes avant={row_count_before} "
        f"doublons_supprimes={duplicate_count} apres_dedup={row_count_after_dedup} "
        f"final={row_count_after} reps_negatifs_nullifies={reps_anomaly_count}"
    )

    write_silver(df, DATASET_NAME)
    spark.stop()


if __name__ == "__main__":
    main()
