"""SafeLift — Job Silver : weight_training.

Nettoyage applique (voir data/silver/CLEANING_LOG.md pour le detail chiffre) :
- Suppression des lignes strictement dupliquees (904 constatees en Bronze sur
  605033 -> comptage exact reaffiche dans les logs de cette execution).
- Suppression des colonnes "Notes" et "Workout Notes" : taux de remplissage
  respectifs de 0.1% et 0.0% en Bronze, sous le seuil de 5% retenu pour
  conserver une colonne (voir CLEANING_LOG.md).
- Conversion de "Weight" (livres, hypothese confirmee empiriquement : valeurs
  dominantes 135/185/225/275/235/230, caracteristiques du chargement de
  disques en livres aux USA — 225 lbs = "deux plaques de 45 lbs", un repere
  tres reconnaissable en musculation) -> lifted_weight_kg (kg). Nom distinct
  de "body_weight_kg" (gym_members) qui mesure une autre grandeur physique
  (poids de la personne, pas poids souleve) : meme convention d'unite (kg)
  mais noms differents pour ne pas confondre deux concepts differents.
- Renommage snake_case des autres colonnes ; "Date" -> performed_at (timestamp
  reel) ; "Seconds" -> duration_seconds (unite deja explicite dans le nom
  source).
- Pas de jointure : cette table reste independante.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver_common import add_silver_lineage, read_latest_bronze, write_silver

DATASET_NAME = "weight_training"
LBS_TO_KG = 0.45359237

COLUMN_RENAMES = {
    "Workout Name": "workout_name",
    "Exercise Name": "exercise_name",
    "Set Order": "set_order",
    "Reps": "reps",
    "Distance": "distance",
}


def main() -> None:
    spark = SparkSession.builder.appName("silver_weight_training").getOrCreate()

    df = read_latest_bronze(spark, DATASET_NAME)
    row_count_before = df.count()

    # Deduplication sur les colonnes metier uniquement (les colonnes de
    # metadonnees d'ingestion Bronze sont de toute facon identiques pour un
    # meme run et n'influencent donc pas la detection de doublons).
    business_columns = [c for c in df.columns if c not in ("ingestion_timestamp", "source_file", "source_dataset")]
    duplicate_count = row_count_before - df.dropDuplicates(business_columns).count()
    df = df.dropDuplicates(business_columns)
    row_count_after_dedup = df.count()

    # Colonnes quasi-vides : sous le seuil de 5% de remplissage retenu
    df = df.drop("Notes", "Workout Notes")

    for source_col, target_col in COLUMN_RENAMES.items():
        df = df.withColumnRenamed(source_col, target_col)

    # Conversion d'unite : Weight (livres, hypothese confirmee empiriquement) -> lifted_weight_kg
    df = df.withColumn("lifted_weight_kg", F.round(F.col("Weight") * LBS_TO_KG, 2)).drop("Weight")

    df = df.withColumnRenamed("Seconds", "duration_seconds")

    # Date -> timestamp reel (etait une chaine en Bronze)
    df = df.withColumn("performed_at", F.to_timestamp("Date", "yyyy-MM-dd HH:mm:ss")).drop("Date")

    df = add_silver_lineage(df)

    row_count_after = df.count()
    print(
        f"[silver_weight_training] lignes avant={row_count_before} "
        f"doublons_supprimes={duplicate_count} apres_dedup={row_count_after_dedup} "
        f"final={row_count_after}"
    )

    write_silver(df, DATASET_NAME)
    spark.stop()


if __name__ == "__main__":
    main()
