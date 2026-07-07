"""SafeLift — Job Silver : gym_members.

Nettoyage applique (voir data/silver/CLEANING_LOG.md pour le detail chiffre) :
- Renommage des colonnes en snake_case, avec suffixe d'unite explicite quand
  l'unite figurait deja dans le nom de colonne source (ex: "Weight (kg)",
  deja en kg -> body_weight_kg — nom distinct de "lifted_weight_kg" dans
  weight_training, qui mesure une autre grandeur physique : le poids de la
  personne, pas le poids souleve).
- Aucun doublon, aucune valeur nulle constatee en Bronze (SCHEMA_NOTES.md) :
  pas de deduplication ni d'imputation necessaire pour cette table.
- Pas de jointure : cette table reste independante.
"""

from pyspark.sql import SparkSession

from silver_common import add_silver_lineage, read_latest_bronze, write_silver

DATASET_NAME = "gym_members"

COLUMN_RENAMES = {
    "Age": "age",
    "Gender": "gender",
    "Weight (kg)": "body_weight_kg",
    "Height (m)": "height_m",
    "Max_BPM": "max_bpm",
    "Avg_BPM": "avg_bpm",
    "Resting_BPM": "resting_bpm",
    "Session_Duration (hours)": "session_duration_hours",
    "Calories_Burned": "calories_burned",
    "Workout_Type": "workout_type",
    "Fat_Percentage": "fat_percentage",
    "Water_Intake (liters)": "water_intake_liters",
    "Workout_Frequency (days/week)": "workout_frequency_days_per_week",
    "Experience_Level": "experience_level",
    "BMI": "bmi",
}


def main() -> None:
    spark = SparkSession.builder.appName("silver_gym_members").getOrCreate()

    df = read_latest_bronze(spark, DATASET_NAME)
    row_count_before = df.count()

    for source_col, target_col in COLUMN_RENAMES.items():
        df = df.withColumnRenamed(source_col, target_col)

    df = add_silver_lineage(df)

    row_count_after = df.count()
    print(f"[silver_gym_members] lignes avant={row_count_before} apres={row_count_after}")

    write_silver(df, DATASET_NAME)
    spark.stop()


if __name__ == "__main__":
    main()
