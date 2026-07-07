# SafeLift — etape 6/6, sous-etape 2/6.
#
# Base Athena + tables externes sur la couche Gold (S3 gold/<table>/).
#
# Choix : aws_glue_catalog_database + aws_glue_catalog_table plutot que
# aws_athena_database (execute une requete CREATE DATABASE et exige un bucket
# de resultats des la creation de la base) ou aws_athena_named_query (ne cree
# pas de table persistante, juste une requete sauvegardee). Depuis la
# depreciation du catalogue de donnees interne "Athena-managed", Athena
# utilise de toute facon AWS Glue Data Catalog comme metastore par defaut :
# declarer directement les ressources Glue est donc le chemin le plus
# explicite et le plus robuste, sans dependre de l'execution d'une requete ni
# d'un bucket de resultats pour la simple creation de metadonnees.
#
# Schema des colonnes : recupere par introspection reelle de app-postgres
# (`information_schema.columns` sur le schema `gold`, 2026-07-06), PAS
# suppose depuis dbt/models/marts/_marts__models.yml (qui ne documente que
# les colonnes couvertes par des tests dbt, pas le schema complet).
#
# Gouvernance RGPD (etape 6/6, sous-etape 4/6, voir scripts/pseudonymize.py) :
# sur fact_workout_session, fact_risk_score et dim_user, la colonne `user_id`
# (bigint, identifiant reel) N'EST PAS exportee vers S3 -- elle est remplacee
# par `user_pseudo_id` (string, HMAC-SHA256) par scripts/upload_gold_to_s3.py
# AVANT ecriture Parquet. Les definitions de colonnes ci-dessous refletent le
# schema EXPORTE (donc `user_pseudo_id`), pas le schema Postgres source.

# LabRole reference en lecture seule : aucune ressource ci-dessous n'exige
# aujourd'hui de role d'execution (Glue Data Catalog = metadonnees pures, pas
# de crawler/job Glue cree ici), mais l'ARN est expose en sortie
# (voir outputs.tf) pour un usage futur (ex. crawler Glue ou DAG Airflow qui
# aurait besoin d'assumer LabRole). Ne jamais creer/modifier ce role ici.
data "aws_iam_role" "lab_role" {
  name = var.lab_role_name
}

resource "aws_glue_catalog_database" "gold" {
  name = var.athena_database_name

  description = "SafeLift - couche Gold (modele en etoile dbt) exposee a Athena via S3 Parquet."
}

locals {
  parquet_storage = {
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    serde_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
  }
}

resource "aws_glue_catalog_table" "fact_workout_session" {
  name          = "fact_workout_session"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.datalake.id}/gold/fact_workout_session/"
    input_format  = local.parquet_storage.input_format
    output_format = local.parquet_storage.output_format

    ser_de_info {
      serialization_library = local.parquet_storage.serde_library
    }

    columns {
      name = "workout_session_id"
      type = "bigint"
    }
    columns {
      name = "exercise_id"
      type = "bigint"
    }
    columns {
      name = "muscle_id"
      type = "bigint"
    }
    columns {
      name = "user_pseudo_id"
      type = "string"
    }
    columns {
      name = "date_id"
      type = "date"
    }
    columns {
      name = "session_date"
      type = "date"
    }
    columns {
      name = "workout_name"
      type = "string"
    }
    columns {
      name = "sets"
      type = "bigint"
    }
    columns {
      name = "reps"
      type = "double"
    }
    columns {
      name = "total_reps"
      type = "double"
    }
    columns {
      name = "lifted_weight_kg"
      type = "double"
    }
    columns {
      name = "duration_seconds"
      type = "double"
    }
  }
}

resource "aws_glue_catalog_table" "fact_risk_score" {
  name          = "fact_risk_score"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.datalake.id}/gold/fact_risk_score/"
    input_format  = local.parquet_storage.input_format
    output_format = local.parquet_storage.output_format

    ser_de_info {
      serialization_library = local.parquet_storage.serde_library
    }

    columns {
      name = "workout_session_id"
      type = "bigint"
    }
    columns {
      name = "exercise_id"
      type = "bigint"
    }
    columns {
      name = "muscle_id"
      type = "bigint"
    }
    columns {
      name = "user_pseudo_id"
      type = "string"
    }
    columns {
      name = "date_id"
      type = "date"
    }
    columns {
      name = "session_date"
      type = "date"
    }
    columns {
      name = "workout_name"
      type = "string"
    }
    columns {
      name = "sets"
      type = "bigint"
    }
    columns {
      name = "reps"
      type = "double"
    }
    columns {
      name = "total_reps"
      type = "double"
    }
    columns {
      name = "lifted_weight_kg"
      type = "double"
    }
    columns {
      name = "duration_seconds"
      type = "double"
    }
    columns {
      name = "base_zone"
      type = "double"
    }
    columns {
      name = "charge_factor"
      type = "double"
    }
    columns {
      name = "volume_factor"
      type = "double"
    }
    columns {
      name = "recup_factor"
      type = "double"
    }
    columns {
      name = "duree_factor"
      type = "double"
    }
    columns {
      name = "raw_risk_score"
      type = "double"
    }
    columns {
      name = "risk_score"
      type = "double"
    }
    columns {
      name = "risk_level"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "dim_exercise" {
  name          = "dim_exercise"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.datalake.id}/gold/dim_exercise/"
    input_format  = local.parquet_storage.input_format
    output_format = local.parquet_storage.output_format

    ser_de_info {
      serialization_library = local.parquet_storage.serde_library
    }

    columns {
      name = "exercise_id"
      type = "bigint"
    }
    columns {
      name = "exercise_name"
      type = "string"
    }
    columns {
      name = "normalized_exercise_name"
      type = "string"
    }
    columns {
      name = "muscle_group"
      type = "string"
    }
    columns {
      name = "equipment"
      type = "string"
    }
    columns {
      name = "is_matched"
      type = "boolean"
    }
    columns {
      name = "match_stage"
      type = "string"
    }
    columns {
      name = "source"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "dim_muscle" {
  name          = "dim_muscle"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.datalake.id}/gold/dim_muscle/"
    input_format  = local.parquet_storage.input_format
    output_format = local.parquet_storage.output_format

    ser_de_info {
      serialization_library = local.parquet_storage.serde_library
    }

    columns {
      name = "muscle_id"
      type = "bigint"
    }
    columns {
      name = "muscle_group"
      type = "string"
    }
    columns {
      name = "base_epidemiological_risk"
      type = "double"
    }
  }
}

resource "aws_glue_catalog_table" "dim_user" {
  name          = "dim_user"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.datalake.id}/gold/dim_user/"
    input_format  = local.parquet_storage.input_format
    output_format = local.parquet_storage.output_format

    ser_de_info {
      serialization_library = local.parquet_storage.serde_library
    }

    columns {
      name = "user_pseudo_id"
      type = "string"
    }
    columns {
      name = "age"
      type = "bigint"
    }
    columns {
      name = "gender"
      type = "string"
    }
    columns {
      name = "body_weight_kg"
      type = "double"
    }
    columns {
      name = "height_m"
      type = "double"
    }
    columns {
      name = "max_bpm"
      type = "bigint"
    }
    columns {
      name = "avg_bpm"
      type = "bigint"
    }
    columns {
      name = "resting_bpm"
      type = "bigint"
    }
    columns {
      name = "session_duration_hours"
      type = "double"
    }
    columns {
      name = "calories_burned"
      type = "double"
    }
    columns {
      name = "workout_type"
      type = "string"
    }
    columns {
      name = "fat_percentage"
      type = "double"
    }
    columns {
      name = "water_intake_liters"
      type = "double"
    }
    columns {
      name = "workout_frequency_days_per_week"
      type = "bigint"
    }
    columns {
      name = "experience_level"
      type = "bigint"
    }
    columns {
      name = "bmi"
      type = "double"
    }
    columns {
      name = "is_weight_training_demo_user"
      type = "boolean"
    }
  }
}

resource "aws_glue_catalog_table" "dim_date" {
  name          = "dim_date"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.datalake.id}/gold/dim_date/"
    input_format  = local.parquet_storage.input_format
    output_format = local.parquet_storage.output_format

    ser_de_info {
      serialization_library = local.parquet_storage.serde_library
    }

    columns {
      name = "date_id"
      type = "date"
    }
    columns {
      name = "date_day"
      type = "date"
    }
    columns {
      name = "day_of_month"
      type = "int"
    }
    columns {
      name = "day_of_week"
      type = "int"
    }
    columns {
      name = "week_of_year"
      type = "int"
    }
    columns {
      name = "month"
      type = "int"
    }
    columns {
      name = "year"
      type = "int"
    }
    columns {
      name = "week_start_date"
      type = "date"
    }
  }
}

resource "aws_glue_catalog_table" "fact_risk_score_demo_synthetic" {
  name          = "fact_risk_score_demo_synthetic"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.datalake.id}/gold/fact_risk_score_demo_synthetic/"
    input_format  = local.parquet_storage.input_format
    output_format = local.parquet_storage.output_format

    ser_de_info {
      serialization_library = local.parquet_storage.serde_library
    }

    columns {
      name = "scenario_id"
      type = "int"
    }
    columns {
      name = "scenario_label"
      type = "string"
    }
    columns {
      name = "muscle_group"
      type = "string"
    }
    columns {
      name = "base_zone"
      type = "double"
    }
    columns {
      name = "charge_factor"
      type = "double"
    }
    columns {
      name = "volume_factor"
      type = "double"
    }
    columns {
      name = "recup_factor"
      type = "double"
    }
    columns {
      name = "duree_factor"
      type = "double"
    }
    columns {
      name = "raw_risk_score"
      type = "double"
    }
    columns {
      name = "risk_score"
      type = "double"
    }
    columns {
      name = "risk_level"
      type = "string"
    }
    columns {
      name = "notes"
      type = "string"
    }
    columns {
      name = "is_synthetic_demo"
      type = "boolean"
    }
  }
}
