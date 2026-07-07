# SafeLift — etape 6/6, sous-etape 2/6 (S3 + Athena).
# Variables partagees par s3.tf / athena.tf / outputs.tf.

variable "aws_region" {
  description = "Region AWS a utiliser explicitement (aucune region par defaut sur le compte lab, voir AWS_LAB_CONSTRAINTS.md)."
  type        = string
  default     = "us-east-1"
}

variable "aws_account_id" {
  description = "Account ID du compte AWS Academy Learner Lab (fige, suffixe le nom du bucket pour garantir l'unicite globale)."
  type        = string
  default     = "097115946702"
}

variable "lab_role_name" {
  description = "Nom du role IAM deja existant dans le compte lab (jamais cree/modifie ici, seulement reference via data source)."
  type        = string
  default     = "LabRole"
}

variable "athena_database_name" {
  description = "Nom de la base Athena/Glue exposant la couche Gold."
  type        = string
  default     = "gold"
}

# Une table par mart Gold (voir dbt/models/marts/_marts__models.yml). Le
# schema colonnes de chaque table est declare explicitement dans athena.tf,
# recupere par introspection reelle de app-postgres (information_schema.columns
# sur le schema gold), pas suppose depuis la doc dbt.
locals {
  gold_tables = [
    "fact_workout_session",
    "fact_risk_score",
    "dim_exercise",
    "dim_muscle",
    "dim_user",
    "dim_date",
    "fact_risk_score_demo_synthetic",
  ]
}
