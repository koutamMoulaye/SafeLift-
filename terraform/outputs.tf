# SafeLift — etape 6/6, sous-etape 2/6.

output "datalake_bucket_name" {
  description = "Nom du bucket S3 du data lake Gold."
  value       = aws_s3_bucket.datalake.id
}

output "athena_database_name" {
  description = "Nom de la base Glue/Athena exposant la couche Gold."
  value       = aws_glue_catalog_database.gold.name
}

output "athena_results_location" {
  description = "Emplacement S3 a passer en ResultConfiguration.OutputLocation lors d'une requete Athena (le workgroup \"primary\" du compte lab n'a pas d'emplacement par defaut, voir AWS_LAB_CONSTRAINTS.md)."
  value       = "s3://${aws_s3_bucket.datalake.id}/athena-results/"
}

output "lab_role_arn" {
  description = "ARN du role LabRole existant (reference en lecture seule, jamais cree/modifie ici)."
  value       = data.aws_iam_role.lab_role.arn
}
