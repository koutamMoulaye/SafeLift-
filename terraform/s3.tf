# SafeLift — etape 6/6, sous-etape 2/6.
# Bucket S3 pour le data lake Gold. A ce stade, seule la couche Gold est
# concernee (pas Bronze/Silver) : cf. tache explicitement bornee a S3+Athena
# sur les 7 tables du modele en etoile dbt.

resource "aws_s3_bucket" "datalake" {
  bucket = "safelift-datalake-${var.aws_account_id}"
}

# Versioning active : protege contre un ecrasement accidentel d'un export
# Gold (le script d'upload peut etre relance plusieurs fois).
resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Chiffrement par defaut SSE-S3 (AES256) : suffisant sur un compte lab,
# pas besoin d'une cle KMS custom (couts/complexite additionnels non
# justifies a ce stade).
resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Blocage total de l'acces public : ce bucket ne sert qu'a Athena/Glue en
# interne au compte, aucun acces public n'est jamais necessaire.
resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Prefixes "dossier" pre-crees pour chaque table Gold (objets cle vide se
# terminant par "/", convention S3 standard pour materialiser une arborescence
# visible avant meme le premier upload reel des Parquet).
resource "aws_s3_object" "gold_table_prefixes" {
  for_each = toset(local.gold_tables)

  bucket  = aws_s3_bucket.datalake.id
  key     = "gold/${each.value}/"
  content = ""
}

# Prefixe dedie aux resultats de requetes Athena (le workgroup "primary" du
# compte lab n'a pas de emplacement de sortie par defaut configure, voir
# AWS_LAB_CONSTRAINTS.md) : reutilise le meme bucket plutot que d'en creer un
# second, pour rester minimal sur ce compte lab.
resource "aws_s3_object" "athena_results_prefix" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "athena-results/"
  content = ""
}
