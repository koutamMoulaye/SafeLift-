# SafeLift — etape 6/6, sous-etape 1/6 (audit uniquement).
# Backend LOCAL explicite (pas de backend S3 distant) : un compte lab
# pedagogique peut restreindre la creation de bucket de state distant, on ne
# le sait pas encore a ce stade de l'audit. Aucune ressource declaree ici
# volontairement -- ce fichier ne sert qu'a valider que `terraform init`
# fonctionne et que le provider AWS peut etre telecharge.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "local" {
    path = "terraform.tfstate"
  }
}

provider "aws" {
  region  = "us-east-1"
  profile = "awslearnerlab"
}
