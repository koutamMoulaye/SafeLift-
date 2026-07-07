"""SafeLift — Fonctions utilitaires partagees par les jobs Silver.

Convention de chemins (identiques sur les conteneurs airflow-*, spark-master et
spark-worker, via le bind mount ./data:/opt/data commun aux trois, cf.
docker-compose.yml) :
  - Bronze : /opt/data/bronze/{dataset}/ingestion_date=YYYY-MM-DD/*.parquet
  - Silver : /opt/data/silver/{dataset}/*.parquet (pas de partition par date :
    Silver est une vue nettoyee cumulative, pas un journal d'ingestion).
"""

import ast
import glob
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

DATA_ROOT = "/opt/data"
BRONZE_ROOT = f"{DATA_ROOT}/bronze"
SILVER_ROOT = f"{DATA_ROOT}/silver"


def latest_bronze_partition_path(dataset_name: str) -> str:
    """Renvoie le chemin de la partition ingestion_date= la plus recente d'une table Bronze."""
    pattern = f"{BRONZE_ROOT}/{dataset_name}/ingestion_date=*"
    partitions = sorted(glob.glob(pattern))
    if not partitions:
        raise FileNotFoundError(f"Aucune partition Bronze trouvee pour {dataset_name} ({pattern})")
    return partitions[-1]


def read_latest_bronze(spark: SparkSession, dataset_name: str) -> DataFrame:
    """Lit uniquement la derniere partition Bronze.

    Bronze est un reload complet du CSV source a chaque run (pas incremental) :
    lire toutes les partitions historiques dupliquerait les lignes d'une
    ingestion a l'autre. Silver ne reflete donc que le dernier etat Bronze
    connu.
    """
    path = latest_bronze_partition_path(dataset_name)
    return spark.read.parquet(path)


@udf(returnType=ArrayType(StringType()))
def _parse_python_list_string(raw_value):
    """UDF : parse une chaine de liste Python (ex. "['A', 'B']") en vraie liste.

    Utilise ast.literal_eval plutot qu'un remplacement de guillemets par regex,
    car c'est genuinement de la syntaxe Python valide (plus robuste face a des
    valeurs contenant des caracteres speciaux). Renvoie None si la valeur est
    nulle ou non parsable — pas d'hypothese silencieuse en cas d'echec.
    """
    if raw_value is None:
        return None
    try:
        parsed = ast.literal_eval(raw_value)
    except (ValueError, SyntaxError):
        return None
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return [str(parsed)]


def parse_stringified_list_column(df: DataFrame, source_col: str, target_col: str) -> DataFrame:
    """Remplace une colonne 'liste Python stringifiee' par une vraie colonne array<string>."""
    return df.withColumn(target_col, _parse_python_list_string(df[source_col])).drop(source_col)


def add_silver_lineage(df: DataFrame) -> DataFrame:
    """Ajoute un horodatage de traitement Silver, en plus des metadonnees Bronze deja presentes."""
    return df.withColumn("silver_processed_at", F.lit(datetime.now(timezone.utc).isoformat()))


def write_silver(df: DataFrame, dataset_name: str) -> None:
    """Ecrit une table Silver : vue nettoyee cumulative, pas de partitionnement par date, ecrasee a chaque run."""
    output_path = f"{SILVER_ROOT}/{dataset_name}"
    df.write.mode("overwrite").parquet(output_path)
