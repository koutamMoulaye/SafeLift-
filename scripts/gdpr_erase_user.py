"""SafeLift — Etape 6/6, sous-etape 4/6 : droit a l'effacement (RGPD Art. 17).

Usage :
    python gdpr_erase_user.py <user_id>                 # dry-run (par defaut)
    python gdpr_erase_user.py <user_id> --confirm        # execution reelle
    python gdpr_erase_user.py <user_id> --confirm --skip-s3

Par defaut, le script tourne en DRY-RUN : il affiche ce qui SERAIT supprime
sans rien modifier. Il faut passer --confirm explicitement pour executer les
suppressions reelles -- coherent avec la culture du projet ("pas de dry-run
qui se fait passer pour un test reel", cf. CLAUDE.md) mais dans l'autre sens
ici : les operations destructives ne doivent JAMAIS etre le comportement par
defaut d'un script d'effacement.

--------------------------------------------------------------------------
COUCHES COUVERTES ET LIMITE ARCHITECTURALE MAJEURE (a lire avant usage reel)
--------------------------------------------------------------------------

`dim_user.user_id` (dbt, `stg_gym_members.sql`) est une cle de substitution
calculee par `row_number() over (order by age, gender, body_weight_kg,
height_m, experience_level)` -- PAS un identifiant stable/naturel. Consequence
directe : si on supprime UNIQUEMENT la ligne dans `gold.dim_user` (Postgres)
sans toucher les couches en amont, le PROCHAIN `dbt run` complet (declenche
automatiquement par `silver_transformation`, lui-meme declenche par
`bronze_ingestion` -- voir CLAUDE.md) recalculerait `gold.dim_user` depuis
`raw.silver_gym_members` et ferait REAPPARAITRE la personne "effacee" (avec
potentiellement un `user_id` different, puisque le row_number() de tous les
utilisateurs suivants se decale des qu'une ligne source disparait).

Consequence : une suppression durable, qui survit a un futur run complet du
pipeline, DOIT remonter jusqu'a la source reellement re-lue a chaque run :
- Bronze : `bronze_ingestion` recharge INTEGRALEMENT le CSV source a chaque
  execution (pas incremental, cf. CLAUDE.md "Bronze = reload complet du CSV
  source a chaque run") -- donc la ligne doit aussi disparaitre du fichier
  CSV original (`data/bronze/raw/gym_members/*.csv`), sans quoi un futur
  `bronze_ingestion` la reintroduirait.
- Toutes les partitions Bronze deja materialisees sur disque
  (`data/bronze/gym_members/ingestion_date=*/`) contiennent DEJA une copie de
  cette ligne (une par run passe) : elles sont donc aussi nettoyees.
- Silver (`data/silver/gym_members/*.parquet`) est entierement recalculee
  depuis la DERNIERE partition Bronze a chaque run (`silver_common.
  latest_bronze_partition_path`) : nettoyee egalement, pour un effet immediat
  cote Gold Postgres sans attendre un rebuild complet.

Ce script agit donc sur 4 couches physiques pour CETTE table (gym_members) :
raw CSV source, toutes les partitions Bronze existantes, Silver, et Gold
Postgres -- plus un rafraichissement optionnel de l'export S3 (voir --skip-s3).
Le matching entre `user_id` (Gold) et la ligne correspondante dans
Bronze/Silver/CSV se fait par egalite EXACTE du tuple (age, gender,
body_weight_kg, height_m, experience_level) -- tuple confirme UNIQUE sur les
973 lignes de `raw.silver_gym_members` (verifie explicitement avant d'ecrire
ce script, voir commentaire dans main()). Ce tuple n'a subi AUCUNE conversion
d'unite entre Bronze et Silver pour `gym_members` (renommage de colonnes pur,
cf. `spark/jobs/silver_gym_members.py`) : le matching par egalite exacte de
flottants est donc fiable ici (pas d'arrondi/conversion intermediaire).

`fact_workout_session`/`fact_risk_score` n'ont PAS cette limite : ce sont des
tables Gold calculees depuis `weight_training` (aucun `user_id` natif, tout
rattache au "demo user" par hypothese documentee) -- leur suppression cote
Postgres (par `user_id`) est immediate et definitive au niveau Gold. Le
Silver `weight_training` n'est PAS modifie par ce script : il n'est
lui-meme jamais associe qu'a un seul `user_id` par hypothese de
demonstration (cf. CLAUDE.md), supprimer une ligne de `weight_training` pour
"un" utilisateur reviendrait a effacer une seance d'entrainement du DEMO USER
tout entier, ce qui n'est pas le perimetre d'une demande d'effacement liee au
PROFIL `dim_user` d'un individu distinct.

--------------------------------------------------------------------------
CE QUE CE SCRIPT NE FAIT PAS (limites assumees et documentees)
--------------------------------------------------------------------------
- Ne touche jamais `600k_fitness_summary`/`600k_fitness_detailed` (catalogue
  d'exercices, aucune donnee personnelle, cf. DATA_CATALOG.md).
- Ne touche jamais les donnees `weight_training` elles-memes (voir ci-dessus).
- Le rafraichissement S3 (etape optionnelle) nécessite des credentials AWS
  valides (profil "awslearnerlab") ; en cas d'echec (ex. session Learner Lab
  expiree), le script le signale clairement mais NE FAIT PAS echouer les
  suppressions locales deja effectuees (Postgres/Bronze/Silver/CSV) --
  l'export S3 devra alors etre relance manuellement une fois les credentials
  rafraichis.
- Protection integree : refuse `--confirm` sur le profil marque
  `is_weight_training_demo_user=true` (le seul profil relie a de vraies
  donnees de séance) sauf si `--i-understand-this-breaks-the-demo` est
  egalement passe -- evite une effacement accidentel qui viderait le seul
  jeu de donnees exploitable du dashboard de demo.
"""

import argparse
import glob
import sys
from pathlib import Path

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pseudonymize import load_pseudonymization_key  # noqa: E402
from upload_gold_to_s3 import (  # noqa: E402
    AWS_PROFILE,
    AWS_REGION,
    BUCKET_NAME,
    GOLD_TABLES,
    export_table_to_parquet,
    get_db_config,
    upload_to_s3,
)

REPO_ROOT = Path(__file__).resolve().parent.parent

# Tuple de matching utilise pour retrouver la ligne d'un user_id Gold dans les
# couches Bronze/Silver/CSV, qui n'ont pas de user_id natif. Confirme UNIQUE
# sur les 973 lignes source (aucune paire de profils ne partage exactement les
# 5 valeurs) avant d'ecrire ce script -- sinon le matching serait ambigu.
SILVER_MATCH_COLUMNS = ["age", "gender", "body_weight_kg", "height_m", "experience_level"]
BRONZE_MATCH_COLUMNS = ["Age", "Gender", "Weight (kg)", "Height (m)", "Experience_Level"]

BRONZE_GYM_MEMBERS_GLOB = str(REPO_ROOT / "data" / "bronze" / "gym_members" / "ingestion_date=*" / "gym_members.parquet")
RAW_CSV_PATH = REPO_ROOT / "data" / "bronze" / "raw" / "gym_members" / "gym_members_exercise_tracking.csv"

TABLES_TO_RESYNC_ON_S3 = ["dim_user", "fact_workout_session", "fact_risk_score"]


def find_silver_parquet_path() -> Path:
    """Le nom de fichier Spark (part-00000-<uuid>...) change a chaque reecriture
    Silver -- ne pas coder en dur le nom, retrouver le seul fichier .parquet
    present dans le dossier au moment de l'execution."""
    candidates = list((REPO_ROOT / "data" / "silver" / "gym_members").glob("*.parquet"))
    if len(candidates) != 1:
        raise RuntimeError(
            f"Attendu exactement 1 fichier .parquet dans data/silver/gym_members/, trouve {len(candidates)}."
        )
    return candidates[0]


def fetch_dim_user_row(conn, user_id: int) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT age, gender, body_weight_kg, height_m, experience_level,
                   is_weight_training_demo_user
            FROM gold.dim_user
            WHERE user_id = %s
            """,
            (user_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = ["age", "gender", "body_weight_kg", "height_m", "experience_level", "is_weight_training_demo_user"]
        return dict(zip(cols, row))


def count_fact_rows(conn, user_id: int) -> dict[str, int]:
    counts = {}
    with conn.cursor() as cur:
        for table in ("fact_workout_session", "fact_risk_score"):
            cur.execute(f"SELECT count(*) FROM gold.{table} WHERE user_id = %s", (user_id,))
            counts[table] = cur.fetchone()[0]
    return counts


def match_silver_row_index(profile: dict) -> int | None:
    path = find_silver_parquet_path()
    df = pd.read_parquet(path, columns=SILVER_MATCH_COLUMNS)
    mask = (
        (df["age"] == profile["age"])
        & (df["gender"] == profile["gender"])
        & (df["body_weight_kg"] == profile["body_weight_kg"])
        & (df["height_m"] == profile["height_m"])
        & (df["experience_level"] == profile["experience_level"])
    )
    matches = df.index[mask].tolist()
    if len(matches) > 1:
        raise RuntimeError(f"Matching ambigu dans Silver : {len(matches)} lignes correspondent au profil (attendu 0 ou 1).")
    return matches[0] if matches else None


def match_bronze_partitions(profile: dict) -> dict[str, int]:
    """Retourne {chemin_partition: index_ligne} pour chaque partition Bronze
    contenant une ligne correspondant au profil."""
    matches = {}
    for partition_path in sorted(glob.glob(BRONZE_GYM_MEMBERS_GLOB)):
        df = pd.read_parquet(partition_path, columns=BRONZE_MATCH_COLUMNS)
        mask = (
            (df["Age"] == profile["age"])
            & (df["Gender"] == profile["gender"])
            & (df["Weight (kg)"] == profile["body_weight_kg"])
            & (df["Height (m)"] == profile["height_m"])
            & (df["Experience_Level"] == profile["experience_level"])
        )
        idx = df.index[mask].tolist()
        if len(idx) > 1:
            raise RuntimeError(f"Matching ambigu dans {partition_path} : {len(idx)} lignes.")
        if idx:
            matches[partition_path] = idx[0]
    return matches


def match_raw_csv_index(profile: dict) -> int | None:
    if not RAW_CSV_PATH.exists():
        return None
    df = pd.read_csv(RAW_CSV_PATH, usecols=BRONZE_MATCH_COLUMNS)
    mask = (
        (df["Age"] == profile["age"])
        & (df["Gender"] == profile["gender"])
        & (df["Weight (kg)"] == profile["body_weight_kg"])
        & (df["Height (m)"] == profile["height_m"])
        & (df["Experience_Level"] == profile["experience_level"])
    )
    matches = df.index[mask].tolist()
    if len(matches) > 1:
        raise RuntimeError(f"Matching ambigu dans le CSV source : {len(matches)} lignes.")
    return matches[0] if matches else None


def erase_gold_postgres(conn, user_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM gold.fact_risk_score WHERE user_id = %s", (user_id,))
        cur.execute("DELETE FROM gold.fact_workout_session WHERE user_id = %s", (user_id,))
        cur.execute("DELETE FROM gold.dim_user WHERE user_id = %s", (user_id,))
    conn.commit()


def erase_silver_row(row_index: int) -> None:
    path = find_silver_parquet_path()
    df = pd.read_parquet(path)
    df = df.drop(index=row_index).reset_index(drop=True)
    df.to_parquet(path, index=False)


def erase_bronze_rows(partition_matches: dict[str, int]) -> None:
    for partition_path, row_index in partition_matches.items():
        df = pd.read_parquet(partition_path)
        df = df.drop(index=row_index).reset_index(drop=True)
        df.to_parquet(partition_path, index=False)


def erase_raw_csv_row(row_index: int) -> None:
    df = pd.read_csv(RAW_CSV_PATH)
    df = df.drop(index=row_index).reset_index(drop=True)
    df.to_csv(RAW_CSV_PATH, index=False)


def resync_s3(conn) -> None:
    """Reexporte les 3 tables affectees et purge les versions S3 anterieures
    (le bucket a le versioning active -- un simple re-upload ne suffit pas a
    faire disparaitre l'ancienne version contenant la personne effacee)."""
    import boto3

    pseudonymization_key = load_pseudonymization_key()
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    s3_client = session.client("s3")

    for table_name in TABLES_TO_RESYNC_ON_S3:
        local_path, row_count = export_table_to_parquet(conn, table_name, GOLD_TABLES[table_name], pseudonymization_key)
        s3_key = upload_to_s3(s3_client, local_path, table_name)
        print(f"    - {table_name}: reexporte ({row_count} lignes) -> s3://{BUCKET_NAME}/{s3_key}")

        # Purge des versions ANTERIEURES a cette reexportation (bucket
        # versionne, cf. terraform/s3.tf) : sans ca, l'ancienne version
        # (avec la personne effacee) reste recuperable via son version-id.
        versions = s3_client.list_object_versions(Bucket=BUCKET_NAME, Prefix=s3_key).get("Versions", [])
        to_delete = [
            {"Key": s3_key, "VersionId": v["VersionId"]}
            for v in versions
            if not v.get("IsLatest")
        ]
        if to_delete:
            s3_client.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": to_delete})
            print(f"      {len(to_delete)} version(s) anterieure(s) purgee(s) sur S3.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Effacement RGPD d'un profil dim_user (SafeLift).")
    parser.add_argument("user_id", type=int)
    parser.add_argument("--confirm", action="store_true", help="Execute reellement les suppressions (sinon dry-run).")
    parser.add_argument("--skip-s3", action="store_true", help="Ne pas tenter de resynchroniser S3.")
    parser.add_argument(
        "--i-understand-this-breaks-the-demo",
        action="store_true",
        help="Requis en plus de --confirm pour effacer le profil demo (is_weight_training_demo_user=true).",
    )
    args = parser.parse_args()

    conn = psycopg2.connect(**get_db_config())
    try:
        profile = fetch_dim_user_row(conn, args.user_id)
        if profile is None:
            print(f"[gdpr_erase_user] user_id={args.user_id} introuvable dans gold.dim_user. Rien a faire.")
            return 1

        fact_counts = count_fact_rows(conn, args.user_id)
        silver_idx = match_silver_row_index(profile)
        bronze_matches = match_bronze_partitions(profile)
        csv_idx = match_raw_csv_index(profile)

        print(f"[gdpr_erase_user] Profil user_id={args.user_id} :")
        print(f"  - gold.dim_user            : 1 ligne (is_weight_training_demo_user={profile['is_weight_training_demo_user']})")
        print(f"  - gold.fact_workout_session : {fact_counts['fact_workout_session']} ligne(s)")
        print(f"  - gold.fact_risk_score      : {fact_counts['fact_risk_score']} ligne(s)")
        print(f"  - Silver gym_members        : {'1 ligne trouvee' if silver_idx is not None else 'AUCUNE correspondance'}")
        print(f"  - Bronze gym_members        : {len(bronze_matches)} partition(s) contenant une correspondance")
        print(f"  - CSV source (raw)          : {'1 ligne trouvee' if csv_idx is not None else 'AUCUNE correspondance'}")

        if profile["is_weight_training_demo_user"] and not args.i_understand_this_breaks_the_demo:
            print(
                "\n[gdpr_erase_user] REFUS : ce profil est is_weight_training_demo_user=true, "
                "le seul relie a de vraies donnees de seance (2164 lignes fact_*). "
                "L'effacer casserait le dashboard de demo. Relancer avec "
                "--i-understand-this-breaks-the-demo si c'est reellement voulu."
            )
            return 1

        if not args.confirm:
            print("\n[gdpr_erase_user] DRY-RUN (aucune modification). Relancer avec --confirm pour executer reellement.")
            return 0

        print("\n[gdpr_erase_user] Execution reelle...")
        erase_gold_postgres(conn, args.user_id)
        print("  - gold.dim_user / fact_workout_session / fact_risk_score : lignes supprimees (commit).")

        if silver_idx is not None:
            erase_silver_row(silver_idx)
            print("  - Silver gym_members : ligne supprimee.")

        if bronze_matches:
            erase_bronze_rows(bronze_matches)
            print(f"  - Bronze gym_members : ligne supprimee dans {len(bronze_matches)} partition(s).")

        if csv_idx is not None:
            erase_raw_csv_row(csv_idx)
            print("  - CSV source (raw) : ligne supprimee (durable a travers de futurs runs bronze_ingestion).")

        if not args.skip_s3:
            print("  - Resynchronisation S3...")
            try:
                resync_s3(conn)
            except Exception as exc:  # noqa: BLE001 -- on ne veut jamais faire echouer le script pour ca
                print(
                    f"    ATTENTION : resynchronisation S3 echouee ({exc!r}). "
                    "Les suppressions locales ci-dessus restent valides. "
                    "Rafraichir les credentials AWS (profil awslearnerlab) et relancer "
                    f"manuellement l'export pour dim_user/fact_workout_session/fact_risk_score."
                )
        else:
            print("  - S3 non resynchronise (--skip-s3).")

        print("\n[gdpr_erase_user] Termine.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
