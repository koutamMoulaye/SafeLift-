# PROGRESS.md — Suivi d'avancement SafeLift

> Legende : ✅ fait · 🔄 en cours · ⏳ a faire
>
> Mettre a jour ce fichier AVANT de considerer une etape terminee, pour qu'il
> reflete toujours l'etat reel du repo. Voir aussi [CLAUDE.md](./CLAUDE.md)
> pour le contexte et les decisions techniques.
>
> **Ce fichier couvre uniquement le Jalon 1 (etapes 1 a 6, clos le
> 2026-07-08)** — voir [PROGRESS_JALON2.md](./PROGRESS_JALON2.md) pour le
> Jalon 2 (streaming affluence temps reel), demarre le 2026-07-09.

## Etape 1/6 — Scaffolding + stack Docker locale — ✅ fait

**Date** : 2026-07-01

**Livre** :
- Structure de repo complete (`airflow/`, `spark/`, `dashboard/`, `dbt/`,
  `data/bronze|silver|gold/`, chacun avec `.gitkeep` la ou vide).
- `docker-compose.yml` demarrant 5 briques fonctionnelles et interconnectees :
  - Zookeeper + Kafka (broker unique), avec creation automatique d'un topic de
    test au demarrage via un conteneur one-shot `kafka-init`.
  - Spark standalone : 1 master + 1 worker (`apache/spark:3.5.8-python3`,
    master/worker demarres via override d'entrypoint, cf. CLAUDE.md).
  - Airflow (LocalExecutor) : `airflow-init` (migration DB + creation user
    admin), `airflow-webserver`, `airflow-scheduler`. Image custom
    (`airflow/Dockerfile`) avec providers Kafka/Spark installes.
  - PostgreSQL applicatif (`app-postgres`, futur warehouse) et PostgreSQL
    Airflow (`airflow-postgres`, metadata uniquement) — deux instances et
    volumes bien separes.
  - Dashboard FastAPI placeholder (`dashboard/main.py`) exposant `/health`.
- `.env.example` avec tous les ports/credentials configurables, `.gitignore`
  couvrant Python/Docker/IDE/`.env`/donnees generees.
- `Makefile` (`make up` / `make down` / `make logs` / `make ps` / `make build`
  / `make restart` / `make clean`).
- `README.md` avec instructions de lancement, tableau des ports exposes, et
  commandes de verification par service.
- `CLAUDE.md` et `PROGRESS.md` initialises.

**Verifications effectuees** (voir details dans la reponse de session) :
- `docker compose config --quiet` : fichier valide, pas d'erreur.
- `docker compose up -d --build` : tous les services demarrent.
- `docker compose ps` : tous les services passent a l'etat `healthy` (sauf
  `kafka-init`, conteneur one-shot qui termine avec le code de sortie 0 apres
  avoir cree le topic).
- Kafka : `kafka-topics --bootstrap-server localhost:9092 --list` liste bien
  le topic `safelift-test-topic`.
- Spark : UI master accessible sur `http://localhost:18080`, le worker
  apparait dans la liste des workers enregistres.
- Airflow : UI webserver accessible sur `http://localhost:18089`, login avec
  l'utilisateur admin cree par `airflow-init`.
- Dashboard : `GET http://localhost:18000/health` renvoie `{"status":"ok"}`.
- PostgreSQL applicatif et Airflow : `pg_isready` OK sur les deux instances,
  bases et identifiants bien distincts.
- Spark : le worker apparait bien dans "Workers (1)" sur l'UI du master.
- Airflow : `airflow users list` confirme la creation de l'utilisateur
  `admin`, page de login accessible (HTTP 200).
- Stack testee de bout en bout avec `docker compose up -d --build` puis
  arretee proprement avec `docker compose down` en fin de verification.

**Bugs rencontres et corriges pendant les tests** :
- `bitnami/spark:3.5` n'existe plus sur Docker Hub (Bitnami a retire les tags
  versionnes gratuits) → remplace par l'image officielle Apache
  `apache/spark:3.5.8-python3`.
- Healthcheck Zookeeper initialement en echec : la commande 4-lettres `ruok`
  est desactivee par defaut et la variable d'env `ZOOKEEPER_4LW_COMMANDS_WHITELIST`
  n'est pas supportee par cette version de l'image → resolu via
  `KAFKA_OPTS: -Dzookeeper.4lw.commands.whitelist=ruok`. Le script du
  healthcheck lui-meme avait aussi un bug (le pipe n'atteignait pas le socket
  `/dev/tcp` imbrique) → corrige avec un descripteur de fichier bidirectionnel
  explicite (`exec 3<>/dev/tcp/...`). Detail complet dans CLAUDE.md.

**Notes / limitations connues** :
- Le Fernet key Airflow est laisse vide dans `.env.example` (chiffrement
  metadata desactive) — acceptable pour un environnement 100% local de
  developpement, a revoir si des credentials sensibles sont stockes dans des
  connexions Airflow plus tard.
- Pas encore de CI ni de tests automatises sur la stack elle-meme (hors scope
  etape 1).

## Etape 2/6 — Ingestion Bronze — 🔄 en cours

**Date** : 2026-07-01

**Livre jusqu'ici** :
- `data/download_datasets.sh` : script de telechargement des datasets sources
  Kaggle vers `data/bronze/raw/` (donnees brutes, non versionnees — cf.
  `.gitignore`). Gere la configuration du token Kaggle (format `KGAT_...` ou
  `~/.kaggle/kaggle.json` existant).
- Environnement Python isole dans un venv dedie au projet (`.venv/` a la
  racine, ignore par git), cree et active automatiquement par le script — plus
  aucune installation `--user`/`--break-system-packages` au niveau systeme.
  Voir `data/requirements.txt` pour les versions pinnees et CLAUDE.md pour le
  detail du bug qui a motive cette isolation.
- 3 datasets Kaggle telecharges et verifies avec succes (**284 MB** au total
  dans `data/bronze/raw/`) :
  - `600k_fitness/` — 600K+ Fitness Exercise & Workout Program
    (`program_summary.csv` : 2 598 lignes / 10 col. ;
    `programs_detailed_boostcamp_kaggle.csv` : 605 033 lignes / 16 col.)
  - `gym_members/` — Gym Members Exercise Dataset
    (`gym_members_exercise_tracking.csv` : 973 lignes / 15 col.)
  - `weight_training/` — 721 Weight Training Workouts
    (`weightlifting_721_workouts.csv` : 9 932 lignes / 10 col.)

**Verifications effectuees** :
- Execution reelle et complete de `bash data/download_datasets.sh` (2 runs :
  un a froid avec creation du venv, un a chaud en reutilisant le venv
  existant) — les 4 CSV sont bien la, `ls -lhR` confirme des tailles
  coherentes, `find -empty` ne remonte aucun fichier vide, `head -5` sur
  chacun des 4 fichiers confirme un contenu CSV valide et coherent avec les
  en-tetes attendus.

### Volet B — Ingestion Bronze (CSV -> Parquet via Airflow) — ✅ fait

**Date** : 2026-07-01

**Livre** :
- `data/bronze/SCHEMA_NOTES.md` : schema reel de chacun des 4 fichiers CSV
  sources, obtenu par inspection directe (module `csv` stdlib, pas de
  supposition), avec anomalies constatees et statut de licence Kaggle des
  datasets 1 et 3. Resume :
  - `600k_fitness_summary` (`program_summary.csv`, 2 598 lignes, 10 col.) :
    `level`/`goal` sont des listes Python stringifiees (`"['A', 'B']"`), pas
    de doublons.
  - `600k_fitness_detailed` (`programs_detailed_boostcamp_kaggle.csv`,
    605 033 lignes, 16 col.) : **904 doublons stricts**, **25 967 valeurs
    negatives dans `reps`** (semantique incertaine, non corrigee ici).
  - `gym_members` (973 lignes, 15 col.) : dataset le plus propre, aucun null,
    aucun doublon.
  - `weight_training` (9 932 lignes, 10 col.) : **790 doublons stricts**
    (≈8%), colonnes `Notes`/`Workout Notes` quasi-integralement vides
    (99.9%/100%).
  - Licences (verifiees via `kaggle datasets metadata`) : dataset 1
    (600k fitness) = **ODbL-1.0** ; dataset 3 (weightlifting) = **`unknown`**
    (non declaree par l'auteur sur Kaggle — a verifier manuellement avant tout
    usage au-dela du cadre pedagogique).
- `airflow/dags/bronze_ingestion.py` : DAG Airflow (4 tasks, une par fichier
  CSV source — **les 2 fichiers `600k_fitness` ne sont pas fusionnes**, ce
  sont deux grains differents, fusionner necessiterait une jointure hors
  perimetre Bronze ; voir SCHEMA_NOTES.md pour la justification complete).
  Chaque task lit un CSV, ajoute `ingestion_timestamp`/`source_file`/
  `source_dataset`, et ecrit en Parquet dans
  `data/bronze/{table}/ingestion_date={{ ds }}/`. Idempotent : la partition du
  jour est entierement supprimee puis reecrite a chaque run (pas d'append).
- `docker-compose.yml` : ajout du bind mount `./data:/opt/airflow/data` sur
  les 3 services Airflow (init/webserver/scheduler) — necessaire pour que le
  DAG accede a `data/bronze/`. Aucun autre service (Kafka, Spark, dashboard)
  modifie.
- `airflow/requirements.txt` : ajout de `pandas==2.1.4` (pin volontaire, pas
  2.2.x — cf. CLAUDE.md) et `pyarrow==18.1.0`.

**Verifications effectuees** :
- `docker compose up -d --build` : rebuild de l'image Airflow (nouvelles deps),
  tous les services repassent `healthy`.
- `airflow dags list-import-errors` : aucune erreur d'import du DAG.
- `airflow dags trigger bronze_ingestion` (execution reelle, pas un dry-run) :
  DAG run `success`, les 4 tasks passent `success`
  (`airflow tasks states-for-dag-run`).
- Log reel de la task `ingest_600k_fitness_detailed` : confirme
  "605033 lignes lues ... -> .../600k_fitness_detailed.parquet".
- **Idempotence verifiee concretement** : re-execution de la task
  `ingest_gym_members` pour la meme date logique (`airflow tasks test ...
  2026-07-01`) — le dossier de partition ne contient toujours qu'un seul
  fichier apres coup (pas d'accumulation).
- Script de verification Python (pandas) execute dans le conteneur Airflow,
  rechargeant chacun des 4 Parquet : shapes correctes (2598, 605033, 973,
  9932 lignes — coherentes avec SCHEMA_NOTES.md), dtypes corrects, presence
  confirmee des 3 colonnes de metadonnees d'ingestion sur les 4 tables.
- Fichiers Parquet confirmes presents sur le systeme de fichiers hote (pas
  seulement dans le conteneur) via le bind mount, tailles coherentes
  (compression Parquet : ex. 282 MB CSV -> 1.4 MB Parquet pour
  `600k_fitness_detailed`).

**Reste a faire pour la suite de l'etape 2** (perimetre pas encore
entierement defini avec l'utilisateur) : producteur Kafka de donnees simulees
+ DAG(s) consommant Kafka.

## Etape 3/6 — Transformation Silver — ✅ fait

**Date** : 2026-07-01

> ⚠️ **TODO manuel Moulaye** : verifier la licence Kaggle du dataset
> `weight_training` (721 Weight Training Workouts,
> `joep89/weightlifting`) avant soutenance — actuellement `unknown`
> (aucune licence declaree par l'auteur, confirme via l'API Kaggle en
> etape 2). Voir `data/bronze/SCHEMA_NOTES.md` section "Statut de licence".

**Livre** :
- `data/bronze/SCHEMA_NOTES.md` (etape 2) sert de base aux regles de
  nettoyage ci-dessous.
- `spark/jobs/silver_common.py` : helpers partages (lecture de la derniere
  partition Bronze, parsing des colonnes "liste Python stringifiee" en
  `array<string>` via `ast.literal_eval`, ecriture Silver).
- `spark/jobs/silver_600k_fitness_summary.py`, `silver_600k_fitness_detailed.py`,
  `silver_gym_members.py`, `silver_weight_training.py` : un job Spark par
  table Bronze, chacun documente en tete de fichier (regles de nettoyage +
  justification). Resume :
  - **Deduplication** : 904 doublons supprimes sur `600k_fitness_detailed`
    (605 033 -> 604 129 lignes), 790 sur `weight_training` (9 932 -> 9 142
    lignes).
  - **`reps` negatifs** (`600k_fitness_detailed`) : 25 908 valeurs (mesurees
    apres dedup) mises a `null` + flag booleen `reps_anomaly_flag`, plutot
    qu'une valeur absolue non justifiee (raisonnement complet dans
    `data/silver/CLEANING_LOG.md`).
  - **Colonnes quasi-vides** (`weight_training.Notes`/`Workout Notes`,
    taux de remplissage 0.1%/0.0%) : droppees (seuil retenu : <5%).
  - **`level`/`goal`** (`600k_fitness_*`) : parsees en vraies colonnes
    `array<string>` (`level_list`/`goal_list`) plutot qu'explode (aurait
    change le grain, produit croise level×goal) ou one-hot (vocabulaire
    fige a l'avance).
  - **Conversion d'unites** : `program_length` -> `program_length_weeks`
    (hypothese "semaines" confirmee empiriquement, 99.5% de correspondance
    avec `max(week)` du grain detaille) ; `weight_training.Weight` (livres,
    confirme empiriquement via les valeurs caracteristiques 135/185/225/275)
    -> `lifted_weight_kg`, **volontairement nomme differemment** de
    `gym_members.body_weight_kg` malgre la meme unite kg (grandeurs
    physiques differentes : poids souleve vs poids corporel — deviation
    documentee et justifiee par rapport a la consigne initiale, voir
    CLEANING_LOG.md section "Harmonisation inter-tables").
  - Toutes les colonnes renommees en snake_case ; convention `_at` pour tout
    timestamp (`created_at`, `last_edited_at`, `performed_at`,
    `silver_processed_at`) ; metadonnees Bronze (`ingestion_timestamp`,
    `source_file`, `source_dataset`) conservees pour la tracabilite.
  - **Aucune jointure** entre les 4 tables (hors perimetre Silver).
- `data/silver/CLEANING_LOG.md` : chaque decision de nettoyage justifiee et
  chiffree (avant/apres par table), y compris les resultats d'execution
  reels.
- `airflow/dags/silver_transformation.py` : DAG a 4 tasks (une par job Spark,
  independantes/paralleles), soumises via `spark-submit --deploy-mode client`
  au cluster `spark://spark-master:7077`.
- `airflow/dags/bronze_ingestion.py` : ajout d'une task
  `trigger_silver_transformation` (`TriggerDagRunOperator`) en aval des 4
  tasks d'ingestion, qui declenche automatiquement `silver_transformation`.
  Choix documente dans le docstring du DAG : un `ExternalTaskSensor` aurait
  ete plus fragile ici car les deux DAGs sont a declenchement manuel
  (`schedule=None`), sans date logique naturellement alignee entre eux.
- `docker-compose.yml` : ajout de `./data:/opt/data` sur les conteneurs
  airflow-*/spark-master/spark-worker (chemin identique partout, necessaire
  pour que driver et executeurs Spark voient le meme systeme de fichiers) et
  de `./spark/jobs:/opt/airflow/spark_jobs` sur les conteneurs airflow-*.
- `airflow/Dockerfile` : ajout d'un JRE (`openjdk-17-jre-headless`, avec
  `JAVA_HOME` calcule dynamiquement pour rester portable entre architectures)
  et changement de l'image de base `apache/airflow:2.10.4-python3.12` ->
  `apache/airflow:2.10.4-python3.10` (voir "Bugs rencontres" ci-dessous).
- `airflow/requirements.txt` : ajout de `pyspark==3.5.8` epingle explicitement
  (la contrainte souple de `apache-airflow-providers-apache-spark` tirait
  sinon la derniere version 4.1.2, incompatible avec le cluster Spark 3.5.8).

**Verifications effectuees** :
- `docker compose up -d --build` : rebuild de l'image Airflow, tous les
  services `healthy`.
- `airflow dags trigger bronze_ingestion` (execution reelle complete) :
  DAG `bronze_ingestion` `success`, declenchement automatique confirme de
  `silver_transformation` (visible via `airflow dags list-runs -d
  silver_transformation`), DAG `silver_transformation` `success` avec les 4
  tasks `success` (`airflow tasks states-for-dag-run`).
- Logs reels de chaque task Spark confirmant les chiffres avant/apres
  (voir `data/silver/CLEANING_LOG.md` section "Resultats d'execution" pour
  le detail complet par table).
- Script de controle Python (pandas) rechargeant les 4 Parquet Silver :
  shapes, colonnes finales et echantillon de 5 lignes par table verifies.
  Coherence verifiee manuellement sur l'echantillon : une ligne
  `weight_training` a `Weight=135` (lbs) en Bronze donne bien
  `lifted_weight_kg=61.23` en Silver ; une ligne `600k_fitness_detailed` a
  `reps` negatif en Bronze apparait bien `reps=NaN` +
  `reps_anomaly_flag=True` en Silver.

**Bugs rencontres et corriges pendant les tests** :
- `JAVA_HOME is not set` : l'image `apache/airflow` n'embarque aucun JRE, or
  pyspark (meme cote driver, mode client) en a besoin -> ajout
  d'`openjdk-17-jre-headless` dans `airflow/Dockerfile`, avec `JAVA_HOME`
  calcule dynamiquement (`readlink -f /usr/bin/java` + symlink stable
  `/usr/lib/jvm/default-java`) pour ne pas coder en dur un chemin
  specifique a une architecture (ex. `-arm64`).
- `PYTHON_VERSION_MISMATCH` (driver Python 3.12 vs executeur Python 3.10) :
  echouait uniquement sur les 2 jobs utilisant une UDF Python
  (`600k_fitness_summary`/`detailed`, parsing `level`/`goal`) — les jobs
  sans UDF (`gym_members`, `weight_training`) passaient car les operations
  DataFrame pures sont traduites en Catalyst/JVM sans lancer de worker
  Python sur l'executeur. Cause : `apache/spark:3.5.8-python3` embarque
  Python 3.10 (fixe par l'image officielle), notre image Airflow tournait en
  Python 3.12. Resolu en alignant l'image Airflow sur
  `apache/airflow:2.10.4-python3.10` plutot que de maintenir une image Spark
  personnalisee.
- `Mkdirs failed to create file:/opt/data/silver/...` : le driver (conteneur
  airflow-*, uid 50000) et les executeurs (conteneur spark-worker, uid 185
  "spark") n'ont pas le meme UID sur le bind mount partage `./data`. Un
  repertoire cree par le driver avec un umask proprietaire-seul (755)
  devient illisible en ecriture pour l'executeur. Resolu avec
  `--conf spark.hadoop.fs.permissions.umask-mode=000` dans la commande
  spark-submit (force Hadoop, utilise en interne par Spark pour l'ecriture
  fichier, a creer repertoires/fichiers en 777 quel que soit l'UID).
- `bitnami/spark:3.5` deja documente en etape 1 (image retiree de Docker
  Hub) — `pyspark` doit rester aligne sur la version reelle du cluster
  (`3.5.8`), pas sur la derniere version PyPI disponible.

**Notes / limitations connues** :
- Les 4 tables Silver restent independantes (pas de cle commune
  materialisee) : la jointure interviendra au modele en etoile en Gold
  (etape 4).
- `distance` (weight_training) et `time_per_workout`/`program_length`
  (600k_fitness, pour `program_length` seulement avant confirmation
  empirique) : unites non documentees dans les metadonnees source pour les
  cas non verifies empiriquement — aucune conversion inventee sans preuve,
  voir CLEANING_LOG.md pour le detail des cas verifies vs non verifies.

## Etape 4/6 — Gold : modele en etoile + dbt + risk_score — ✅ fait

**Date** : 2026-07-03

> ⚠️ **TODO manuel Moulaye** (rappel de l'etape 3) : verifier la licence
> Kaggle du dataset `weight_training` avant soutenance — toujours `unknown`.
> Voir `data/bronze/SCHEMA_NOTES.md`.

**Livre** :
- `spark/jobs/load_silver_to_postgres.py` : charge les 4 tables Silver dans
  Postgres (`app-postgres`, schema `raw`) via JDBC, pour alimenter dbt (qui
  opere sur SQL, pas directement sur Parquet).
- `dbt/` : projet dbt complet (`dbt-core`/`dbt-postgres` 1.8.2, dans un venv
  isole `/opt/dbt_venv` separe de l'environnement Airflow) :
  - **staging** (vues) : `stg_gym_members`, `stg_600k_fitness_detailed`,
    `stg_weight_training`.
  - **marts** (tables, schema `gold`) : `dim_exercise` (3 177 lignes, dont
    50 exercices weight_training sans correspondance catalogue),
    `dim_muscle` (9 zones + `base_epidemiological_risk`), `dim_user` (973
    profils gym_members), `dim_date` (1 073 jours), `fact_workout_session`
    (2 164 lignes), `fact_risk_score` (2 164 lignes, formule deterministe).
  - 45 tests dbt (generiques + 2 singuliers), tous passants.
- `airflow/dags/gold_dbt_run.py` : charge Silver->Postgres puis `dbt run` +
  `dbt test`, declenche automatiquement par `silver_transformation`
  (`TriggerDagRunOperator`, meme mecanisme que bronze->silver). Echec
  explicite du DAG si un test dbt echoue.
- `data/gold/GOLD_MODEL_DECISIONS.md` : toutes les hypotheses de
  modelisation documentees et chiffrees (voir resume ci-dessous).
- `airflow/Dockerfile` : ajout du venv dbt isole (`/opt/dbt_venv`, cree sous
  `USER airflow` — l'image `apache/airflow` refuse `pip install` en root).
- `docker-compose.yml` : ajout du mount `./dbt:/opt/airflow/dbt`.

**Decisions/hypotheses cles (voir GOLD_MODEL_DECISIONS.md pour le detail
complet et chiffre)** :
- **Taux de matching exercice reel : 31/81 = 38.3%** des exercices distincts
  de `weight_training` retrouves dans le catalogue `600k_fitness_detailed`.
  Les 50 non-matches sont ajoutes a `dim_exercise` avec `muscle_group='unknown'`,
  `is_matched=false` — aucune ligne de fait perdue.
- **`muscle_group` = classification heuristique par mots-cles**, PAS une
  taxonomie medicale validee (aucune source ne fournit cette colonne
  nativement). `equipment` de `dim_exercise` toujours `NULL` (attribut du
  PROGRAMME dans la source, pas de l'exercice individuel).
- **`base_epidemiological_risk`** : 0.25 (shoulder) / 0.20 (knee) / 0.18
  (lower_back) — ordres de grandeur retenus pour ce projet, non issus d'une
  etude citee. **0.10 par defaut pour toute autre zone — hypothese de
  modelisation explicitement marquee comme telle, PAS une donnee
  epidemiologique verifiee.**
- **`dim_user` : rattachement de weight_training a UN SEUL profil**
  (`user_id=9`, experience_level max) — **hypothese de demonstration**, pas
  une jointure reelle (aucune cle commune entre les 2 sources).
- **`risk_score`** : `base_zone x charge_factor(1.3) x volume_factor([0.5,2.0]) x recup_factor(1.4) x duree_factor(1.2)`,
  normalise 0-100 (bornes theoriques 0.05-1.092). Tous les facteurs sont des
  colonnes visibles de `fact_risk_score` (pas de boite noire).

**Verifications effectuees** :
- Chaine complete `bronze_ingestion -> silver_transformation -> gold_dbt_run`
  declenchee de bout en bout (execution reelle, pas un dry-run) : toutes les
  tasks `success`, y compris les 2 `TriggerDagRunOperator` en cascade.
  `dbt run` : 9/9 modeles. `dbt test` : 45/45 tests.
- Distribution reelle de `risk_score` (2 164 lignes) : **Faible 2 117
  (97.8%)**, **Modere 47 (2.2%)**, **Eleve 0 (0%)**. Min=0.00, max=43.97,
  moyenne=10.97. Absence de score "Eleve" expliquee (duree_factor ne se
  declenche jamais sur ce dataset, `duration_seconds` etant a 0 sur la
  quasi-totalite des lignes source) et documentee comme un constat honnete,
  pas masquee.
- Tracabilite verifiee manuellement sur un exemple reel (ligne 172, "Squat",
  zone knee) : `0.20 x 1.3 x 1.9543 x 1.0 x 1.0 = 0.5081` ->
  `risk_score = 43.97` (Modere) — calcul manuel conforme a la valeur stockee.

**Bugs reels rencontres et corriges** (detail complet dans
GOLD_MODEL_DECISIONS.md) : `round(double precision, integer)` inexistant en
Postgres (cast `::numeric`) ; driver JDBC invisible du `DriverManager` nu via
py4j a cause de l'isolation de classloader Spark (`--packages`) — resolu en
utilisant `psycopg2` pur Python pour le DDL ; `DROP TABLE` JDBC en conflit
avec les vues dbt staging qui en dependent (resolu avec `truncate=true`) ;
**grain duplique sur `fact_workout_session` reellement detecte par le test
dbt dedie** (32 groupes en double, agregation sur `exercise_name` brut au
lieu de `normalized_exercise_name` — corrige, 2196 -> 2164 lignes correctes) ;
`dbt-postgres==1.8.9` inexistant (pin sur `1.8.2`) ; `pip install` en root
refuse par l'image `apache/airflow` (bascule vers `USER airflow`).

### Etape 4/6 — Volet B : correctifs matching + risk_score — ✅ fait

**Date** : 2026-07-03 (meme jour, suite a revue des faiblesses identifiees)

**Correctif 1 — Matching exercise_name : 38.3% -> 90.1%.** Pipeline etendu
de 1 a 4 etapes en cascade dans `dim_exercise.sql` :
1. Match strict (inchange) : 31/81 (38.3%)
2. **Nouveau** : match sur nom de base (`normalize_exercise_base_name.sql`,
   equipement generique retire + pluriel simple) : +21 -> 52/81 (64.2%)
3. **Nouveau** : fuzzy matching (`rapidfuzz`, `scripts/fuzzy_match_exercises.py`,
   seuil 85%, execute hors dbt car dbt-postgres ne supporte pas les modeles
   Python) : +6 -> 58/81 (71.6%) apres exclusion de 2 faux positifs verifies
4. **Nouveau** : mapping manuel (`dbt/seeds/manual_exercise_muscle_mapping.csv`,
   15 lignes justifiees, couvrant 98% du volume residuel) : +15 -> **73/81
   (90.1%)**. 8 exercices (9.9%, tres faible volume) restent non resolus,
   assume et documente plutot que force.
- **Echantillon fuzzy verifie manuellement** : 8 candidats genuinement
  nouveaux inspectes un par un, **2 faux positifs detectes (25% d'erreur)**
  — `low incline bench`->`Incline Bench Row` (poussee vs tirage) et
  `glute extension`->`Leg Extension` (fessiers vs quadriceps) — exclus
  explicitement et re-mappes manuellement.

**Correctif 2 — risk_score : 0% -> 1.2% "Eleve" (recalibrage honnete).**
Root cause : la borne de normalisation max incluait un plafond
`duree_factor=1.2` jamais atteint en pratique (`duration_seconds` fiable a
0% sur ce dataset), compressant artificiellement tous les scores reels.
Corrige en excluant ce plafond du calcul des bornes (nouvelles variables dbt
partagees `risk_score_min_raw=0.05`/`risk_score_max_raw=0.86`, contre
`1.092` avant) — **aucune valeur de facteur ni aucun seuil n'a ete modifie**,
seule une borne theorique jamais realisee a ete recalibree. Impact mesure :
Faible 97.8%->88.5%, Modere 2.2%->10.3%, **Eleve 0%->1.2% (26 lignes)**,
score max 43.97->100.00.
- **Table de demonstration synthetique ajoutee malgre tout**
  (`gold.fact_risk_score_demo_synthetic`, 9 scenarios fictifs) : anticipe le
  besoin du futur dashboard (bascule vue reelle/demo) independamment de la
  rarete naturelle des cas "Eleve" reels (1.2%). Table Postgres separee +
  colonne `is_synthetic_demo=true`, jamais melangee aux vraies statistiques.

**DAG `gold_dbt_run.py` etendu** (6 tasks au lieu de 3) :
`load_silver_to_postgres -> dbt_seed -> dbt_run_staging ->
fuzzy_match_exercises -> dbt_run -> dbt_test`.

**Verifications** : chaine complete `bronze_ingestion -> silver_transformation
-> gold_dbt_run` re-executee de bout en bout avec succes (6/6 tasks),
`dbt run` 10/10 modeles, **`dbt test` 60/60 tests** (contre 45 avant, tests
ajoutes sur `match_stage` et la table de demo).

## Etape 5/6 — Serving : API + dashboard — 🔄 en cours (backend verifie, UI a confirmer)

**Date** : 2026-07-04

**Perimetre** : API + dashboard visuel uniquement. PAS de streaming Kafka,
PAS de nutrition, PAS de ML (rappel explicite de la consigne, a respecter
pour les jalons suivants).

**Livre** :
- `dashboard/main.py` : API FastAPI complete, remplace le placeholder
  `/health`-only. 5 endpoints :
  - `GET /health` (inchange)
  - `GET /users` : liste `gold.dim_user` (973 profils reels), avec un champ
    `has_risk_data` calcule (seul `user_id=9`, le "demo user" documente en
    etape 4, en a) pour que le dashboard puisse le signaler clairement plutot
    que d'afficher un ecran vide sans explication.
  - `GET /users/{user_id}/risk` : dernier `risk_score` par `muscle_group`
    (via `DISTINCT ON`), avec TOUS les facteurs visibles (`base_zone`,
    `charge_factor`, `volume_factor`, `recup_factor`, `duree_factor`,
    `exercise_name`, `session_date`) — pas de score agrege seul.
  - `GET /users/{user_id}/risk/history` : moyenne de `risk_score` par
    `session_date`, pour une courbe de tendance simple.
  - `GET /demo/scenarios` : les 9 scenarios synthetiques
    (`gold.fact_risk_score_demo_synthetic`), chacun avec
    `is_synthetic_demo=true`.
  - Connexion Postgres via `psycopg2` (coherent avec
    `spark/jobs/load_silver_to_postgres.py` et
    `scripts/fuzzy_match_exercises.py`), pool de connexions minimal
    (`psycopg2.pool.SimpleConnectionPool`).
- `dashboard/static/` : dashboard servi par FastAPI (`StaticFiles` +
  `FileResponse` sur `/`) :
  - `index.html` : silhouette humaine SVG schematique (vue de face), zones
    cliquables (`data-muscle="..."`) mappees sur les valeurs de
    `muscle_group` : `shoulder`, `chest`, `abs`, `arms`, `legs`, `knee`, plus
    `back`/`lower_back` representees en pointille (zones dorsales,
    simplification assumee sur une vue de face, indiquee au clic).
  - `dashboard.css` : couleurs vert/ambre/rouge/gris (Faible/Modere/Eleve/pas
    de donnee), bandeau demo (fond ambre, sticky), style volontairement
    simple (pas d'animation, pas d'optimisation mobile — demo jury sur
    ecran/projecteur).
  - `dashboard.js` : selecteur utilisateur (`/users`), toggle
    reel/demo, panneau lateral affichant tous les facteurs au clic sur une
    zone, mini-graphique de tendance (SVG natif, pas de librairie de
    graphiques), **separation stricte** reel/demo (jamais les deux sources
    affichees ou melangees simultanement sur le meme ecran).
- `dashboard/Dockerfile` : copie desormais aussi `static/`.
- `docker-compose.yml` : `dashboard` depend maintenant de `app-postgres`
  (`service_healthy`) et recoit les variables `APP_POSTGRES_*` (memes noms
  que `.env`, service en lecture seule sur le schema `gold`).

**Verifications effectuees (backend, automatisees)** :
- `docker compose up -d --build dashboard` : conteneur `healthy`.
- Les 5 endpoints testes reellement via `curl` sur le service demarre (pas
  de dry-run) : `/health` OK, `/users` renvoie les 973 profils avec
  `user_id=9` trie en premier (`has_risk_data=true`), `/users/9/risk`
  renvoie 8 zones avec tous les facteurs (**zone `shoulder` a `Modere`
  (45.00)** — confirme un cas non-vert exploitable pour la demo),
  `/users/9/risk/history` renvoie une serie temporelle exploitable,
  `/demo/scenarios` renvoie bien 3 scenarios par niveau
  (Faible/Modere/Eleve confirmes par comptage).
- Gestion d'erreur verifiee : `GET /users/9999/risk` (utilisateur
  inexistant) renvoie `404` (pas un plantage silencieux).
- Fichiers statiques verifies servis (`/`, `/static/dashboard.js`,
  `/static/dashboard.css` tous `200`).

> ⚠️ **Verification visuelle (silhouette coloree, toggle demo, captures
> d'ecran) NON effectuee par Claude Code** : l'extension navigateur
> (Claude in Chrome) n'etait pas connectee lors de cette session. Le backend
> est entierement verifie (donnees reelles, tous les endpoints), mais
> l'affichage du dashboard (couleurs des zones, bandeau demo, panneau de
> detail) reste a confirmer visuellement par Moulaye avant de considerer
> cette etape completement terminee. URL : http://localhost:18000.

### Etape 5/6 — Correctifs post-retour utilisateur — 🔄 en cours (idem : UI a confirmer)

**Date** : 2026-07-04

**Correctif A — Selecteur d'utilisateur trompeur (corrige)** :
- `GET /users` renvoie desormais `{users_with_data: [...], users_without_data: [...]}`
  au lieu d'une liste plate (verifie via `curl` : `users_with_data` ne
  contient bien que `user_id=9`, les 972 autres dans `users_without_data`).
- Dropdown : `<optgroup>` "Profils avec données de séance réelles" en
  premier (pre-selectionne par defaut), puis `<optgroup>` secondaire avec le
  label explicite demande ("Profils sans séance réelle (1 seul profil de
  weight_training a pu être rattaché à dim_user — hypothèse de
  démonstration documentée)").
- **Limite assumee** : un `<select>` HTML natif ne peut pas etre
  "replie/deplie" (pas de collapse natif sur les `<optgroup>`) sans
  construire un composant de liste personnalise — non fait ici pour rester
  en vanilla JS et robuste sans possibilite de verification visuelle en
  direct. L'objectif principal (jury ne tombe jamais sur un ecran vide par
  defaut) est atteint via la pre-selection automatique du premier profil
  `users_with_data`.

**Correctif B — Silhouette et panneau redesignes** :
- Silhouette remplacee : contour du corps trace en UNE seule courbe de
  Bezier continue (cou -> epaules -> taille -> hanches, aucun rectangle),
  plus bras/jambes/tete/mollets egalement en courbes. Zones colorables
  superposees en semi-transparence (`fill-opacity: 0.82`, pas de contour
  epais) pour un rendu plus doux ; `back`/`lower_back` restent en pointille
  fin (rappel visuel de la simplification dorsale sur vue de face).
  Selection/survol via `filter: drop-shadow(...)` plutot qu'un contour
  epais.
- Panneau de detail : score global ajoute en tete (MAX des risk_score par
  zone, PAS la moyenne — choix documente dans dashboard.js et
  GOLD_MODEL_DECISIONS.md : un outil de risque doit signaler la zone la
  plus a risque, pas la diluer dans une moyenne). Tableau brut des 5
  facteurs remplace par des barres horizontales, chaque barre normalisee
  sur les bornes min/max documentees du facteur concerne (memes bornes que
  `data/gold/GOLD_MODEL_DECISIONS.md`).
- Passe de coherence visuelle : variables CSS centralisees (`:root`) pour
  les couleurs/espacements, reutilisees silhouette + panneau + bandeau demo.

**Verifications effectuees** :
- `docker compose up -d --build dashboard` : conteneur `healthy`.
- `GET /users` reteste via `curl` : structure groupee confirmee.
- Tous les endpoints re-testes `200` apres rebuild (`/`, `/health`,
  `/static/dashboard.js`, `/static/dashboard.css`, `/users/9/risk`,
  `/demo/scenarios`).
- **Analyse statique du SVG/JS/CSS** (verification programmatique via
  Node.js, en l'absence de navigateur) : structure XML du bloc `<svg>`
  entierement bien formee (balises equilibrees, tous les `path`
  correctement fermes par `Z`), syntaxe JS validee (`node --check`),
  accolades CSS equilibrees (38/38). Confirme l'absence d'erreur de
  structure qui empecherait le rendu, mais NE CONFIRME PAS la qualite
  visuelle/esthetique du rendu (proportions, alignement) — necessite un
  navigateur, indisponible lors de cette session.

> ⚠️ Meme limite qu'a la premiere livraison de l'etape 5 : verification
> visuelle non effectuee par Claude Code (extension navigateur
> indisponible). A confirmer par Moulaye sur http://localhost:18000.

### Etape 5/6 — Refonte visuelle "theme sombre / app tracker" (Round 2) — 🔄 en cours (idem : UI a confirmer)

**Date** : 2026-07-04

**Contexte** : apres le Correctif B (silhouette Bezier + score global + barres),
le rendu visuel global etait juge encore insuffisant par Moulaye — demande
explicite d'une refonte complete du LOOK & FEEL (API/logique metier
inchangees) vers un style "dashboard fitness" sombre, a suivre "dans
l'esprit, pas a l'identique". Hors perimetre explicite : pas de navigation
multi-pages, pas d'ecrans Nutrition/Programme/Objectifs/Parametres, pas de
photo de profil, pas de systeme XP/niveau gamifie.

**Changements** :
- `dashboard.css` : reecriture complete. Palette sombre centralisee via
  variables CSS `:root` (`--bg: #0d1117`, cards `#161b23`/`#1c2333`, texte
  `#e6e8ec`/muted `#8b93a7`, accents violet/orange/teal par KPI, code
  couleur de risque conserve mais adapte au fond sombre). Coins arrondis
  (`--radius-card: 14px`), ombres douces (`--shadow-card`).
- `index.html` : reecriture complete de la structure `<main>` :
  - Rangee de 4 cards KPI en haut : derniere seance (nom exercice + date),
    jauge radiale (score global, remplace l'ancien badge texte), tendance
    (delta en points), nombre de zones en alerte (Modere+Eleve).
  - Nouveau panneau "Zones sensibles" (liste, PAS uniquement la silhouette) :
    une ligne par zone en Modere/Eleve uniquement (Faible et pas-de-donnee
    exclus pour eviter le bruit), avec le/les facteur(s) dominant(s) explique(s)
    en langage clair — objectif : comprendre le risque AVANT de cliquer sur
    la silhouette.
  - Silhouette enrichie : pectoraux desormais en 2 moities distinctes,
    lignes decoratives suggerant la sangle abdominale, nouvelle zone
    "mollets" (`data-muscle="calves"`) ajoutee pour la completude
    anatomique — **toujours grisee** car `dim_muscle` ne produit jamais la
    valeur `calves` (aucune donnee reelle pour cette zone), avec message
    explicatif honnete au clic plutot que de l'omettre silencieusement.
    Toutes les zones precedentes conservees (dos/bas du dos en pointille,
    epaules, bras, quadriceps, genoux).
  - Panneau de detail par zone (au clic) et graphique de tendance
    conserves et restyles au theme sombre, logique inchangee.
- `dashboard.js` : reecriture complete pour piloter la nouvelle structure.
  Logique conservee et adaptee (couleurs mises a jour, `colorZones`,
  `scoreBadge`, `renderFactorBars`, selection/detail de zone au clic,
  `drawHistoryChart`, `loadUsers` avec `optgroup` avec/sans donnees,
  toggle reel/demo). Nouvelle logique ajoutee :
  - `computeGlobalScore` / `renderGauge` : jauge SVG via la technique
    `stroke-dasharray`/`stroke-dashoffset` (circonference = 2×π×50), score
    global = MAX des zones (choix conserve et redocumente).
  - `computeTrend(history)` : compare la moyenne de la 2e moitie de
    l'historique a la 1re moitie plutot qu'une comparaison "semaine
    calendaire vs semaine calendaire" — les seances reelles sont espacees
    irregulierement sur ~3 ans, une comparaison calendaire stricte donnerait
    souvent des echantillons vides. Libelle honnete dans l'UI ("2e moitié
    vs 1re moitié de l'historique"), pas de fausse promesse de comparaison
    hebdomadaire.
  - `getDominantFactors(entry)` : facteurs comportementaux (charge/volume/
    recup/duree, `base_zone` exclu car c'est une caracteristique fixe de la
    zone et non un comportement recent) au-dessus de leur valeur neutre 1.0,
    tries par ecart decroissant, top 1-2 renvoyes avec un libelle en
    langage clair ; fallback si aucun facteur n'est eleve (score explique
    uniquement par `base_zone`).
  - `renderSensitiveZones(entries)` : filtre Modere/Eleve, une ligne par
    zone (nom + badge colore + explication des facteurs dominants).
  - `renderLastSessionKpi` / `renderAlertsKpi` / `renderTrendKpi` : les 3
    autres cards KPI, toutes calculees cote client a partir des 3 endpoints
    existants (`/users/{id}/risk`, `/users/{id}/risk/history`,
    `/demo/scenarios`) — **aucun nouvel endpoint** n'a ete necessaire,
    conformement a la contrainte explicite de Moulaye.
  - `renderCalvesExplanation()` : message dedie au clic sur la zone
    "mollets" (toujours grisee), explique pourquoi (`dim_muscle` ne produit
    jamais `calves`) plutot que de laisser un message generique trompeur.

**Verifications effectuees** :
- Coherence statique JS/HTML : script Node.js confirmant que les 31 appels
  `getElementById(...)` de `dashboard.js` correspondent tous a un `id`
  existant dans `index.html` (0 manquant), et que les 9 zones `data-muscle`
  de la silhouette (`back, shoulder, arms, chest, abs, lower_back, legs,
  knee, calves`) sont bien celles attendues par `colorZones`/`onZoneClick`.
- `node --check dashboard.js` : syntaxe valide.
- Accolades CSS equilibrees (55/55).
- 3 blocs `<svg>` de `index.html` verifies bien formes (balises equilibrees)
  via un verificateur de pile Node.js.
- `docker compose up -d --build dashboard` : conteneur reconstruit et
  `healthy`.
- Endpoints retestes via `curl` apres rebuild : `/health` (200, `{"status":
  "ok"}`), `/users` (structure groupee confirmee : `user_id=9` seul dans
  `users_with_data`), `/users/9/risk` (8 zones dont `shoulder` en Modere a
  45.00 — bon cas de test pour le panneau Zones sensibles et les facteurs
  dominants), `/users/9/risk/history`, `/demo/scenarios` (9 scenarios,
  structure inchangee). Fichiers statiques confirmes servis avec le nouveau
  contenu (`dashboard.js` contient bien `renderSensitiveZones`/`renderGauge`
  /`computeTrend`/`getDominantFactors` ; `index.html` contient bien
  `kpi-row`/`gauge-svg`/`sensitive-zones-list`).

> ⚠️ Meme limite que les livraisons precedentes de l'etape 5 : verification
> visuelle du rendu (proportions de la jauge, lisibilite des cards sur fond
> sombre, alignement du panneau Zones sensibles) NON effectuee par Claude
> Code (extension navigateur Chrome indisponible sur toutes les sessions a
> ce jour). Les verifications ci-dessus couvrent l'exactitude structurelle
> (HTML/CSS/JS bien formes, coherence des references, endpoints
> fonctionnels) mais PAS la qualite esthetique du rendu final. A confirmer
> visuellement par Moulaye sur http://localhost:18000 avant de considerer
> cette etape terminee.

### Etape 5/6 — Feature A : Simulateur what-if — 🔄 en cours (backend + structure frontend verifies, rendu visuel a confirmer)

**Date** : 2026-07-06.

**Objectif** : l'utilisateur choisit un exercice deja pratique + des
parametres hypothetiques (charge, reps, series, duree) et voit le risque de
blessure PREDIT sur la zone concernee, sans rien ecrire en base. Formule
deterministe uniquement (pas de ML), reutilisant les memes constantes que
dbt.

**Livre** :
- `dashboard/risk_formula.py` : module Python pur (aucun acces DB), extrait
  de `dbt/models/marts/fact_risk_score.sql`. Fonctions
  `compute_charge_factor`, `compute_volume_factor`, `compute_recup_factor`,
  `compute_duree_factor`, `risk_level_from_score`, `compute_risk_score`.
  Constantes dupliquees a l'identique de dbt (seuils/penalites/bornes de
  normalisation/seuils de niveau) — voir CLAUDE.md pour le detail complet
  des deviations assumees (charge_factor/volume_factor compares a la
  moyenne historique plutot qu'a la semaine precedente, faute de notion de
  "semaine" pour une hypothese ponctuelle).
- `dashboard/main.py` : 2 nouveaux endpoints —
  - `GET /users/{user_id}/exercises` : exercices REELLEMENT deja loggues
    par cet utilisateur (`gold.fact_workout_session`), pour peupler le
    selecteur du simulateur (pas le catalogue complet de 3 177 exercices).
  - `POST /api/simulate-risk` : calcule `risk_score_simule`/
    `risk_level_simule` a partir des parametres hypothetiques, compare a
    `risk_score_actuel`/`risk_level_actuel` (dernier `gold.fact_risk_score`
    connu pour cet exercice, repli sur la zone si jamais loggue), renvoie
    chaque facteur avec une `explication` en langage clair (repli
    exercice->zone->neutre toujours explicite, jamais silencieux).
  - `MUSCLE_LABELS_FR` : petit dictionnaire de libelles FR, duplique
    intentionnellement de celui deja present dans `dashboard.js` (voir
    CLAUDE.md).
- `dashboard/Dockerfile` : ajout de `COPY risk_formula.py .`.
- `dashboard/static/index.html` : nouvelle section `<section
  class="simulator-panel">` (selecteur d'exercice + 4 sliders charge/reps/
  series/duree + bouton Simuler + zone de resultat), ajoutee APRES
  `.main-grid` — silhouette SVG existante non modifiee (0 changement dans
  le bloc `<svg id="silhouette">`).
- `dashboard/static/dashboard.css` : styles du panneau (`.simulator-panel`,
  `.simulator-form`, `.btn-primary`, `.sim-result`) + classe de surcouche
  `.zone.sim-highlight` (contour pointille, `stroke-dasharray`) appliquee
  sur les elements `.zone` EXISTANTS — ne modifie que `stroke` (couleur =
  risque simule), jamais `fill` (qui continue de representer le risque
  reel).
- `dashboard/static/dashboard.js` : `loadSimulatorExercises`,
  `pickDominantSimFactor`, `buildSimMessage`, `renderSimResult` (reutilise
  `renderFactorBars` existant via un objet adapte), `onSimSubmit`,
  `applySimHighlight`/`clearSimHighlight`, `applySimulatorAvailability`
  (desactive le panneau en mode demo). Cable dans `onUserChange` (recharge
  les exercices du nouvel utilisateur) et `applyModeToUI` (toggle
  disponibilite selon reel/demo).

**Verifications effectuees (toutes reelles, aucun dry-run)** :
- `docker compose up -d --build dashboard` : conteneur reconstruit et
  `healthy` (2 fois : apres l'ajout backend, puis apres l'ajout frontend).
- **3 cas reels testes sur `POST /api/simulate-risk`** (`user_id=9`,
  `exercise_id=1870` "Overhead Press (Barbell)", baseline reelle mesuree en
  base : moyenne charge 67.79kg, moyenne volume 24.14 reps, dernier
  `risk_score` connu = 17.58/Faible) :
  1. **Charge en forte hausse** (150kg, 3x10, 45min) -> `risk_score_simule
     = 43.68 (Modere)`, delta `+26.1`. Calcul manuel de verification :
     `0.25 x 1.3 x 1.243 x 1.0 x 1.0 = 0.404` ->
     `100*(0.404-0.05)/0.81 = 43.7` — coherent (ecart d'arrondi
     negligeable).
  2. **Charge en forte baisse** (20kg, 2x8, 30min) -> `risk_score_simule =
     14.28 (Faible)`, delta `-3.3`. Confirme que `charge_factor` reste
     neutre (1.0) meme en forte baisse (la formule dbt ne recompense
     jamais une baisse de charge, uniquement une penalite sur la hausse
     >10% — comportement de la formule d'origine, pas un bug).
  3. **Neutre** (68kg, 3x8, 45min, ~moyenne habituelle) -> `risk_score_simule
     = 24.51 (Faible)`, delta `+6.93`.
  - Dans les 3 cas, `risk_score_actuel = 17.58 (Faible)` renvoye par
    l'endpoint **verifie identique** a la requete directe
    `SELECT risk_score FROM gold.fact_risk_score WHERE user_id=9 AND
    exercise_id=1870 ORDER BY session_date DESC LIMIT 1` (valeur reelle en
    base, memes 17.58).
- **Repli zone teste** : `exercise_id=134` ("Arnold Press", zone shoulder,
  JAMAIS loggue par `user_id=9`) -> baseline repliee sur la moyenne de la
  zone (56.2kg/35 reps, explication du repli explicite dans la reponse),
  `risk_score_actuel` replie sur le dernier score de la zone (`45.00
  Modere`, le 2018-09-29) — **verifie identique** a la requete directe
  filtrant sur `muscle_id` (zone shoulder) au lieu de `exercise_id`.
- **404 verifies** : `user_id` inexistant -> 404, `exercise_id` inexistant
  -> 404.
- Encodage UTF-8 des libelles francais (`Épaules / deltoïdes`, accents)
  **confirme correct** au niveau HTTP (`PYTHONIOENCODING=utf-8` + lecture
  binaire) — un artefact d'affichage mojibake observe dans un premier test
  etait uniquement du a l'encodage de la console Windows locale utilisee
  pour le test, pas un bug de l'API (voir CLAUDE.md si besoin de detail).
- **Verification structurelle du frontend** (Node.js, navigateur
  indisponible) : `node --check dashboard.js` (syntaxe valide), les 28
  references distinctes `getElementById(...)` correspondent toutes a un
  `id` reellement present dans `index.html` (0 manquant), balises
  `<section>`/`<div>` equilibrees (4/4, 18/18), les 3 blocs `<svg>`
  toujours bien formes et **inchanges** (verification explicite que la
  silhouette existante n'a pas ete touchee), accolades CSS equilibrees
  (69/69).
- Fichiers statiques reserves apres rebuild : `/`, `/static/dashboard.js`,
  `/static/dashboard.css` tous `200`, contenu confirme a jour
  (`renderSimResult`/`onSimSubmit`/`pickDominantSimFactor` presents dans le
  JS servi, `simulator-panel`/`sim-exercise-select` presents dans le HTML
  servi).
- **Non-regression verifiee** : `/users`, `/users/9/risk`,
  `/users/9/risk/history`, `/demo/scenarios` tous retestes `200` apres le
  rebuild — rien de casse dans les endpoints/fonctionnalites existants du
  jalon 1.

**Limite honnete documentee** : `recup_factor` du simulateur compare la
date reelle d'aujourd'hui a la vraie derniere `session_date` de la zone en
base (rien d'invente) — mais la derniere seance reelle de `weight_training`
remonte au 2018-09-29, donc ce facteur reste quasi toujours neutre (1.0) en
pratique (ecart > 48h quasi systematique). Constat honnete, pas de date
fictive substituee pour forcer une demonstration du facteur — meme
philosophie que le recalibrage `duree_factor` de l'etape 4.

> ⚠️ **Verification visuelle du panneau Simulateur (rendu des sliders, du
> message genere, des barres de facteurs, de la surcouche pointillee sur la
> silhouette) NON effectuee par Claude Code** : extension navigateur
> indisponible sur cette session comme sur les 3 precedentes. Backend et
> structure JS/HTML/CSS entierement verifies (voir ci-dessus), seul le
> rendu visuel/esthetique reste a confirmer par Moulaye sur
> http://localhost:18000.

## Etape 6/6 — Terraform + AWS S3/Athena (AWS Academy Learner Lab) — 🔄 en cours

### Sous-etape 1/6 — Audit lecture-seule du compte lab — ✅ fait

**Date** : 2026-07-06 (audit initial le 2026-07-04, bloque par des
credentials expires ; complete le 2026-07-06 apres rafraichissement).

**Contexte** : perimetre = Terraform (S3 + Athena) sur un compte AWS
Academy Learner Lab. Aucune ressource AWS creee a ce stade — audit
lecture-seule uniquement, pour ecrire le Terraform de la sous-etape 2/6
sans mauvaise surprise (droits IAM restreints typiques des comptes lab
pedagogiques).

**Livre** :
- `terraform/AWS_LAB_CONSTRAINTS.md` : document de reference complete
  (identite active, policies IAM, acces S3/Athena, region, ARN a utiliser).
- `terraform/versions.tf` (deja present depuis le premier passage du
  2026-07-04) : provider `hashicorp/aws ~> 5.0`, backend `local`
  explicitement choisi (pas de backend S3 distant tant que les droits de
  creation de bucket/policies ne sont pas confirmes en ecriture — l'audit
  actuel est lecture-seule).

**Verifications effectuees (toutes reelles, aucun dry-run)** :
- `aws sts get-caller-identity --profile awslearnerlab` : succes. Compte
  `097115946702`, role actif assume :
  `arn:aws:sts::097115946702:assumed-role/voclabs/user4161432=KOUTAM_Moulaye_Mohamed`.
- `aws iam list-attached-role-policies --role-name voclabs` : succes, 3
  policies (`voc-cancel-cred`, `Pvoclabs1`, `Pvoclabs2`).
- `aws iam get-role --role-name voclabs` : **echec attendu et documente**
  (`AccessDenied`, deny explicite pose par `Pvoclabs2`) — verrouillage
  volontaire du role de controle Vocareum, pas contourne.
- `aws iam get-role --role-name LabRole` : succes. ARN
  `arn:aws:iam::097115946702:role/LabRole`, `MaxSessionDuration=3600`,
  assumable par ~49 services AWS (dont s3/athena/glue/lambda/ec2).
- `aws iam list-attached-role-policies --role-name LabRole` : succes, 7
  policies (4 AWS managees + 3 policies specifiques a l'instance de lab).
- `aws s3 ls --profile awslearnerlab` : succes (exit 0), sortie vide ->
  aucun bucket existant, acces S3 de base confirme.
- `aws athena list-work-groups --profile awslearnerlab --region
  us-east-1` : succes, workgroup `primary` (ENABLED, engine version 3)
  deja present (cree par le setup du lab, pas par ce projet).
- `aws configure get region --profile awslearnerlab` : vide (exit 1) ->
  aucune region par defaut configuree sur le compte.

**Conclusion / decisions pour la suite** :
- **`voclabs`** = role assume par le provider Terraform (via le profil
  `awslearnerlab`) ; **`LabRole`** = role distinct, deja existant dans le
  compte, destine a etre **reference** (jamais cree/modifie) par Terraform
  pour les ressources (ex. role d'execution Glue/EC2).
- **Region `us-east-1` a toujours passer explicitement** (provider
  Terraform + chaque commande CLI) : aucune region par defaut sur le
  compte.
- Aucun blocage IAM identifie pour S3 (lecture confirmee) et Athena
  (workgroup `primary` accessible). Le detail complet est dans
  `terraform/AWS_LAB_CONSTRAINTS.md`, suffisant pour ecrire les premieres
  ressources Terraform sans nouvelle verification manuelle.

### Sous-etape 2/6 — Ressources Terraform S3 + Athena (reelles) + export Gold — ✅ fait

**Date** : 2026-07-06 (meme jour que la sous-etape 1/6).

**Perimetre** : S3 + Athena uniquement (pas de CI/CD GitHub Actions, pas de
pseudonymisation/RGPD a ce stade — rappel explicite recu, a respecter pour
les sous-etapes suivantes). Aucune ressource IAM creee.

**Livre** :
- `terraform/variables.tf`, `terraform/s3.tf`, `terraform/athena.tf`,
  `terraform/outputs.tf` (fichiers separes par domaine).
- `aws_s3_bucket.datalake` (`safelift-datalake-097115946702`) : versioning
  active, chiffrement SSE-S3/AES256 par defaut, acces public totalement
  bloque. 8 objets prefixe (`gold/<table>/` x7 + `athena-results/`).
- `aws_glue_catalog_database.gold` + 7 `aws_glue_catalog_table` (une par
  table Gold), format Parquet, schema colonnes recupere par introspection
  reelle de `information_schema.columns` sur `app-postgres` (schema
  `gold`), pas suppose depuis dbt. Choix documente : Glue Catalog direct
  plutot que `aws_athena_database`/`aws_athena_named_query` (voir
  CLAUDE.md).
- `data.aws_iam_role.lab_role` (nom `LabRole`) : reference en lecture
  seule, expose en output (`lab_role_arn`), aucune ressource IAM creee.
- `scripts/upload_gold_to_s3.py` + `scripts/requirements_aws.txt` : export
  des 7 tables `gold.*` (Postgres) vers Parquet local
  (`data/gold/<table>/<table>.parquet`, deja gitignore) puis upload S3
  (`boto3`, profil `awslearnerlab`, region `us-east-1`). Schema pyarrow
  explicite par table, aligne sur `terraform/athena.tf`. Execute dans un
  venv Windows natif dedie `.venv-aws/` (voir "Bugs/particularites
  rencontres" ci-dessous).
- `.gitignore` : ajout de `.venv-aws/` et `terraform/tfplan.out`.

**Verifications effectuees (toutes reelles)** :
- `terraform plan` : 20 to add, 0 to change, 0 to destroy (verifie avant
  apply, aucune ressource IAM dans le plan).
- `terraform apply` : **20 added, 0 changed, 0 destroyed** — succes complet.
- `scripts/upload_gold_to_s3.py` execute une fois : 7 tables exportees,
  **9 569 lignes au total** (fact_workout_session 2 164, fact_risk_score
  2 164, dim_exercise 3 177, dim_muscle 9, dim_user 973, dim_date 1 073,
  fact_risk_score_demo_synthetic 9) — tous coherents avec les chiffres deja
  documentes en etape 4.
- `aws s3 ls s3://safelift-datalake-097115946702/gold/ --recursive` :
  confirme les 7 fichiers Parquet reels presents (1.3 KB a 100.6 KB) sous
  leurs prefixes respectifs.
- **Requete Athena reelle** (`aws athena start-query-execution` +
  `get-query-results`, workgroup `primary`) : `SELECT COUNT(*) FROM
  gold.fact_risk_score` -> **`2164`**, identique au chiffre dbt connu.
  Requete complementaire `GROUP BY risk_level` -> `Eleve=26`,
  `Faible=1915`, `Modere=223`, identique a la distribution recalibree
  documentee en etape 4 (CLAUDE.md).
- `aws ce get-cost-and-usage` : accessible, `$0` sur la periode
  2026-07-01/07 (delai de publication Cost Explorer habituel ~24h ; volume
  reel de toute facon largement sous le budget de $50 du compte lab).

**Bugs/particularites rencontres et corriges** :
- Le `.venv/` existant (etape 2, telechargement Kaggle) a ete cree sous WSL
  Ubuntu lors d'une session precedente (`pyvenv.cfg` -> `/usr/bin/python3.12`,
  chemin `/mnt/c/...`) ; son binaire `bin/python` ne resout pas dans la
  session Git Bash (MINGW64) native Windows utilisee pour cette sous-etape.
  Resolu en creant un second venv Windows natif dedie `.venv-aws/`
  (`python -m venv .venv-aws`), sans toucher a `.venv/` existant.
- Le workgroup Athena `primary` (deja present sur le compte lab) n'a pas
  d'emplacement de sortie par defaut configure (`ResultConfiguration={}`) :
  chaque requete Athena doit donc passer explicitement
  `OutputLocation=s3://.../athena-results/` (prefixe cree a cet effet dans
  le meme bucket, plutot qu'un bucket separe).
- Types Postgres `numeric` retournes en `Decimal` par `psycopg2`, non
  directement compatibles avec un tableau pyarrow de type `double` : cast
  explicite `float(value)` ajoute dans `to_arrow_value()` avant construction
  du tableau pyarrow.

**Reste a faire** : sous-etape 3/6 (CI/CD) non definie a ce stade — traitee
apres la sous-etape 4/6 ci-dessous, a la demande explicite de l'utilisateur.

### Sous-etape 4/6 — Gouvernance RGPD (pseudonymisation, chiffrement, retention, effacement) — ✅ fait

**Date** : 2026-07-07.

**Perimetre** : Bloc 5 de la certification (gouvernance/RGPD). 5 volets
demandes : pseudonymisation, chiffrement (repos/transit), politique de
retention, droit a l'effacement, data catalog. Voir
`docs/RGPD_GOVERNANCE.md` et `docs/DATA_CATALOG.md` pour le detail complet
(ce resume n'en reprend que les points structurants).

**Livre** :
- `scripts/pseudonymize.py` : module Python pur (aucun acces DB),
  `pseudonymize_user_id(user_id, key)` (HMAC-SHA256), `load_pseudonymization_key()`
  (lit `PSEUDONYMIZATION_KEY` depuis l'environnement/`.env`, erreur explicite
  si absente). **Execute reellement** (`python scripts/pseudonymize.py`,
  auto-test integre) : coherence (meme id+cle -> meme pseudonyme), unicite
  (2 id distincts -> pseudonymes distincts), sensibilite a la cle (cle
  differente -> pseudonyme different).
- **Decision d'architecture tranchee** : pseudonymisation appliquee
  UNIQUEMENT a la couche de restitution externe (export S3/Athena), jamais
  au pipeline interne (Bronze/Silver/Gold Postgres/API dashboard) — voir
  CLAUDE.md pour la justification complete (calcul fait en Python pur plutot
  que dans un modele dbt, pour ne jamais exposer la cle secrete dans du SQL
  compile/des logs Postgres).
- `.env`/`.env.example` : nouvelle variable `PSEUDONYMIZATION_KEY` (cle reelle
  generee via `secrets.token_hex(32)` dans `.env`, valeur factice + avertissement
  dans `.env.example`).
- `scripts/upload_gold_to_s3.py` modifie : `dim_user`/`fact_workout_session`/
  `fact_risk_score` (les 3 tables Gold porteuses de `user_id`) exportent
  desormais `user_pseudo_id` a la place — **teste reellement contre le
  Postgres local** (colonne `user_id` confirmee absente du Parquet exporte,
  `user_pseudo_id` confirme coherent avec un calcul direct independant, sur
  les 3 tables).
- `terraform/athena.tf` : colonnes Glue mises a jour en consequence
  (`user_pseudo_id` string a la place de `user_id` bigint sur les 3 tables
  concernees). `terraform validate` : succes. **`terraform apply` PAS
  effectue** (credentials AWS Learner Lab expires pendant cette session) —
  a faire des que les credentials sont rafraichis (voir TODO Moulaye dans
  CLAUDE.md).
- `docs/RGPD_GOVERNANCE.md` : chiffrement au repos (SSE-S3 confirme via
  `terraform/s3.tf` ; Postgres local confirme NON chiffre, `SHOW ssl;` ->
  `off`, limitation assumee et documentee) et en transit (S3 : HTTPS par
  defaut boto3, confirme par absence de tout `endpoint_url`/`use_ssl=False`
  dans le code ; Postgres : confirme NON chiffre en transit egalement,
  meme limitation) ; politique de retention ecrite par table (12 mois pour
  les tables sans donnee personnelle, 36 mois glissants pour les donnees
  physiologiques/de seance puis agregation anonymisee, `dim_user` = duree du
  compte + 30 jours) — **pas de purge automatique implementee**, politique
  ecrite uniquement, conformement au perimetre demande.
- `scripts/gdpr_erase_user.py` : script d'effacement RGPD (Art. 17), dry-run
  par defaut, `--confirm` pour executer reellement, `--skip-s3` optionnel,
  garde-fou `--i-understand-this-breaks-the-demo` requis pour effacer le
  profil `is_weight_training_demo_user=true`.
  - **Decouverte structurante durant l'ecriture** : `dim_user.user_id` est une
    cle de substitution (`row_number()` dbt sur 5 colonnes triees), pas un
    identifiant stable — une suppression seulement cote `gold.dim_user`
    serait annulee par le prochain `dbt run` complet (Gold est entierement
    recalcule depuis `raw.silver_gym_members`). Le script agit donc sur 4
    couches physiques pour `gym_members` : CSV source
    (`data/bronze/raw/gym_members/*.csv`), toutes les partitions Bronze
    deja materialisees sur disque, Silver, et Gold Postgres — matching par
    egalite EXACTE du tuple (age, gender, body_weight_kg, height_m,
    experience_level), **confirme unique sur les 973 lignes source avant
    d'ecrire le script** (`SELECT count(*) - count(DISTINCT (...))` = 0).
    `fact_workout_session`/`fact_risk_score` supprimees directement par
    `user_id` cote Gold (pas de limite equivalente, aucun agregat partage
    entre utilisateurs a ce grain).
  - **Teste reellement, pas seulement ecrit** :
    - Dry-run sur `user_id=9` (demo user) : compteurs confirmes conformes
      aux chiffres connus (2164 lignes `fact_workout_session`/`fact_risk_score`,
      3 partitions Bronze, 1 ligne Silver/CSV) ; garde-fou verifie (refuse
      sans `--i-understand-this-breaks-the-demo`, exit code 1).
    - Dry-run sur `user_id=4` (profil sans donnee de seance, choisi pour
      etre sans risque) : 0 lignes fact_*, matching confirme sur les autres
      couches.
    - **Execution reelle** (`--confirm`) sur `user_id=4` : suppression
      verifiee sur les 4 couches (`gold.dim_user` 973->972,
      `gold.fact_workout_session`/`fact_risk_score` inchangees car deja a 0
      pour ce profil, Silver 973->972, les 3 partitions Bronze 973->972
      chacune, CSV source 973->972) ; tentative de resynchronisation S3
      declenchee et **echouee proprement** (credentials AWS invalides
      pendant le test -> message d'avertissement clair, script continue
      sans crasher, suppressions locales deja commitees non annulees) —
      confirme que le mecanisme de degradation gracieuse fonctionne
      reellement, pas juste en theorie.
    - **Sauvegarde prealable puis restauration** : fichiers concernes
      (CSV source, 3 partitions Bronze, Silver) sauvegardes avant le test
      destructif (le test modifie durablement la copie locale du dataset
      Kaggle) ; a la demande explicite de l'utilisateur apres le test
      reussi, restaures a l'identique (973 lignes partout), puis
      `gold.dim_user`/`fact_workout_session`/`fact_risk_score` recrees via
      `dbt run --select dim_user fact_workout_session fact_risk_score`
      depuis `raw.silver_gym_members` (jamais modifiee par le script, donc
      restauration exacte confirmee : `user_id=4` de nouveau present,
      demo user toujours `user_id=9`, 973/2164/2164 lignes).
- `docs/DATA_CATALOG.md` : catalogue complet des 4 tables Bronze + 3 Silver +
  7 tables Gold + export S3, avec pour chacune description, colonnes
  sensibles, base legale RGPD presumee, duree de conservation, statut de
  pseudonymisation.

**Limite majeure identifiee et documentee (non corrigee a ce stade)** :
`user_id` (surrogate `row_number()`) n'etant pas un identifiant stable,
toute pseudonymisation/effacement reste structurellement fragile face a un
changement de volumetrie de `gym_members`. Amelioration future proposee (pas
implementee) : remplacer le surrogate par une cle stable (hash d'un
identifiant naturel, ou UUID persiste dans une table de mapping).

**Mise a jour du 2026-07-07 (meme jour, credentials AWS rafraichies)** :
- Cause reelle du blocage initial identifiee : PAS une expiration de session
  Learner Lab, mais un fichier `~/.aws/credentials` malforme
  (`aws_access_key_id=aws_access_key_id=ASIA...`, la valeur contenait son
  propre nom de champ en double) — corrige manuellement.
- `terraform plan` (credentials valides) : 3 to change (les 3 tables Glue
  `dim_user`/`fact_workout_session`/`fact_risk_score`, colonne `user_id`
  bigint -> `user_pseudo_id` string), 0 to add/destroy.
- `terraform apply` : **3 changed, 0 added, 0 destroyed** — succes.
- `scripts/upload_gold_to_s3.py` relance en conditions reelles : 7 tables
  re-exportees vers S3 (9 569 lignes au total), `user_pseudo_id` confirme
  present a la place de `user_id` sur les 3 tables concernees.
- **Requetes Athena reelles de validation** : `SELECT count(*), count(DISTINCT
  user_pseudo_id) FROM gold.fact_risk_score` -> `2164, 1` (coherent : un seul
  `user_id` reel — le demo user — derriere ces 2164 lignes) ;
  `SELECT user_pseudo_id FROM gold.dim_user LIMIT 3` -> 3 hachages
  hexadecimaux de 64 caracteres confirmes, aucun `user_id` en clair visible
  cote AWS.

### Sous-etape 3/6 — CI/CD GitHub Actions (Terraform validate + plan) — ✅ fait

**Date** : 2026-07-08.

**Perimetre** : pipeline GitHub Actions qui valide le code Terraform
(`terraform/`) a chaque pull request/push le touchant, plus un
declenchement manuel. Contrainte structurante deja actee (compte AWS
Academy Learner Lab, credentials temporaires expirant en quelques heures) :
**aucun `terraform apply` automatique**, uniquement `fmt`/`init`/`validate`/
`plan`. L'apply reel reste toujours declenche manuellement par
l'utilisateur, comme pour les sous-etapes 2/6 et 4/6.

**Livre** :
- `.github/workflows/terraform-ci.yml` : job unique `Validate & Plan`,
  declenche sur `pull_request` (chemin `terraform/**`), `push` sur `main`
  (chemin `terraform/**`), et `workflow_dispatch` (test manuel sans
  dependre d'un vrai push/PR). Etapes dans l'ordre : checkout ->
  `hashicorp/setup-terraform@v3` (version `1.5.7`, alignee sur
  `required_version >= 1.5.0` de `terraform/versions.tf`) -> `terraform fmt
  -check -recursive -diff` (echoue explicitement avec message clair si le
  code n'est pas formate) -> `terraform init` (backend local, ne necessite
  aucun acces AWS) -> `terraform validate` (**vrai garde-fou qualite** du
  pipeline, ne necessite aucun acces AWS, doit toujours reussir) ->
  reconstruction de `~/.aws/credentials` (profil `awslearnerlab`, coherent
  avec `terraform/versions.tf` et l'usage local) a partir des secrets
  GitHub `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN` ->
  `terraform plan` avec `continue-on-error: true` explicite (un token
  Learner Lab expire ou des secrets non configures est un cas normal sur ce
  type de compte, ne doit jamais bloquer la pull request) -> etape de
  rapport clair lisant `steps.plan.outcome` (pas `conclusion`, qui serait
  toujours "success" a cause de `continue-on-error`) pour afficher soit un
  succes avec resume, soit un avertissement explicite ("credentials AWS
  probablement expirees... voir AWS_LAB_CONSTRAINTS.md") -> commentaire
  automatique du resume de plan sur la pull request (`actions/github-script@v7`,
  seulement si `github.event_name == 'pull_request'` ET plan reussi ;
  jamais sur un `workflow_dispatch`/push, ce qui explique le `skipped`
  observe lors des tests ci-dessous).
- `.github/workflows/README.md` : documentation dediee — pourquoi jamais
  d'apply automatique, tableau recapitulatif de ce que verifie chaque
  etape (et si un echec y est tolere), procedure d'ajout des 3 secrets
  GitHub (`Settings > Secrets and variables > Actions > New repository
  secret`, URL directe `https://github.com/koutamMoulaye/SafeLift-/settings/secrets/actions`),
  procedure a suivre si `plan` echoue en CI (rafraichir les credentials
  Learner Lab, mettre a jour les 3 secrets, relancer via
  `workflow_dispatch`).
- Aucune valeur de credential committee nulle part : uniquement des
  references `${{ secrets.XXX }}` dans le YAML.

**Verifications effectuees (2 runs reels sur GitHub Actions, aucun
dry-run)** — run `28973493056` sur `koutamMoulaye/SafeLift-`,
https://github.com/koutamMoulaye/SafeLift-/actions/runs/28973493056 :

1. **Tentative 1 (`workflow_dispatch`, secrets GitHub jamais configures a ce
   moment)** : job `Validate & Plan` conclusion **`success`** globale.
   `checkout`/`setup-terraform`/`fmt -check`/`init`/`validate` tous
   `success`. Etape `terraform plan` : **echec reel confirme** (annotation
   GitHub native "Terraform exited with code 1", provenant de
   `hashicorp/setup-terraform` qui remonte le code de sortie reel meme sous
   `continue-on-error`) — cause reelle identifiee avec l'utilisateur : les 3
   secrets GitHub n'avaient jamais ete crees (pas une expiration Learner
   Lab a proprement parler, mais le meme comportement cote pipeline).
   `continue-on-error: true` a bien empeche cet echec de faire echouer le
   job (distinction verifiee concretement entre `outcome` = `failure` de
   l'etape plan et `conclusion` = `success` du job global, exactement le
   comportement recherche). Etape "Commentaire du resume sur la PR" :
   `skipped` (attendu, ce run n'est pas une pull request).
2. **Tentative 2 (re-run du meme run apres ajout des 3 secrets GitHub avec
   des credentials Learner Lab fraiches)** : **10/10 etapes `success`**, y
   compris `terraform plan` cette fois (job id `85979351748`, verifie
   individuellement via l'API GitHub `check-runs/{id}/annotations` : plus
   aucune annotation `failure`, uniquement l'avertissement Node.js 20 sans
   rapport avec Terraform). Confirme que le chemin "credentials valides"
   fonctionne reellement, pas seulement le chemin degrade.

Verification faite via l'API publique GitHub (`api.github.com/repos/.../actions/runs`,
`.../check-runs/{id}/annotations`) plutot que le CLI `gh` (non installe
dans cette session) — le depot est public, ces lectures ne necessitent
aucune authentification.

**Limite assumee** : le commentaire automatique de resume de plan sur les
pull requests (point 5 de la demande initiale) n'a pas pu etre teste en
conditions reelles dans cette session (les 2 runs reels ci-dessus etaient
un `workflow_dispatch`/re-run, pas une vraie pull request) — le code est en
place (`if: github.event_name == 'pull_request' && steps.plan.outcome ==
'success'`) et le comportement `skipped` observe sur les 2 runs est
coherent avec la condition, mais son execution reelle sur une vraie PR
reste a confirmer a la prochaine pull request touchant `terraform/**`.

**Reste a faire** : aucune sous-etape restante pour l'etape 6/6 — voir
resume de cloture du Jalon 1 ci-dessous.

---

## 🏁 Cloture du Jalon 1 — SafeLift (etapes 1 a 6) — 2026-07-08

Les 6 etapes prevues pour ce jalon de la certification RNCP36739 sont
desormais toutes livrees et verifiees en conditions reelles (pas de
pseudo-code, pas de TODO vague). Recapitulatif :

| Etape | Contenu | Statut |
|---|---|---|
| 1/6 | Scaffolding repo + stack Docker locale (Kafka/Zookeeper, Spark, Airflow, 2x Postgres, dashboard placeholder) | ✅ fait |
| 2/6 | Ingestion Bronze (Kaggle -> CSV -> Parquet via Airflow) | ✅ fait (volet B) — *producteur Kafka de donnees simulees non traite, hors perimetre finalement retenu pour ce jalon* |
| 3/6 | Transformation Silver (nettoyage/normalisation Spark, orchestre par Airflow) | ✅ fait |
| 4/6 | Gold : modele en etoile (dbt sur Postgres) + `risk_score` deterministe | ✅ fait |
| 5/6 | Serving : API FastAPI + dashboard theme sombre + simulateur what-if | ✅ fait (backend et structure entierement testes ; **rendu visuel non confirme par Claude Code**, extension navigateur indisponible sur toutes les sessions — voir TODO Moulaye ci-dessous) |
| 6/6 | Terraform (S3 + Athena) + gouvernance RGPD + CI/CD GitHub Actions | ✅ fait — audit (1/6), ressources reelles (2/6), CI/CD (3/6), RGPD (4/6) toutes verifiees en conditions reelles |

**Chaine de bout en bout operationnelle et testee** : Kaggle -> Bronze ->
Silver -> Gold (etoile + risk_score) -> Serving (API + dashboard), avec
declenchement en cascade automatique Bronze -> Silver -> Gold en local
(Airflow), export Gold -> S3/Athena (manuel, script dedie), pseudonymisation
a la restitution externe, et desormais un pipeline CI/CD qui valide chaque
changement Terraform automatiquement (sans jamais appliquer seul).

**Limites/TODO connus qui survivent a la cloture du jalon** (aucun n'est
bloquant pour la certification, tous documentes honnetement plutot que
masques) :
- Licence Kaggle du dataset `weight_training` toujours `unknown` — a
  verifier manuellement par Moulaye avant soutenance (voir
  `data/bronze/SCHEMA_NOTES.md`).
- Rendu visuel du dashboard (theme sombre, jauge, panneau Simulateur)
  jamais confirme par Claude Code faute d'extension navigateur connectee —
  a confirmer par Moulaye sur http://localhost:18000.
- `user_id` (`dim_user`) reste un surrogate `row_number()` non stable :
  toute pseudonymisation/effacement RGPD reste structurellement fragile a
  un changement de volumetrie de `gym_members` (voir CLAUDE.md).
- Commentaire automatique de plan sur pull request (CI/CD) code mais pas
  encore declenche par une vraie PR.
- Producteur Kafka de donnees simulees (volet non traite de l'etape 2/6) —
  a definir explicitement si un jalon futur en a besoin.
