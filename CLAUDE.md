# CLAUDE.md — Memoire de projet SafeLift

> Ce fichier est la source de verite pour reprendre le projet dans une nouvelle
> session, meme sans historique de conversation. A lire EN PREMIER, avec
> [PROGRESS.md](./PROGRESS.md) (Jalon 1, etapes 1 a 6, clos),
> [PROGRESS_JALON2.md](./PROGRESS_JALON2.md) (Jalon 2, streaming affluence,
> clos) et [PROGRESS_JALON3.md](./PROGRESS_JALON3.md) (Jalon 3, nutrition +
> ML bonus, en cours), avant de proposer quoi que ce soit.
>
> Regle : ce fichier doit toujours refleter l'etat REEL du repo. Le mettre a
> jour AVANT de considerer une etape terminee.

## Contexte global

SafeLift est un projet Data Engineering realise dans le cadre de la
certification RNCP36739 (M2 Data Engineering & IA). Il simule un pipeline de
donnees de bout en bout pour un cas d'usage de type "detection d'anomalies /
suivi d'activite" (a preciser/affiner au fil des etapes fonctionnelles).

Le projet est organise en **jalons**. Le **Jalon 1** (6 etapes, voir
[PROGRESS.md](./PROGRESS.md) pour le detail et le statut de chacune) couvre
le pipeline batch complet (Kaggle -> Bronze -> Silver -> Gold -> Serving ->
AWS/Terraform -> gouvernance RGPD -> CI/CD) et est **clos depuis le
2026-07-08**. Le **Jalon 2** (voir
[PROGRESS_JALON2.md](./PROGRESS_JALON2.md)), demarre et **clos le
2026-07-09** (5/5 sous-etapes), ajoute un flux de **streaming temps reel**
(affluence par salle de sport, Kafka -> Spark Structured Streaming) et une
boucle evenementielle utilisateur complete (Kafka -> Postgres -> trigger
dbt) pour couvrir le Bloc 2 (pipelines temps reel) de la certification.
Le **Jalon 3** (voir [PROGRESS_JALON3.md](./PROGRESS_JALON3.md)), demarre
le 2026-07-09, ajoute la **nutrition** (API USDA FoodData Central,
`dim_nutrition`/`fact_nutrition_target`) et une couche **ML bonus**
(sous-etapes suivantes). Chaque etape/sous-etape doit etre livree de
maniere 100% fonctionnelle (pas de pseudo-code, pas de TODO vague) avant
de passer a la suivante.

## Architecture cible (finale, toutes etapes confondues)

```
Ingestion   : Kafka (streaming) + Airflow (orchestration) + Spark (traitement)
Data Lake   : Architecture medaillon Bronze / Silver / Gold, format Parquet
Warehouse   : PostgreSQL + dbt (transformations) + Great Expectations (qualite)
Serving     : API FastAPI + dashboard
Infra       : Docker Compose en local (etapes 1-5)
              -> Terraform + AWS S3/Athena : etape 6, en cours (audit +
              premieres ressources S3/Athena appliquees, voir sous-etapes
              1/6 et 2/6 ci-dessous)
```

Le projet est conteneurise de bout en bout : chaque service tourne dans son
propre conteneur Docker, orchestres via un unique `docker-compose.yml` a la
racine du repo.

## Decisions techniques prises (et pourquoi)

Ces decisions sont figees pour la coherence du projet. Les remettre en cause
uniquement avec une raison explicite, et mettre a jour cette section si elles
changent.

- **Kafka + Zookeeper (pas KRaft)** : images `confluentinc/cp-zookeeper:7.6.1`
  et `confluentinc/cp-kafka:7.6.1`. Choix explicite de Zookeeper plutot que le
  mode KRaft (sans Zookeeper) car l'enonce du projet/certification demande
  explicitement Zookeeper + Kafka, et c'est encore l'architecture la plus
  documentee/enseignee.
- **Deux listeners Kafka** (`PLAINTEXT` interne sur le reseau docker, port
  9092 ; `PLAINTEXT_HOST` externe sur le port hote `KAFKA_EXTERNAL_PORT_EXPOSED`,
  mappe sur le port conteneur 9093) : necessaire pour que les services internes
  (Airflow, Spark) et les outils externes (client Kafka sur la machine hote)
  puissent tous les deux se connecter au broker sans conflit d'adresse annoncee.
- **Topic de test cree via un conteneur "one-shot" (`kafka-init`)** plutot que
  manuellement : garantit que `docker compose up` seul suffit a avoir un
  environnement Kafka pret a l'emploi et testable immediatement.
- **Spark en mode standalone** (`apache/spark:3.5.8-python3`, 1 master +
  1 worker) : suffisant pour le developpement local a ce stade ; pas besoin de
  YARN/K8s pour l'instant. Les jobs Spark seront montes depuis `./spark/jobs`.
  Image officielle Apache (pas Bitnami) : Bitnami a retire les tags versionnes
  gratuits de `bitnami/spark` (seul `latest`/versions majeures recentes restent
  disponibles hors abonnement), l'image `bitnami/spark:3.5` n'existe donc plus.
  L'image `apache/spark` ne fournit pas de mode "master/worker" cle en main
  (pensee pour spark-submit/K8s) : le master et le worker sont demarres en
  overridant l'entrypoint pour appeler directement
  `spark-class org.apache.spark.deploy.master.Master` /
  `org.apache.spark.deploy.worker.Worker spark://spark-master:7077`.
- **Airflow en LocalExecutor** (pas CeleryExecutor) : un seul worker suffit en
  local, evite la complexite additionnelle de Celery/Redis pour cette phase du
  projet. Image `apache/airflow:2.10.4-python3.12`, etendue via
  `airflow/Dockerfile` pour installer les providers Kafka/Spark listes dans
  `airflow/requirements.txt` (installation a la construction de l'image, pas au
  runtime via `_PIP_ADDITIONAL_REQUIREMENTS`, pour des demarrages rapides et
  reproductibles).
- **Deux instances PostgreSQL bien distinctes** :
  - `airflow-postgres` : metadata DB d'Airflow UNIQUEMENT (DAGs runs, taches,
    connexions...). Volume `airflow_postgres_data`, port hote
    `AIRFLOW_POSTGRES_PORT_EXPOSED` (15433 par defaut).
  - `app-postgres` : futur data warehouse / schema en etoile applicatif (peuple
    a partir de l'etape 4 avec dbt). Volume `app_postgres_data`, port hote
    `APP_POSTGRES_PORT_EXPOSED` (15432 par defaut).
  - Ne JAMAIS les fusionner ni confondre leurs identifiants : c'est une source
    frequente de bugs si un DAG se connecte par erreur a la mauvaise base.
- **Tous les ports sont configurables via `.env`** (jamais hardcodes dans
  `docker-compose.yml`), avec des valeurs par defaut volontairement "non
  standards" (ex: Airflow sur 18089 et non 8080) pour limiter les conflits si
  plusieurs projets Docker tournent en parallele sur la machine du
  developpeur.
- **`COMPOSE_PROJECT_NAME=safelift`** et reseau Docker nomme explicitement
  `safelift-network` : isole ce projet des autres projets Docker locaux.
- **Dashboard placeholder en FastAPI** (`dashboard/main.py`) : ne fait
  qu'exposer `/health`. Sera etoffe en toute fin de projet (etape serving) une
  fois le warehouse Gold disponible.
- **`.env` reel jamais commite** (`.gitignore`), seul `.env.example` est
  versionne avec des valeurs factices/placeholders.
- **Healthcheck Zookeeper** : la commande 4 lettres `ruok` est desactivee par
  defaut sur `confluentinc/cp-zookeeper:7.6.1` et cette version de l'image ne
  supporte pas de variable `ZOOKEEPER_4LW_COMMANDS_WHITELIST` dediee (verifie
  en inspectant `/etc/confluent/docker/zookeeper.properties.template` dans le
  conteneur). Contournement : `KAFKA_OPTS: -Dzookeeper.4lw.commands.whitelist=ruok`
  (le script de lancement passe `KAFKA_OPTS` tel quel a la JVM). Le test du
  healthcheck utilise aussi un piege classique bash a eviter : `echo ruok |
  timeout 2 bash -c 'cat < /dev/tcp/...'` n'envoie PAS "ruok" dans le socket
  (le pipe alimente `timeout`, pas le `cat` imbriqué) ; il faut ouvrir un
  descripteur bidirectionnel explicite : `exec 3<>/dev/tcp/host/port && echo -n
  ruok >&3 && timeout 2 cat <&3`.
- **Environnement Python isole dans un venv projet (`.venv/` a la racine)**
  pour tous les scripts hors Docker (ex: `data/download_datasets.sh`), plutot
  que des installs `--user`/`--break-system-packages` sur le systeme. Cause
  racine du bug qui a motive ce choix : la CLI `kaggle` echouait avec
  `ModuleNotFoundError: No module named 'kagglesdk.competitions.legacy'` — ce
  n'etait PAS un conflit entre plusieurs installations Python (une seule
  installation de `kagglesdk` a ete trouvee sur le systeme), mais un bug reel
  dans les wheels PyPI `kagglesdk` 0.1.31 et 0.1.32 : leur propre fichier
  `kagglesdk/competitions/types/host_service.py` importe
  `kagglesdk.competitions.legacy.types.legacy_competition_host_service`, un
  module absent du package publie (verifie en telechargeant et inspectant le
  `.whl` directement depuis PyPI). Fix : pin `kagglesdk==0.1.30` (derniere
  version sans ce module casse, toujours compatible avec la contrainte
  `kaggle==2.2.3` -> `kagglesdk<1.0,>=0.1.30`), installe uniquement dans
  `.venv/` via `data/requirements.txt`.
  Complication rencontree sur cette machine (WSL Ubuntu) : `python3 -m venv`
  echoue sans le paquet systeme `python3.12-venv` (necessite `sudo`,
  indisponible dans cette session). Solution generique adoptee dans le script :
  tenter `python3 -m venv` et, en cas d'echec (ensurepip absent), basculer
  automatiquement sur le paquet PyPI `virtualenv` (installe via
  `pip install --user --break-system-packages`), qui embarque son propre pip
  et ne necessite aucun paquet systeme ni privilege root.
- **`bitnami/spark:3.5` n'existe plus** (Bitnami a retire les tags versionnes
  gratuits mi-2025). Remplace par l'image officielle `apache/spark:3.5.8-python3`,
  qui n'a pas de mode "master/worker" cle en main : le master et le worker sont
  lances en overridant l'entrypoint avec `spark-class
  org.apache.spark.deploy.master.Master` / `...deploy.worker.Worker
  spark://spark-master:7077` directement (voir docker-compose.yml).
- **Bronze = une table par fichier CSV source, jamais de jointure.** Les 2
  fichiers du dataset `600k_fitness` (`program_summary.csv` et
  `programs_detailed_boostcamp_kaggle.csv`) restent 2 tables Bronze distinctes
  (`600k_fitness_summary` / `600k_fitness_detailed`) car ce sont deux grains
  differents (programme vs detail exercice/semaine/jour) : les fusionner
  demanderait une jointure sur `title`, hors perimetre du Bronze. Le DAG
  `bronze_ingestion.py` a donc 4 tasks (une par CSV), pas 3.
- **Partitionnement Bronze par `ingestion_date={{ ds }}`** (date logique du
  DAG run Airflow, pas la date reelle d'execution) : c'est le pattern
  idempotent standard Airflow — rejouer le meme run logique ecrase la meme
  partition au lieu d'accumuler des doublons. La task supprime entierement le
  dossier de partition avant de reecrire (`shutil.rmtree` puis
  `mkdir`+`to_parquet`), verifie concretement en re-executant une task pour la
  meme date logique (voir PROGRESS.md etape 2 volet B).
- **`./data:/opt/airflow/data` monte sur les services Airflow** (init,
  webserver, scheduler) dans `docker-compose.yml` — necessaire pour que les
  DAGs lisent `data/bronze/raw/` et ecrivent dans `data/bronze/{table}/`.
  Aucun autre service (Kafka, Spark, dashboard) n'a ete modifie pour cette
  etape.
- **`pandas` pin sur `2.1.4`, pas `2.2.x`**, dans `airflow/requirements.txt` :
  `apache-airflow-providers-google` et `apache-airflow-providers-snowflake`
  (deja presents dans l'image de base `apache/airflow`, meme si non utilises
  par ce projet) exigent `pandas<2.2,>=2.1.2`. Installer 2.2.x provoque un
  conflit de dependances signale par pip au build de l'image Docker.
- **`data/bronze/SCHEMA_NOTES.md` et `data/silver/CLEANING_LOG.md` sont des
  exceptions explicites dans `.gitignore`** : le reste de `data/bronze/*` et
  `data/silver/*` est ignore (donnees brutes/generees), mais ces deux
  fichiers sont de la documentation versionnee, pas des donnees.
- **Silver : image Airflow alignee sur Python 3.10 (`apache/airflow:2.10.4-python3.10`),
  pas 3.12.** PySpark exige que driver et executeurs tournent sur la meme
  version mineure de Python des qu'un UDF Python est utilise (nos jobs
  `silver_600k_fitness_*` en utilisent une pour parser `level`/`goal`).
  L'image officielle `apache/spark:3.5.8-python3` embarque Python 3.10 (non
  modifiable sans maintenir une image Spark personnalisee) : c'est donc le
  cote Airflow (plus simple a changer, un seul Dockerfile) qui a ete aligne.
  `pyspark==3.5.8` est egalement epingle explicitement dans
  `airflow/requirements.txt` (la contrainte souple de
  `apache-airflow-providers-apache-spark` tire sinon la derniere version
  PyPI, incompatible avec la version reelle du cluster).
- **JRE ajoute dans `airflow/Dockerfile`** (`openjdk-17-jre-headless`) : pyspark
  a besoin d'une JVM meme cote driver (spark-submit en mode client), et
  l'image `apache/airflow` de base n'en embarque aucune. `JAVA_HOME` est
  calcule dynamiquement au build (`readlink -f /usr/bin/java` + symlink
  stable `/usr/lib/jvm/default-java`) plutot que code en dur
  (`.../java-17-openjdk-arm64`), pour rester portable entre architectures.
- **`spark.hadoop.fs.permissions.umask-mode=000` dans la commande spark-submit**
  (voir `airflow/dags/silver_transformation.py`) : le driver Spark (conteneur
  airflow-*, uid 50000) et les executeurs (conteneur spark-worker, uid 185
  "spark") n'ont pas le meme UID sur le bind mount `./data` partage. Sans ce
  reglage, un repertoire cree par le driver (umask 755, proprietaire seul)
  est illisible en ecriture par l'executeur -> `IOException: Mkdirs failed`.
  Ce reglage force Hadoop (utilise en interne par Spark pour l'IO fichier) a
  toujours creer repertoires/fichiers en 777.
- **Silver lit uniquement la DERNIERE partition Bronze**
  (`silver_common.latest_bronze_partition_path`), jamais l'historique complet
  des `ingestion_date=` : Bronze est un reload complet du CSV source a
  chaque run (pas incremental), donc lire toutes les partitions dupliquerait
  les lignes d'une ingestion a l'autre.
- **Chemin `/opt/data` commun aux conteneurs airflow-*/spark-master/spark-worker**
  (`./data:/opt/data` dans `docker-compose.yml`, en plus de
  `/opt/airflow/data` deja utilise par `bronze_ingestion.py`) : necessaire
  car en mode client, le driver Spark (dans le conteneur qui a soumis le
  job) et les executeurs (spark-worker) doivent voir le meme chemin absolu
  pour lire/ecrire les fichiers du data lake de maniere coherente. Le chemin
  `/opt/airflow/data` existant n'a pas ete renomme (pour ne pas casser le
  DAG Bronze deja teste), d'ou la coexistence des deux points de montage
  pointant vers le meme `./data` hote.
- **Dependance bronze_ingestion -> silver_transformation via
  `TriggerDagRunOperator`** (task ajoutee en fin de `bronze_ingestion.py`),
  pas un `ExternalTaskSensor` : les deux DAGs sont a declenchement manuel
  (`schedule=None`), un `ExternalTaskSensor` aurait exige de faire
  correspondre les dates logiques des deux DAG runs, fragile pour des
  declenchements manuels independants. Verifie concretement : trigger de
  `bronze_ingestion` -> declenchement automatique confirme de
  `silver_transformation` (visible dans `airflow dags list-runs`).
- **Silver : parsing `level`/`goal` (listes Python stringifiees) en vraies
  colonnes `array<string>`** plutot qu'explode ou one-hot — voir
  `data/silver/CLEANING_LOG.md` pour le raisonnement complet.
- **`weight_kg` volontairement evite comme nom de colonne commun** entre
  `gym_members` et `weight_training` malgre la meme unite (kg) : ce sont deux
  grandeurs physiques differentes (poids corporel vs poids souleve). Noms
  retenus : `body_weight_kg` / `lifted_weight_kg`. Voir CLEANING_LOG.md.
- **Gold : dbt opere sur Postgres (`app-postgres`, schema `raw` -> `staging`
  -> `gold`), pas directement sur les Parquet Silver.** Un job Spark
  (`spark/jobs/load_silver_to_postgres.py`, JDBC) recharge les 4 tables
  Silver dans `raw.*` avant que dbt ne construise dessus. `truncate=true`
  (pas `overwrite`/DROP) obligatoire : les modeles staging dbt sont des VUES
  qui referencent `raw.*`, un DROP TABLE echoue des le 2e run.
- **dbt tourne dans un venv Python isole `/opt/dbt_venv`**, separe de
  l'environnement Airflow (`dbt-core`/Airflow ont des contraintes de version
  conflictuelles sur `click`/`jinja2`...). `dbt-core==1.8.2` /
  `dbt-postgres==1.8.2` (`1.8.9` n'existe pas pour `dbt-postgres`). Toujours
  invoque via chemin complet (`/opt/dbt_venv/bin/dbt`), jamais dans le PATH
  global. Le venv doit etre cree sous `USER airflow` (l'image `apache/airflow`
  refuse `pip install` en root).
- **fact_workout_session = weight_training UNIQUEMENT** (decision
  d'architecture actee) ; `600k_fitness` (summary+detailed) sert seulement de
  catalogue pour `dim_exercise`, jamais de source de faits, jamais de
  jointure directe avec weight_training au niveau des faits.
- **`muscle_group` = classification heuristique par mots-cles**
  (`dbt/macros/classify_muscle_group.sql`), AUCUNE source ne fournit cette
  colonne nativement. Taux de matching reel exercice weight_training <->
  catalogue 600k_fitness : **38.3%** (31/81). Les non-matches recoivent
  `muscle_group='unknown'`, `is_matched=false`, ajoutes a `dim_exercise` sans
  perdre de ligne de fait. Detail complet et avertissements :
  `data/gold/GOLD_MODEL_DECISIONS.md`.
- **`dim_muscle.base_epidemiological_risk`** : 0.25/0.20/0.18 pour
  shoulder/knee/lower_back, **0.10 par defaut ailleurs — hypothese de
  modelisation explicitement marquee comme telle dans le code ET la doc,
  PAS une donnee epidemiologique verifiee.**
- **`dim_user` : weight_training rattache a UN SEUL profil de gym_members**
  (`user_id=9`, experience_level max) — **hypothese de demonstration**,
  aucune cle commune reelle entre les 2 sources. Colonne
  `is_weight_training_demo_user` marque ce profil.
- **`risk_score`** (dbt, `fact_risk_score.sql`) : formule deterministe
  `base_zone x charge_factor(1.3 si +10%/semaine) x volume_factor([0.5,2.0])
  x recup_factor(1.4 si <48h) x duree_factor(1.2 si >2h)`, normalisee 0-100
  (bornes theoriques 0.05-1.092). Tous les facteurs sont des colonnes
  visibles (pas de boite noire). Distribution reelle observee (2164 lignes) :
  Faible 97.8%, Modere 2.2%, Eleve 0% — absence d'"Eleve" expliquee par
  `duree_factor` qui ne se declenche jamais sur ce dataset
  (`duration_seconds` a 0 quasi-partout), documentee comme un constat, pas
  masquee.
- **Tests dbt natifs (pas Great Expectations)** : `not_null`/`unique`/
  `relationships`/`accepted_values` (integres a dbt-core) + 2 tests
  singuliers SQL. Couvre les 3 exigences (bornes risk_score, integrite
  referentielle, pas de doublon de grain) sans dependance supplementaire.
- **Bug reel detecte par les tests** : grain de `fact_workout_session`
  initialement agrege sur `exercise_name` brut au lieu de
  `normalized_exercise_name` -> 32 groupes en double, detectes par
  `tests/assert_fact_workout_session_grain_unique.sql`. Corrige (2196 -> 2164
  lignes). Preuve que les tests dbt de ce projet detectent de vrais bugs, pas
  du theatre.
- **Matching exercise_name : pipeline a 4 etapes en cascade** (strict ->
  nom de base sans equipement -> fuzzy `rapidfuzz` seuil 85% -> mapping
  manuel), taux **38.3% -> 90.1%** (73/81). Le fuzzy matching tourne HORS
  dbt (`scripts/fuzzy_match_exercises.py`, Python + `rapidfuzz`) car
  dbt-postgres ne supporte pas les modeles Python (reserves a
  Snowflake/Databricks/BigQuery) — ecrit `raw.fuzzy_exercise_matches`, relu
  par `dim_exercise.sql`. **Echantillon fuzzy verifie manuellement : 25%
  d'erreur (2/8 faux positifs)** — jamais faire confiance a un score fuzzy
  eleve sans verification humaine. Detail complet :
  `data/gold/GOLD_MODEL_DECISIONS.md` sections 2/2bis/2ter.
- **risk_score recalibre (0% -> 1.2% "Eleve")** : la borne de normalisation
  incluait un plafond `duree_factor=1.2` jamais atteint en pratique
  (`duration_seconds` fiable a 0% sur ce dataset), compressant tous les
  scores reels. Corrige en excluant ce plafond du calcul des bornes
  (`risk_score_min_raw`/`risk_score_max_raw`, variables dbt partagees) —
  **aucun poids ni seuil modifie**, seule une borne theorique jamais
  realisee recalibree. Voir GOLD_MODEL_DECISIONS.md section 8.
- **`gold.fact_risk_score_demo_synthetic`** : table SEPAREE (pas un flag)
  de 9 scenarios fictifs, `is_synthetic_demo=true` sur 100% des lignes,
  jamais melangee aux vraies stats. Anticipe le besoin du futur dashboard
  (bascule vue reelle/demo), maintenue meme apres l'apparition de vrais
  scores "Eleve" (utile pour des exemples canoniques garantis).
- **Etape 5 (serving) : dashboard FastAPI + silhouette SVG, PAS de framework
  frontend.** `dashboard/static/` sert du HTML/CSS/JS natif (fetch, pas de
  React/Vue) — suffisant pour une demo jury de 5 minutes, evite une chaine de
  build frontend. Silhouette = zones SVG cliquables avec `data-muscle`
  mappe directement sur `muscle_group` (`dim_muscle`) ; `back`/`lower_back`
  affiches en pointille sur la vue de face (zones anatomiquement dorsales,
  simplification assumee et indiquee au clic).
- **`GET /users` expose `has_risk_data`** : sur les 973 profils `dim_user`,
  UN SEUL (`user_id=9`, le "demo user" de l'etape 4) a reellement des
  donnees dans `fact_risk_score`. Plutot que de laisser le dashboard
  afficher un ecran vide sans explication pour les 972 autres, l'API
  signale explicitement ce cas — coherent avec l'hypothese de rattachement
  dim_user deja documentee (GOLD_MODEL_DECISIONS.md section 5).
- **Separation stricte reel/demo cote frontend** : un seul toggle, jamais
  les deux sources affichees simultanement ; bandeau ambre sticky visible en
  permanence quand le mode demo est actif.
- **Dashboard connecte a `app-postgres` en LECTURE SEULE** (schema `gold`
  uniquement) : `depends_on: app-postgres (service_healthy)` ajoute dans
  `docker-compose.yml`, aucun autre service touche.
- **`GET /users` renvoie `{users_with_data, users_without_data}`** (pas une
  liste plate) : sur 973 profils `dim_user`, un seul (`user_id=9`) a des
  donnees reelles. Le dropdown pre-selectionne toujours un profil
  `users_with_data` en premier (jamais un ecran vide par defaut) ; les
  profils sans donnee restent accessibles dans un `<optgroup>` secondaire
  au label explicite. Limite assumee : pas de collapse natif sur un
  `<optgroup>` HTML (pas de composant de liste personnalise construit, pour
  rester en vanilla JS et robuste sans verification visuelle en direct).
- **Silhouette en courbes de Bezier (pas de rectangles/ellipses juxtaposes)** :
  contour du corps = UNE seule courbe continue (cou/epaules/taille/hanches),
  zones colorables superposees en semi-transparence (`fill-opacity`, pas de
  contour epais) pour un rendu plus doux. Zones dorsales (`back`/
  `lower_back`) toujours en pointille fin (rappel de simplification).
- **Score global du panneau de detail = MAX des risk_score par zone, PAS
  une moyenne** : un outil de suivi de risque doit signaler la zone la plus
  a risque, pas la diluer (7 zones Faible + 1 Eleve ne doit jamais
  apparaitre "Modere" en moyenne). Choix documente dans `dashboard.js` et
  `data/gold/GOLD_MODEL_DECISIONS.md`.
- **Barres de facteurs normalisees PAR FACTEUR** (pas une echelle commune) :
  chaque barre utilise les bornes min/max documentees de CE facteur precis
  (`FACTOR_BOUNDS` dans `dashboard.js`, memes valeurs que
  GOLD_MODEL_DECISIONS.md section 8) — une barre pleine signifie "ce
  facteur est a son maximum possible dans le modele", comparable entre
  facteurs malgre des unites/amplitudes differentes.
- **Refonte visuelle theme sombre "app tracker" (Round 2, 2026-07-04)** :
  demande explicite de Moulaye apres jugement "encore insuffisant" du
  premier design (silhouette Bezier + score/barres). Refonte du LOOK &
  FEEL uniquement (API/logique metier inchangees) : palette sombre
  centralisee (variables CSS `:root`), 4 cards KPI (derniere seance, jauge
  radiale, tendance, zones en alerte), jauge SVG via
  `stroke-dasharray`/`stroke-dashoffset`, nouveau panneau "Zones sensibles"
  (liste textuelle des zones Modere/Eleve avec facteurs dominants explique
  en langage clair, AVANT le clic sur la silhouette), silhouette enrichie
  (pectoraux en 2 moities, zone "mollets" ajoutee pour completude
  anatomique mais toujours grisee — `dim_muscle` ne produit jamais
  `calves`). Toute la nouvelle logique (tendance, facteurs dominants,
  KPIs) est calculee cote client a partir des 3 endpoints EXISTANTS —
  aucun nouvel endpoint necessaire.
- **`computeTrend` compare 2 moities chronologiques de l'historique**, pas
  un decoupage calendaire ("semaine vs semaine") : les seances reelles sont
  espacees irregulierement sur ~3 ans, une comparaison calendaire stricte
  donnerait souvent des echantillons vides. Libelle honnete dans l'UI
  ("2e moitié vs 1re moitié"), pas de fausse promesse de granularite
  hebdomadaire.
- **`getDominantFactors` exclut `base_zone`** du calcul des facteurs
  "explicatifs" d'une zone sensible : `base_zone` est une caracteristique
  fixe de la zone (pas un comportement recent de l'utilisateur), inclure ce
  facteur rendrait l'explication trompeuse ("c'est de ta faute" alors que
  c'est structurel). Seuls charge/volume/recup/duree_factor au-dessus de
  1.0 (neutre) sont candidats, top 1-2 par ecart decroissant.
- **Verification visuelle du dashboard toujours non effectuee par Claude
  Code** (3 sessions consecutives desormais, extension navigateur
  indisponible) : analyse statique programmatique faite a la place (XML du
  SVG bien forme, JS/CSS syntaxiquement valides, coherence des `id`
  HTML<->JS verifiee par script), mais qualite visuelle/esthetique
  (proportions, lisibilite sur fond sombre, alignement) non confirmee. Voir
  TODO Moulaye ci-dessous.
- **Etape 6 : compte AWS Academy Learner Lab — role assume par le provider
  Terraform (`voclabs`, via profil `~/.aws/credentials` `[awslearnerlab]`)
  distinct du role a attacher aux ressources creees (`LabRole`, ARN
  `arn:aws:iam::097115946702:role/LabRole`).** Ne jamais creer/modifier
  `LabRole` dans Terraform (existe deja dans le compte lab), seulement le
  referencer via `data "aws_iam_role"`. `aws iam get-role --role-name
  voclabs` echoue avec un `AccessDenied` explicite (deny pose par la policy
  `Pvoclabs2`) — comportement attendu du lab (verrouillage volontaire du
  role de controle Vocareum), pas un bug a contourner. Aucune region par
  defaut configuree sur le compte -> **toujours `us-east-1` explicite**,
  dans le provider Terraform ET dans chaque commande AWS CLI. Detail complet
  de l'audit (identite, policies, acces S3/Athena, region) dans
  `terraform/AWS_LAB_CONSTRAINTS.md`.
- **Etape 6, sous-etape 2/6 : Athena via `aws_glue_catalog_database` +
  `aws_glue_catalog_table`, PAS `aws_athena_database`/`aws_athena_named_query`.**
  Athena utilise de toute facon AWS Glue Data Catalog comme metastore par
  defaut (le catalogue "Athena-managed" interne est deprecie) : declarer
  directement les ressources Glue est le chemin le plus direct, evite de
  dependre de l'execution d'une requete `CREATE DATABASE` et d'un bucket de
  resultats des la creation de la base. Schema des colonnes de chaque table
  Athena (`terraform/athena.tf`) recupere par **introspection reelle** de
  `information_schema.columns` sur `app-postgres` (schema `gold`), pas
  suppose depuis `dbt/models/marts/_marts__models.yml` (qui ne documente
  que les colonnes couvertes par un test dbt, pas le schema complet).
- **`.venv-aws/` : second venv Python dedie au script
  `scripts/upload_gold_to_s3.py`, separe du `.venv/` existant (etape 2).**
  Le `.venv/` existant a ete cree sous WSL Ubuntu lors d'une session
  precedente (`pyvenv.cfg` pointe vers `/usr/bin/python3.12`, chemin
  `/mnt/c/...`) ; son binaire `bin/python` ne resout pas dans une session
  Git Bash (MINGW64) native Windows — environnement shell different de
  celui qui avait cree `.venv/` a l'origine. Plutot que de recreer/casser
  `.venv/` (potentiellement encore utilise depuis WSL), un second venv
  Windows natif (`python -m venv .venv-aws`) a ete cree, dependances
  pinnees dans `scripts/requirements_aws.txt`.
- **`scripts/upload_gold_to_s3.py` tourne HORS Docker, connexion a
  `app-postgres` via `host=localhost` + `APP_POSTGRES_PORT_EXPOSED` (15432),
  PAS `host=app-postgres`/port interne 5432** (contrairement a
  `dashboard/main.py`/`scripts/fuzzy_match_exercises.py`, qui tournent dans
  des conteneurs sur le reseau Docker interne) : ce script s'execute depuis
  le poste de developpement, pas dans un conteneur.

### Feature A — Simulateur what-if (2026-07-06)

- **`dashboard/risk_formula.py` : duplication ASSUMEE et documentee de la
  formule de risque de `dbt/models/marts/fact_risk_score.sql`.** dbt calcule
  `risk_score` en BATCH sur l'historique reel (agregations SQL par
  semaine/session, deja enregistrees) ; ce module calcule un score
  EQUIVALENT a la volee sur une hypothese NON enregistree, qui n'a par
  definition aucune "semaine suivante"/"session suivante" a agreger. Les
  CONSTANTES (seuil charge +10%, penalite charge x1.3, bornes volume
  [0.5, 2.0], seuil recup 48h, penalite recup x1.4, seuil duree 7200s,
  penalite duree x1.2, bornes de normalisation 0.05/0.86, seuils de niveau
  33/66) sont recopiees a l'identique de dbt. **Toute evolution future de
  `fact_risk_score.sql` ou des vars dbt `risk_score_min_raw`/
  `risk_score_max_raw` (`dbt/dbt_project.yml`) DOIT etre repercutee
  manuellement dans `risk_formula.py`** — aucune synchronisation
  automatique entre les deux. `base_zone` n'est PAS duplique (ce n'est pas
  une constante de formule mais une DONNEE par zone) : toujours lu en
  direct dans `gold.dim_muscle.base_epidemiological_risk`.
- **`charge_factor`/`volume_factor` hypothetiques comparent a la MOYENNE
  historique reelle de l'utilisateur, pas a la semaine precedente
  (deviation assumee vs dbt).** dbt compare la charge/volume de la semaine
  courante a la semaine precedente (`lag()` SQL) — une hypothese ponctuelle
  n'a pas de "semaine precedente". La moyenne historique
  (`avg(lifted_weight_kg)`/`avg(total_reps)` sur `gold.fact_workout_session`)
  est l'equivalent le plus proche et le plus stable pour une simulation
  hors calendrier. Memes seuils/penalites/bornes que dbt, seule la base de
  comparaison change.
- **Repli en cascade documente pour les baselines charge/volume/score
  actuel : EXERCICE precis d'abord, puis ZONE musculaire (tous exercices
  confondus), puis neutre (1.0)/absent si aucune des deux n'existe.**
  Jamais silencieux : chaque repli est explicite dans le champ
  `explication` du facteur concerne (`POST /api/simulate-risk`). Necessaire
  car la plupart des `exercise_id` du catalogue `dim_exercise` (3 177
  lignes) n'ont jamais ete pratiques par l'unique "demo user" (`user_id=9`,
  81 exercices distincts reellement loggues) — voir
  `data/gold/GOLD_MODEL_DECISIONS.md` section 5.
- **`recup_factor` hypothetique compare la VRAIE derniere `session_date`
  de la zone (deja en base) a la date reelle d'AUJOURD'HUI — jamais
  invente, mais quasi toujours neutre (1.0) sur ce dataset en pratique.**
  La derniere seance reelle de `weight_training` remonte au 2018-09-29 ;
  compare a une date d'appel API bien plus recente, l'ecart depasse presque
  toujours 48h. **Constat honnete documente** (meme philosophie que le
  recalibrage `duree_factor` de l'etape 4), pas de date fictive substituee
  pour "forcer" une demonstration du facteur.
- **`GET /users/{user_id}/exercises` (endpoint de support, ajoute pour
  cette feature) : restreint aux exercices REELLEMENT deja loggues par cet
  utilisateur** (`gold.fact_workout_session`), pas le catalogue complet
  `dim_exercise` — necessaire pour que le selecteur du simulateur ne
  propose que des exercices avec une baseline reelle exploitable.
  `POST /api/simulate-risk` reste neanmoins robuste a un `exercise_id`
  jamais pratique (repli zone documente ci-dessus), au cas ou l'endpoint
  serait appele directement hors du dashboard.
- **`MUSCLE_LABELS_FR` duplique intentionnellement entre
  `dashboard/static/dashboard.js` et `dashboard/main.py`.** Petit
  dictionnaire statique (10 entrees), pas assez volumineux pour justifier
  une source de verite partagee (fichier JSON/config charge des deux
  cotes) — mais **si l'un des deux dictionnaires change, l'autre doit etre
  mis a jour en miroir**, sans quoi le libelle FR affiche par le frontend
  (deja connu cote client) et celui renvoye par l'API (`muscle_zone`)
  pourraient diverger silencieusement.
- **Surcouche silhouette SVG = classe CSS `.sim-highlight` (contour
  pointille) appliquee sur les elements `.zone` EXISTANTS, aucune geometrie
  SVG ajoutee ni modifiee.** Respecte la contrainte "ne pas modifier la
  silhouette existante" : le remplissage (`fill`) continue de representer
  le risque REEL (`colorZones`), la surcouche ne touche que le `stroke`
  (couleur = niveau de risque SIMULE), ajoutee/retiree dynamiquement par
  `applySimHighlight()`/`clearSimHighlight()`.
- **Simulateur desactive en mode demo** (`applySimulatorAvailability()`) :
  les scenarios synthetiques (`gold.fact_risk_score_demo_synthetic`) n'ont
  pas d'`user_id`/`exercise_id` reels a simuler — coherent avec la
  separation stricte reel/demo deja actee en etape 5.

### Etape 6/6, sous-etape 4/6 — Gouvernance RGPD (2026-07-07)

Voir `docs/RGPD_GOVERNANCE.md` et `docs/DATA_CATALOG.md` pour le detail
complet. Resume des decisions structurantes :

- **Pseudonymisation (`scripts/pseudonymize.py`, HMAC-SHA256) appliquee
  UNIQUEMENT a la couche de restitution externe (export S3/Athena), PAS au
  pipeline interne (Bronze/Silver/Gold Postgres/API dashboard).** Le pipeline
  interne tourne 100% en reseau Docker/localhost (jamais expose sur
  Internet) et a besoin de `user_id` en clair comme cle de jointure simple
  (modele en etoile, API du dashboard, simulateur what-if). L'export S3
  (`scripts/upload_gold_to_s3.py`) remplace `user_id` par `user_pseudo_id`
  sur `dim_user`/`fact_workout_session`/`fact_risk_score` (les 3 seules
  tables Gold porteuses de cet identifiant) — `user_id` reel ne quitte
  jamais `app-postgres`. `terraform/athena.tf` mis a jour en consequence
  (colonnes Glue `user_pseudo_id` string) mais **pas encore applique sur AWS**
  (credentials Learner Lab expires lors de cette session, cf. PROGRESS.md).
- **Calcul HMAC fait en Python pur, PAS dans un modele dbt**, malgre la
  suggestion initiale : dbt substitue `env_var()` en clair dans le SQL
  compile (`dbt/target/compiled/...`, lisible sur disque + logs Postgres),
  ce qui exposerait la cle secrete — deviation assumee et documentee dans
  `scripts/pseudonymize.py`.
- **`PSEUDONYMIZATION_KEY`** : nouvelle variable `.env`/`.env.example`, cle
  HMAC generee via `secrets.token_hex(32)`, jamais en dur dans le code.
- **Chiffrement** : SSE-S3/AES256 deja actif (confirme, `terraform/s3.tf`,
  etape 6 sous-etape 2/6). Postgres local (`app-postgres`) **confirme NON
  chiffre** (`SHOW ssl;` -> `off`) et connexions `psycopg2` (dashboard +
  scripts) **confirmees non chiffrees en transit** — limitation assumee et
  documentee (pas corrigee : generer/monter un certificat TLS avec les
  permissions Unix strictes attendues par Postgres est peu fiable sur un bind
  mount NTFS Windows ; le trafic reste de toute facon confine au reseau
  Docker interne/localhost). Proposition production : AWS RDS avec stockage
  chiffre + `rds.force_ssl=1`.
- **Politique de retention ecrite** (pas de purge automatique implementee a
  ce stade) : 12 mois pour les tables sans donnee personnelle (catalogue
  d'exercices), 36 mois glissants pour les donnees physiologiques/de seance,
  puis agregation anonymisee au-dela ; `dim_user` conserve "duree du compte +
  30 jours" avant anonymisation. Detail complet dans
  `docs/RGPD_GOVERNANCE.md` section 3.
- **`scripts/gdpr_erase_user.py` (droit a l'effacement, RGPD Art. 17),
  execute reellement (dry-run ET execution reelle, pas juste ecrit).**
  Decouverte structurante en l'ecrivant : `dim_user.user_id` est une cle de
  substitution (`row_number()` sur 5 colonnes triees, `stg_gym_members.sql`),
  PAS un identifiant stable — supprimer une ligne UNIQUEMENT dans
  `gold.dim_user` serait annule des le prochain `dbt run` complet (Gold est
  entierement recalcule depuis `raw.silver_gym_members` a chaque run). Une
  suppression durable doit donc remonter jusqu'a la source reellement relue a
  chaque run de `bronze_ingestion` : le script agit sur 4 couches physiques
  pour `gym_members` (CSV source, toutes les partitions Bronze deja
  materialisees sur disque, Silver, Gold Postgres), matching par egalite
  EXACTE du tuple (age, gender, body_weight_kg, height_m, experience_level)
  — **confirme unique sur les 973 lignes avant d'ecrire le script** (sinon le
  matching serait ambigu). `fact_workout_session`/`fact_risk_score` sont
  supprimees directement cote Gold Postgres par `user_id` (pas de limite
  equivalente : aucun agregat partage entre utilisateurs a ce grain).
  Rafraichissement S3 optionnel (`--skip-s3`), avec purge des versions
  anterieures (bucket versionne) — echoue proprement (warning, pas de crash)
  si les credentials AWS sont invalides, sans annuler les suppressions
  locales deja commitees.
  - **Garde-fou integre** : refuse `--confirm` sur le profil
    `is_weight_training_demo_user=true` (le seul relie a de vraies donnees
    de seance, 2164 lignes) sauf flag d'override explicite — evite de casser
    accidentellement le seul jeu de donnees exploitable du dashboard.
  - **Teste reellement** : dry-run sur `user_id=9` (demo user) confirme les
    compteurs connus (2164/2164) et le refus du garde-fou ; execution reelle
    (`--confirm`) sur `user_id=4` (profil sans donnee de seance, choisi pour
    etre sans risque) — verifie sur les 4 couches (Postgres 973->972,
    Silver/Bronze x3/CSV 973->972 lignes chacun), **puis restaure a partir
    d'une sauvegarde** (decision explicite de l'utilisateur, pour ne pas
    alterer durablement le jeu de donnees de demo/soutenance) en recopiant
    les fichiers sauvegardes et en relancant `dbt run` (qui a recree
    `gold.dim_user`/`fact_*` a l'identique depuis `raw.silver_gym_members`,
    jamais touchee par le script). Le mecanisme d'echec gracieux du
    rafraichissement S3 a egalement ete verifie en conditions reelles
    (credentials AWS invalides pendant le test -> warning clair, aucune
    suppression locale annulee).
- **Limite majeure identifiee et documentee, non corrigee a ce stade** :
  `user_id` n'etant pas un identifiant stable (recalcule par `row_number()` a
  chaque rebuild complet), toute pseudonymisation/effacement reste fragile
  face a un futur changement de volumetrie de `gym_members`. Amelioration
  future proposee (pas implementee) : remplacer le surrogate par une cle
  stable (hash d'un identifiant naturel, ou UUID persiste dans une table de
  mapping).

### Etape 6/6, sous-etape 3/6 — CI/CD GitHub Actions (2026-07-08)

Voir `.github/workflows/terraform-ci.yml` et `.github/workflows/README.md`
pour le detail complet (commentaires en francais dans le YAML lui-meme).
Resume des decisions structurantes :

- **Jamais de `terraform apply` automatique en CI, uniquement
  `fmt`/`init`/`validate`/`plan`.** Contrainte deja actee des le debut de
  l'etape 6 (compte AWS Academy Learner Lab, credentials temporaires
  expirant en quelques heures) : un apply automatique serait a la fois peu
  fiable (dependrait d'une session lab active au moment precis du run CI,
  hors du controle du code) et risque sur un compte pedagogique partage.
  L'apply reste toujours declenche manuellement en session, comme pour les
  sous-etapes 2/6 et 4/6 deja realisees.
- **`terraform plan` avec `continue-on-error: true` explicite**, et rapport
  lu depuis `steps.plan.outcome` (pas `conclusion`, qui reste toujours
  `success` sous `continue-on-error`) : un token Learner Lab expire (ou des
  secrets GitHub pas encore configures) est un cas normal et frequent sur ce
  type de compte, ne doit jamais faire echouer tout le pipeline ni bloquer
  une pull request pour une raison hors du controle du code Terraform.
  `terraform validate` (avant `plan`, sans acces AWS) reste le vrai
  garde-fou qualite du pipeline et doit toujours reussir.
- **Credentials AWS injectes via 3 secrets GitHub distincts**
  (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`,
  jamais en clair dans le YAML), reconstitues en `~/.aws/credentials` sous
  le meme profil nomme `awslearnerlab` que `terraform/versions.tf` et
  l'usage local — pas de modification de `versions.tf` pour cette seule
  contrainte CI.
- **Testee reellement en conditions reelles, les 2 chemins** (voir
  PROGRESS.md pour le detail des 2 runs) : un premier run
  (`workflow_dispatch`) avant que les secrets GitHub existent a confirme le
  chemin degrade (echec reel de `plan`, annotation GitHub native "Terraform
  exited with code 1", mais `continue-on-error` a bien empeche l'echec du
  job global — `outcome`=`failure` de l'etape vs `conclusion`=`success` du
  job, verifie via l'API GitHub) ; un second run (re-run du meme workflow
  apres ajout des 3 secrets avec des credentials Learner Lab fraiches) a
  confirme le chemin nominal (10/10 etapes `success`, plus aucune
  annotation d'echec). Verification faite via l'API publique GitHub
  (`api.github.com/repos/.../actions/runs`, `.../check-runs/{id}/annotations`)
  plutot que le CLI `gh` (non installe dans cette session) — le depot est
  public, ces lectures ne necessitent aucune authentification.
- **Commentaire automatique du resume de plan sur les pull requests**
  (`actions/github-script@v7`, condition `github.event_name == 'pull_request'
  && steps.plan.outcome == 'success'`) : implemente mais **pas encore
  declenche par une vraie pull request** dans cette session (les 2 runs de
  verification etaient un `workflow_dispatch`/re-run) — comportement
  `skipped` observe sur les 2 runs, coherent avec la condition, executable
  reellement des la prochaine pull request touchant `terraform/**`.

### Jalon 2, sous-etape 1/5 — Simulateur d'affluence + producteur Kafka (2026-07-09)

Voir `PROGRESS_JALON2.md` pour le detail complet (verifications reelles,
echantillon de messages, tests dbt). Resume des decisions structurantes :

- **`dim_gym` (Gold) contient 5 salles 100% FICTIVES** (seed dbt
  `dbt/seeds/dim_gym_seed.csv`, pas de source Bronze/Silver) : aucun
  dataset Kaggle d'affluence de salle de sport n'existe, decision du
  cahier des charges du Jalon 2 de retenir un simulateur custom plutot
  qu'un dataset synthetique externe. Documente dans le commentaire de
  `dim_gym.sql` ET dans `data/gold/GOLD_MODEL_DECISIONS.md` section 9 —
  meme pattern que `dim_muscle`/`fact_risk_score_demo_synthetic` au
  Jalon 1 (donnees fictives toujours marquees comme telles a 2 endroits).
- **Topic Kafka dedie `safelift-gym-occupancy`, distinct de
  `safelift-test-topic`** (etape 1/6 du Jalon 1, verification initiale du
  broker uniquement, jamais reutilise pour de vraies donnees applicatives).
  Cree via le conteneur `kafka-init` existant (etendu pour creer 2 topics)
  plutot que via un `AdminClient` cote script Python — garde une source
  unique de verite pour les topics du projet, meme mecanisme qu'au
  Jalon 1. `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` reste actif sur le
  broker (filet de securite deja present, independant de ce choix).
- **`confluent-kafka` choisi pour `scripts/simulate_gym_occupancy.py`**
  (pas `kafka-python`) : coherent avec
  `apache-airflow-providers-apache-kafka` (deja dans
  `airflow/requirements.txt` depuis le Jalon 1), qui s'appuie lui-meme sur
  `confluent-kafka>=2.3.0` (verifie via les metadonnees PyPI du provider)
  — evite d'introduire une deuxieme librairie Kafka Python dans le projet.
- **Simulateur = processus Docker long-courant (service `gym-simulator`),
  PAS un DAG Airflow** : c'est un flux de streaming continu (1 message
  toutes les 5-10s par salle), pas un batch planifie — un DAG Airflow
  (conçu pour des executions ponctuelles/planifiees) serait le mauvais
  outil ici. Meme famille de service que `dashboard` (Dockerfile dedie,
  `restart: unless-stopped`), mais healthcheck base sur un fichier de
  heartbeat (pas de port HTTP expose, ce service ne fait que produire vers
  Kafka).
  - **`gym-simulator` depend de `gold.dim_gym` deja materialisee par dbt**
    (lit la table une seule fois au demarrage, avec retry/backoff jusqu'a
    10 tentatives si absente) — limite assumee et documentee dans
    `PROGRESS_JALON2.md` : sur un environnement totalement neuf (jamais de
    `dbt run` execute), ce service echouera au demarrage tant que
    `dbt seed`+`dbt run --select dim_gym` (ou le DAG complet
    `gold_dbt_run`) n'a pas tourne au moins une fois.
- **Pattern d'affluence simule = lissage exponentiel vers une cible
  horaire + bruit gaussien, PAS un bruit uniforme i.i.d.** (fonctions
  `peak_ratio()`/`next_occupancy()` du script) : cible differente en
  semaine (pics 7h-9h/18h-21h a 75% de la capacite, modere 35% le reste de
  la journee, quasi vide la nuit) et le week-end (plateau 10h-19h a 45%,
  pas de double pic domicile-travail). Choix explicitement demande pour
  produire une trajectoire credible (comme une vraie affluence qui
  monte/descend progressivement) plutot qu'un tirage aleatoire independant
  a chaque message — pertinent aussi pour le futur consumer Spark
  Structured Streaming (fenetres temporelles plus interessantes a
  demontrer sur une serie qui a une vraie tendance).
- **Testee reellement en conditions reelles** (voir `PROGRESS_JALON2.md`
  pour le detail complet) : dbt seed/run/test executes directement dans le
  conteneur Airflow existant (`/opt/dbt_venv/bin/dbt`, memes credentials
  que `gold_dbt_run.py`) plutot que via le DAG complet (aurait retraite
  inutilement tout Bronze/Silver pour ajouter une seule dimension statique
  sans dependance externe) ; topic confirme cree par les logs de
  `kafka-init` ; build + demarrage reels du service ; **10 messages reels
  consommes via `kafka-console-consumer`**, identiques aux logs du
  producteur ; emission continue confirmee sur plusieurs cycles (~1
  minute, offsets 0 a 39) ; **arret propre reellement teste**
  (`docker compose stop`, equivalent SIGTERM — logs confirment le flush
  final et la sortie propre, pas de kill force necessaire) ; service
  redemarre ensuite pour fonctionnement continu ; non-regression
  confirmee sur les 9 services existants du Jalon 1 (tous restes
  `healthy`).
- **Test execute pendant une heure creuse simulee (~21h)** : ratios
  d'occupation observes coherents avec la tranche "moderee" (21h-23h,
  cible 35%), pas avec un pic (qui viserait 75%) — comportement attendu a
  cette heure-la, documente explicitement comme tel dans
  `PROGRESS_JALON2.md` pour ne pas etre confondu avec un bug.

### Jalon 2, sous-etape 2/5 — Consumer Spark Structured Streaming (2026-07-09)

Voir `PROGRESS_JALON2.md` pour le detail complet (verifications reelles,
requetes montrant la mise a jour en temps reel, test de resilience) et
`data/gold/GOLD_MODEL_DECISIONS.md` section 10 pour la justification
complete des choix ci-dessous. Resume des decisions structurantes :

- **Etat courant uniquement (une ligne par salle dans
  `gold.gym_occupancy_live`), PAS d'historique/fenetre temporelle** —
  decision deja actee. Cette table N'EST PAS geree par dbt (creee et
  maintenue directement par le job Spark via psycopg2), contrairement au
  reste du schema Gold.
- **`startingOffsets=latest`** : au (re)demarrage, seuls les nouveaux
  messages sont consommes — coherent avec un etat courant (rejouer
  l'historique n'apporterait rien, retarderait juste la fraicheur de la
  table). Consequence assumee : les messages publies pendant un arret du
  job sont definitivement perdus.
- **Upsert via `INSERT ... ON CONFLICT (gym_id) DO UPDATE` (psycopg2),
  pas DELETE+INSERT ni le writer JDBC de Spark** (qui ne supporte pas
  l'upsert nativement) : primitive Postgres atomique en une seule
  instruction, strictement equivalente en resultat a un DELETE+INSERT
  explicite mais plus simple. Chaque micro-batch est minuscule (3-5
  lignes), un `.collect()` cote driver dans `foreachBatch` suffit
  largement.
- **`charge_category`** : Faible <40%, Moderee 40-70%, Elevee >70% —
  seuils simples et documentes (meme esprit que les seuils 33/66 de
  `risk_level`), PAS issus d'une etude/norme citee — hypothese de
  modelisation, meme statut que `base_epidemiological_risk`.
- **Aucun operateur stateful** (pas d'agregation/watermark/join) : le
  checkpoint Spark ne contient que les offsets Kafka + le log de commit du
  sink, geres uniquement par le driver — contrairement a
  `silver_transformation.py`, aucun repertoire n'est partage entre le
  conteneur driver (`spark-streaming-gym`) et `spark-worker`, donc pas
  besoin du `spark.hadoop.fs.permissions.umask-mode=000` utilise en Silver.
- **`spark-streaming-gym` = service Docker dedie (mode client sur le
  cluster Spark existant), PAS un DAG Airflow** : processus long-courant de
  streaming, pas un batch planifie — meme raisonnement que
  `gym-simulator`. Image `spark/Dockerfile.streaming` = MEME image que
  `spark-master`/`spark-worker` (`apache/spark:3.5.8-python3`) + 
  `psycopg2-binary`, pour garantir que driver et executeurs tournent avec
  exactement la meme version Spark/Python (evite la classe de bug
  PYTHON_VERSION_MISMATCH deja rencontree en Silver).
- **3 bugs reels rencontres et corriges en testant** (detail complet dans
  `PROGRESS_JALON2.md`) : `spark-submit` absent du `PATH` (chemin complet
  requis, `/opt/spark/bin/spark-submit`) ; `$HOME=/nonexistent` pour
  l'utilisateur `spark` de l'image officielle, faisant echouer Ivy
  (`--packages`) faute de pouvoir ecrire son cache (corrige avec `--conf
  spark.jars.ivy=/tmp/.ivy2`) ; volume Docker de checkpoint monte
  `root:root` par defaut a son premier usage, illisible en ecriture par
  l'utilisateur non-root `spark` (corrige en pre-creant le repertoire avec
  le bon proprietaire dans `Dockerfile.streaming`, Docker copiant ensuite
  cette permission vers le volume nomme lors de son premier montage).
- **Resilience aux messages JSON malformes testee reellement** (pas
  seulement en theorie) : un message non-JSON injecte manuellement sur le
  topic via `kafka-console-producer` a bien ete logue et ignore
  (`from_json` en mode PERMISSIVE, jamais d'exception), le stream a
  continue sans interruption sur les micro-batches suivants — confirme
  dans les logs reels du job.

### Jalon 2, sous-etape 3/5 — Inputs utilisateur temps reel (2026-07-09)

Voir `PROGRESS_JALON2.md` et `data/gold/GOLD_MODEL_DECISIONS.md` section 11
pour le detail complet (verifications reelles, test end-to-end, delai
mesure). Resume des decisions structurantes :

- **UNE SEULE formule de risk_score (celle de dbt), zero duplication** —
  decision deja actee : `scripts/consume_user_inputs.py` ne recalcule
  jamais le risque lui-meme, il fait entrer la donnee dans le pipeline
  (`raw.realtime_user_sessions` -> staging -> `fact_workout_session` ->
  `fact_risk_score`) puis declenche le run dbt EXISTANT.
- **`raw.realtime_user_sessions` : DEJA au grain "1 ligne = 1 exercice
  complet"** (sets/reps agreges cote formulaire), DIFFERENT du grain
  per-set de `silver_weight_training`. `user_id` obligatoire (contrairement
  a weight_training, qui n'en a aucun nativement).
- **`stg_weight_training.sql` reste INCHANGE** (pas agrege directement) :
  agreger cette table aurait modifie l'`occurrence_count` utilise par
  `dim_exercise.sql` pour departager les graphies d'exercice, remettant en
  cause silencieusement le taux de matching deja verifie (38.3%->90.1%).
  L'union se fait dans un NOUVEAU modele staging
  (`stg_workout_sessions_unified.sql`), qui agrege `stg_weight_training`
  EN INTERNE (meme logique qu'avant, deplacee depuis `fact_workout_session.sql`)
  avant l'UNION ALL avec `stg_realtime_user_sessions` (deja au bon grain).
  `dim_exercise.sql` continue de lire `stg_weight_training` directement,
  jamais le modele unifie.
- **Grain de `fact_workout_session` etendu a `(user_id, session_date,
  workout_name, exercise_id)`** (user_id ajoute) : necessaire des lors que
  plusieurs utilisateurs reels distincts peuvent contribuer des faits (avant,
  tous rattaches au meme demo_user par construction).
  `coalesce(u.user_id, demo_user.user_id)` remplace l'ancien
  `cross join demo_user` inconditionnel — verifie neutre pour les 2164
  lignes weight_training existantes.
- **`POST /users/{user_id}/sessions` (dashboard) ne write JAMAIS en base**,
  uniquement Kafka (`safelift-user-inputs`) — point architectural demontre.
  Erreurs Kafka gerees explicitement (callback de livraison + `flush`
  verifie) -> HTTP 503 si echec, jamais un succes trompeur.
- **Formulaire "Logger une seance" restreint a un `<select>` d'exercices
  DEJA pratiques** (reutilise `GET /users/{user_id}/exercises`, deja
  utilise par le simulateur what-if) plutot qu'un champ texte libre :
  garantit par construction que `exercise_name` matche
  `gold.dim_exercise` (sinon ligne orpheline, `risk_score` non calculable).
  Validation UNIQUEMENT cote UI, ni l'API ni le consumer ne valident contre
  `dim_exercise` — limite assumee.
- **Run dbt COMPLET (`gold_dbt_run` existant), PAS de run partiel par
  utilisateur** : explicitement envisage puis ecarte — `fact_risk_score.sql`
  calcule des facteurs par fenetre glissante (`lag()`/`avg() over`), dbt
  `--select` filtre des modeles pas des lignes, un run partiel correct
  necessiterait un modele incremental (refonte hors perimetre) ou
  recalculerait de toute facon toute la fenetre. Declenchement via l'API
  REST Airflow (`POST /api/v1/dags/gold_dbt_run/dagRuns`), `conf`
  transmis pour tracabilite uniquement (non lu par le DAG).
  `AIRFLOW__API__AUTH_BACKENDS` etendu a `basic_auth` (le defaut,
  `session`, exige un cookie web) ; identifiants admin existants reutilises
  (simplification assumee, pas de compte de service dedie).
- **`auto.offset.reset=earliest` pour ce consumer, CONTRASTE DELIBERE avec
  `startingOffsets=latest` du simulateur d'affluence (sous-etape 1/5)** :
  une seance utilisateur est une donnee importante (jamais perdue
  silencieusement), contrairement a un etat d'affluence ephemere.
  "earliest" ne joue qu'au tout premier demarrage du groupe de consumers ;
  les redemarrages suivants reprennent du dernier offset commit (persiste
  par Kafka).
- **2 bugs reels trouves et corriges en testant** :
  1. **Contention de ressources Spark** (le plus significatif) :
     `spark-streaming-gym` (long-courant, sous-etape 2/5) capturait les 2
     coeurs du worker des son demarrage et les gardait indefiniment ->
     `load_silver_to_postgres` (1re task de `gold_dbt_run`) restait
     bloque `WAITING` avec 0 coeur alloue, confirme via l'API JSON du
     master Spark, un premier test etant reste bloque >3 minutes sans
     progresser. Corrige avec `--conf spark.cores.max=1` sur
     `spark-streaming-gym`.
  2. **`dim_date` bornee uniquement par l'historique weight_training
     (2015-2018)** : une seance saisie en 2026 tombait hors plage ->
     `date_id` NULL -> `week_start_date` NULL -> le LEFT JOIN sur
     `week_start_date` (deux NULL jamais egaux en SQL) ne matche plus ->
     `risk_score` NULL SILENCIEUSEMENT pour toute seance temps reel.
     Corrige en unionnant les 2 sources de dates avant de calculer
     min/max dans `dim_date.sql`.
- **Test end-to-end reel sur `user_id=9`** ("Bicep Curl (Barbell)", 70kg x
  5 series x 15 reps) : score `arms` passe de **15.19 a 7.81**
  (nouvelle ligne 100% reelle, `session_date=2026-07-08`, recalculee par
  la formule dbt existante). **Delai reel mesure : ~70 secondes**
  (soumission -> fin du run dbt) — le timeout de polling cote frontend a
  ete corrige de 60s (valeur initiale supposee, non mesuree) a 120s suite
  a cette mesure. `dbt test` : 72/72 PASS avec la ligne reelle en base,
  aucune regression sur les 2164 lignes historiques.

### Jalon 2, sous-etape 4/5 — Dashboard temps reel affluence, SSE (2026-07-09)

Voir `PROGRESS_JALON2.md` et `data/gold/GOLD_MODEL_DECISIONS.md` section 12
pour le detail complet. Resume des decisions structurantes :

- **Server-Sent Events, pas de WebSocket, pas de polling** — decision deja
  actee, respectee. Independant du polling deja en place pour le recalcul
  de risque (sous-etape 3/5), qui n'a pas ete touche.
- **`GET /gyms/occupancy/stream` : evenement envoye UNIQUEMENT si les
  donnees ont change** (comparaison sur les lignes seules, pas sur le
  payload complet — voir bug ci-dessous) — interroge `gold.gym_occupancy_live`
  toutes les 3s cote serveur, coherent avec le rythme d'emission du
  simulateur (5-10s) et le trigger Spark (5s).
- **Deconnexion propre garantie par construction** : chaque requete SQL
  passe par le pool de connexions existant (`get_cursor()`, context
  manager qui rend toujours sa connexion immediatement) — pas de connexion
  Postgres dediee et retenue pour la duree de vie du flux SSE. Verifie
  concretement (compteur `pg_stat_activity` identique avant/apres une
  coupure brutale de connexion).
- **`GET /gyms/{gym_id}/best_slot` : recommandation basee sur le PATTERN
  THEORIQUE du simulateur (`peak_ratio()`, duplique dans
  `_theoretical_occupancy_ratio()`), PAS sur un historique reel observe**
  — `gold.gym_occupancy_live` vient de demarrer, historique trop court
  pour etre statistiquement valable. Limitation EXPLICITEMENT assumee et
  signalee (`is_theoretical_pattern_based: true` + champ `methodology`),
  jamais presentee comme une vraie prediction. Meme raisonnement de
  duplication documentee que `MUSCLE_LABELS_FR` (deux images Docker
  separees, pas de mecanisme de partage de code).
- **Bug reel trouve et corrige** : la deduplication comparait initialement
  le PAYLOAD COMPLET (incluant `server_time`, qui change a chaque
  iteration) — la deduplication ne se declenchait donc jamais, un
  evenement partait toutes les 3s meme sans changement reel. **Constate
  directement en testant** (2 evenements consecutifs avec des donnees
  identiques observes via `curl -N`, alors que le simulateur emet toutes
  les 5-10s). Corrige en comparant uniquement les lignes de donnees.
- **Testee reellement en conditions de streaming continu** (pas un test
  one-shot) : capture de 40s via `curl -N` sur une connexion HTTP UNIQUE
  et continue — 7 evenements distincts, valeurs reellement differentes a
  chaque fois, timestamps espaces irregulierement (6-9s, coherent avec le
  cycle amont, pas un simple timer fixe). Aucune fuite de connexion
  Postgres constatee apres deconnexion brutale du client.
- **Artefact d'affichage UTF-8 (`basÃ©e` au lieu de `basée`) verifie
  non-bug** : meme classe de probleme deja documentee pour Feature A
  (encodage de la console Windows locale utilisee pour le test, pas un
  bug de l'API) — confirme en decodant les octets bruts de la reponse HTTP
  et en inspectant les codepoints Unicode directement (`U+00E9` correct).

### Jalon 2, sous-etape 5/5 — Verification globale + demo (2026-07-09, cloture du Jalon 2)

Aucun nouveau developpement — verification bout-en-bout uniquement. Voir
`PROGRESS_JALON2.md` pour le detail complet des mesures et
`docs/DEMO_SCRIPT_JALON2.md` pour le support de soutenance. Resume des
constats structurants :

- **Le correctif `spark.cores.max=1` (bug de contention trouve en
  sous-etape 3/5) tient DANS LA DUREE** : verifie sur une fenetre de 13
  minutes (26 mesures/30s) avec les deux streams actifs simultanement.
  Preuve directe et horodatee : `coresused` reste a `1/2` en continu,
  bascule a `2/2` EXACTEMENT au moment d'un run dbt declenche par une
  seance utilisateur soumise au milieu de la fenetre, puis revient a
  `1/2` a la mesure suivante — le job batch acquiert bien son coeur libre
  et le relache a la fin, contrairement au comportement bloque
  d'avant-correctif.
- **2e mesure independante du delai de recalcul de risque : 110s** (vs 70s
  en sous-etape 3/5) — variance attribuee a la charge concurrente du
  systeme. Le timeout de polling frontend (`dashboard.js`) a ete releve
  de 120s a 180s en consequence (120s ne laissait plus qu'une marge de
  10s face au pire cas observe). **Ne jamais fixer une marge de securite
  sur une seule mesure quand une deuxieme est disponible.**
- **Test reel de panne/reprise** (`docker compose stop`/`start
  gym-simulator` pendant que les 11 autres services tournent) : aucun
  crash en cascade, `spark-streaming-gym` continue sans erreur (les
  micro-batches vides sont deja silencieusement ignores par le code
  existant, `if total == 0: return` — comportement attendu, pas un
  bug), reprise propre au redemarrage (`dim_gym` rechargee, numerotation
  des batches Spark continue sans ecart). `RestartCount` reste a `0`
  partout — confirme que l'arret etait volontaire, pas un crash suivi
  d'un redemarrage automatique par `restart: unless-stopped`.
- **`docs/DEMO_SCRIPT_JALON2.md`** : toutes les durees annoncees au jury
  sont des mesures reelles, jamais devinees — le delai de recalcul est
  deliberement presente comme une fourchette "1 a 2 minutes" (pas un
  chiffre unique trop precis) precisement a cause de la variance 70s/110s
  constatee. Inclut un plan de secours (grille de lecture `docker compose
  ps` sans paniquer devant le jury) et 7 questions probables du jury avec
  pistes de reponse courtes.
- **Jalon 2 (5/5 sous-etapes) cloture le 2026-07-09** — 4 bugs reels
  trouves et corriges au total sur l'ensemble du jalon (contention de
  coeurs Spark, `dim_date` trop etroite, deduplication SSE cassee,
  chemin/permissions au demarrage de `spark-streaming-gym`), tous
  documentes avec cause racine et correctif, aucun masque silencieusement.

### Jalon 3, sous-etape 1/6 — Ingestion nutrition + calculs deterministes (2026-07-09)

Voir `PROGRESS_JALON3.md` et `data/gold/GOLD_MODEL_DECISIONS.md`
section 13 pour le detail complet. Resume des decisions structurantes :

- **⚠️ Rappel du cadre ethique, a repeter a chaque usage de ces
  donnees** : BMR/TDEE/besoin proteique sont des formules STANDARD
  deterministes (litterature sportive generaliste), **PAS des
  recommandations medicales/nutritionnelles personnalisees** — SafeLift ne
  remplace ni coach ni medecin.
- **Endpoint USDA `/foods/search` choisi plutot que `/food/{fdcId}`** :
  une seule requete par mot-cle renvoie plusieurs aliments AVEC leurs
  nutriments deja inclus, meilleur rapport simplicite/couverture que des
  appels individuels par aliment. `dataType` restreint a
  `Foundation,SR Legacy` (aliments de reference, valeurs par 100g
  fiables) — exclut `Branded`/`Survey` (bruit, portions variables).
- **DAG `nutrition_ingestion` self-contained**, independant de la cascade
  Kaggle (`bronze_ingestion` -> `silver_transformation` -> `gold_dbt_run`)
  — domaine different. Reutilise neanmoins `load_silver_to_postgres.py`
  existant (table `usda_nutrition` ajoutee au dictionnaire `TABLES`) et
  fait un `dbt run --select` SCOPE (pas tout `gold_dbt_run` —
  `fact_nutrition_target` ne depend que de `dim_user` deja construite).
- **BMR = Mifflin-St Jeor (1990)**, formule standard citee. **TDEE = BMR x
  facteur d'activite**, deduit de `workout_frequency_days_per_week`
  (mapping 2/3/4/5 jours -> 1.375/1.55/1.725/1.9, tables
  Harris-Benedict/Mifflin usuelles). **Besoin proteique = 1.6 a 2.2 g/kg**
  deduit de `experience_level` (1/2/3) — la fourchette elle-meme est
  issue de la litterature sportive, mais CE mapping precis vers
  `experience_level` est une simplification du projet, documentee comme
  telle (limite assumee).
- **Cle API (`USDA_API_KEY`) jamais en dur, jamais loggee meme
  partiellement** (`_redact_secret()` applique systematiquement) — verifie
  par une recherche exhaustive de la valeur exacte de la cle sur
  l'ensemble des logs du run reel : 0 occurrence trouvee.
- **⚠️ Bug reel + rappel operationnel important pour tout futur DAG** :
  `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true` (deja actee) met tout
  NOUVEAU DAG en pause a sa premiere apparition — **un DAG en pause ne
  voit AUCUNE task planifiee par le scheduler, MEME pour un run declenche
  manuellement** (hypothese initiale contraire infirmee en testant
  reellement : le run reste `queued` indefiniment, aucune task ne demarre
  jamais). Les 3 DAGs precedents avaient ete depauses manuellement lors de
  sessions anterieures sans que cette etape soit documentee explicitement.
  **`airflow dags unpause <dag_id>` est desormais une etape obligatoire
  et documentee apres la creation de tout nouveau DAG dans ce projet.**
- **Teste en conditions reelles** : ingestion API reelle (119 aliments,
  25 ignores pour macro-nutriment manquant), 16/16 tests dbt PASS,
  `gold.fact_nutrition_target` = 973 lignes (= nombre exact d'utilisateurs
  `dim_user`), sanity check reel isolant l'effet de l'activite (poids
  quasi-identique, frequence d'entrainement differente -> TDEE different
  dans le sens attendu), verification arithmetique directe
  (`tdee_kcal / bmr_kcal` == `activity_factor` exact sur l'echantillon).

### Jalon 3, sous-etape 2/6 — Dashboard nutrition (2026-07-09)

Voir `PROGRESS_JALON3.md` pour le detail complet. Resume des decisions
structurantes :

- **`GET /users/{user_id}/nutrition` ne recalcule RIEN** : lecture directe
  de `gold.fact_nutrition_target`/`gold.dim_nutrition` (deja calculees par
  dbt en sous-etape 1/6) — meme principe que les autres endpoints de ce
  fichier (`/users/{id}/risk`, `/gyms/occupancy/stream`), aucune logique
  metier dupliquee cote API.
- **Le champ `disclaimer` de la reponse API est la SOURCE UNIQUE DE
  VERITE du texte d'avertissement** (`NUTRITION_DISCLAIMER`, constante
  Python) — le frontend l'affiche tel quel, ne le reformule jamais. Evite
  qu'une version differente/abregee soit ecrite par erreur cote JS.
- **⚠️ Avertissement ethique = contrainte NON NEGOCIABLE de visibilite**,
  traitee comme une contrainte dure : place en TOUT PREMIER element de la
  section (avant les chiffres), AUCUNE classe `hidden`/regle CSS
  `display:none` par defaut (verifie explicitement par lecture du code),
  seul le toggle demo/reel (deja utilise pour masquer simulateur/logger
  seance, memes raisons : pas d'`user_id` reel en mode demo) peut la
  masquer — jamais un clic supplementaire de l'utilisateur.
- **8 aliments suggeres = les plus riches en proteines** (`ORDER BY
  protein_g_per_100g DESC LIMIT 8`) — choix simple et explicable,
  coherent avec le contexte fitness/musculation du reste du projet (pas
  un algorithme de recommandation personnalise).
- **Section "Nutrition" desactivee en mode demo**, meme raisonnement deja
  applique au simulateur what-if et au formulaire "Logger une seance" :
  les scenarios synthetiques n'ont pas d'`user_id` reel a associer a une
  cible nutritionnelle.
- **Teste sur 3 profils reels distincts** (40kg/64.1kg/129.9kg -> TDEE
  1711/2446/3271 kcal), memes chiffres exacts que la sous-etape 1/6
  (confirme que l'API relit bien les valeurs dbt sans les alterer).
  Verification VISUELLE (capture d'ecran) non effectuee (extension Chrome
  indisponible sur cette session comme sur toutes les precedentes) —
  documente comme limite, verification structurelle du DOM/CSS faite a la
  place.

### Jalon 3, sous-etape 3/6 — Preparation des donnees ML (2026-07-09)

Voir `data/ml/ML_DATA_PREP.md` pour le detail complet (grain, features,
chiffres reels, verification anti-fuite). Resume des decisions
structurantes :

- **Script Python standalone (`scripts/prepare_ml_features.py`), pas un
  modele dbt** : sortie attendue = fichiers Parquet train/test (pas des
  tables Postgres), split temporel = decision de consommation des donnees
  (hors perimetre naturel de dbt), lags calendaires exacts plus simples a
  exprimer/verifier en pandas pour ce volume de donnees. Coherent avec les
  scripts standalone deja existants (`upload_gold_to_s3.py`,
  `fuzzy_match_exercises.py`). Ne fait AUCUN calcul de risque : lit
  uniquement `gold.fact_risk_score` (deja calculee par dbt en Jalon 1) et
  agrege/decale dans le temps.
- **Objectif ML deja tranche (non renegociable a cette sous-etape)** :
  predire le `risk_score` de la SEMAINE SUIVANTE par (utilisateur, zone),
  jamais reapprendre la formule deterministe actuelle de
  `fact_risk_score.sql` — la cible est deliberement decalee d'une semaine
  dans le futur pour eviter une fuite de donnees triviale (le modele ne
  doit pas pouvoir se contenter de recopier une formule connue).
- **Grain `(user_id, muscle_group, week_start_date)`, MEME colonne
  `gold.dim_date.week_start_date` deja utilisee en interne par
  `fact_risk_score.sql`** pour calculer `charge_factor`/`volume_factor` —
  coherence garantie avec la definition de "semaine" deja actee. Aucune
  semaine "a zero" inventee : le `GROUP BY` sur des lignes reellement
  existantes garantit qu'une semaine sans seance ne produit simplement
  aucune ligne.
- **AUCUNE fuite temporelle par construction** : chaque
  `lag_N_risk_score` recherche la valeur a la semaine calendaire EXACTE
  (`week_start_date - N semaines`, lookup pandas `MultiIndex` exact),
  `NULL` si cette semaine precise n'a aucune donnee — **jamais interpolee
  ni remplacee par la derniere valeur observee plus loin dans le passe**
  (inventerait une regularite hebdomadaire qui n'existe pas dans un
  historique reel irregulier). Meme logique en sens inverse pour la
  cible (`week_start_date + 1 semaine`).
- **Verifie reellement, pas juste par relecture de code** : (1) aucun
  chevauchement de dates entre train et test (`max(train) < min(test)`) ;
  (2) verification manuelle d'une ligne (`arms`, semaine 2016-01-18)
  contre une requete independante sur les donnees brutes, `lag_1` et
  cible confirmes exacts ; (3) les 2 lignes orphelines a
  `week_start_date=2026-07-06` (tests reels du formulaire "Logger une
  seance", Jalon 2) confirmees `NULL` sur lag/cible et absentes de
  train/test — exclues NATURELLEMENT par la logique de lookup exact, sans
  filtre special-case ecrit pour ce cas.
- **Split TEMPOREL (pas aleatoire)** sur les SEMAINES DISTINCTES du jeu
  labelise (pas sur le nombre brut de lignes, deforme par des zones a
  beaucoup plus de lignes que d'autres) : 20% des semaines les plus
  recentes en test. Cutoff reel obtenu : `2018-03-19` (107 semaines
  train / 27 semaines test, soit 491 lignes train / 161 lignes test).
- **⚠️ Limite majeure, a rappeler explicitement a la prochaine sous-etape
  (entrainement) : jeu de donnees MONO-UTILISATEUR** (`user_id=9`
  uniquement a un historique `fact_risk_score` exploitable, les 972
  autres profils `dim_user` n'ont aucune seance reelle rattachee — meme
  contrainte deja documentee en Jalon 1, `GOLD_MODEL_DECISIONS.md`
  section 5). Volume tres petit pour du ML (491/161 lignes, 8 zones tres
  inegalement representees, `legs` = 18 lignes labelisees au total) —
  honnetement quantifie et non minimise dans `ML_DATA_PREP.md`, comme
  demande explicitement.
- **Ecart de ~8 ans dans l'historique constate et documente, pas masque** :
  historique reel ininterrompu 2015-10-19 -> 2018-09-24 (Kaggle
  `weight_training`), puis 2 lignes isolees a 2026-07-06 issues de mes
  propres tests reels du Jalon 2 (formulaire temps reel) — trop loin de
  tout historique adjacent pour etre exploitables, transparentes dans
  `weekly_features_full.parquet` mais absentes de train/test.
- **`.gitignore` : `data/ml/*` ignore, exceptions `!data/ml/.gitkeep` et
  `!data/ml/ML_DATA_PREP.md`** — meme pattern deja applique a
  `data/bronze`/`data/silver`/`data/gold` (donnees generees non
  versionnees, documentation versionnee).

### Jalon 3, sous-etape 4/6 — Entrainement + evaluation du modele ML (2026-07-09)

Voir `data/ml/ML_TRAINING_RESULTS.md` pour le detail complet (tableau
comparatif, importances, conclusion). Resume des decisions structurantes :

- **`scikit-learn==1.5.2` ajoute a `airflow/requirements.txt`, image
  `safelift-airflow:local` reconstruite** (`docker compose build
  airflow-init` — c'est ce service, pas `airflow-webserver`, qui possede
  le `build:` du Dockerfile ; `airflow-webserver`/`airflow-scheduler`
  referencent seulement l'image par tag, verifie apres une premiere
  tentative de rebuild silencieusement no-op sur le mauvais service).
  `joblib`/`scipy`/`threadpoolctl` installes en dependances transitives,
  aucune version epinglee separement pour ceux-ci.
- **UN SEUL modele poole sur les 8 zones** (`muscle_group` en one-hot),
  **pas 8 modeles independants** — decision deja actee en sous-etape 3/6
  (`ML_DATA_PREP.md` section 6) : `legs` (18 lignes labelisees au total)
  et `unknown` (50, categorie fourre-tout du fuzzy matching, pas une
  vraie zone anatomique) sont structurellement trop petits pour un
  entrainement par zone.
- **Imputation des NULL des lags par 0** (pas de suppression de lignes,
  pas de moyenne du train) : documente dans `impute_lag_nulls()`, choix
  neutre applique identiquement sur train et test — supprimer des lignes
  aurait encore reduit un train deja a 491 lignes.
- **Aucun hyperparametre cherche par validation croisee sur le test set**
  (volume trop faible pour un split train/validation supplementaire
  fiable) : `alpha=1.0` (Ridge), `max_depth=4`/`n_estimators=100`
  (RandomForest) sont des valeurs par defaut raisonnables fixees AVANT
  toute evaluation — le test set ne sert qu'a l'evaluation finale, jamais
  a ajuster quoi que ce soit.
- **Baseline naive obligatoire et jamais contournee** : predire
  `risk_score_avg` (semaine courante) tel quel comme prediction de la
  semaine suivante. RMSE baseline = **14.6866** vs Ridge **9.2689** vs
  RandomForest **9.1152** (TEST set, 161 lignes) — **les deux modeles ML
  battent nettement la baseline** (~37% de reduction RMSE), resultat
  rapporte tel quel (le code aurait rapporte l'inverse a l'identique si la
  baseline avait gagne).
- **RandomForest retenu** (RMSE legerement meilleur que Ridge, ecart
  modeste et documente comme tel — pas presente comme une victoire
  ecrasante) **ET** parce que son signal dominant
  (`lag_1_risk_score`+`lag_2_risk_score` = 45.7% d'importance cumulee) est
  **plus aligne sur l'objectif reellement vise** (predire une TENDANCE
  temporelle) que Ridge, dont les plus gros coefficients sont les dummies
  `muscle_group` (capture surtout le niveau de risque de base propre a
  chaque zone, deja connu via `dim_muscle.base_epidemiological_risk`,
  plutot qu'une vraie dynamique).
- **`duree_factor_avg` : coefficient ET importance a 0 dans les DEUX
  modeles** — coherent avec un constat deja documente en Jalon 1
  (`GOLD_MODEL_DECISIONS.md` section 8, `duration_seconds` quasi
  toujours 0 sur ce dataset) : quasi aucune variance a exploiter, pas un
  bug du script d'entrainement.
- **`data/ml/model.pkl` (joblib, pipeline scikit-learn complet + metadonnees)
  et `data/ml/training_metrics.json` restent NON versionnes** (couverts
  par `data/ml/*` deja ignore) — artefacts regenerables par une
  re-execution du script, meme logique que `train.parquet`/`test.parquet`.
  Seul `ML_TRAINING_RESULTS.md` (documentation) est une exception versionnee.
- **Conclusion volontairement nuancee dans `ML_TRAINING_RESULTS.md`** : le
  gain du ML sur la baseline est reel et mesurable, mais **strictement
  borne au perimetre mono-utilisateur** deja documente en sous-etape 3/6 —
  explicitement NON presente comme une preuve de generalisation, et NE
  remplace PAS la formule deterministe `risk_score` utilisee par le
  dashboard (rappel deja acte : toute evolution vers du ML doit rester une
  etape explicitement identifiee, jamais une modification silencieuse de
  `fact_risk_score.sql`).

### Jalon 3, sous-etape 5/6 — Integration pipeline + dashboard de la prediction ML (2026-07-09)

Voir `PROGRESS_JALON3.md` pour le detail complet (verifications reelles,
requetes, logs de la cascade Airflow). Resume des decisions structurantes :

- **`scripts/score_risk_trend.py` IMPORTE (jamais ne duplique)**
  `fetch_weekly_aggregates`/`build_features` de `prepare_ml_features.py` et
  `impute_lag_nulls`/`NUMERIC_FEATURES`/`CATEGORICAL_FEATURES` de
  `train_risk_trend_model.py` (`sys.path.insert` sur `SCRIPTS_DIR`, pas de
  package Python formel — coherent avec l'absence de `__init__.py` dans
  `scripts/`) : garantit que le scoring utilise EXACTEMENT les memes
  features que l'entrainement (evite un "training/serving skew" silencieux,
  bug classique en ML).
- **`gold.ml_risk_prediction` creee directement par le script (psycopg2),
  PAS geree par dbt** — meme raisonnement que `gold.gym_occupancy_live`
  (Jalon 2) : la source de cette table est un modele ML externe, pas une
  transformation SQL des donnees Gold existantes. Cle primaire
  `(user_id, muscle_group)` + rafraichissement complet (`TRUNCATE` puis
  reinsertion dans la meme transaction) a chaque execution : ce script
  maintient un ETAT COURANT ("meilleure prediction disponible maintenant"),
  pas un historique de predictions passees a preserver.
- **Contrainte structurelle, pas un filtre ajoute** : seuls les
  `(user_id, muscle_group)` deja presents dans `gold.fact_risk_score`
  peuvent apparaitre dans `fetch_weekly_aggregates()` — aucune
  extrapolation possible sur un historique vide par construction, pas
  besoin d'une verification explicite supplementaire. Concretement :
  8 lignes ecrites, toutes pour `user_id=9`.
- **DAG `ml_scoring` declenche par `TriggerDagRunOperator` en fin de
  `gold_dbt_run`** (apres `dbt_test`, meme mecanisme que
  `bronze_ingestion -> silver_transformation -> gold_dbt_run`) : consequence
  assumee, ce DAG se redeclenche donc aussi automatiquement apres qu'une
  seance temps reel (Jalon 2) ait provoque un run `gold_dbt_run` — la
  prediction ML reste a jour avec les memes donnees fraiches que
  `risk_score`, sans mecanisme de synchronisation separe. Si `dbt_test`
  echoue, `ml_scoring` n'est PAS declenche (`trigger_rule` par defaut =
  `all_success`) : jamais de scoring sur des donnees Gold potentiellement
  invalides. Perimetre volontairement borne au SCORING : aucun
  reentrainement automatique (`train_risk_trend_model.py` reste une action
  manuelle explicite, sous-etape 4/6).
- **`GET /users/{user_id}/risk/prediction` : 2 cas "non disponible"
  distingues explicitement**, jamais un 500 — table absente
  (`to_regclass('gold.ml_risk_prediction')`, le DAG `ml_scoring` n'a jamais
  tourne) vs table presente mais sans ligne pour cet utilisateur (pas
  assez d'historique). Le champ `disclaimer` (texte source unique de
  verite, meme mecanisme que `NUTRITION_DISCLAIMER`) est **toujours**
  present dans la reponse, disponible ou non.
- **Dashboard : encart "Tendance prédictive" DELIBEREMENT distinct
  visuellement** du risk_score deterministe (bordure pointillee violette
  `--accent-purple`, badge "EXPERIMENTAL" toujours visible — jamais une
  simple nuance de ton facile a manquer) — jamais fusionne avec la
  silhouette/jauge/zones sensibles. Desactive en mode demo (memes raisons
  que le simulateur what-if/nutrition : pas d'`user_id` reel a associer aux
  scenarios synthetiques). Si aucune prediction disponible pour le profil
  selectionne : message explicite "Non disponible pour ce profil — {reason
  de l'API}", **jamais l'encart masque sans explication**.
- **Testee reellement en conditions de bout en bout** (pas juste le script
  isole) : `airflow dags trigger gold_dbt_run` -> cascade complete observee
  via `airflow tasks states-for-dag-run` (10 tasks `success`, dont
  `trigger_ml_scoring`), run `ml_scoring` confirme `success` en ~1.6s,
  8 lignes fraiches (`scored_at` horodate au moment du run) confirmees en
  base par requete SQL directe. Endpoint teste sur 3 cas : `user_id=9`
  (8 predictions), un utilisateur sans historique (`available:false` +
  message clair), un `user_id` inexistant (404 propre). Frontend verifie
  structurellement (JS/HTML/CSS bien formes — balises `section`/`div`
  equilibrees 7/7 et 39/39, accolades CSS 128/128, tous les nouveaux `id`
  references par `dashboard.js` resolus dans `index.html`) et servi
  reellement par le conteneur (`curl` confirme le nouveau contenu present
  dans `/`, `/static/dashboard.js`, `/static/dashboard.css`) — **rendu
  visuel dans un navigateur non confirme** (meme limite deja documentee
  pour tout le reste du dashboard, extension Chrome indisponible).
- **Constat honnete non masque, documente plutot que corrige** : les
  predictions `arms`/`back` s'appuient sur la semaine la plus recente
  disponible pour ces zones, qui se trouve etre un point isole
  (2026-07-06, test reel du formulaire temps reel de Jalon 2) sans aucun
  contexte de lags reels (`lag_1`/`lag_2`/`lag_3` tous imputes a 0, faute
  de semaine calendaire adjacente) — la prediction est techniquement
  produite (le pipeline ne plante pas) mais structurellement moins fiable
  que les 6 autres zones, ancrees sur l'historique dense reel de 2018.
  Aucun filtre special-case n'a ete ajoute pour masquer ce cas : le
  comportement du modele reste honnetement visible via `based_on_week`
  (2026 vs 2018), affiche tel quel dans l'API et le dashboard.

### Jalon 3, sous-etape 6/6 — Refonte UX/UI par onglets + corrections de débordement (2026-07-09)

Voir `PROGRESS_JALON3.md` pour le detail complet (verifications reelles,
bug de faux positif rencontre, calibration du seuil de troncature).
Resume des decisions structurantes :

- **Navigation par onglets SPA-style (changement de vue 100% JS via toggle
  de classes CSS), PAS de rechargement de page ni de routes serveur
  separees** : decision deja actee, necessaire pour preserver la
  connexion SSE de l'affluence (qui vit en JS, independante du DOM
  affiche/masque) et eviter tout temps de chargement pendant une demo
  live. `.tab-panel { display:none; }` masque un onglet inactif SANS
  jamais retirer ses elements du DOM — c'est cette garantie (pas de
  destruction DOM) qui rend la survie de la SSE automatique, sans
  mecanisme special a ecrire.
- **3 onglets, regroupement par theme fonctionnel** : "Risque &
  Entraînement" (silhouette, KPIs, zones sensibles, simulateur what-if,
  logger une seance, tendance predictive ML), "Affluence" (SSE + creneau
  recommande), "Nutrition" (TDEE/BMR, proteines, aliments). Le simulateur
  what-if n'etait pas explicitement assigne dans la demande initiale —
  regroupe dans "Risque & Entraînement" par defaut (seul emplacement
  logique), pour ne perdre aucune fonctionnalite deja testee.
- **Header sticky unique regroupant bandeau demo + header + nav
  d'onglets** (`.app-header-sticky`, `position:sticky; top:0;`) plutot
  que rendre chaque element sticky independamment avec des offsets `top`
  calcules a la main : le selecteur d'utilisateur et le toggle demo sont
  des REGLAGES GLOBAUX (pas propres a un onglet), ils doivent rester
  visibles en permanence quel que soit l'onglet actif et le defilement.
- **2 familles de correctifs anti-debordement distinctes selon le type
  d'element** (decouverte structurante de cette passe) :
  1. Elements HTML normaux (span/div/h3) : `overflow:hidden;
     text-overflow:ellipsis; white-space:nowrap;` fonctionne de maniere
     fiable — mais necessite `min-width:0` explicite sur le conteneur
     flex/grid parent (`.kpi-card`, `.zone-name`, `.gym-card`,
     `.nutrition-food-item`, `.ml-prediction-info`), sinon l'item refuse
     par defaut de retrecir sous la largeur intrinseque de son texte et
     desactive silencieusement l'ellipsis (piege CSS classique,
     applicable aussi bien a Flexbox qu'a CSS Grid).
  2. `<select>` natifs (exercices, scenarios demo) : CSS
     `text-overflow:ellipsis` est **peu fiable sur l'etat FERME** d'un
     select selon les navigateurs (coupure brutale sans "…" dans
     certains cas). Correctif principal cote JS : `truncateForSelect()`
     coupe le texte AVANT de l'inserer comme `<option>` ; texte complet
     toujours accessible via l'attribut `title` (liste ouverte ET boite
     fermee, cette derniere synchronisee par `updateSelectTitle()` sur
     l'evenement `change` — attache UNE SEULE FOIS dans `init()`, jamais
     dans les fonctions de repopulation appelees a chaque changement
     d'utilisateur, pour ne jamais empiler de listeners dupliques sur un
     select reutilise).
- **Seuil de troncature des selects calibre sur des donnees reelles, pas
  suppose** : un premier seuil de 55 caracteres ne declenchait JAMAIS sur
  les 81 exercices reels de `user_id=9` alors que le label le plus long
  (54 caracteres) deborde deja visuellement d'une colonne de 320px a
  ~0.9rem (largeur en PIXELS, pas en nombre de caracteres — un seuil basé
  uniquement sur le compte de caracteres est un raisonnement insuffisant
  pour une police proportionnelle). Resserre a 42 caracteres, verifie
  faire effectivement declencher la troncature sur 25/81 labels reels.
- **Harmonisation des espacements** : `margin-top: 20px` redondant retire
  de `.log-session-panel`/`.occupancy-panel`/`.nutrition-panel` (ces 3
  panneaux cumulaient cette marge AVEC le `gap:24px` du conteneur flex
  parent, contrairement aux autres panneaux qui n'avaient que le gap) —
  `.tab-panel.active { gap: 24px }` gere desormais l'espacement de
  maniere uniforme pour tous les panneaux d'un meme onglet.
- **Bug reel trouve et corrige pendant la verification (pas dans le
  produit final, dans le processus de verification lui-meme)** : un
  premier controle automatique de l'equilibre des balises `<div>` a
  signale un faux desequilibre (44 ouvrantes / 43 fermantes) — cause par
  le propre commentaire HTML explicatif de cette passe, qui citait
  litteralement une balise `<div class="tab-panel">` en exemple dans son
  texte (capturee par le regex de verification comme une vraie balise).
  Reformule pour ne plus contenir de balise litterale dans un
  commentaire. Rappel utile pour toute future verification automatique
  par comptage de balises sur ce projet : les commentaires HTML peuvent
  produire des faux positifs s'ils citent des exemples de code.
- **SSE de l'affluence : preuve de survie au changement d'onglet en 2
  couches** — (1) preuve PAR CONSTRUCTION : `occupancyEventSource`
  (variable JS) n'est reference que dans `connectOccupancyStream()`
  (appelee UNE SEULE FOIS dans `init()`), `switchTab()` ne la touche
  jamais et ne ferme/recree jamais l'`EventSource` (confirme par
  recherche exhaustive dans `dashboard.js`) ; (2) preuve reseau REELLE
  complementaire : `curl -N --max-time 35` sur `/gyms/occupancy/stream` a
  recu 5 evenements reels sur la fenetre, confirmant le flux serveur
  actif independamment de tout etat frontend.
- **Verification visuelle reelle (captures d'ecran, rendu effectif a
  l'ecran des 3 onglets/transitions/absence de chevauchement) NON
  effectuee** — extension Chrome indisponible, meme limite documentee
  depuis l'etape 5 du Jalon 1. L'absence de chevauchement a
  1920x1080/1366x768 a ete verifiee par CALCUL de mise en page CSS
  (max-width du conteneur principal inferieur aux deux largeurs cibles,
  aucun positionnement absolu dans le CSS hors SVG a coordonnees
  relatives au viewBox), pas par observation directe.

### Passe de style "holographique/neon" (post-sous-étape 6/6, 2026-07-09)

Voir `PROGRESS_JALON3.md` pour le detail complet (verifications reelles,
bug de specificite CSS trouve et corrige). Resume des decisions
structurantes :

- **Reste strictement en SVG/CSS 2D** : aucun moteur 3D/particules ajoute
  (contraire a la consigne initiale du projet, "silhouette schematique,
  pas de 3D anatomique") — l'effet "holographique" vient uniquement
  d'effets lumineux (`filter:drop-shadow`, `text-shadow`, degrades CSS)
  appliques a des elements 2D existants.
- **`--accent-cyan` : nouvelle couleur de marque, RESERVEE aux elements
  neutres** (nav d'onglets active, bordures de card sans code couleur de
  risque propre, chiffres "hero" generiques) — jamais utilisee pour les
  codes couleur de risque (Faible/Modere/Eleve, inchanges) ni pour
  remplacer `--accent-teal` existant (boutons/highlights deja en place,
  non touches pour ne rien casser visuellement d'etabli).
- **Lueur des zones/gauge pilotee par variable CSS injectee en JS
  (`--zone-glow-color`/`--gauge-glow`), JAMAIS par `style.filter` direct** :
  un `style.filter` inline depuis JS aurait une specificite superieure a
  TOUTE regle CSS de classe (`:hover`/`.selected`), ecrasant silencieusement
  ces etats au lieu de s'y additionner. La variable CSS permet aux 3
  couches (etat repos, hover, selected) de composer leurs propres
  `drop-shadow` sans jamais s'ecraser mutuellement.
- **Silhouette enrichie SANS toucher aux courbes de contour existantes**
  (deja eprouvees, jamais visuellement verifiees en navigateur donc
  jugees trop risquees a modifier sans retour visuel) : 2 nouvelles zones
  "trapezes" ajoutees (`data-muscle="shoulder"`, meme muscle_group que les
  deltoides, aucune nouvelle donnee necessaire), 16->18 zones cliquables
  au total.
- **Langage "gros chiffre + label discret" generalise** via
  `.hero-stat-value`/`.hero-stat-unit` (classes CSS partagees) : TDEE
  (nutrition), besoin proteique (nutrition), pourcentage d'occupation
  (affluence, couleur/lueur suivant la categorie de charge — jamais
  cyan neutre pour ces elements DEJA codes par couleur de risque), score
  de zone (`scoreBadge()` reecrit).
- **⚠️ Bug reel trouve et corrige, PREEXISTANT depuis la sous-etape 5/6,
  jamais dans le nouveau code de cette passe** : `.ml-prediction-panel`
  seul (specificite CSS (0,1,0)) perdait TOUJOURS face au selecteur
  partage `.bodymap-panel, .side-panel > div` (specificite (0,1,1)) pour
  les proprietes `background`/`border`/`border-radius`/`box-shadow` — ce
  panneau etant un `div` enfant direct de `.side-panel`, il correspond
  aux deux selecteurs, et en CSS la specificite tranche independamment de
  l'ordre d'apparition dans le fichier. **Consequence concrete : la
  bordure pointillee violette et l'ombre distinctive de l'encart "Tendance
  prédictive" n'avaient JAMAIS reellement rendu depuis leur creation** —
  jamais detecte faute de verification visuelle reelle. Corrige avec un
  selecteur combine ID+classe (`#ml-prediction-panel.ml-prediction-panel`).
  **Rappel utile pour toute future regle CSS de ce projet visant a
  surcharger le style par defaut d'un enfant direct d'un conteneur deja
  cible par un selecteur combinateur (`>`)** : verifier la specificite
  calculee, pas seulement l'ordre d'apparition dans le fichier.
- **Risque de debordement anticipe et corrige AVANT mise en prod (pas en
  reaction a un bug constate)** : en concevant le pattern "gros chiffre",
  une chaine combinee type "2446 kcal/jour" a `font-size:2rem` a ete
  identifiee comme deborderait une card de 260px — corrige en separant
  systematiquement le CHIFFRE (`.hero-stat-value`, toujours court) de son
  UNITE (`.hero-stat-unit`, span imbrique, police reduite).
- **Lisibilite garantie par construction, pas juste "verifiee au jugé"** :
  tous les `text-shadow` sont des halos flous DERRIERE un glyphe qui reste
  100% opaque au premier plan (technique standard "neon text") — ne
  floute jamais le texte lui-meme, contrairement a un `filter:blur()`
  global qui affecterait le glyphe entier. Blur radius volontairement
  modeste (12-18px).

## Conventions de nommage

- Services Docker Compose et conteneurs : prefixe `safelift-` (ex:
  `safelift-kafka`, `safelift-airflow-webserver`).
- Variables d'environnement : `SCREAMING_SNAKE_CASE`, groupees par service dans
  `.env.example` avec des commentaires de section.
- Commentaires dans le code : en francais. Noms de variables, services,
  fichiers : en anglais.

## Etat d'avancement des 6 etapes

Voir [PROGRESS.md](./PROGRESS.md) pour le detail complet. Resume :

1. ✅ Scaffolding repo + stack Docker locale (Kafka/Zookeeper, Spark,
   Airflow, 2x Postgres, dashboard placeholder)
2. 🔄 En cours — volet A (telechargement Kaggle) et volet B (ingestion
   Bronze CSV -> Parquet via `airflow/dags/bronze_ingestion.py`, teste et
   verifie en conditions reelles) faits ; reste a faire : producteur Kafka +
   DAG(s) consommant Kafka
3. ✅ Transformation Silver (nettoyage/normalisation via Spark, orchestre par
   `airflow/dags/silver_transformation.py`, declenche automatiquement par
   `bronze_ingestion`, teste et verifie en conditions reelles — voir
   `data/silver/CLEANING_LOG.md`)
4. ✅ Gold : modele en etoile (dbt sur Postgres) + risk_score deterministe,
   orchestre par `airflow/dags/gold_dbt_run.py` (6 tasks, dont fuzzy
   matching hors dbt), declenche automatiquement par `silver_transformation`,
   teste et verifie en conditions reelles (dbt run 10/10, dbt test 60/60).
   Matching exercise_name 38.3%->90.1% (pipeline 4 etapes), risk_score
   recalibre honnetement (0%->1.2% "Eleve") — voir
   `data/gold/GOLD_MODEL_DECISIONS.md`
5. 🔄 En cours — Serving : API FastAPI (7 endpoints, +2 avec la Feature A
   ci-dessous) + dashboard theme sombre (KPIs, jauge radiale, zones
   sensibles, silhouette SVG enrichie, panneau Simulateur what-if).
   Backend entierement teste (endpoints reels, gestion d'erreur 404,
   fichiers statiques) ; **verification visuelle (rendu du theme sombre,
   jauge, cards KPI, panneau Simulateur, captures d'ecran) pas encore
   confirmee** (extension navigateur indisponible sur 4 sessions
   consecutives) — voir PROGRESS.md etape 5, TODO Moulaye ci-dessous.
   **Feature A (2026-07-06) : simulateur what-if** — `POST
   /api/simulate-risk` + `GET /users/{id}/exercises`, formule deterministe
   partagee `dashboard/risk_formula.py` (duplication assumee et documentee
   de `fact_risk_score.sql`), teste avec 3 cas reels (charge en forte
   hausse/baisse/neutre sur "Overhead Press (Barbell)", `user_id=9`) +
   verification du repli zone (exercice jamais pratique), `risk_score_actuel`
   confirme identique a `gold.fact_risk_score` dans les 2 cas (au niveau
   exercice ET repli zone). Panneau frontend ajoute en surcouche pure
   (silhouette SVG non modifiee), verifie structurellement (JS/HTML/CSS
   bien formes, tous les `id` references resolus) mais **pas visuellement**
   (meme limite que le reste du dashboard).
6. ✅ fait — Terraform + AWS S3/Athena (AWS Academy Learner Lab).
   Sous-etape 1/6 (audit lecture-seule du compte lab) faite le 2026-07-06 :
   credentials valides, role actif `voclabs` identifie, ARN `LabRole`
   recupere pour usage futur, acces S3/Athena confirmes, region
   `us-east-1` a utiliser explicitement (aucune region par defaut sur le
   compte). Sous-etape 2/6 (2026-07-06, meme jour) : bucket S3
   `safelift-datalake-097115946702` + base/tables Athena (Glue Catalog)
   appliques pour de vrai (`terraform apply`, 20 ressources, 0 IAM cree),
   script `scripts/upload_gold_to_s3.py` execute reellement (9 569 lignes,
   7 tables Gold exportees en Parquet et uploadees), requetes Athena reelles
   confirmant la coherence des donnees (`COUNT(*) fact_risk_score = 2164`,
   distribution risk_level identique a celle documentee en etape 4). Detail
   complet dans `terraform/AWS_LAB_CONSTRAINTS.md`. Sous-etape 4/6 (gouvernance
   RGPD : pseudonymisation HMAC-SHA256, chiffrement, retention, droit a
   l'effacement) faite le 2026-07-07, voir `docs/RGPD_GOVERNANCE.md` et
   `docs/DATA_CATALOG.md`. Sous-etape 3/6 (CI/CD GitHub Actions : validate +
   plan, jamais d'apply auto) faite le 2026-07-08 — voir
   `.github/workflows/terraform-ci.yml`/`README.md` et PROGRESS.md pour le
   detail des 2 runs reels de verification. **Jalon 1 (etapes 1 a 6) cloture
   le 2026-07-08** — voir le resume de cloture en fin de PROGRESS.md.

## Etat d'avancement du Jalon 2 (streaming affluence)

Voir [PROGRESS_JALON2.md](./PROGRESS_JALON2.md) pour le detail complet.
Resume :

1. ✅ fait (2026-07-09) — Simulateur d'affluence + producteur Kafka :
   `dim_gym` (Gold, 5 salles fictives), topic Kafka dedie
   `safelift-gym-occupancy`, service Docker `gym-simulator`
   (`scripts/simulate_gym_occupancy.py`, pattern d'affluence realiste par
   lissage + bruit, PAS un DAG Airflow). Teste en conditions reelles
   (dbt test 5/5, topic confirme, 10 messages reels consommes via
   `kafka-console-consumer`, arret propre SIGTERM verifie).
2. ✅ fait (2026-07-09, meme jour) — Consumer Spark Structured Streaming :
   `spark/jobs/stream_gym_occupancy.py` (`startingOffsets=latest`, schema
   JSON explicite, `occupancy_rate`/`charge_category` calcules, upsert
   `ON CONFLICT` vers `gold.gym_occupancy_live`, aucun operateur stateful),
   service Docker `spark-streaming-gym` (mode client sur le cluster Spark
   existant, PAS un DAG Airflow). Teste en conditions reelles : table
   confirmee a jour en temps reel (2 requetes a ~25s d'intervalle, toutes
   colonnes variables changees, une salle a bascule Faible->Moderee en
   direct), resilience aux messages JSON malformes verifiee en injectant
   un message invalide reel sur le topic (loggue + ignore, stream
   poursuivi), fonctionnement continu confirme (`healthy`, 0 restart).
3. ✅ fait (2026-07-09, meme jour) — Inputs utilisateur temps reel :
   `POST /users/{user_id}/sessions` (dashboard) publie sur Kafka
   (`safelift-user-inputs`, jamais d'ecriture DB directe -- decouplage
   evenementiel), `scripts/consume_user_inputs.py` (insere dans
   `raw.realtime_user_sessions`, declenche `gold_dbt_run` via l'API REST
   Airflow), union `stg_weight_training`/`stg_realtime_user_sessions` au
   niveau staging (`stg_workout_sessions_unified.sql`, formule risk_score
   UNIQUE et non dupliquee). Panneau "Logger une séance" cote dashboard
   (polling automatique jusqu'a changement du score). **Test end-to-end
   reel sur user_id=9** : score passe de 15.19 a 7.81 (zone arms) apres
   soumission d'une seance, delai reel mesure ~70s. 2 bugs reels trouves et
   corriges (contention de coeurs Spark entre spark-streaming-gym et les
   jobs batch ; dim_date trop etroite pour des dates 2026). Voir
   PROGRESS_JALON2.md et GOLD_MODEL_DECISIONS.md section 11.
4. ✅ fait (2026-07-09, meme jour) — Dashboard temps reel affluence (SSE) :
   `GET /gyms/occupancy/stream` (Server-Sent Events, decision deja actee --
   pas de WebSocket/polling pour cette fonctionnalite, independant du
   polling existant du recalcul de risque), `GET /gyms/{gym_id}/best_slot`
   (recommandation de creneau basee sur le pattern THEORIQUE du simulateur,
   PAS un historique reel -- limitation assumee et documentee). Section
   "Affluence en direct" cote dashboard (jauges par salle, pastille "EN
   DIRECT" pulsante, EventSource natif avec reconnexion automatique). 1 bug
   reel trouve et corrige (deduplication SSE cassee par l'inclusion de
   `server_time` dans la comparaison -- constate directement via `curl -N`
   montrant des evenements dupliques). Verifie reellement : 7 evenements
   distincts sur une connexion SSE unique de 40s, aucune fuite de connexion
   Postgres apres deconnexion brutale. Voir PROGRESS_JALON2.md et
   GOLD_MODEL_DECISIONS.md section 12.
5. ✅ fait (2026-07-09, meme jour) — Verification globale + script de
   demonstration (clot le Jalon 2, aucun nouveau developpement) : fenetre
   de stabilite de 13 minutes en conditions reelles (26 mesures/30s,
   allocation de coeurs Spark + statut/RestartCount des 4 services), avec
   une seance utilisateur reelle declenchee au milieu (delai mesure : 110s,
   2e mesure independante apres les 70s de la sous-etape 3/5 -- timeout de
   polling frontend releve de 120s a 180s en consequence). **Preuve
   horodatee que le correctif `spark.cores.max=1` tient dans la duree**
   (`coresused` 1/2 -> 2/2 -> 1/2 exactement pendant le run dbt declenche,
   puis retour a la normale). Test reel de panne/reprise
   (`docker compose stop/start gym-simulator`) : les 11 autres services
   restent `healthy`, aucun crash en cascade, `spark-streaming-gym`
   continue sans erreur (micro-batches vides silencieusement ignores),
   reprise propre au redemarrage (recharge `dim_gym`, `spark-streaming-gym`
   reprend sans ecart de numerotation de batch). `docs/DEMO_SCRIPT_JALON2.md`
   (nouveau) : script chronometre pour la soutenance, options de
   transition pendant l'attente, plan de secours si un service ne repond
   pas, 7 questions probables du jury avec pistes de reponse. **Jalon 2
   (5/5 sous-etapes) cloture le 2026-07-09.** Voir PROGRESS_JALON2.md
   (resume de cloture en fin de fichier).

## Etat d'avancement du Jalon 3 (nutrition + ML bonus)

Voir [PROGRESS_JALON3.md](./PROGRESS_JALON3.md) pour le detail complet.
Resume :

1. ✅ fait (2026-07-09) — Ingestion nutrition + dimension + calculs
   deterministes : `airflow/dags/nutrition_ingestion.py` (DAG
   self-contained, 5 tasks, appel reel API USDA FoodData Central ->
   Bronze -> Silver -> Postgres -> dbt scope restreint),
   `gold.dim_nutrition` (119 aliments reels), `gold.fact_nutrition_target`
   (973 utilisateurs, BMR Mifflin-St Jeor + TDEE + besoin proteique cible
   — formules standard deterministes, PAS des recommandations medicales
   personnalisees, voir GOLD_MODEL_DECISIONS.md section 13). Teste en
   conditions reelles : 16/16 tests dbt PASS (scope nutrition), 93/93 en
   comptant tout le projet (aucune regression Jalon 1/2), cle API jamais loggee
   (verifie par recherche exhaustive dans les logs), sanity check reel
   confirmant qu'a poids egal, plus de jours d'entrainement = TDEE plus
   eleve. 1 bug reel corrige (DAG reste bloque en pause a sa premiere
   apparition, `airflow dags unpause` necessaire — a refaire pour tout
   futur nouveau DAG de ce projet, voir rappel operationnel plus bas).
2. ✅ fait (2026-07-09, meme jour) — Dashboard nutrition :
   `GET /users/{user_id}/nutrition` (lecture directe de
   `fact_nutrition_target`/`dim_nutrition`, aucun recalcul cote API,
   champ `disclaimer` toujours present dans la reponse), section
   "Nutrition" cote dashboard (cards BMR/TDEE, jauge proteique, 8 aliments
   suggeres) avec **avertissement ethique affiche en tout premier element
   de la section, jamais cache** (contrainte non negociable). Teste sur 3
   profils reels differents (40kg -> TDEE 1711 ; 64.1kg -> TDEE 2446 ;
   129.9kg -> TDEE 3271, coherent avec la sous-etape 1/6). Verification
   VISUELLE (capture d'ecran) non effectuee (extension Chrome
   indisponible) — verification structurelle du DOM/CSS confirme
   l'absence de toute classe/regle masquant l'avertissement par defaut.
3. ✅ fait (2026-07-09, meme jour) — Preparation des donnees ML (pas
   d'entrainement) : `scripts/prepare_ml_features.py` agrege
   `gold.fact_risk_score` par `(user_id, muscle_group, week_start_date)`,
   construit des lags calendaires exacts (1/2/3 semaines, `NULL` si la
   semaine precise n'existe pas, jamais interpole) et la cible
   `target_next_week_risk_score` (semaine suivante). Split TEMPOREL (pas
   aleatoire) : cutoff `2018-03-19`, 491 lignes train / 161 lignes test.
   **Mono-utilisateur** (`user_id=9` uniquement) — limite documentee en
   detail, avec chiffres exacts et verification anti-fuite reelle, dans
   `data/ml/ML_DATA_PREP.md`.
4. ✅ fait (2026-07-09, meme jour) — Entrainement + evaluation du modele
   ML bonus : `scripts/train_risk_trend_model.py` (`scikit-learn==1.5.2`
   ajoute a `airflow/requirements.txt`, image `safelift-airflow:local`
   reconstruite). **UN SEUL modele poole sur les 8 zones** (`muscle_group`
   en one-hot), pas 8 modeles independants (decision deja actee en
   sous-etape 3/6). 3 approches comparees sur le TEST set (161 lignes) :
   baseline naive (RMSE 14.69), Ridge (RMSE 9.27), RandomForest
   `max_depth=4` (RMSE 9.12, **modele retenu**) — les deux modeles ML
   battent nettement la baseline (~37% de reduction RMSE). Feature
   importance RandomForest dominee par `lag_1`/`lag_2_risk_score` (45.7%
   cumule) — signal temporel coherent avec l'objectif, pas du bruit.
   Modele sauvegarde (`data/ml/model.pkl`, pipeline scikit-learn complet +
   metadonnees). **Conclusion honnete documentee dans
   `data/ml/ML_TRAINING_RESULTS.md`** : gain reel mais strictement borne
   au perimetre mono-utilisateur deja documente, pas une preuve de
   generalisation.
5. ✅ fait (2026-07-09, meme jour) — Integration pipeline + dashboard de la
   prediction ML : `scripts/score_risk_trend.py` (charge `data/ml/model.pkl`,
   REUTILISE `prepare_ml_features.fetch_weekly_aggregates`/`build_features`
   et `train_risk_trend_model.impute_lag_nulls` par import, aucune
   duplication) ecrit `gold.ml_risk_prediction` (table creee directement en
   psycopg2, PAS geree par dbt). Ne produit une prediction QUE pour les
   `(user_id, muscle_group)` avec au moins une semaine d'historique reel
   (concretement `user_id=9` uniquement, 8 lignes) — aucune extrapolation
   pour un utilisateur sans historique. DAG `airflow/dags/ml_scoring.py`
   declenche automatiquement en fin de `gold_dbt_run` (`trigger_ml_scoring`
   ajoute apres `dbt_test`, meme mecanisme `TriggerDagRunOperator` que le
   reste de la cascade). `GET /users/{user_id}/risk/prediction` (dashboard) :
   lecture seule, `available:false` + message clair (jamais 500) si la table
   n'existe pas encore ou si l'utilisateur n'a aucune prediction. Encart
   dashboard "Tendance prédictive" **volontairement distinct** du risk_score
   deterministe (bordure pointillee violette + badge EXPERIMENTAL, jamais
   fusionne visuellement), desactive en mode demo, message explicite
   "non disponible pour ce profil" si aucune prediction. **Testee en
   conditions reelles de bout en bout** : DAG `gold_dbt_run` declenche
   manuellement -> cascade complete jusqu'a `ml_scoring` confirmee
   (`trigger_ml_scoring` puis run `ml_scoring` tous deux `success`), 8
   predictions ecrites avec un `scored_at` frais ; endpoint teste sur
   `user_id=9` (8 predictions) ET sur un utilisateur sans historique
   (`available:false`, message clair) ET sur un `user_id` inexistant (404
   propre). **Constat honnete non masque** : pour les zones `arms`/`back`,
   la semaine la plus recente disponible est un point isole de test
   temps reel (2026-07-06, Jalon 2) sans aucun contexte de lags reels
   (imputes a 0) — prediction techniquement produite mais moins fiable que
   les 6 autres zones, ancrees sur l'historique dense reel de 2018.
6. ✅ fait (2026-07-09, meme jour) — Refonte UX/UI par onglets +
   corrections de debordement : navigation SPA-style par 3 onglets
   ("Risque & Entraînement", "Affluence", "Nutrition", changement de vue
   100% JS, aucun rechargement/route serveur), pas de nouvelle
   fonctionnalite. Header (selecteur utilisateur + toggle demo) regroupe
   avec le bandeau demo et la nav dans un wrapper `sticky`, visible en
   permanence quel que soit l'onglet. Debordements de texte corriges (2
   familles : ellipsis+title sur elements HTML normaux, troncature JS
   explicite sur les `<select>` natifs — CSS seul peu fiable sur leur
   etat ferme). Espacements harmonises (`margin-top` redondant retire de
   3 panneaux). **SSE de l'affluence confirmee survivre au changement
   d'onglet** (preuve par construction : `switchTab()` ne touche jamais
   `occupancyEventSource` ni ne ferme la connexion — les panneaux
   masques via `display:none` restent dans le DOM ; verifie aussi en
   conditions reelles, 5 evenements SSE recus sur une fenetre de 35s via
   `curl -N`). Voir PROGRESS_JALON3.md pour le detail complet (bug de
   faux positif sur le comptage de balises, calibration reelle du seuil
   de troncature sur les 81 exercices de `user_id=9`).
7. ✅ fait (2026-07-09, meme jour) — Passe de style "holographique/neon"
   (post-sous-etape 6/6, purement visuelle, aucune nouvelle
   fonctionnalite) : nouvelle couleur de marque `--accent-cyan` (elements
   neutres uniquement, codes couleur de risque inchanges), lueurs
   `filter:drop-shadow`/`text-shadow` sur la silhouette (zones actives +
   anneau du score global) et les chiffres "hero", silhouette enrichie de
   2 zones "trapezes" (mappees sur `shoulder`, meme muscle_group — 16->18
   zones cliquables), langage "gros chiffre + label discret" generalise
   (TDEE, proteines, occupancy, score de zone). **Bug reel trouve et
   corrige, preexistant depuis la sous-etape 5/6** : specificite CSS
   insuffisante (`.ml-prediction-panel` seul perdait face au selecteur
   partage `.bodymap-panel, .side-panel > div`) faisait que la bordure
   pointillee violette de "Tendance prédictive" n'avait JAMAIS reellement
   rendu — corrige avec un selecteur combine ID+classe. Risque de
   debordement anticipe et corrige AVANT mise en prod (chiffre/unite
   systematiquement separes en 2 tailles de police distinctes). Non-
   regression confirmee reellement (backend, SSE 5 evenements/35s,
   simulateur what-if, 12 services `healthy`). Voir PROGRESS_JALON3.md
   pour le detail complet.

## Prochaines actions prevues

> ⚠️ **TODO manuel Moulaye (1)** : verifier la licence Kaggle du dataset
> `weight_training` (721 Weight Training Workouts, `joep89/weightlifting`)
> avant soutenance — actuellement `unknown` (aucune licence declaree par
> l'auteur, confirme via l'API Kaggle). Voir
> `data/bronze/SCHEMA_NOTES.md` section "Statut de licence".

> ⚠️ **TODO manuel Moulaye (2)** : confirmer visuellement le dashboard
> (http://localhost:18000), apres 3 iterations (design initial + correctifs
> A/B + refonte theme sombre, toutes le 2026-07-04) — theme sombre "app
> tracker" (fond quasi-noir, cards arrondies, ombres douces), 4 cards KPI
> en tete (derniere seance, jauge radiale score global, tendance, zones en
> alerte), panneau "Zones sensibles" (liste des zones Modere/Eleve avec
> facteurs dominants en langage clair), silhouette enrichie (pectoraux en 2
> moities, zone mollets ajoutee mais toujours grisee), dropdown utilisateur
> groupe (profils avec donnees en premier, pre-selectionnes), toggle demo
> (bandeau ambre), **et desormais le panneau "Simulateur what-if" (Feature
> A, 2026-07-06)** : selecteur d'exercice + 4 sliders, message genere,
> barres de facteurs, surcouche pointillee sur la silhouette. Non verifie
> visuellement par Claude Code (extension navigateur indisponible sur les
> 4 sessions a ce jour) — backend et structure XML/JS/CSS entierement
> testes, y compris coherence des `id` HTML<->JS (voir PROGRESS.md etape
> 5), seul le rendu visuel/esthetique reste a confirmer.

- Chaine complete operationnelle de bout en bout : Kaggle -> Bronze -> Silver
  -> Gold (modele en etoile + risk_score) -> Serving (API + dashboard),
  declenchement en cascade automatique pour Bronze/Silver/Gold
  (`bronze_ingestion -> silver_transformation -> gold_dbt_run`), testee en
  conditions reelles. Voir PROGRESS.md etapes 2/3/4/5 pour le detail complet.
- Definir precisement la suite du perimetre fonctionnel de l'etape 2 :
  producteur Kafka de donnees simulees + DAG(s) consommant Kafka (ce volet
  n'a jamais ete traite, independamment des etapes 3/Silver/4/Gold/5/Serving
  qui, elles, sont terminees ou en cours).
- Etape 6/6 terminee : Terraform + AWS S3/Athena sur AWS Academy Learner Lab.
  Sous-etape 1/6 (audit) et sous-etape 2/6 (ressources S3+Athena reelles +
  export Gold->S3 + requetes Athena de validation) terminees (2026-07-06),
  voir `terraform/AWS_LAB_CONSTRAINTS.md`. Sous-etape 4/6 (gouvernance RGPD)
  terminee (2026-07-07), voir `docs/RGPD_GOVERNANCE.md`/`docs/DATA_CATALOG.md`.
  Sous-etape 3/6 (CI/CD GitHub Actions) terminee (2026-07-08), voir
  `.github/workflows/`. **Jalon 1 (etapes 1 a 6) cloture le 2026-07-08** —
  resume complet en fin de PROGRESS.md. Rappel explicite recu pour l'etape 5
  (pas de streaming Kafka, pas de nutrition, pas de ML a ce stade) toujours
  valable pour le perimetre fonctionnel du dashboard.

- **`terraform/athena.tf` (colonnes `user_pseudo_id`) applique reellement sur
  AWS le 2026-07-07** (memes credentials rafraichies par l'utilisateur en
  cours de session — le blocage initial n'etait PAS une expiration mais un
  fichier `~/.aws/credentials` malforme, `aws_access_key_id` contenant son
  propre nom de cle en double dans la valeur, corrige manuellement).
  `terraform plan` : 3 to change (les 3 tables Glue concernees), 0 to
  add/destroy ; `terraform apply` : succes. `scripts/upload_gold_to_s3.py`
  relance : 7 tables re-exportees (9 569 lignes). **Requetes Athena reelles
  de validation** : `SELECT count(*), count(DISTINCT user_pseudo_id) FROM
  gold.fact_risk_score` -> `2164, 1` (coherent : un seul `user_id` reel dans
  cette table) ; `SELECT user_pseudo_id FROM gold.dim_user LIMIT 3` ->
  hachages hexadecimaux de 64 caracteres confirmes (aucun `user_id` en clair
  visible cote AWS).
- Le `risk_score` (Gold) est une formule 100% deterministe (pas de ML) —
  toute evolution future vers du ML devra etre une etape explicitement
  identifiee, pas une modification silencieuse de `fact_risk_score.sql`.

## Comment reprendre une session de travail

1. Lire ce fichier (`CLAUDE.md`), `PROGRESS.md` (Jalon 1),
   `PROGRESS_JALON2.md` (Jalon 2) et `PROGRESS_JALON3.md` (Jalon 3) en
   entier.
2. Verifier l'etat reel du repo (`git log`, `git status`) pour confirmer que
   ces fichiers sont a jour par rapport au code.
3. Si un ecart est constate entre la memoire (ce fichier) et le repo reel,
   faire confiance au repo et corriger ce fichier en consequence.
4. Ne pas anticiper les etapes futures tant qu'elles n'ont pas ete
   explicitement demandees.
5. **Apres avoir cree un NOUVEAU DAG Airflow** : penser a le depauser
   explicitement (`airflow dags unpause <dag_id>`, via
   `docker compose exec airflow-webserver ...`) — `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true`
   met tout nouveau DAG en pause par defaut, et un DAG en pause ne voit
   AUCUNE task planifiee par le scheduler meme pour un run declenche
   manuellement (le run reste `queued` indefiniment). Voir
   PROGRESS_JALON3.md sous-etape 1/6 pour le detail du bug reel rencontre.
