# CLAUDE.md — Memoire de projet SafeLift

> Ce fichier est la source de verite pour reprendre le projet dans une nouvelle
> session, meme sans historique de conversation. A lire EN PREMIER, avec
> [PROGRESS.md](./PROGRESS.md), avant de proposer quoi que ce soit.
>
> Regle : ce fichier doit toujours refleter l'etat REEL du repo. Le mettre a
> jour AVANT de considerer une etape terminee.

## Contexte global

SafeLift est un projet Data Engineering realise dans le cadre de la
certification RNCP36739 (M2 Data Engineering & IA). Il simule un pipeline de
donnees de bout en bout pour un cas d'usage de type "detection d'anomalies /
suivi d'activite" (a preciser/affiner au fil des etapes fonctionnelles).

Le projet est decoupe en **6 etapes** (voir [PROGRESS.md](./PROGRESS.md) pour
le detail et le statut de chacune). Chaque etape doit etre livree de maniere
100% fonctionnelle (pas de pseudo-code, pas de TODO vague) avant de passer a
la suivante.

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
6. 🔄 En cours — Terraform + AWS S3/Athena (AWS Academy Learner Lab).
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
   `docs/DATA_CATALOG.md` — sous-etape 3/6 (CI/CD GitHub Actions) volontairement
   sautee/pas encore definie, traitee apres la 4/6 a la demande explicite de
   l'utilisateur.

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
- Etape 6 en cours : Terraform + AWS S3/Athena sur AWS Academy Learner Lab.
  Sous-etape 1/6 (audit) et sous-etape 2/6 (ressources S3+Athena reelles +
  export Gold->S3 + requetes Athena de validation) terminees (2026-07-06),
  voir `terraform/AWS_LAB_CONSTRAINTS.md`. Sous-etape 4/6 (gouvernance RGPD)
  terminee (2026-07-07), voir `docs/RGPD_GOVERNANCE.md`/`docs/DATA_CATALOG.md`.
  Sous-etape 3/6 (CI/CD GitHub Actions) toujours non definie. Rappel explicite
  recu pour l'etape 5 (pas de streaming Kafka, pas de nutrition, pas de ML a ce
  stade) toujours valable pour le perimetre fonctionnel du dashboard.

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

1. Lire ce fichier (`CLAUDE.md`) et `PROGRESS.md` en entier.
2. Verifier l'etat reel du repo (`git log`, `git status`) pour confirmer que
   ces fichiers sont a jour par rapport au code.
3. Si un ecart est constate entre la memoire (ce fichier) et le repo reel,
   faire confiance au repo et corriger ce fichier en consequence.
4. Ne pas anticiper les etapes futures tant qu'elles n'ont pas ete
   explicitement demandees.
