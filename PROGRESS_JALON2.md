# PROGRESS_JALON2.md — Suivi d'avancement SafeLift, Jalon 2 (streaming affluence)

> Meme legende/regle que PROGRESS.md (✅ fait · 🔄 en cours · ⏳ a faire,
> toujours a jour AVANT de considerer une etape terminee).
>
> **Pourquoi un fichier separe de PROGRESS.md** : PROGRESS.md (Jalon 1,
> etapes 1 a 6) depasse desormais 1000 lignes — au-dela d'un certain volume,
> y ajouter un nouveau jalon degraderait la lisibilite des deux sans
> beneice reel (les deux jalons sont fonctionnellement independants : le
> Jalon 1 est clos et ne sera plus modifie). Voir CLAUDE.md pour le pointeur
> vers ce fichier et le contexte global du projet.

## Contexte du Jalon 2

Le Jalon 1 (pipeline batch complet : Kaggle -> Bronze -> Silver -> Gold ->
Serving -> AWS/Terraform -> gouvernance RGPD -> CI/CD) est clos depuis le
2026-07-08 (voir le resume de cloture en fin de PROGRESS.md). Le Jalon 2
ajoute un **flux de streaming temps reel** (affluence par salle de sport)
pour rendre le Bloc 2 de la certification (pipelines temps reel)
incontestable — perimetre distinct du Bloc 1 (deja couvert par le Jalon 1).

Decoupage (5 sous-etapes, toutes traitees — **Jalon 2 complet**) :
1. Simulateur d'affluence + producteur Kafka — ✅ fait
2. Consumer Spark Structured Streaming — ✅ fait
3. Inputs utilisateur temps reel (Kafka -> raw -> dbt trigger) — ✅ fait
4. Dashboard temps reel affluence (SSE) — ✅ fait
5. Verification globale + script de demonstration — ✅ fait

## Sous-etape 1/5 — Simulateur d'affluence + producteur Kafka — ✅ fait

**Date** : 2026-07-09.

**Perimetre explicitement borne** : simulateur + producteur Kafka
UNIQUEMENT. Pas de consumer Spark Structured Streaming (sous-etape
suivante), pas de dashboard (le dashboard actuel, Jalon 1, reste
inchange).

**Decision deja actee (rappel)** : le consumer de la sous-etape suivante
sera en Spark Structured Streaming — format de message anticipe en
consequence (JSON simple, un message = un evenement d'affluence a un
instant T pour une salle donnee).

### Livre

- **`dbt/seeds/dim_gym_seed.csv` + `dbt/models/marts/dim_gym.sql`** :
  nouvelle dimension `gold.dim_gym`, 5 salles **100% FICTIVES**
  (gym_id, gym_name, city, neighborhood, capacity_max) — aucun dataset
  Kaggle d'affluence disponible, decision du cahier des charges du Jalon 2
  de retenir un simulateur custom plutot qu'un dataset synthetique
  externe. Documente dans le commentaire en tete de `dim_gym.sql` ET dans
  `data/gold/GOLD_MODEL_DECISIONS.md` section 9 (meme pattern de
  documentation que `dim_muscle`/`fact_risk_score_demo_synthetic` au
  Jalon 1). Tests dbt : `unique`/`not_null` sur `gym_id`, `unique`/`not_null`
  sur `gym_name`, `not_null` sur `capacity_max` — **5/5 PASS** (execution
  reelle, voir ci-dessous).
- **Topic Kafka dedie `safelift-gym-occupancy`** (partitions=1,
  replication-factor=1, coherent avec le cluster mono-broker existant) :
  cree via le conteneur `kafka-init` existant (etendu pour creer 2 topics
  au lieu d'un seul), pas via un `AdminClient` cote script — source unique
  de verite pour les topics du projet, meme mecanisme qu'au Jalon 1. Le
  topic de test `safelift-test-topic` (etape 1/6 du Jalon 1) reste
  inchange et n'est pas reutilise ici.
- **`scripts/simulate_gym_occupancy.py`** : processus long-courant (PAS un
  DAG Airflow — c'est un flux de streaming, pas un batch planifie).
  - Lit `gold.dim_gym` une seule fois au demarrage (table statique) avec
    retry/backoff (jusqu'a 10 tentatives) si la table n'existe pas encore.
  - Publie un message JSON par salle toutes les 5 a 10 secondes
    (configurable via `GYM_SIMULATOR_INTERVAL_MIN_SECONDS`/
    `GYM_SIMULATOR_INTERVAL_MAX_SECONDS`, `.env`), avec exactement les 4
    champs demandes : `gym_id`, `timestamp` (ISO 8601, UTC), 
    `current_occupancy`, `capacity`.
  - **Pattern d'affluence realiste, pas un bruit uniforme** :
    `peak_ratio(dt)` definit un ratio cible (0 a 1 de `capacity_max`) selon
    l'heure ET le jour de la semaine (semaine : pics 7h-9h/18h-21h a 0.75,
    modere 9h-18h/21h-23h a 0.35, nuit 23h-7h a 0.05 ; week-end : plateau
    10h-19h a 0.45, sinon 0.05 — pas de double pic domicile-travail un jour
    ferie). `next_occupancy()` fait converger l'occupation courante vers
    cette cible par lissage exponentiel (`SMOOTHING=0.35`) + bruit gaussien
    (`NOISE_STD_RATIO=0.05`), ce qui produit une trajectoire lissee dans le
    temps (comme une vraie affluence qui monte/descend progressivement)
    plutot qu'un tirage i.i.d. independant a chaque message.
  - Producteur Kafka : `confluent-kafka` (choix documente dans le script et
    `scripts/requirements_gym_simulator.txt` : coherent avec
    `apache-airflow-providers-apache-kafka`, deja present dans
    `airflow/requirements.txt`, qui s'appuie lui-meme sur `confluent-kafka`
    — verifie via les metadonnees PyPI du provider, `confluent-kafka>=2.3.0`).
  - Cle de message Kafka = `gym_id` (bonne pratique de partitionnement,
    utile si le topic gagne des partitions plus tard).
  - Arret propre : gestion de `SIGINT`/`SIGTERM` (flag + flush final du
    producteur avant `sys.exit(0)`), boucle d'attente decoupee en pas de
    0.5s pour reagir vite a un signal plutot qu'un `time.sleep()` bloquant.
  - Jamais de crash silencieux : logs clairs a chaque emission (callback de
    livraison Kafka, succes ET echec) et sur toute exception inattendue
    (`logger.exception`, la boucle continue avec les autres salles plutot
    que de tout arreter pour un seul message en echec).
  - Heartbeat (`/tmp/gym_simulator_heartbeat`, reecrit a chaque cycle
    reussi) relu par le healthcheck Docker (pas de port HTTP expose par ce
    service, contrairement au dashboard).
- **`scripts/Dockerfile.gym_simulator`** + **`scripts/requirements_gym_simulator.txt`** :
  image dediee (`python:3.12-slim`, meme base que `dashboard/Dockerfile`),
  build context `./scripts` (le script reste au meme endroit que les
  autres scripts autonomes du projet, comme demande).
- **`docker-compose.yml`** : nouveau service `gym-simulator`
  (`restart: unless-stopped`, `depends_on` `kafka-init`
  (`service_completed_successfully`) et `app-postgres`
  (`service_healthy`), healthcheck base sur le fichier de heartbeat,
  variables d'environnement dediees). `kafka-init` etendu pour creer les 2
  topics (test + gym-occupancy) au lieu d'un seul.
- **`.env` / `.env.example`** : nouvelles variables
  `KAFKA_GYM_OCCUPANCY_TOPIC=safelift-gym-occupancy`,
  `GYM_SIMULATOR_INTERVAL_MIN_SECONDS=5`,
  `GYM_SIMULATOR_INTERVAL_MAX_SECONDS=10`.
- **`dbt/seeds/_seeds__properties.yml`** / **`dbt/models/marts/_marts__models.yml`** :
  entrees ajoutees pour `dim_gym_seed`/`dim_gym` (descriptions + tests).

### Verifications effectuees (toutes reelles, aucun dry-run)

1. **dbt seed + run + test, executes directement dans le conteneur Airflow
   existant** (`/opt/dbt_venv/bin/dbt`, memes credentials Postgres que
   `airflow/dags/gold_dbt_run.py`) plutot que de declencher tout le DAG
   `gold_dbt_run` (aurait retraite inutilement tout le pipeline
   Bronze/Silver existant pour ajouter une seule dimension statique sans
   dependance sur les autres sources) :
   - `dbt seed --select dim_gym_seed` : **`INSERT 5` — succes**.
   - `dbt run --select dim_gym` : **`SELECT 5` — succes**, table
     `gold.dim_gym` creee.
   - `dbt test --select dim_gym` : **5/5 PASS** (`not_null`/`unique` sur
     `gym_id`/`gym_name`, `not_null` sur `capacity_max`).
2. **Topic Kafka** : `kafka-init` relance (`docker compose up -d kafka-init`)
   — logs confirmes : `Created topic safelift-gym-occupancy.`, puis liste
   des 2 topics (`safelift-gym-occupancy`, `safelift-test-topic`).
3. **Build + demarrage du service** : `docker compose build gym-simulator`
   (succes, wheels `confluent-kafka`/`psycopg2-binary` disponibles pour la
   plateforme du runner) puis `docker compose up -d gym-simulator` —
   conteneur `healthy` en quelques secondes.
4. **Logs reels au demarrage** : `5 salle(s) chargee(s) depuis gold.dim_gym :
   SafeLift Bastille, SafeLift Part-Dieu, SafeLift Vieux-Port, SafeLift
   Capitole, SafeLift Chartrons`, puis un message livre par salle
   immediatement, avec confirmation de livraison individuelle
   (partition/offset) pour chacun.
5. **Echantillon reel consomme via `kafka-console-consumer`**
   (`--from-beginning --max-messages 10`) — 10 messages recus, identiques
   aux logs du producteur :
   ```
   {"gym_id": 1, "timestamp": "2026-07-08T21:13:12.051249+00:00", "current_occupancy": 45, "capacity": 140}
   {"gym_id": 2, "timestamp": "2026-07-08T21:13:12.051249+00:00", "current_occupancy": 50, "capacity": 180}
   {"gym_id": 3, "timestamp": "2026-07-08T21:13:12.051249+00:00", "current_occupancy": 33, "capacity": 100}
   {"gym_id": 4, "timestamp": "2026-07-08T21:13:12.051249+00:00", "current_occupancy": 48, "capacity": 130}
   {"gym_id": 5, "timestamp": "2026-07-08T21:13:12.051249+00:00", "current_occupancy": 29, "capacity": 90}
   {"gym_id": 1, "timestamp": "2026-07-08T21:13:22.355460+00:00", "current_occupancy": 47, "capacity": 140}
   {"gym_id": 2, "timestamp": "2026-07-08T21:13:22.355460+00:00", "current_occupancy": 65, "capacity": 180}
   {"gym_id": 3, "timestamp": "2026-07-08T21:13:22.355460+00:00", "current_occupancy": 37, "capacity": 100}
   {"gym_id": 4, "timestamp": "2026-07-08T21:13:22.355460+00:00", "current_occupancy": 59, "capacity": 130}
   {"gym_id": 5, "timestamp": "2026-07-08T21:13:22.355460+00:00", "current_occupancy": 28, "capacity": 90}
   ```
6. **Emission continue confirmee sur plusieurs cycles** (offsets 0 a 39
   observes sur ~1 minute, intervalle observe entre cycles ~9-15s, coherent
   avec `[5,10]s` + le temps de traitement), log `Cycle 6 termine (5
   salle(s) publiee(s))` confirmant le compteur de cycle.
   - **Test effectue pendant une heure creuse simulee (~21h13-21h14,
     tranche "moderee" 21h-23h, ratio cible 0.35)** — ratios reellement
     observes entre 0.2 et 0.42 selon les salles/le bruit gaussien
     (ex. `45/140=0.32`, `65/180=0.36`, `37/100=0.37`), **coherents avec la
     tranche moderee et NON avec un pic** (qui aurait vise ~0.75). C'est le
     comportement attendu a cette heure-la, pas un bug — voir la consigne
     explicite de ne pas confondre heure creuse et dysfonctionnement.
7. **Arret propre verifie reellement** (`docker compose stop -t 15
   gym-simulator`, equivalent a un `SIGTERM`) : logs confirmes —
   `Signal 15 recu -- arret propre demande (fin du cycle en cours).` puis
   `Arret demande -- flush final du producteur Kafka...` puis `Simulateur
   arrete proprement.` (exit propre, pas de kill force necessaire dans le
   delai de grace de 15s).
8. **Service redemarre pour fonctionnement continu** (`docker compose up -d
   gym-simulator`) — `healthy` a nouveau, `restart: unless-stopped` en
   place pour survivre a un redemarrage de l'hote/crash.
9. **Non-regression** : `docker compose ps` — les 9 services du Jalon 1
   (Zookeeper, Kafka, Spark x2, Airflow x3, Postgres x2, dashboard) restent
   tous `healthy` apres l'ajout de `gym-simulator` et la modification de
   `kafka-init`.

### Limite assumee

`gold.dim_gym` doit avoir ete materialisee par un `dbt seed`+`dbt run` au
moins une fois avant que `gym-simulator` puisse demarrer correctement (le
script retente avec backoff, mais finira par abandonner — exit 1 — si la
table n'existe vraiment jamais). Sur un environnement totalement neuf
(`docker compose up -d` a partir de zero, jamais de `dbt run` execute), ce
service echouera donc au demarrage tant que le DAG `gold_dbt_run` (ou une
commande dbt manuelle ciblee, comme utilisee pour cette verification) n'a
pas ete execute au moins une fois. Pas corrige a ce stade (hors perimetre
de la sous-etape 1/5) — a garder en tete si une future sous-etape
automatise le provisioning complet d'un environnement neuf.

## Sous-etape 2/5 — Consumer Spark Structured Streaming — ✅ fait

**Date** : 2026-07-09 (meme jour que la sous-etape 1/5).

**Perimetre explicitement borne** : consumer Spark Structured Streaming
UNIQUEMENT. Pas de dashboard (le dashboard actuel, Jalon 1, reste
inchange), pas encore de recommandation de creneau.

**Decision deja actee (rappel)** : approche simple — Spark Structured
Streaming lit le topic en continu et maintient l'ETAT COURANT (dernier
message connu par salle) dans une table Postgres, PAS de fenetre
temporelle/agregation historique a ce stade.

### Livre

- **`spark/jobs/stream_gym_occupancy.py`** : job Spark Structured
  Streaming.
  - `readStream` format `kafka`, `subscribe=safelift-gym-occupancy`,
    **`startingOffsets=latest`** — choix documente dans le docstring du
    module ET dans `data/gold/GOLD_MODEL_DECISIONS.md` section 10 (etat
    courant, pas d'historique a preserver ; consequence assumee : les
    messages publies pendant un arret du job sont definitivement perdus).
  - Parsing JSON via `from_json` avec un **schema Spark explicite**
    (`MESSAGE_SCHEMA` : `gym_id`/`timestamp`/`current_occupancy`/`capacity`),
    aucune inference automatique.
  - Calcule `occupancy_rate` (`current_occupancy / capacity`) et
    `charge_category` (Faible <40%, Moderee 40-70%, Elevee >70% — seuils
    documentes et justifies dans GOLD_MODEL_DECISIONS.md section 10).
  - `writeStream` via `foreachBatch` (`upsert_batch`) : **upsert manuel
    `INSERT ... ON CONFLICT (gym_id) DO UPDATE`** (psycopg2, pas le writer
    JDBC de Spark qui ne supporte pas nativement l'upsert) — prefere a un
    DELETE+INSERT explicite (option suggeree initialement) car strictement
    equivalent en resultat mais en une seule instruction Postgres atomique.
    Chaque micro-batch est minuscule (3 a 5 lignes), `.collect()` cote
    driver suffit largement.
  - Checkpoint Spark (`/tmp/spark-checkpoints/gym_occupancy`, sur un volume
    Docker dedie `spark_streaming_checkpoints`) — aucun operateur stateful
    dans ce job (pas d'agregation/watermark/join), donc pas de repertoire a
    partager avec `spark-worker` (contrairement a `silver_transformation.py`).
  - Erreurs JSON malformees : filtrees explicitement par micro-batch
    (champs requis null), loguees en `WARNING` avec le compte de messages
    ignores, **jamais d'exception remontee** — le stream continue. Meme
    philosophie pour une erreur Postgres transitoire (log + micro-batch
    ignore, pas de crash).
  - `gold.gym_occupancy_live` creee via `psycopg2` au demarrage du job
    (`CREATE TABLE IF NOT EXISTS`, PAS un modele dbt — cette table est
    geree entierement par ce job Spark).
  - Heartbeat (`/tmp/spark_streaming_heartbeat`, reecrit apres chaque
    micro-batch reussi) relu par le healthcheck Docker.
- **`spark/Dockerfile.streaming`** : etend `apache/spark:3.5.8-python3`
  (MEME image que `spark-master`/`spark-worker`, garantit driver/executeurs
  sur la meme version Spark/Python) avec `psycopg2-binary` en plus. Le
  script du job n'est pas copie dans l'image (mont en bind
  `./spark/jobs:/opt/spark-jobs`, comme `spark-master`/`spark-worker`).
- **`docker-compose.yml`** : nouveau service `spark-streaming-gym`
  (`restart: unless-stopped`), qui soumet le job en mode client
  (`spark-submit --master spark://spark-master:7077 --deploy-mode client`)
  au cluster Spark existant et reste demarre en continu (PAS un DAG
  Airflow — processus long-courant de streaming). `--packages
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8` pour le connecteur
  Kafka. Volume `spark_streaming_checkpoints` dedie (ajoute a la liste des
  volumes nommes du projet).

### Verifications effectuees (toutes reelles, aucun dry-run)

1. **2 bugs reels rencontres et corriges pendant le demarrage** (voir
   section "Bugs" ci-dessous pour le detail) : chemin `spark-submit`
   absent du `PATH` (corrige avec le chemin complet
   `/opt/spark/bin/spark-submit`) puis echec de creation du repertoire de
   checkpoint (`$HOME=/nonexistent` pour l'utilisateur "spark" de l'image
   officielle -> Ivy ne pouvait pas ecrire son cache ; volume Docker monte
   root:root par defaut -> le job ne pouvait pas y creer de sous-repertoire).
2. **Build + demarrage reels** : `docker compose build spark-streaming-gym`
   (succes, resolution Ivy du connecteur Kafka et de ses dependances
   transitives confirmee dans les logs — `kafka-clients`, `lz4-java`,
   `snappy-java`, `commons-pool2`...) puis `docker compose up -d
   spark-streaming-gym` — connexion confirmee au cluster
   (`Connected to Spark cluster with app ID app-...`, executeur enregistre
   sur `spark-worker`), puis `Stream demarre (checkpoint=...). En attente
   de messages...`.
3. **Table `gold.gym_occupancy_live` confirmee a jour en temps reel** —
   requetee 2 fois a ~25 secondes d'intervalle, changements confirmes sur
   TOUTES les colonnes variables (`last_message_timestamp`,
   `current_occupancy`, `occupancy_rate`, `charge_category`, `updated_at`) :
   ```
   -- 1re requete (21:55:06)
    gym_id | last_message_timestamp        | current_occupancy | capacity | occupancy_rate | charge_category | updated_at
    1      | 2026-07-08 21:55:02.64018+00  | 37                | 140      | 0.264           | Faible          | 2026-07-08 21:55:06.30472+00
    2      | 2026-07-08 21:55:02.64018+00  | 89                | 180      | 0.494           | Moderee         | 2026-07-08 21:55:06.30472+00
    3      | 2026-07-08 21:55:02.64018+00  | 34                | 100      | 0.340           | Faible          | 2026-07-08 21:55:06.30472+00
    4      | 2026-07-08 21:55:02.64018+00  | 49                | 130      | 0.377           | Faible          | 2026-07-08 21:55:06.30472+00
    5      | 2026-07-08 21:55:02.64018+00  | 29                | 90       | 0.322           | Faible          | 2026-07-08 21:55:06.30472+00

   -- 2e requete (21:55:31, ~25s plus tard)
    gym_id | last_message_timestamp        | current_occupancy | capacity | occupancy_rate | charge_category | updated_at
    1      | 2026-07-08 21:55:27.154918+00 | 58                | 140      | 0.414           | Moderee         | 2026-07-08 21:55:31.261164+00
    2      | 2026-07-08 21:55:27.154918+00 | 82                | 180      | 0.456           | Moderee         | 2026-07-08 21:55:31.261164+00
    3      | 2026-07-08 21:55:27.154918+00 | 37                | 100      | 0.370           | Faible          | 2026-07-08 21:55:31.261164+00
    4      | 2026-07-08 21:55:27.154918+00 | 55                | 130      | 0.423           | Moderee         | 2026-07-08 21:55:31.261164+00
    5      | 2026-07-08 21:55:27.154918+00 | 34                | 90       | 0.378           | Faible          | 2026-07-08 21:55:31.261164+00
   ```
   Les 5 salles de `dim_gym` sont bien toutes representees dans les 2
   requetes. Cas notable : la salle `gym_id=1` bascule reellement de
   `Faible` (0.264) a `Moderee` (0.414) entre les deux requetes — confirme
   que le calcul de `charge_category` est bien recalcule a chaque
   micro-batch, pas fige a la creation de la ligne.
4. **Resilience aux messages JSON malformes testee reellement** (pas
   seulement en theorie) : un message non-JSON (`"ceci nest pas du json
   valide"`) injecte manuellement sur le topic via
   `kafka-console-producer`. Log reel confirme :
   `Batch 11 : 1 message(s) JSON malforme(s) ou incomplet(s) ignore(s) sur
   6 -- stream poursuivi.`, suivi immediatement de
   `Batch 11 : 5 salle(s) mise(s) a jour dans gold.gym_occupancy_live.`
   (les 5 messages valides du meme micro-batch traites normalement) puis
   `Batch 12 : 5 salle(s) mise(s) a jour...` (le stream continue sans
   interruption sur les micro-batches suivants). **Confirme que le job ne
   crashe pas sur une erreur de parsing**, comme demande.
5. **Fonctionnement continu sans crash confirme** : `docker compose ps` —
   `Up 2 minutes (healthy)`, `RestartCount=0`. Healthcheck (fichier de
   heartbeat) passant.
6. **Non-regression** : les 11 services du projet (Jalon 1 + gym-simulator
   + spark-streaming-gym) restent tous `healthy` apres l'ajout de ce
   service.

### Bugs reels rencontres et corriges pendant les tests

- **`spark-submit: command not found`** : `/opt/spark/bin` n'est PAS dans
  le `PATH` par defaut de l'image `apache/spark:3.5.8-python3` (confirme
  via `docker inspect`, meme raison pour laquelle
  `spark-master`/`spark-worker` utilisaient deja un chemin complet dans
  leur entrypoint). Corrige en appelant `/opt/spark/bin/spark-submit` en
  chemin complet dans la commande du service `spark-streaming-gym`.
- **`FileNotFoundException` sur
  `/nonexistent/.ivy2/cache/resolved-....xml`** : l'utilisateur `spark` de
  l'image officielle a `$HOME=/nonexistent` (repertoire inexistant) — Ivy
  (utilise par `--packages` pour resoudre le connecteur Kafka depuis Maven
  Central) essaie par defaut d'ecrire son cache dans `$HOME/.ivy2`.
  Corrige avec `--conf spark.jars.ivy=/tmp/.ivy2` (chemin explicite,
  toujours inscriptible quel que soit l'utilisateur).
- **`IOException: mkdir of file:/tmp/spark-checkpoints/gym_occupancy
  failed`** : le point de montage du volume Docker nomme
  (`spark_streaming_checkpoints`) est cree `root:root` par defaut lors de
  son tout premier montage (volume vide), alors que le job tourne sous
  l'utilisateur non-root `spark`. Corrige en PRE-creant le repertoire
  `/tmp/spark-checkpoints` avec le bon proprietaire directement dans
  `spark/Dockerfile.streaming` (`RUN mkdir -p ... && chown -R spark:spark
  ...`) : Docker copie les permissions du repertoire de l'IMAGE vers le
  volume nomme lors de son tout premier montage — le volume existant
  (cree avant ce correctif) a du etre supprime (`docker volume rm`, aucune
  donnee de checkpoint n'existait encore) pour que le nouveau montage
  reparte d'un repertoire vide correctement initialise.

### Limite assumee

`gold.gym_occupancy_live` n'est PAS geree par dbt (contrairement au reste
du schema `gold`) : un `dbt run` complet ne la cree ni ne la supprime
jamais, mais elle n'apparait pas non plus dans les tests dbt existants
(`dbt test` ne la couvre pas). Coherent avec le perimetre de cette
sous-etape (etat courant maintenu par Spark, pas par dbt) mais a garder en
tete si une future sous-etape veut lui appliquer les memes garanties de
qualite (tests not_null/unique) que le reste du schema Gold.

## Sous-etape 3/5 — Inputs utilisateur temps reel — ✅ fait

**Date** : 2026-07-09 (meme jour que les sous-etapes 1/5 et 2/5).

**Perimetre explicitement borne** : formulaire + Kafka + consumer +
declenchement dbt + polling frontend. Pas de recreation de la formule de
risque (decision deja actee : UNE SEULE formule, celle de dbt).

**Decisions deja actees (rappel)** :
- Aucune duplication de la logique de calcul du risque — le stream
  declenche un run dbt CIBLE (en pratique le DAG `gold_dbt_run` existant
  dans son integralite, run partiel juge trop complexe a isoler
  proprement, voir ci-dessous) qui applique la formule existante.
- Les seances temps reel n'ecrivent PAS dans `weight_training.csv`
  (dataset Kaggle statique) : nouvelle table `raw.realtime_user_sessions`,
  unifiee avec `silver.weight_training` au niveau d'un modele dbt Staging,
  AVANT `fact_workout_session`.

### Livre

- **`raw.realtime_user_sessions`** (Postgres) : creee par
  `scripts/consume_user_inputs.py` au demarrage (`CREATE TABLE IF NOT
  EXISTS`, psycopg2). Colonnes : `user_id` (obligatoire, contrairement a
  weight_training), `exercise_name`, `lifted_weight_kg`, `reps`, `sets`,
  `duration_seconds`, `performed_at`, `ingested_at`. Grain = 1 ligne par
  exercice complet (sets/reps deja agreges cote formulaire).
- **Topic Kafka `safelift-user-inputs`** : cree via `kafka-init` (3e topic,
  meme mecanisme que les 2 precedents).
- **`POST /users/{user_id}/sessions`** (`dashboard/main.py`) : verifie que
  l'utilisateur existe (`gold.dim_user`), PUBLIE l'evenement sur Kafka
  (jamais d'ecriture DB directe — decouplage evenementiel demontre), gere
  explicitement les erreurs Kafka (callback de livraison + `flush`
  verifie, HTTP 503 si echec/timeout, jamais un succes trompeur).
- **`scripts/consume_user_inputs.py`** (service continu, meme famille que
  `gym-simulator`) : lit `safelift-user-inputs`, valide le schema
  (champs requis + types + valeurs positives — message invalide : logue et
  ignore, jamais de crash), insere dans `raw.realtime_user_sessions`, puis
  declenche `gold_dbt_run` via l'API REST Airflow
  (`POST /api/v1/dags/gold_dbt_run/dagRuns`, `conf` transmis pour
  tracabilite uniquement). `auto.offset.reset=earliest` (PAS `latest`,
  contrairement au simulateur d'affluence) : une seance utilisateur est une
  donnee importante, jamais perdue silencieusement — voir
  `data/gold/GOLD_MODEL_DECISIONS.md` section 11 pour la justification
  complete de ce contraste deliberer avec la sous-etape 1/5.
- **`AIRFLOW__API__AUTH_BACKENDS`** etendu (`docker-compose.yml`,
  `basic_auth` ajoute a `session`) : le backend par defaut n'accepte pas
  l'authentification HTTP simple, verifie concretement avant correctif.
- **dbt** : 2 nouveaux modeles staging (`stg_realtime_user_sessions.sql`,
  `stg_workout_sessions_unified.sql` — l'union proprement dite),
  `fact_workout_session.sql` simplifie (consomme le modele unifie,
  `coalesce(user_id, demo_user_id)`), `dim_date.sql` corrige (bug reel, voir
  ci-dessous), grain de `fact_workout_session` etendu a `(user_id,
  session_date, workout_name, exercise_id)`. Detail complet et
  justification de CHAQUE choix : `data/gold/GOLD_MODEL_DECISIONS.md`
  section 11.
- **Dashboard** : nouveau panneau "Logger une séance" (`index.html`,
  `<select>` d'exercices REUTILISE de `GET /users/{user_id}/exercises`,
  deja utilise par le simulateur what-if — garantit que l'exercice choisi
  matche toujours `dim_exercise`), `dashboard.js` (`onLogSessionSubmit` :
  POST puis polling de `GET /users/{id}/risk` toutes les 3s jusqu'a
  detecter un changement de score sur la zone concernee, timeout 120s,
  affichage d'un statut "Recalcul en cours..." puis du delai reel observe
  a la fin).

### Bugs reels rencontres et corriges pendant les tests

- **Contention de ressources Spark (le plus significatif)** :
  `spark-streaming-gym` (sous-etape 2/5) capturait les 2 coeurs du worker
  des son demarrage et les gardait indefiniment (job Structured Streaming
  long-courant, ne relache jamais ses coeurs) -> `load_silver_to_postgres`
  (1re task de `gold_dbt_run`) restait bloque `WAITING` avec 0 coeur,
  confirme via l'API JSON du master Spark (`coresused: 2/2`,
  `"state": "WAITING", "cores": 0`), un premier test etant reste bloque
  plus de 3 minutes sans progresser. Corrige avec
  `--conf spark.cores.max=1` sur `spark-streaming-gym` — verifie
  concretement apres correctif (`coresused: 1/2`, `load_silver_to_postgres`
  demarre et termine en ~20s).
- **`dim_date` trop etroite (bug reel, pas juste theorique)** : bornee
  uniquement par l'historique `weight_training` (2015-2018), une seance
  saisie en 2026 tombait hors plage -> `date_id` NULL -> `week_start_date`
  NULL -> `risk_score` NULL silencieusement pour toute seance temps reel.
  Corrige en unionnant les 2 sources avant de calculer min/max dans
  `dim_date.sql`. Voir GOLD_MODEL_DECISIONS.md section 11 pour le detail
  complet de la chaine de causalite.
- **`airflow.api.auth.backend.session` (backend par defaut) rejette les
  appels HTTP simples** : confirme via `airflow config get-value api
  auth_backends` avant correctif, puis testee reellement en conditions
  reelles apres ajout de `basic_auth` (`curl -u admin:... -X POST
  .../dagRuns` reussi, DAG run cree avec `state: queued`).

### Verifications effectuees (toutes reelles, aucun dry-run)

1. **dbt run + test AVANT toute donnee reelle** (table
   `raw.realtime_user_sessions` creee vide manuellement pour valider la
   chaine structurellement) : 13/13 modeles `OK`, **aucune regression**
   (`fact_workout_session`/`fact_risk_score` toujours 2164 lignes), 72/72
   tests `PASS`.
2. **Trigger Airflow teste isolement d'abord** (`curl` manuel avec
   `dag_run_id=manual_api_test_1`) avant integration dans le consumer :
   DAG cree avec succes (`state: queued`), puis — apres avoir debloque la
   contention Spark ci-dessus — **6/6 tasks `success` en conditions
   reelles**, aucune regression de row count.
3. **Test end-to-end REEL complet sur `user_id=9`** ("Bicep Curl
   (Barbell)", 70kg x 5 series x 15 reps, 45 min) :
   - Score AVANT (zone `arms`) : `15.19` (Faible), derniere seance connue
     le 2018-09-28.
   - Soumission `POST /users/9/sessions` : **HTTP 202**, `{"status":
     "queued", ...}`.
   - **Logs reels du consumer** :
     `Seance inseree : user_id=9, Bicep Curl (Barbell), 70.0kg x 5 series x
     15 reps.` puis `DAG gold_dbt_run declenche (dag_run_id=realtime_input_user9_d4252c79)
     pour user_id=9.`
   - **Ligne confirmee dans `raw.realtime_user_sessions`** (id=1,
     `performed_at=2026-07-08 22:51:09`).
   - **DAG run confirme `success`** (6/6 tasks :
     `load_silver_to_postgres`, `dbt_seed`, `dbt_run_staging`,
     `fuzzy_match_exercises`, `dbt_run`, `dbt_test`).
   - **Score APRES** (zone `arms`) : **`7.81` (Faible)**,
     `session_date=2026-07-08`, `workout_session_id=1`,
     `exercise_name=Bicep Curl (Barbell)` — ligne 100% reelle, recalculee
     par la formule dbt EXISTANTE (base_zone x charge_factor x
     volume_factor x recup_factor x duree_factor), aucun code duplique.
   - **`GET /users/9/risk` (API dashboard) confirme refleter immediatement
     la nouvelle valeur** (requete directe apres la fin du DAG run).
   - **`gold.fact_workout_session` : 2164 -> 2165 lignes** (la nouvelle
     seance, aucune perte).
   - **`dbt test` re-execute avec la ligne reelle en base : 72/72 PASS**.
4. **Delai reel mesure (soumission -> fin du run dbt)** : horodatage de
   soumission `2026-07-08T22:51:09.279Z`, fin du DAG run (rapportee par
   Airflow) `2026-07-08T22:52:19.306Z` -> **~70 secondes**. Repartition
   approximative : `load_silver_to_postgres` (seul step Spark) ~20s, reste
   du pipeline dbt (`dbt_seed`/`dbt_run_staging`/`fuzzy_match_exercises`/
   `dbt_run`/`dbt_test`) ~50s. **Le timeout de polling cote frontend a ete
   corrige de 60s (valeur initiale supposee AVANT mesure) a 120s** suite a
   cette mesure reelle — ne jamais deviner un delai sans le mesurer sur ce
   projet.
5. **Verification structurelle du frontend** (Node.js, navigateur
   indisponible comme sur toutes les sessions precedentes) : `node --check
   dashboard.js` (syntaxe valide), 37 references `getElementById(...)`
   toutes resolues dans `index.html` (0 manquante), balises `<section>`
   (5/5) et `<div>` (21/21) equilibrees, accolades CSS equilibrees
   (77/77).
6. **Non-regression finale** : les 12 services persistants du projet
   (Jalon 1 + gym-simulator + spark-streaming-gym + user-inputs-consumer)
   tous `healthy` apres l'ensemble des changements.

### Limites assumees (documentees, non corrigees a ce stade)

- **Run dbt COMPLET, pas partiel par utilisateur** : `fact_risk_score.sql`
  calcule des facteurs par fenetre glissante (`lag()`/`avg() over`) par
  (utilisateur, zone, semaine) — dbt `--select` filtre des modeles, pas des
  lignes ; un run partiel correct necessiterait soit un modele
  incremental (refonte hors perimetre), soit de recalculer quand meme
  toute la fenetre (le "partiel" ne gagnerait rien). Juge acceptable pour
  le volume de donnees de ce projet (delai mesure ~70s).
- **`exercise_name` non valide contre `dim_exercise` cote API/consumer** :
  seule la mitigation cote UI (formulaire limite a un `<select>` d'exercices
  deja pratiques) protege contre les lignes orphelines — un appel direct a
  l'API avec un exercice inconnu produirait une ligne silencieusement
  orpheline (exercise_id/muscle_id NULL, risk_score non calculable pour
  cette ligne).
- **Grain `fact_workout_session` et doublons intra-jour realtime** : si un
  meme utilisateur soumettait 2 seances temps reel sur le meme exercice le
  meme jour, les 2 resteraient des lignes distinctes (contrairement a
  weight_training, agrege par jour) — non teste (scenario de test = une
  seule seance), risque de violation du test de grain juge tres faible
  (namespace `workout_name` disjoint) mais non elimine.
- **Identifiants admin Airflow reutilises** pour l'authentification API du
  consumer (simplification assumee, pas de compte de service dedie).

## Sous-etape 4/5 — Dashboard temps reel affluence (SSE) — ✅ fait

**Date** : 2026-07-09 (meme jour que les sous-etapes 1/5 a 3/5).

**Perimetre explicitement borne** : dashboard temps reel affluence
UNIQUEMENT. Le mecanisme de polling deja en place pour le recalcul de
risque (sous-etape 3/5) n'a pas ete touche.

**Decision deja actee (rappel)** : Server-Sent Events (SSE) pour la courbe
d'affluence — pas de WebSocket, pas de polling pour cette fonctionnalite.

### Livre

- **`GET /gyms/occupancy/stream`** (`dashboard/main.py`) : flux SSE,
  interroge `gold.gym_occupancy_live` toutes les 3s cote serveur, ne
  repousse un evenement que si les donnees ont reellement change.
  Deconnexion geree via `request.is_disconnected()` (verifie a chaque
  iteration), chaque requete SQL passe par le pool de connexions existant
  (`get_cursor()`), aucune connexion retenue entre deux iterations.
- **`GET /gyms/{gym_id}/best_slot`** : recommandation du creneau le moins
  charge dans les 4 prochaines heures, basee sur le **pattern THEORIQUE**
  du simulateur (`_theoretical_occupancy_ratio()`, duplique de `peak_ratio()`
  dans `scripts/simulate_gym_occupancy.py`) — PAS sur un historique reel
  observe (`gold.gym_occupancy_live` vient de demarrer, trop court pour
  etre statistiquement valable). Champ `is_theoretical_pattern_based: true`
  + `methodology` (texte) renvoyes explicitement. Voir
  `data/gold/GOLD_MODEL_DECISIONS.md` section 12 pour le detail complet.
- **Dashboard** : nouvelle section "Affluence en direct" (`index.html`) —
  une carte par salle de `dim_gym` (jauge de remplissage colore par
  `charge_category`), pastille "🔴 EN DIRECT" pulsante, panneau de
  recommandation de creneau pour la salle selectionnee (clic sur une
  carte). `dashboard.js` : `connectOccupancyStream()` (EventSource natif,
  reconnexion automatique du navigateur JAMAIS desactivee/court-circuitee),
  `renderOccupancyGrid()`, `selectGym()`/`loadBestSlot()`.

### Bug reel rencontre et corrige pendant les tests

**Deduplication SSE cassee** : la comparaison "donnees changees ?" portait
initialement sur le PAYLOAD COMPLET envoye (incluant `server_time`,
recalcule a chaque iteration) — `server_time` changeant toujours, la
deduplication ne se declenchait jamais (un evenement partait a chaque poll
de 3s, meme sans changement reel). **Constate directement en testant** (le
premier `curl -N` de verification montrait 2 evenements consecutifs avec
des donnees IDENTIQUES a 3s d'intervalle, alors que le simulateur emet
toutes les 5-10s — signe evident que la dedup ne fonctionnait pas).
Corrige en comparant uniquement les lignes de donnees (`gym_id`/
`occupancy`/...), `server_time` etant ajoute apres coup au payload
effectivement envoye. Voir GOLD_MODEL_DECISIONS.md section 12.

### Verifications effectuees (toutes reelles, aucun dry-run)

1. **`curl -N` sur 20s (test initial, AVANT correctif dedup)** : evenements
   consecutifs avec donnees identiques constates -> bug identifie et
   corrige (voir ci-dessus).
2. **`curl -N` sur 20s (APRES correctif)** : 3 evenements, tous
   `new-data` (aucun doublon).
3. **`curl -N` sur 40s (verification finale, apres rebuild complet du
   dashboard)** : **7 evenements distincts recus sur une connexion HTTP
   UNIQUE et CONTINUE** (pas 7 requetes separees), valeurs de
   `current_occupancy` genuinement differentes a chaque evenement
   (52 -> 55 -> 49 -> 41 -> 58 -> 41 -> 50 pour `gym_id=1`), timestamps
   `server_time` espaces de facon irreguliere (6 a 9s, coherent avec le
   cycle du simulateur/Spark, pas un intervalle fixe de 3s -- confirme que
   la dedup fonctionne, pas juste qu'un timer tourne).
4. **Test de deconnexion propre** : nombre de connexions Postgres actives
   (`pg_stat_activity`, utilisateur `safelift_app`) mesure a **3** avant
   l'ouverture d'une connexion SSE, coupure brutale de la connexion apres
   3s (`timeout 3 curl -N ...`), nombre de connexions Postgres remesure a
   **3** juste apres -- **aucune fuite constatee**. Logs `uvicorn`
   confirment la requete `GET /gyms/occupancy/stream` cloturee proprement
   (`200 OK` logue apres la fin de la connexion, pas de requete bloquee
   indefiniment).
5. **`GET /gyms/{gym_id}/best_slot` teste sur les 5 salles reelles** :
   toutes repondent `200`, `expected_occupancy_rate=0.35` (test effectue
   ~09:48 UTC un jeudi, palier "modere" 9h-18h du pattern), 
   `recommended_slot_utc` == l'instant present a quelques secondes pres
   (coherent : aucun palier plus bas que 0.35 dans les 4h suivantes a
   cette heure-la un jour de semaine, "maintenant" est bien optimal).
   `GET /gyms/999/best_slot` (salle inexistante) -> `404` confirme.
6. **Verification de l'encodage UTF-8** : un artefact d'affichage
   suspect (`basÃ©e` au lieu de `basée`) observe dans un premier
   temps sur la sortie de `python -m json.tool` dans le terminal Windows
   local -- **confirme non-bug** en decodant les octets bruts de la
   reponse HTTP directement (`urllib` + inspection des codepoints Unicode,
   `U+00E9` = "é" confirme correct) : meme classe d'artefact que celui
   deja documente en Feature A (encodage de la console Windows locale,
   pas un bug de l'API).
7. **Verification structurelle du frontend** (Node.js, navigateur
   Chrome toujours indisponible dans cette session comme dans toutes les
   precedentes) : `node --check dashboard.js` (syntaxe valide), 41
   references `getElementById(...)` toutes resolues dans `index.html` (0
   manquante), balises `<section>` (6/6) et `<div>` (24/24) equilibrees,
   accolades CSS equilibrees (99/99).
8. **Non-regression** : fichiers statiques (`/`, `dashboard.js`,
   `dashboard.css`) tous `200` avec le nouveau contenu confirme present ;
   les 12 services persistants du projet restent tous `healthy`.

### Limite assumee

**Verification VISUELLE du rendu (jauge de remplissage, pastille pulsante,
mise a jour sans reload) NON effectuee par Claude Code** — extension
navigateur Chrome indisponible sur cette session comme sur toutes les
precedentes du projet. Verifications ci-dessus = preuve reelle et directe
que le flux SSE fonctionne (evenements HTTP successifs sur connexion
unique, donnees changeantes, deconnexion propre) et que le code
frontend est structurellement correct, mais PAS confirmation du rendu
esthetique final (alignement des cartes, lisibilite de la jauge sur fond
sombre, fluidite visuelle de la pulsation). A confirmer par Moulaye sur
http://localhost:18000, section "Affluence en direct" en bas de page.

## Sous-etape 5/5 — Verification globale + script de demonstration — ✅ fait

**Date** : 2026-07-09 (meme jour que les sous-etapes 1/5 a 4/5).

**Perimetre explicitement borne** : verification bout-en-bout globale +
script de demonstration pour la soutenance — **aucun nouveau
developpement**, clot le Jalon 2.

### 1. Fenetre de stabilite de 13 minutes, en conditions reelles

**Objectif** : confirmer que le correctif `spark.cores.max=1` (bug de
contention de coeurs Spark, trouve et corrige en sous-etape 3/5) **tient
dans la duree**, pas seulement au moment ou il a ete applique, et qu'un
input utilisateur declenche pendant que les deux streams tournent ne
bloque rien.

**Protocole** : script de supervision dedie
(`stability_monitor.sh`, tmp de session), interrogeant toutes les 30s
pendant 13 minutes : l'allocation de coeurs du master Spark
(`http://spark-master:8080/json/`), le statut Docker et le
`RestartCount` de `gym-simulator`/`spark-streaming-gym`/
`user-inputs-consumer`/`dashboard`. Une seance utilisateur reelle
declenchee manuellement au milieu de la fenetre (`POST /users/9/sessions`,
"Neutral Chin", 25kg x 6 series x 12 reps).

**Resultats (26 mesures, 2026-07-09T10:08:04Z -> 10:21:23Z)** :
- **`coresused` reste a `1/2` en continu**, sauf une seule mesure a
  `2/2` exactement au moment du run dbt declenche par la seance milieu de
  fenetre (10:10:51Z) puis retour a `1/2` a la mesure suivante
  (10:11:24Z) — **preuve directe et horodatee que le job batch a bien pu
  acquerir son coeur libre et le relacher a la fin**, confirmant que le
  correctif tient dans la duree (contrairement au comportement d'avant
  correctif ou le job batch restait bloque indefiniment a 0 coeur).
- **`RestartCount` reste a `0` pour les 4 services sur toute la fenetre**
  (aucun crash).
- **DAG `gold_dbt_run` declenche par la seance milieu de fenetre confirme
  `success`** (`realtime_input_user9_7ce183e5`, soumission
  `10:10:13.542Z` -> fin `10:12:03.175949Z`, **delai reel mesure : 110s**
  -- 2e mesure independante apres les 70s de la sous-etape 3/5, variance
  attribuee a la charge concurrente du systeme pendant cette fenetre de
  stabilite). Score reellement mis a jour : zone `back`
  (exercice "Neutral Chin") passe a `risk_score=0.00`, nouvelle ligne
  `session_date=2026-07-09`.
- **Timeout de polling frontend releve de 120s a 180s** suite a cette 2e
  mesure (120s ne laissait plus qu'une marge de 10s face au pire cas
  observe, juge insuffisant pour la fiabilite en conditions de demo) —
  voir `dashboard/static/dashboard.js`.

### 2. Test reel de panne/reprise (gym-simulator)

**Protocole** : `docker compose stop gym-simulator` pendant que les 11
autres services tournent, verification du comportement du reste du
systeme, puis `docker compose start gym-simulator`, verification de la
reprise.

**Resultats** :
- **Pendant l'arret** (10:22:06Z -> 10:24:05Z, ~2 minutes) : les 11 autres
  services restent tous `healthy`. `GET /health` et `GET /users/9/risk`
  (dashboard) repondent normalement (`200`). Le flux SSE
  `/gyms/occupancy/stream` **reste ouvert** et continue de repondre avec
  la derniere donnee connue (pas d'erreur, pas de fermeture de
  connexion). `spark-streaming-gym` continue de tourner **sans la moindre
  erreur** : plus aucune ligne `"Batch N : ... mise(s) a jour"` dans les
  logs (micro-batches vides silencieusement ignores par le code existant,
  `if total == 0: return`), comportement attendu et non un bug.
- **Au redemarrage** : logs confirmes -- `gym-simulator` recharge
  `gold.dim_gym` (5 salles) et republie immediatement des messages ;
  `spark-streaming-gym` reprend la consommation des le message suivant
  (`Batch 1542` juste apres `Batch 1541`, aucun ecart dans la numerotation
  des batches malgre l'interruption de la source).
- **`RestartCount` de tous les services reste a `0` apres le test** —
  confirme que l'arret/redemarrage etait bien volontaire (`docker compose
  stop`/`start`, pas un crash suivi d'un redemarrage automatique via
  `restart: unless-stopped`), et qu'**aucun autre service n'a crash en
  cascade** suite a l'arret de `gym-simulator`.

### 3. Script de démonstration pour la soutenance

**`docs/DEMO_SCRIPT_JALON2.md`** (nouveau) : script chronometre (6 etapes,
~3-4 minutes hors questions), 2 options de transition pendant l'attente du
recalcul (explication orale de l'architecture, ou affichage live du DAG
Airflow), grille de lecture "que faire si un service ne repond pas" (sans
paniquer devant le jury), et 7 questions probables du jury avec pistes de
reponse courtes (SSE vs WebSocket, delai de recalcul, recommandation de
creneau = ML ou pattern theorique, panne de service, montee en charge
multi-utilisateurs, lab AWS). **Toutes les durees annoncees dans ce
document sont des mesures reelles** (70s et 110s pour le recalcul,
delibrement presentees comme une fourchette "1 a 2 minutes" au jury plutot
qu'un chiffre unique trop precis).

### Limite assumee

Comme pour toutes les sous-etapes precedentes du Jalon 2 : **verification
VISUELLE du rendu (jauges, pastille pulsante) toujours NON effectuee par
Claude Code** (extension navigateur Chrome indisponible sur cette session
comme sur toutes les precedentes). Les verifications de cette sous-etape
portent sur la STABILITE et la RESILIENCE du systeme (mesures reelles,
logs reels, codes HTTP reels), pas sur l'esthetique — a confirmer par
Moulaye avant la soutenance, idealement en suivant le script de demo lui-meme
comme repetition generale.

## 🏁 Jalon 2 (sous-etapes 1/5 a 5/5) — complet

Les 5 sous-etapes prevues sont livrees et verifiees en conditions reelles :

| Sous-etape | Contenu | Statut |
|---|---|---|
| 1/5 | Simulateur d'affluence + producteur Kafka (`dim_gym`, topic dedie, service `gym-simulator`) | ✅ fait |
| 2/5 | Consumer Spark Structured Streaming (`gold.gym_occupancy_live`, service `spark-streaming-gym`) | ✅ fait |
| 3/5 | Inputs utilisateur temps reel (Kafka -> Postgres -> trigger dbt, formule de risque unique) | ✅ fait |
| 4/5 | Dashboard temps reel affluence (SSE, recommandation de creneau) | ✅ fait |
| 5/5 | Verification globale (stabilite 13 min, test de panne/reprise) + script de demo | ✅ fait |

**4 bugs reels trouves et corriges pendant le developpement** (tous
documentes en detail dans `data/gold/GOLD_MODEL_DECISIONS.md` sections
9-12) : contention de coeurs Spark, `dim_date` trop etroite pour des
dates futures, deduplication SSE cassee par l'inclusion de `server_time`,
et le chemin `spark-submit`/cache Ivy/permissions de volume au demarrage
de `spark-streaming-gym`. Aucun masque ou contourne silencieusement —
chacun documente avec sa cause racine et son correctif.
