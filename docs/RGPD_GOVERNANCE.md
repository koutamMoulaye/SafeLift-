# Gouvernance RGPD — SafeLift (etape 6/6, sous-etape 4/6)

> Ce document couvre les 4 volets de gouvernance demandes pour cette
> sous-etape : pseudonymisation, chiffrement (repos/transit), duree de
> conservation, droit a l'effacement. Voir aussi
> [DATA_CATALOG.md](./DATA_CATALOG.md) (volet 5, catalogue de donnees) et
> `data/gold/GOLD_MODEL_DECISIONS.md` / `data/silver/CLEANING_LOG.md` pour le
> contexte des tables elles-memes.
>
> **Contexte a rappeler explicitement** : ce projet est une demonstration
> pedagogique (RNCP36739) sur des jeux de donnees Kaggle publics. Les
> "utilisateurs" (`dim_user`, 973 profils issus de `gym_members`) ne sont pas
> de vraies personnes ayant consenti a l'usage de leurs donnees dans CE
> projet — aucun consentement RGPD n'a ete reellement recueilli. Les mesures
> ci-dessous sont implementees et testees COMME SI ces donnees etaient
> reelles, precisement pour demontrer la maitrise des mecanismes de
> gouvernance (objectif du Bloc 5), pas parce qu'une obligation legale reelle
> s'applique aujourd'hui a ce jeu de donnees Kaggle.

## 1. Pseudonymisation (HMAC-SHA256)

### 1.1 Colonnes directement identifiantes

- **`user_id`** (`gold.dim_user`, `gold.fact_workout_session`,
  `gold.fact_risk_score`) : identifiant direct, cle de jointure de tout le
  modele en etoile. **Seule colonne traitee par la pseudonymisation** (voir
  ci-dessous).
- **Quasi-identifiants examines** dans `dim_user` : `age` + `gender` +
  `body_weight_kg` + `height_m` + `bmi` combines pourraient en theorie
  re-identifier un individu dans un tres petit echantillon (ex. un
  sous-groupe filtre a 2-3 personnes). Sur 973 profils, ce risque est jugé
  faible pour l'usage actuel (dashboard interne, pas de filtre public par
  combinaison d'attributs), mais **non nul** — a re-evaluer si le dashboard
  devenait un jour public ou si un filtre "recherche par profil" etait
  ajoute. Aucune action prise a ce stade (au-dela de la pseudonymisation de
  `user_id`) : documente comme un risque residuel accepte, pas ignore.
- **Donnees de sante au sens large (Art. 9 RGPD)** : `max_bpm`/`avg_bpm`/
  `resting_bpm` (frequence cardiaque), `fat_percentage`, `body_weight_kg`,
  `calories_burned` sont des donnees physiologiques — categorie particuliere
  de donnees RGPD. Elles restent NECESSAIRES au calcul du `risk_score` (raison
  d'etre du projet) : la pseudonymisation de `user_id` protege le LIEN vers
  une identite, pas la sensibilite intrinseque de ces valeurs elles-memes
  (c'est la difference entre pseudonymisation et anonymisation — voir 1.3).

### 1.2 Implementation : `scripts/pseudonymize.py`

- Fonction `pseudonymize_user_id(user_id: int, key: str) -> str` : HMAC-SHA256
  de `user_id`, digest hexadecimal (64 caracteres), cle lue depuis la
  variable d'environnement `PSEUDONYMIZATION_KEY` (jamais en dur dans le
  code — voir `.env.example`/`.env`, `load_pseudonymization_key()`).
- **Teste** (`python scripts/pseudonymize.py`, execute reellement) :
  - meme `user_id` + meme cle -> toujours le meme pseudonyme (necessaire pour
    que les jointures `fact_*` <-> `dim_user` restent valides une fois
    pseudonymisees) ;
  - deux `user_id` distincts -> pseudonymes distincts ;
  - cle differente -> pseudonyme different (la cle est bien le secret, pas
    `user_id`) ;
  - irreversibilite (impossible de retrouver `user_id` sans la cle) : propriete
    cryptographique standard de HMAC, pas testable par du code, mais garantie
    tant que la cle reste secrete et suffisamment longue (32 octets generes
    via `secrets.token_hex(32)`, voir `.env.example`).

### 1.3 Decision d'architecture : ou s'applique la pseudonymisation ?

**Tranchee : couche de restitution externe UNIQUEMENT (export S3/Athena),
PAS le pipeline interne (Bronze/Silver/Gold Postgres/API dashboard).**

Justification :
- Le pipeline interne (Airflow, Spark, Postgres `app-postgres`, API FastAPI du
  dashboard) tourne entierement dans le reseau Docker interne du projet ou en
  localhost — **jamais expose sur Internet**, contexte pedagogique/demo. Il a
  besoin de `user_id` en clair comme cle de jointure technique simple pour
  tout le modele en etoile (facts <-> dim_user) et pour l'API du dashboard
  (`GET /users/{user_id}/risk`, etc.).
- Re-architecturer l'integralite de l'API/dashboard autour d'un identifiant
  pseudonymise casserait cette simplicite (tous les endpoints, le simulateur
  what-if de la Feature A, le selecteur d'utilisateur du frontend) pour un
  gain de securite nul dans un environnement 100% local non expose.
- L'export S3/Athena (`scripts/upload_gold_to_s3.py`) est en revanche le point
  ou les donnees QUITTENT reellement l'environnement controle, vers un compte
  AWS cloud, potentiellement interrogeable par des outils/tiers plus larges
  (Athena, futurs consommateurs BI). C'est la ou la pseudonymisation apporte
  une vraie valeur de protection.
- **Concretement** : `dim_user`, `fact_workout_session`, `fact_risk_score`
  exportes vers S3 remplacent la colonne `user_id` (bigint) par
  `user_pseudo_id` (string, HMAC-SHA256) — `user_id` reel **ne quitte jamais**
  `app-postgres`. `fact_risk_score_demo_synthetic` n'a pas de `user_id`
  (100% synthetique) : non concernee. `terraform/athena.tf` mis a jour en
  consequence (colonnes Glue `user_pseudo_id` de type `string` sur ces 3
  tables) — **necessite un `terraform apply` avec des credentials AWS
  valides pour prendre effet sur le compte reel** (non fait dans cette
  session, credentials Learner Lab expires — voir note de session dans
  PROGRESS.md).

### 1.4 Pourquoi pas un modele dbt (malgre la suggestion initiale) ?

Deviation assumee : dbt substitue les valeurs de `env_var()` **en clair**
dans le SQL compile (`dbt/target/compiled/...sql`, lisible sur disque) et
potentiellement dans les logs de requetes Postgres — ce qui va a l'encontre
de l'objectif meme de proteger la cle secrete. Le calcul est donc fait en
Python pur (`scripts/pseudonymize.py`, aucun acces DB), execute uniquement au
moment de l'export S3, sans jamais faire transiter la cle par une couche
SQL/logs.

## 2. Chiffrement

### 2.1 Au repos (S3 / data lake)

- **Confirme** : `aws_s3_bucket_server_side_encryption_configuration` dans
  `terraform/s3.tf` (SSE-S3 / AES256), applique sur le bucket
  `safelift-datalake-097115946702` lors du `terraform apply` reel de la
  sous-etape 2/6 (2026-07-06). Chiffrement transparent, gere par AWS, cle
  geree par AWS (pas de cle KMS custom — jugee suffisante pour ce compte lab,
  cout/complexite additionnels non justifies a ce stade).

### 2.2 Au repos (PostgreSQL local, `app-postgres`)

- **Limitation confirmee et assumee, non corrigee** : le volume Docker nomme
  `app_postgres_data` (image `postgres:16-alpine`) **n'est chiffre par aucun
  mecanisme applicatif** — Postgres ne chiffre pas ses fichiers de donnees
  par defaut, et rien dans `docker-compose.yml` n'ajoute de chiffrement au
  niveau du volume. Sur Docker Desktop/Windows (backend WSL2), ce volume vit
  physiquement dans le disque virtuel WSL2 (`ext4.vhdx`) ; il peut
  beneficier indirectement du chiffrement de disque Windows (BitLocker) SI
  celui-ci est active sur la machine hote — mais ce n'est ni garanti ni
  verifie par ce projet, et ne serait de toute facon qu'un chiffrement au
  niveau materiel/OS, pas applicatif.
- **Pourquoi non corrige ici** : un environnement 100% local de
  developpement/demo, non expose sur Internet, ne justifie pas la complexite
  additionnelle (chiffrement au niveau du systeme de fichiers du conteneur,
  gestion de cles) pour ce jalon.
- **Proposition production** : migrer `app-postgres` vers AWS RDS for
  PostgreSQL avec le chiffrement de stockage active (`StorageEncrypted=true`,
  cle KMS geree ou custom) — chiffrement transparent au niveau du volume EBS
  sous-jacent, sans changement applicatif cote dbt/Spark/dashboard (memes
  requetes SQL).

### 2.3 En transit

- **S3 (upload/lecture Parquet, requetes Athena)** : **HTTPS par defaut**,
  comportement natif du SDK `boto3`/AWS — confirme par inspection du code
  (`scripts/upload_gold_to_s3.py`, `terraform/*.tf`) : aucun `endpoint_url`
  ni `use_ssl=False` n'est configure nulle part dans ce projet (verifie par
  recherche explicite dans le code source, hors dependances tierces dans
  `.venv-aws/`/`.venv/`). S3 n'accepte de toute facon pas de connexion HTTP
  non chiffree sur la plupart de ses points de terminaison.
- **PostgreSQL (`app-postgres`)** : **NON chiffre, confirme reellement**
  (`docker exec safelift-app-postgres psql ... -c "SHOW ssl;"` -> `off`).
  Les connexions du dashboard (reseau Docker interne) et de
  `scripts/upload_gold_to_s3.py` (`localhost:15432` depuis l'hote) transitent
  donc en clair. Ni `dashboard/main.py` ni `scripts/upload_gold_to_s3.py` ne
  fixent de `sslmode` explicite sur leur connexion `psycopg2` : le
  comportement par defaut (`prefer`) se rabat silencieusement sur du clair
  des lors que le serveur ne propose pas SSL — confirme ici volontairement
  plutot que laisse implicite.
  - **Pourquoi non corrige ici** : activer TLS sur l'image officielle
    `postgres:16-alpine` necessite generer un certificat, le monter avec des
    permissions Unix strictes (`600`, proprietaire `postgres`) — or ce
    projet tourne sous Docker Desktop/Windows avec des bind mounts NTFS, ou
    la correspondance des permissions Unix n'est pas fiable (source connue
    de blocages `FATAL: private key file ... has group or world access`).
    Non trivial a fiabiliser dans cet environnement pour ce jalon ; le trafic
    reste de toute facon confine au reseau Docker interne / `localhost` (pas
    de trajet reseau externe), ce qui limite reellement l'exposition dans ce
    contexte local.
  - **Proposition production** : AWS RDS for PostgreSQL avec
    `rds.force_ssl=1` (TLS impose cote serveur) + `sslmode=verify-full` cote
    client (bundle de certificats RDS officiel) — pas de gestion manuelle de
    certificats auto-signes.

## 3. Duree de conservation (politique, non implementee techniquement)

> Perimetre explicite de ce jalon : ecrire la politique, PAS de job de purge
> automatique (hors perimetre temps, a implementer dans une iteration
> ulterieure si demande explicitement).

| Couche | Table(s) | Duree proposee | Justification |
|---|---|---|---|
| Bronze (raw) | `600k_fitness_summary`, `600k_fitness_detailed` | 12 mois glissants | Catalogue d'exercices, pas de donnee personnelle — retention limitee par hygiene de stockage, pas par obligation RGPD. |
| Bronze (raw) | `gym_members` | 36 mois glissants | Donnees de sante pseudo-personnelles ; alignee sur une duree "compte actif + historique d'entrainement" realiste pour une appli fitness. |
| Bronze (raw) | `weight_training` | 36 mois glissants | Meme raisonnement ; les donnees sources couvrent deja ~3 ans d'historique reel (jusqu'a 2018-09-29). |
| Silver | Les 4 memes tables | Alignee sur Bronze correspondant | Silver est entierement recalculee depuis Bronze a chaque run (pas de retention independante a definir). |
| Gold | `dim_exercise`, `dim_muscle`, `dim_date` | Illimitee | Tables de reference/dimension, aucune donnee personnelle. |
| Gold | `dim_user` (profil) | Duree du compte + 30 jours (periode de grace), puis anonymisation | Coherent avec un droit a l'effacement standard : suppression differee courte pour permettre une re-activation accidentelle, avant anonymisation definitive. |
| Gold | `fact_workout_session`, `fact_risk_score` (donnees de seance, granularite fine) | 36 mois glissants, puis agregation mensuelle anonymisee au-dela | Les seances detaillees perdent leur utilite operationnelle passe ce delai ; conserver seulement des agregats (ex. tendance mensuelle par zone) au-dela, sans lien a `user_id`. |
| Gold | `fact_risk_score_demo_synthetic` | Illimitee | 100% synthetique, aucune donnee personnelle, `is_synthetic_demo=true` sur 100% des lignes. |
| S3/Athena (export) | 7 tables Gold exportees | Alignee sur la table Gold source correspondante | Le cycle de vie S3 devrait suivre celui de la donnee Gold d'origine (purge/anonymisation repercutee a l'export au meme rythme) — non automatise a ce stade (pas de regle de cycle de vie S3 `lifecycle` configuree). |

**Non fait volontairement a ce stade** : job Airflow de purge automatique,
regles de cycle de vie S3 (`aws_s3_bucket_lifecycle_configuration`),
suppression automatique passe les durees ci-dessus. Cette politique sert de
reference ecrite pour une implementation future explicitement demandee.

## 4. Droit a l'effacement

Voir [DATA_CATALOG.md](./DATA_CATALOG.md) pour le catalogue complet des
tables, et `scripts/gdpr_erase_user.py` (docstring du script) pour le detail
technique complet de l'implementation, des couches couvertes et des limites
assumees.

## 5. Re-verification suite a l'extension multi-profils (2026-07-11)

L'extension de l'historique demo a 5 profils `dim_user` distincts (voir
`data/gold/GOLD_MODEL_DECISIONS.md` section 5) ne remet en cause AUCUN
mecanisme RGPD deja documente ci-dessus (pseudonymisation HMAC-SHA256,
droit a l'effacement) — les deux ont ete RE-TESTES reellement sur cette
nouvelle realite, pas juste supposes compatibles :

- **`scripts/pseudonymize.py`** : testee sur les 5 `user_id` demo (9, 21,
  34, 46, 83) avec la cle reelle (`PSEUDONYMIZATION_KEY`) — coherence
  confirmee (meme `user_id` -> toujours le meme pseudonyme sur 2 appels
  successifs) et absence de collision entre les 5 profils.
- **`scripts/gdpr_erase_user.py`** : re-teste en conditions reelles sur
  `user_id=21` (un des NOUVEAUX profils demo, PAS `user_id=9` — choix
  explicite pour ne jamais perturber le profil demo principal). Dry-run
  puis execution reelle (`--confirm --skip-s3
  --i-understand-this-breaks-the-demo`, ce dernier flag desormais requis
  car `is_weight_training_demo_user=true` sur 5 profils au lieu d'1 seul) :
  suppression confirmee sur les 4 couches (Postgres Gold 973->972 lignes
  `dim_user`, 406 lignes `fact_workout_session` supprimees, Silver/Bronze/CSV
  973->972 lignes chacun), **puis restauree** a partir d'une sauvegarde des
  fichiers CSV/Bronze/Silver (meme methodologie que le test precedent sur
  `user_id=4`) suivie d'un `dbt run` complet (recalcule Gold depuis
  `raw.silver_gym_members`, jamais modifiee directement par ce script) —
  99/99 tests dbt PASS apres restauration, `user_id=21` confirme de nouveau
  present avec ses 406 lignes `fact_workout_session`. `--skip-s3` utilise
  intentionnellement (l'export S3 est explicitement laisse desynchronise
  dans cette tache, voir `terraform/AWS_LAB_CONSTRAINTS.md`).
- Messages du garde-fou (`is_weight_training_demo_user=true`) et
  commentaires du docstring mis a jour pour ne plus dire "le seul profil"
  (desormais factuellement faux) mais "un des 5 profils demo".
