# Contraintes du compte AWS Lab (AWS Academy / Vocareum) — audit etape 6/6

> Document de reference : ce qui a ete verifie, ce qui fonctionne, ce qui
> echoue, avant d'ecrire la moindre ressource Terraform. A completer au fur
> et a mesure. Aucune ressource AWS n'a ete creee pendant cet audit.

## Outils locaux (etat au 2026-07-04)

- **AWS CLI** : absent au debut de l'audit (ni dans le PATH, ni dans les
  emplacements d'installation Windows habituels). Installe via
  `winget install --id Amazon.AWSCLI -e` -> `aws-cli/2.35.15`.
- **Terraform** : absent au debut de l'audit. Installe via
  `winget install --id Hashicorp.Terraform -e` -> `Terraform v1.15.7`.
- Les deux binaires ne sont pas encore resolus par le `PATH` du shell
  courant (necessite un redemarrage de session shell) ; appeles par chemin
  complet en attendant :
  - `C:\Program Files\Amazon\AWSCLIV2\aws.exe`
  - `%LOCALAPPDATA%\Microsoft\WinGet\Links\terraform.exe`

## Fichier de credentials

- Trouve a `~/.aws/credentials.txt` (nom NON standard — AWS CLI attend
  `~/.aws/credentials`, sans extension). **Renomme en `~/.aws/credentials`**
  pour etre reconnu automatiquement par le profil nomme `awslearnerlab`.
- Contenu (noms de champs uniquement, valeurs jamais affichees) : profil
  `[awslearnerlab]` avec `aws_access_key_id`, `aws_secret_access_key`,
  `aws_session_token` — coherent avec des identifiants TEMPORAIRES d'un lab
  AWS Academy/Vocareum (session token present = ce ne sont pas des
  identifiants IAM long-terme classiques).
- Aucun fichier `~/.aws/config` present : aucune region n'etait configuree
  nulle part avant cet audit (ni variable d'environnement `AWS_*`, ni
  fichier config).

## ⚠️ Constat bloquant : credentials expires

- `aws sts get-caller-identity --profile awslearnerlab` echoue avec :
  `InvalidClientTokenId: The security token included in the request is
  invalid`.
- Cause tres probable : le fichier `credentials.txt` date du **19/06/2025**
  (`LastWriteTime` du fichier) — plus d'un an avant cet audit (2026-07-04).
  Les sessions AWS Academy Learner Lab emettent des identifiants temporaires
  valables seulement quelques heures ; un fichier aussi ancien est
  quasi-certainement expire, independamment de toute autre cause.
- **Consequence** : impossible de verifier a ce stade l'ARN du role actif,
  les policies IAM attachees, l'acces S3/Athena, ni la region reelle du
  compte lab tant que des identifiants frais n'ont pas ete recopies dans
  `~/.aws/credentials` (portail Vocareum -> bouton "AWS Details" ->
  bloc `aws_access_key_id` / `aws_secret_access_key` / `aws_session_token`
  a jour). Action manuelle requise (jamais demandee en clair dans le chat
  Claude Code, uniquement via rafraichissement du fichier local par
  l'utilisateur).

## Verifications non encore effectuees (bloquees par les credentials expires)

A relancer des que les identifiants sont rafraichis :

- [ ] `aws sts get-caller-identity --profile awslearnerlab` — ARN du role
      actif (a documenter ici une fois obtenu).
- [ ] `aws iam get-role` / `aws iam list-attached-role-policies` sur le role
      identifie — policies IAM reellement attachees (ou echec documente si
      refuse par le lab, ce qui est frequent sur ce type de compte).
- [ ] `aws s3 ls` — acces S3 de base en lecture.
- [ ] `aws athena list-work-groups` — acces Athena de base.
- [ ] Confirmation de la region active reelle (`aws configure get region`
      ne renvoyait rien : aucune region par defaut configuree ; le provider
      Terraform ci-dessous force `us-east-1` en attendant confirmation).

## Terraform

- Dossier `terraform/` cree a la racine du repo, avec un unique fichier
  `versions.tf` (aucune ressource declaree) :
  - `required_version >= 1.5.0`, provider `hashicorp/aws ~> 5.0`.
  - **Backend `local`** explicitement choisi pour cette sous-etape (state
    sur disque, `terraform.tfstate`) — **PAS de backend S3 distant** tant
    que les droits du compte lab (creation de bucket, versioning, policies)
    n'ont pas ete confirmes. Un compte lab pedagogique restreint
    frequemment la creation de ressources IAM/bucket persistantes.
  - Provider `aws` configure avec `region = "us-east-1"` (region du lab
    indiquee par l'utilisateur, PAS encore confirmee via une commande AWS
    reelle a cause du blocage credentials ci-dessus) et
    `profile = "awslearnerlab"`.
- `terraform init` : **succes**. Provider `hashicorp/aws v5.100.0`
  telecharge et verifie (signature HashiCorp), backend local configure,
  fichier de verrouillage `.terraform.lock.hcl` genere.
- `terraform init` ne necessite aucun appel reseau vers AWS lui-meme (juste
  le registry Terraform pour telecharger le plugin provider) : son succes
  ne prouve donc PAS que les credentials AWS sont valides, seulement que la
  configuration locale Terraform est correcte.

## ✅ Audit complete (2026-07-06) — credentials rafraichis

Identifiants temporaires rafraichis par l'utilisateur dans
`~/.aws/credentials` (profil `awslearnerlab`). Toutes les verifications
lecture-seule ci-dessous ont ete executees avec succes (aucune ressource
creee).

### Identite active

- `aws sts get-caller-identity --profile awslearnerlab` :
  - **Account** : `097115946702`
  - **Arn** : `arn:aws:sts::097115946702:assumed-role/voclabs/user4161432=KOUTAM_Moulaye_Mohamed`
  - Le role reellement **assume par l'utilisateur/le provider Terraform**
    est donc `voclabs` (pas `LabRole`). C'est le comportement standard AWS
    Academy Learner Lab : l'utilisateur (via SSO Vocareum) assume `voclabs`,
    tandis que `LabRole` est un role **separe**, destine a etre attache aux
    ressources creees (instance profile EC2, role d'execution
    Glue/Lambda/etc.), pas au provider AWS lui-meme.

### Policies attachees

- **Role `voclabs`** (celui utilise par le provider Terraform via le
  profil `awslearnerlab`) — `aws iam list-attached-role-policies
  --role-name voclabs` : succes.
  - `voc-cancel-cred` (`arn:aws:iam::097115946702:policy/voc-cancel-cred`)
  - `Pvoclabs1` (`arn:aws:iam::097115946702:policy/Pvoclabs1`)
  - `Pvoclabs2` (`arn:aws:iam::097115946702:policy/Pvoclabs2`)
  - **`aws iam get-role --role-name voclabs` echoue explicitement** :
    `AccessDenied ... explicit deny in an identity-based policy:
    arn:aws:iam::097115946702:policy/Pvoclabs2`. Confirme que `Pvoclabs2`
    bloque delibrement la lecture des details du role de controle
    Vocareum lui-meme (comportement attendu sur ce type de compte lab,
    pas un blocage a contourner).
- **Role `LabRole`** (destine aux ressources Terraform, ex. execution
  role Glue/EC2) — `aws iam list-attached-role-policies --role-name
  LabRole` : succes, 7 policies attachees :
  - `AmazonSSMManagedInstanceCore`, `AmazonEKSClusterPolicy`,
    `AmazonEC2ContainerRegistryReadOnly`, `AmazonEKSWorkerNodePolicy`
    (policies AWS managees)
  - `c218948a...-VocLabPolicy1-96CT3AoVq6dM`,
    `...-VocLabPolicy2-R6zcT6VHzAOB`, `...-VocLabPolicy3-IYQG1ot10bv8`
    (policies specifiques a l'instance de lab, contenu non inspecte a ce
    stade — non necessaire pour l'audit lecture-seule).

### Acces S3 et Athena

- `aws s3 ls --profile awslearnerlab` : **succes (exit code 0)**, sortie
  vide -> aucun bucket S3 n'existe actuellement sur le compte (pas une
  erreur de droit, juste un compte vide). Confirme l'acces S3 de base
  fonctionnel.
- `aws athena list-work-groups --profile awslearnerlab --region
  us-east-1` : **succes**, un workgroup existant :
  - `primary` (State: `ENABLED`, EngineVersion effective : `Athena engine
    version 3`, cree le `2026-06-30T13:39:47+02:00` — donc par le setup du
    lab lui-meme, pas par ce projet).

### Region

- `aws configure get region --profile awslearnerlab` : **vide** (exit
  code 1, aucune sortie) -> **aucune region par defaut configuree** dans
  `~/.aws/config` ni via variable d'environnement.
- **Region a utiliser explicitement partout** : `us-east-1` (deja utilisee
  avec succes pour l'appel Athena ci-dessus, coherente avec le provider
  Terraform deja configure dans `versions.tf`). Ne jamais compter sur une
  region par defaut implicite — la passer explicitement dans le provider
  Terraform (`region = "us-east-1"`) et dans chaque commande CLI
  (`--region us-east-1`).

### ARN a utiliser dans Terraform

- **ARN du role d'execution pour les ressources** (Glue, EC2, Lambda...) :
  `arn:aws:iam::097115946702:role/LabRole`
  (RoleId `AROARNHEPX3HNCKZGQVQD`, `MaxSessionDuration=3600`,
  `AssumeRolePolicyDocument` liste ~49 services AWS pouvant assumer ce
  role, dont `s3.amazonaws.com`, `athena.amazonaws.com`,
  `glue.amazonaws.com`, `lambda.amazonaws.com`, `ec2.amazonaws.com` —
  couvre largement les besoins previsibles S3+Athena de l'etape 6).
  **Ce role existe deja dans le compte lab** : il ne doit **jamais** etre
  cree/modifie par Terraform (pas de `aws_iam_role.LabRole`), seulement
  **reference** via `data "aws_iam_role" "lab_role" { name = "LabRole" }`
  et son `.arn`, pour l'attacher (ex. `iam_instance_profile` ou role Glue)
  aux ressources creees.
- **Provider AWS** (authentification Terraform elle-meme) : continue
  d'utiliser `profile = "awslearnerlab"` (qui resout vers le role
  `voclabs` assume automatiquement par la session Vocareum) — ne pas
  confondre avec `LabRole` ci-dessus, qui est un role distinct pour les
  ressources, jamais assume directement par le provider.

### Conclusion : plus aucun blocage connu pour ecrire les ressources Terraform

Toutes les verifications prevues sont maintenant passees (compte actif,
acces S3 lecture confirme, acces Athena confirme, region identifiee, ARN
`LabRole` obtenu). Rien n'empeche de passer a la sous-etape 2/6 (premieres
ressources Terraform S3 + Athena) au moment ou elle sera explicitement
demandee.

## ✅ Sous-etape 2/6 (2026-07-06) — Ressources S3 + Athena appliquees pour de vrai

Perimetre explicitement borne a S3 + Athena (pas de CI/CD GitHub Actions, pas
de pseudonymisation/RGPD a ce stade). Aucune ressource IAM creee.

### Ressources Terraform (20 ressources, `terraform apply` reel)

Fichiers : `terraform/variables.tf`, `terraform/s3.tf`, `terraform/athena.tf`,
`terraform/outputs.tf`.

- **`aws_s3_bucket.datalake`** : `safelift-datalake-097115946702` (suffixe
  account ID pour l'unicite globale). Versioning active
  (`aws_s3_bucket_versioning`), chiffrement par defaut SSE-S3/AES256
  (`aws_s3_bucket_server_side_encryption_configuration`), acces public
  totalement bloque (`aws_s3_bucket_public_access_block`, les 4 flags a
  `true`).
- **8 objets prefixe** (`aws_s3_object`, cle vide se terminant par `/`) :
  un par table Gold (`gold/<table>/`) + un pour les resultats Athena
  (`athena-results/`).
- **`aws_glue_catalog_database.gold`** (nom `gold`) : choisi plutot que
  `aws_athena_database` (execute une requete `CREATE DATABASE` et exige un
  bucket de resultats des la creation) ou `aws_athena_named_query` (ne cree
  pas de table persistante) — Athena utilise de toute facon AWS Glue Data
  Catalog comme metastore par defaut, declarer directement les ressources
  Glue est le chemin le plus direct et le plus robuste.
- **7 `aws_glue_catalog_table`** (une par table Gold), format Parquet
  (`ParquetHiveSerDe`), `location = s3://.../gold/<table>/`. **Schema des
  colonnes recupere par introspection REELLE de `app-postgres`**
  (`information_schema.columns` sur le schema `gold`, pas suppose depuis
  dbt) :
  - `fact_workout_session` (12 col.), `fact_risk_score` (20 col.),
    `dim_exercise` (8 col.), `dim_muscle` (3 col.), `dim_user` (17 col.),
    `dim_date` (8 col.), `fact_risk_score_demo_synthetic` (13 col.).
  - Mapping de types : Postgres `bigint`->Athena `bigint`, `integer`->`int`,
    `text`/`character varying`->`string`, `boolean`->`boolean`,
    `numeric`/`double precision`->`double`, `date`->`date`.
- **`data.aws_iam_role.lab_role`** (nom `LabRole`) : reference en LECTURE
  SEULE (data source, pas de ressource). Aucune ressource ci-dessus n'exige
  aujourd'hui de role d'execution (pas de crawler/job Glue cree) ; l'ARN est
  expose en output (`lab_role_arn`) pour un usage futur. **Aucun
  `aws_iam_role`/`aws_iam_policy`/`aws_iam_user` declare.**

`terraform plan` : 20 to add, 0 to change, 0 to destroy (verifie avant
apply). `terraform apply` : **20 added, 0 changed, 0 destroyed**, succes
complet.

### Script d'export/upload (`scripts/upload_gold_to_s3.py`)

- Connexion `psycopg2` directe a `app-postgres` (port EXPOSE sur l'hote,
  `APP_POSTGRES_PORT_EXPOSED=15432`, PAS le port interne Docker 5432 — ce
  script tourne hors conteneur).
- Pour chacune des 7 tables Gold : `SELECT` complet -> tableau `pyarrow`
  avec schema EXPLICITE (memes noms/types que `terraform/athena.tf`,
  conversion `Decimal`->`float` geree explicitement pour les colonnes
  Postgres `numeric`) -> fichier local `data/gold/<table>/<table>.parquet`
  (dossier deja gitignore, comme le reste de `data/gold/*`) -> upload S3 via
  `boto3` (`profile=awslearnerlab`, `region=us-east-1`) vers
  `s3://safelift-datalake-097115946702/gold/<table>/<table>.parquet`.
- **Environnement d'execution** : venv Windows natif DEDIE `.venv-aws/`
  (root du repo), separe du `.venv/` existant (etape 2, Kaggle). Cause :
  `.venv/` a ete cree sous WSL Ubuntu (`pyvenv.cfg` -> `/usr/bin/python3.12`,
  chemin `/mnt/c/...`) lors d'une session precedente ; son binaire
  `bin/python` ne resout pas dans la session Git Bash (MINGW64) native
  Windows utilisee pour cette sous-etape — environnement shell different de
  celui qui avait cree `.venv/`. Plutot que de recreer/casser `.venv/`
  (potentiellement encore utilise depuis WSL par
  `data/download_datasets.sh`), un second venv Windows natif dedie
  (`python -m venv .venv-aws`) a ete cree pour ce script uniquement.
  Dependances pinnees dans `scripts/requirements_aws.txt`
  (`boto3==1.43.40`, `pandas==3.0.3`, `psycopg2-binary==2.9.12`,
  `pyarrow==24.0.0`).
- **Execution reelle (pas un dry-run)** : script lance une fois, toutes les
  7 tables exportees et uploadees avec succes :

  | Table | Lignes exportees |
  |---|---|
  | fact_workout_session | 2 164 |
  | fact_risk_score | 2 164 |
  | dim_exercise | 3 177 |
  | dim_muscle | 9 |
  | dim_user | 973 |
  | dim_date | 1 073 |
  | fact_risk_score_demo_synthetic | 9 |

  Total : 9 569 lignes, 7 fichiers Parquet. Tous les chiffres coherents
  avec ceux deja documentes en etape 4 (Gold).

### Verification S3

`aws s3 ls s3://safelift-datalake-097115946702/gold/ --recursive` :
confirme les 7 objets prefixe (0 octet) + les 7 fichiers Parquet reels
(tailles de 1.3 KB a 100.6 KB selon la table), tous sous `gold/<table>/`.

### Verification Athena (requetes reelles, pas un dry-run)

- `SELECT COUNT(*) FROM gold.fact_risk_score` (via `aws athena
  start-query-execution` + `get-query-results`, workgroup `primary`,
  `OutputLocation=s3://.../athena-results/`) : **`cnt = 2164`** — identique
  au chiffre documente en etape 4 (dbt test de grain unique, meme table).
- `SELECT risk_level, COUNT(*) FROM gold.fact_risk_score GROUP BY
  risk_level` : **`Eleve=26`, `Faible=1915`, `Modere=223`** — identique a la
  distribution recalibree documentee dans CLAUDE.md ("Faible 97.8%->88.5%,
  Modere 2.2%->10.3%, Eleve 0%->1.2% (26 lignes)"). Confirme que les
  fichiers Parquet exportes refletent fidelement l'etat reel de
  `app-postgres`, sans divergence de schema ni de valeurs.

### Cout observe

`aws ce get-cost-and-usage` (Cost Explorer) accessible sur ce compte lab,
periode 2026-07-01 -> 2026-07-07 : **`$0`** sur toutes les journees
interrogees. Cout reel non nul mais non encore visible (delai de
publication Cost Explorer, generalement ~24h) : le volume reel (quelques
centaines de Ko sur S3, 2 scans Athena sur des donnees de quelques dizaines
de Ko chacun) represente de toute facon une fraction de centime, tres en
dessous du budget de $50 du compte lab.

### Points d'attention pour la suite

- Les tables Athena pointent vers un SEUL fichier Parquet par table (pas de
  partitionnement) : suffisant pour ce volume (max ~3 177 lignes), a
  revisiter uniquement si le volume Gold grossit significativement.
- Le script d'upload n'est PAS encore orchestre par Airflow (execution
  manuelle uniquement, tel que demande) : une sous-etape ulterieure devra
  explicitement traiter l'automatisation (DAG dedie ou extension d'un DAG
  existant) si besoin.
- Re-executer `scripts/upload_gold_to_s3.py` ecrase les fichiers Parquet
  locaux et les objets S3 existants (memes cles) — comportement voulu pour
  rafraichir les donnees Gold sur S3, le versioning S3 active conserve les
  versions precedentes en cas de besoin de rollback.

## Prochaine action

Sous-etape 2/6 (S3 + Athena) terminee et verifiee de bout en bout. Ne pas
anticiper une sous-etape 3/6 (CI/CD, pseudonymisation/RGPD, ou toute autre
extension) sans demande explicite — voir regle du CLAUDE.md "ne pas
anticiper les etapes futures tant qu'elles n'ont pas ete explicitement
demandees".
