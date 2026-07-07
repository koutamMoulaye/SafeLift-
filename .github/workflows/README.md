# CI/CD Terraform — SafeLift (etape 6/6, sous-etape 3/6)

Ce dossier contient `terraform-ci.yml`, le pipeline GitHub Actions qui
valide le code Terraform (`terraform/`) a chaque pull request/push le
touchant, plus un declenchement manuel (`workflow_dispatch`).

## Pourquoi il n'y a JAMAIS de `terraform apply` automatique

Le compte AWS utilise pour ce projet est un **compte AWS Academy Learner
Lab** (voir `terraform/AWS_LAB_CONSTRAINTS.md`) : les credentials qu'il
fournit sont **temporaires** (`aws_session_token` inclus), avec une
expiration de l'ordre de quelques heures par session de lab. Un `apply`
automatique declenche par CI serait :
- **Non fiable** : le job pourrait echouer aleatoirement selon que la
  session lab de l'utilisateur est active ou non au moment du run CI,
  independamment de la qualite du code Terraform.
- **Risque pour un compte pedagogique partage/limite en cout** : appliquer
  des changements d'infrastructure sans supervision humaine directe sur un
  compte de ce type (budget limite, ressources parfois reinitialisees par
  le lab) est une decision qui doit rester **volontaire et tracee**, pas
  automatisee.

**Ce pipeline se limite donc a `fmt` + `init` + `validate` + `plan`.**
`terraform apply` reste **toujours** execute manuellement, en session, par
l'utilisateur (memes commandes qu'en local — voir le reste de la doc du
projet, ex. PROGRESS.md sous-etape 2/6 pour un exemple d'apply reel deja
effectue).

## Ce que verifie chaque etape

| Etape | Necessite un acces AWS reel ? | Peut faire echouer le job ? |
|---|---|---|
| `terraform fmt -check` | Non | Oui — vrai probleme de style a corriger (`terraform fmt -recursive` en local) |
| `terraform init` | Non (backend local, cf. `terraform/versions.tf`) | Oui — probleme reel de config/provider |
| `terraform validate` | Non | Oui — **le vrai garde-fou qualite** de ce pipeline, doit toujours reussir |
| `terraform plan` | **Oui** | **Non** (`continue-on-error: true` explicite) — un echec ici est traite comme "credentials probablement expirees", pas comme un echec de qualite de code |

## Ajouter les secrets GitHub (credentials AWS)

Le plan Terraform a besoin de credentials AWS valides pour s'executer (le
provider `terraform/versions.tf` reference le profil nomme
`awslearnerlab` — le workflow recree ce meme profil dans le runner CI a
partir des 3 secrets ci-dessous, sans modifier `versions.tf`).

1. Recuperer des credentials **fraiches** depuis la console AWS Academy
   Learner Lab (bouton "AWS Details" > "Show" sur la page du lab) : elles se
   presentent sous la forme d'un bloc `[default]` avec
   `aws_access_key_id` / `aws_secret_access_key` / `aws_session_token`.
2. Dans le depot GitHub : **Settings > Secrets and variables > Actions >
   New repository secret**, ajouter les 3 secrets suivants (noms EXACTS,
   sensibles a la casse) :
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_SESSION_TOKEN`
3. Aucune autre configuration necessaire : le workflow les reference via
   `${{ secrets.AWS_ACCESS_KEY_ID }}` etc., jamais en clair dans le YAML.

**Ces credentials expirent en quelques heures.** C'est normal et attendu
(voir tableau ci-dessus) — pas la peine de "corriger" quoi que ce soit dans
le code si le plan echoue pour cette raison.

## Que faire si `terraform plan` echoue en CI ?

1. Regarder le message de l'etape "Rapport clair du resultat du plan" dans
   les logs du run : si c'est bien une erreur d'authentification/expiration
   AWS (`ExpiredToken`, `InvalidClientTokenId`, `AuthorizationHeaderMalformed`...),
   c'est la cause attendue — voir section precedente.
2. Recuperer des credentials fraiches depuis AWS Academy Learner Lab et
   **mettre a jour les 3 secrets GitHub** (memes noms, on peut juste
   ecraser les anciennes valeurs — Settings > Secrets and variables >
   Actions > cliquer sur le secret > Update).
3. Relancer le workflow : soit en repoussant un commit sur la PR, soit via
   **Actions > Terraform CI (SafeLift) > Run workflow** (bouton
   `workflow_dispatch`, ne necessite aucun changement de code).
4. Si l'echec persiste avec des credentials fraiches confirmees valides
   (`aws sts get-caller-identity` reussit en local avec les memes valeurs),
   **alors seulement** investiguer une vraie regression du code Terraform.

## Test reel effectue

Voir PROGRESS.md (etape 6/6, sous-etape 3/6) pour le resultat du run de
test reel de ce workflow (lien/description du run GitHub Actions).
