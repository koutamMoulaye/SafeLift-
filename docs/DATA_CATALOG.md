# Data Catalog — SafeLift (etape 6/6, sous-etape 4/6)

> Catalogue de toutes les tables du data lake (Bronze/Silver/Gold). Voir
> [RGPD_GOVERNANCE.md](./RGPD_GOVERNANCE.md) pour le detail des mesures
> (pseudonymisation, chiffrement, retention, effacement) et
> `data/gold/GOLD_MODEL_DECISIONS.md` / `data/silver/CLEANING_LOG.md` /
> `data/bronze/SCHEMA_NOTES.md` pour le detail technique de chaque table.
>
> **Base legale RGPD presumee** : ce projet est une demonstration pedagogique
> sur des jeux de donnees Kaggle publics — aucun consentement RGPD reel n'a
> ete recueilli aupres des personnes representees dans `gym_members`. La
> colonne "Base legale" ci-dessous indique la base qui SERAIT applicable si ce
> systeme traitait de vraies donnees utilisateur en production (objectif
> pedagogique du Bloc 5 : demontrer la maitrise du raisonnement, pas une
> conformite reelle de ce jeu de donnees Kaggle).

## Bronze (raw, partitionne par `ingestion_date`)

| Table | Description | Colonnes sensibles | Base legale presumee | Retention | Pseudonymisee |
|---|---|---|---|---|---|
| `600k_fitness_summary` | Catalogue de programmes d'entrainement (metadonnees : niveau, objectif, duree). Source : `program_summary.csv`. | Aucune (pas de donnee personnelle — catalogue d'exercices/programmes). | Non applicable. | 12 mois glissants (hygiene de stockage). | Non applicable. |
| `600k_fitness_detailed` | Detail exercice/semaine/jour de chaque programme. Source : `programs_detailed_boostcamp_kaggle.csv`. | Aucune. | Non applicable. | 12 mois glissants. | Non applicable. |
| `gym_members` | 973 profils individuels : age, genre, poids, taille, frequence cardiaque, % de masse grasse, type d'entrainement. Source : `gym_members_exercise_tracking.csv`. | **Toutes les colonnes physiologiques** (donnees de sante Art. 9 RGPD) : `Age`, `Gender`, `Weight (kg)`, `Height (m)`, `Max_BPM`/`Avg_BPM`/`Resting_BPM`, `Fat_Percentage`, `BMI`, etc. Aucun identifiant direct dans cette table (pas de nom/email) — l'identifiant `user_id` n'existe qu'a partir du Staging dbt (surrogate key, voir RGPD_GOVERNANCE.md 1.1). | Consentement explicite (Art. 9.2.a) si donnees reelles. | 36 mois glissants. | Non (couche interne, jamais exportee telle quelle vers S3). |
| `weight_training` | Journal personnel de seances de musculation sur ~3 ans (721 seances). Source : `weightlifting_721_workouts.csv`. Licence Kaggle **`unknown`** (a verifier avant soutenance, cf. `data/bronze/SCHEMA_NOTES.md`). | Aucun identifiant natif (pas de nom/email/ID dans la source). Devient indirectement personnel une fois reparti (par bloc chronologique) entre 5 "demo users" en Gold depuis le 2026-07-11 (hypothese de demonstration, pas une vraie identite — voir GOLD_MODEL_DECISIONS.md section 5). | Non applicable en l'etat (aucune identite associee dans la source). | 36 mois glissants. | Non applicable (pas d'identifiant a la source). |

## Silver (cleaned, recalculee depuis Bronze a chaque run)

| Table | Description | Colonnes sensibles | Base legale presumee | Retention | Pseudonymisee |
|---|---|---|---|---|---|
| `600k_fitness_summary` / `600k_fitness_detailed` | Versions nettoyees (dedup, `level`/`goal` parses en `array<string>`). | Aucune. | Non applicable. | Alignee sur Bronze. | Non applicable. |
| `gym_members` | Version nettoyee (renommage snake_case, aucune transformation de valeur pour les colonnes physiologiques). | Memes colonnes physiologiques que Bronze. | Consentement explicite si donnees reelles. | Alignee sur Bronze (36 mois). | Non. |
| `weight_training` | Version nettoyee (dedup 790 lignes, conversion lbs->kg, colonnes quasi-vides supprimees). | Aucun identifiant natif. | Non applicable en l'etat. | Alignee sur Bronze (36 mois). | Non applicable. |

## Gold (modele en etoile, Postgres `app-postgres` schema `gold`)

| Table | Description | Colonnes sensibles | Base legale presumee | Retention | Pseudonymisee |
|---|---|---|---|---|---|
| `dim_exercise` | Catalogue d'exercices (3 177 lignes), matching exercise_name <-> muscle_group. | Aucune. | Non applicable. | Illimitee. | Non applicable. |
| `dim_muscle` | 9 zones musculaires + `base_epidemiological_risk` (hypothese de modelisation). | Aucune. | Non applicable. | Illimitee. | Non applicable. |
| `dim_date` | Dimension calendaire. | Aucune. | Non applicable. | Illimitee. | Non applicable. |
| `dim_user` | 973 profils (`user_id` + attributs physiologiques + `is_weight_training_demo_user`, **vrai sur 5 lignes depuis le 2026-07-11**, une seule avant). | `user_id` (identifiant direct) + toutes les colonnes physiologiques (Art. 9). Quasi-identifiants combines (age+gender+poids+taille+bmi) — risque residuel documente, voir RGPD_GOVERNANCE.md 1.1. | Consentement explicite (Art. 9.2.a) si donnees reelles. | Duree du compte + 30 jours, puis anonymisation. | **Oui, a l'export S3** : `user_id` -> `user_pseudo_id` (HMAC-SHA256). En clair dans Postgres (couche interne, jamais exposee publiquement — voir RGPD_GOVERNANCE.md 1.3 pour la justification). |
| `fact_workout_session` | 2 169 lignes (2 164 avant l'extension multi-profils du 2026-07-11, +5 lignes temps reel entre-temps), 1 ligne = 1 exercice realise dans une seance (charge, repetitions, duree), reparties sur 5 `user_id` distincts. | `user_id` (FK) ; `lifted_weight_kg`/`duration_seconds` sont des donnees d'activite physique liees a un individu. | Consentement explicite si donnees reelles. | 36 mois glissants, puis agregation anonymisee. | **Oui, a l'export S3** (memes modalites que `dim_user`). |
| `fact_risk_score` | 2 169 lignes, memes grain que `fact_workout_session` + score de risque deterministe (`base_zone`, `charge_factor`, etc.). | `user_id` (FK) ; le `risk_score` lui-meme est une donnee derivee de sante (evaluation d'un risque physiologique individuel). | Consentement explicite si donnees reelles. | 36 mois glissants, puis agregation anonymisee. | **Oui, a l'export S3** (memes modalites). |
| `fact_risk_score_demo_synthetic` | 9 scenarios 100% fictifs (`is_synthetic_demo=true` sur toutes les lignes), aucun lien a un `user_id` reel. | Aucune (donnees inventees pour la demo). | Non applicable. | Illimitee. | Non applicable (pas de `user_id`). |

## S3 / Athena (export externe, `s3://safelift-datalake-097115946702/gold/`)

Memes 7 tables Gold, memes colonnes sensibles/base legale/retention que
ci-dessus, **a une exception pres** : `dim_user`, `fact_workout_session` et
`fact_risk_score` exportent `user_pseudo_id` (string HMAC-SHA256) a la place
de `user_id` (bigint) — voir `scripts/upload_gold_to_s3.py` et
`terraform/athena.tf`. Chiffrement au repos : SSE-S3/AES256 (voir
`terraform/s3.tf`). Acces public entierement bloque.

⚠️ **DESYNCHRONISE depuis l'extension multi-profils du 2026-07-11** (voir
`terraform/AWS_LAB_CONSTRAINTS.md`) : cet export S3 reflete encore l'ancien
etat mono-profil de Gold (dernier export reel : 2026-07-07). A refaire
avant la demo finale, une fois des credentials AWS Learner Lab rafraichis
(decision explicitement differee, hors perimetre de cette extension).
