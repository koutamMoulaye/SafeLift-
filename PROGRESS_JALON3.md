# PROGRESS_JALON3.md — Suivi d'avancement SafeLift, Jalon 3 (nutrition + ML bonus)

> Meme legende/regle que PROGRESS.md/PROGRESS_JALON2.md (✅ fait ·
> 🔄 en cours · ⏳ a faire, toujours a jour AVANT de considerer une
> sous-etape terminee). Voir CLAUDE.md pour le pointeur vers ce fichier et
> le contexte global du projet.

## Contexte du Jalon 3

Le Jalon 1 (pipeline batch complet) et le Jalon 2 (streaming temps reel)
sont clos. Le Jalon 3 ajoute la **nutrition** (API USDA FoodData Central)
et une **couche ML bonus** (sous-etapes suivantes, pas celle-ci).

Decoupage prevu (6 sous-etapes) :
1. Ingestion nutrition + dimension + calculs deterministes — ✅ fait
2. Dashboard nutrition — ✅ fait
3. Preparation des donnees ML (pas d'entrainement) — ✅ fait
4. Entrainement + evaluation du modele ML — ✅ fait
5. Integration pipeline + dashboard de la prediction ML — ✅ fait
6. Refonte UX/UI par onglets + corrections de débordement — ✅ fait

## Sous-etape 1/6 — Ingestion nutrition + dimension + calculs deterministes — ✅ fait

**Date** : 2026-07-09.

**Perimetre explicitement borne** : ingestion USDA + `dim_nutrition` +
`fact_nutrition_target` UNIQUEMENT. Pas de dashboard a ce stade.

### ⚠️ Rappel du cadre ethique (voir aussi GOLD_MODEL_DECISIONS.md section 13)

Les formules de `fact_nutrition_target` (BMR, TDEE, besoin proteique) sont
des formules **standard, generalistes, deterministes** — **PAS des
recommandations medicales ou nutritionnelles personnalisees**. SafeLift ne
remplace ni un coach sportif diplome, ni un medecin, ni un dieteticien.

### Livre

- **`airflow/dags/nutrition_ingestion.py`** : DAG self-contained (5 tasks,
  independant de `bronze_ingestion`/`silver_transformation`/`gold_dbt_run`
  — domaine different) :
  1. `ingest_usda_nutrition` (PythonOperator) : appelle l'API USDA
     FoodData Central (`/foods/search`, ~31 mots-cles, `dataType`
     restreint a `Foundation,SR Legacy`), deduplique par `fdc_id`, ecrit
     en Bronze (`data/bronze/usda_nutrition/ingestion_date={{ ds }}/`,
     idempotent — meme convention que `bronze_ingestion.py`). Cle API
     (`USDA_API_KEY`) lue uniquement via variable d'environnement, jamais
     en dur, jamais loggee meme partiellement (`_redact_secret()`). Retry
     avec backoff (3 tentatives, 5s) sur rate limit (HTTP 429)/erreurs
     reseau, puis **echec explicite** de la task si l'API reste
     indisponible.
  2. `silver_usda_nutrition` (spark-submit) : dedup par `fdc_id`, trim
     `food_name`.
  3. `load_usda_nutrition_to_postgres` (spark-submit) : reutilise
     `spark/jobs/load_silver_to_postgres.py` existant (`usda_nutrition`
     ajoutee au dictionnaire `TABLES`).
  4. `dbt_run_nutrition` / `dbt_test_nutrition` : `dbt run`/`test --select
     stg_usda_nutrition dim_nutrition fact_nutrition_target` (scope
     restreint, PAS tout `gold_dbt_run` — `fact_nutrition_target` ne
     depend que de `dim_user` deja construite).
- **`dbt/models/staging/stg_usda_nutrition.sql`** + **`dbt/models/marts/dim_nutrition.sql`** :
  catalogue d'aliments (`fdc_id`, `food_name`, `food_category`,
  `kcal_per_100g`, `protein_g_per_100g`, `carbs_g_per_100g`,
  `fat_g_per_100g` — tous par 100g, aucune conversion d'unite).
- **`dbt/models/marts/fact_nutrition_target.sql`** : 1 ligne par
  utilisateur de `dim_user`, calcule :
  - **BMR** (Mifflin-St Jeor, 1990) a partir de `age`/`gender`/
    `body_weight_kg`/`height_m` (deja dans `dim_user` depuis
    `gym_members`).
  - **TDEE = BMR x facteur d'activite**, facteur deduit de
    `workout_frequency_days_per_week` (mapping documente,
    GOLD_MODEL_DECISIONS.md section 13).
  - **Besoin proteique cible (g/jour) = `protein_g_per_kg_target` x
    poids**, `protein_g_per_kg_target` (1.6 a 2.2 g/kg) deduit de
    `experience_level` (mapping documente, meme section).
  - Tous les facteurs intermediaires restent des colonnes VISIBLES
    (meme philosophie "pas de boite noire" que `fact_risk_score`).
- **2 tests dbt singuliers** :
  `assert_tdee_within_plausible_range.sql` (1000-6000 kcal),
  `assert_protein_target_plausible.sql` (jamais negatif, jamais >4g/kg).
- **`data/gold/GOLD_MODEL_DECISIONS.md` section 13** (nouvelle) :
  documentation complete des formules/hypotheses/limites + rappel du
  cadre ethique.
- **`docker-compose.yml`** : `USDA_API_KEY` ajoutee a
  `x-airflow-common-env` (variable d'environnement uniquement).

### Verifications effectuees (toutes reelles, aucun dry-run)

1. **DAG execute de bout en bout, 2 runs reels successifs (idempotence
   confirmee)** : 5/5 tasks `success` sur les deux runs
   (`manual__2026-07-09T18:32:43+00:00` et
   `manual__2026-07-09T19:23:52+00:00` — les deux ont fini par s'executer
   apres correction du blocage de pause DAG, voir "Bugs reels" ci-dessous
   — executes en parallele, tous deux `success`, confirmant que
   l'ingestion/le chargement/le run dbt restent corrects si rejoues).
2. **Ingestion API reelle** : logs confirmes —
   `[nutrition_ingestion] 119 aliments distincts ecrits dans
   .../usda_nutrition.parquet (25 ignores pour macro-nutriment manquant)`.
   119 aliments distincts (dans la fourchette visee, ~31 mots-cles x
   jusqu'a 4 resultats, dedupliques par `fdc_id`).
3. **Aucune fuite de la cle API dans les logs** : recherche exhaustive de
   la valeur exacte de `USDA_API_KEY` sur l'ensemble des logs Airflow du
   run (`grep -rl` recursif sur `/opt/airflow/logs/`) — **0 occurrence**
   trouvee.
4. **`dbt test` (scope nutrition, dans le DAG) : 16/16 PASS**, dont les 2
   tests plausibilite dedies (`assert_tdee_within_plausible_range`,
   `assert_protein_target_plausible`) et les tests generiques
   (`not_null`/`unique`/`relationships`). **`dbt test` SANS scope (projet
   complet, execute separement en verification) : 93/93 PASS** — aucune
   regression sur le reste du schema Gold (Jalon 1/2). Note operationnelle :
   un premier `dbt test` complet a montre seulement 41/41 tests
   (partial-parse cache dbt perime, contenant un etat incomplet du projet
   suite aux modifications de fichiers) — `dbt --no-partial-parse test`
   a confirme les 93 tests reels, tous PASS. Pas un bug applicatif, un
   artefact de cache CLI dbt.
5. **Echantillon reel `dim_nutrition`** (10 aliments) : macro-nutriments
   coherents et varies (ex. `Beans, black, mature seeds, raw` — 341
   kcal/100g, 21.6g proteines ; `Broccoli, raw` — 31 kcal/100g, 2.57g
   proteines ; `Protein supplement, milk based, Muscle Milk, powder` —
   411 kcal/100g, 45.7g proteines). 119 lignes au total dans
   `gold.dim_nutrition`.
6. **`gold.fact_nutrition_target` : 973 lignes** (= nombre exact
   d'utilisateurs dans `gold.dim_user`, aucun utilisateur sans cible
   nutritionnelle).
7. **Sanity check reel sur profils varies** (poids/genre/age extremes) :

   | user_id | age | genre | poids (kg) | jours/sem. | activity_factor | BMR (kcal) | TDEE (kcal) | proteines cible (g/j) |
   |---|---|---|---|---|---|---|---|---|
   | 665 | 47 | Female | 40.0 | 3 | 1.55 | 1104 | 1711 | 64.0 |
   | 210 | 26 | Male | 111.5 | 2 | 1.375 | 2178 | 2994 | 178.4 |
   | 885 | 55 | Male | 129.9 | 3 | 1.55 | 2110 | 3271 | 207.8 |
   | 197 | 26 | Female | 64.1 | 5 | 1.9 | 1288 | 2446 | 141.0 |

   Coherence confirmee : utilisateur le plus leger (40kg) -> TDEE le plus
   bas (1711 kcal) ; utilisateurs les plus lourds (111.5kg/129.9kg) ->
   TDEE les plus eleves (2994/3271 kcal).
8. **Isolation reelle de l'effet activite** (poids/age/genre quasi
   identiques, frequence d'entrainement variable — 22 utilisateurs Male,
   75-85kg, 25-35 ans) : ex. `user_id=220` (76.5kg, 2j/sem.,
   `activity_factor=1.375`) -> TDEE **2566** kcal vs `user_id=183`
   (76.5kg EXACTEMENT le meme poids, 3j/sem., `activity_factor=1.55`) ->
   TDEE **2899** kcal — **confirme que plus de jours d'entrainement =
   TDEE plus eleve, a poids egal**, exactement le sanity check demande.
9. **Verification arithmetique directe** : `tdee_kcal / bmr_kcal` recalcule
   manuellement sur un echantillon correspond exactement a
   `activity_factor` (ex. `user_id=264` : `3620 / 1906 = 1.899 ≈ 1.9`),
   confirme que la formule appliquee correspond bien au modele documente.

### Bugs reels rencontres et corriges pendant les tests

- **DAG reste bloque `queued` indefiniment au premier declenchement** :
  `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true` (convention deja
  actee du projet, voir CLAUDE.md) met tout NOUVEAU DAG en pause a sa
  premiere apparition. Contrairement a une hypothese initiale (confirmee
  fausse en testant), **un DAG en pause ne voit AUCUNE task planifiee par
  le scheduler, meme pour un run declenche manuellement** — le run reste
  `queued` sans qu'aucune task ne demarre jamais. Les 3 autres DAGs du
  projet (`bronze_ingestion`, `silver_transformation`, `gold_dbt_run`)
  avaient ete depauses manuellement lors de sessions precedentes (jamais
  documente explicitement comme etape necessaire). Corrige avec
  `airflow dags unpause nutrition_ingestion`. **A refaire pour tout
  futur nouveau DAG de ce projet** — noter cette etape manuelle
  desormais explicitement dans CLAUDE.md.
- **Interruption de session (~45 minutes) pendant l'attente d'un run** :
  le scheduler a signale `Heartbeat recovered after 2682.98 seconds` --
  la machine hote (ou le moteur Docker/VM sous-jacent) a probablement ete
  mise en veille pendant l'attente. Pas un bug applicatif : redemarrage
  propre du conteneur `airflow-scheduler` (`docker compose restart`)
  suffisant pour repartir sur une base saine.

### Limite assumee

Comme documente en detail dans `data/gold/GOLD_MODEL_DECISIONS.md`
section 13 : le mapping `experience_level -> protein_g_per_kg_target` est
une simplification deliberee (proxy indirect, pas une mesure directe de
masse musculaire/objectif d'entrainement). La fourchette 1.6-2.2 g/kg
elle-meme est bien issue de la litterature sportive, mais CE mapping
precis (1/2/3 -> 1.6/1.9/2.2) est une decision de modelisation du projet,
pas une methode validee independamment.

### Prochaine action

Sous-etape 2/6 (dashboard nutrition) traitee — voir ci-dessous.

## Sous-etape 2/6 — Dashboard nutrition — ✅ fait

**Date** : 2026-07-09 (meme jour que la sous-etape 1/6).

**Perimetre explicitement borne** : section dashboard "Nutrition"
UNIQUEMENT. Pas de ML ici.

### ⚠️ Contrainte non negociable : avertissement ethique toujours visible

Rappel (voir aussi section 13 de GOLD_MODEL_DECISIONS.md et la sous-etape
1/6 ci-dessus) : les chiffres affiches sont des ESTIMATIONS generiques
(formule Mifflin-St Jeor), **PAS des recommandations medicales
personnalisees**. L'avertissement doit etre visible DES l'affichage de la
section, sans action de l'utilisateur — traite ici comme une contrainte
dure, pas une preference de design.

### Livre

- **`GET /users/{user_id}/nutrition`** (`dashboard/main.py`) : lecture
  directe de `gold.fact_nutrition_target` (BMR, TDEE, facteur d'activite,
  besoin proteique cible) + `gold.dim_nutrition` (8 aliments les plus
  riches en proteines pour 100g, `ORDER BY protein_g_per_100g DESC LIMIT 8`
  — pertinent pour un contexte fitness/musculation). **Aucun recalcul cote
  API** : les valeurs sont deja calculees par dbt (sous-etape 1/6), l'API
  se contente de les lire. Le champ `disclaimer` est **TOUJOURS present**
  dans la reponse (source unique de verite du texte d'avertissement, pour
  que le frontend ne puisse jamais l'omettre ou le reformuler par erreur).
  404 explicite si l'utilisateur n'existe pas ou (cas theorique, jamais
  observe) n'a pas de cible nutritionnelle calculee.
- **Section "Nutrition"** (`dashboard/static/index.html`) : theme sombre
  identique au reste du dashboard (memes variables CSS `:root`), inseree
  entre "Logger une séance" et "Affluence en direct" (sections liees a
  l'utilisateur selectionne groupees ensemble). Contenu :
  - **Avertissement ethique en TOUT PREMIER element de la section**
    (`#nutrition-disclaimer`, avant meme les chiffres), style banniere
    (fond ambre, meme esprit visuel que le bandeau mode demo mais couleur
    distincte pour ne pas confondre les deux avertissements) — texte
    fourni par l'API elle-meme (`data.disclaimer`), jamais reformule cote
    JS.
  - Card "Dépense énergétique estimée" : BMR + TDEE (mis en avant en plus
    grand), meta-ligne avec le facteur d'activite/jours d'entrainement/
    poids.
  - Card "Besoin protéique cible" : jauge de remplissage (echelle
    d'affichage 0-300g/jour, marge confortable au-dessus du maximum
    reellement observe sur les 973 profils, ~208g/j) + valeur exacte +
    detail du calcul (g/kg x poids).
  - Liste des 8 aliments suggeres (nom, kcal/100g, proteines/100g).
  - **Desactivee en mode demo** (memes raisons que le simulateur what-if/
    logger seance : les scenarios synthetiques n'ont pas d'`user_id` reel).
- **`dashboard/static/dashboard.js`** : `loadNutrition(userId)` (appelee
  depuis `onUserChange()`, comme les autres sections liees a
  l'utilisateur), `applyNutritionAvailability()` (toggle demo, cablee dans
  `applyModeToUI()` et `init()`).
- **`dashboard/static/dashboard.css`** : nouvelles regles `.nutrition-*`,
  coherentes avec les variables de theme existantes (`--bg-card`,
  `--bg-card-alt`, `--accent-teal`, `--color-modere` pour l'avertissement).

### Verifications effectuees (toutes reelles, aucun dry-run)

1. **Endpoint teste sur 3 utilisateurs reels differents**, memes profils
   que le sanity check de la sous-etape 1/6 :

   | user_id | poids (kg) | BMR (kcal) | TDEE (kcal) | protéines cible (g/j) |
   |---|---|---|---|---|
   | 665 | 40.0 | 1104 | **1711** | 64.0 |
   | 197 | 64.1 | 1288 | **2446** | 141.0 |
   | 885 | 129.9 | 2110 | **3271** | 207.8 |

   Confirme coherent avec les valeurs deja verifiees en sous-etape 1/6
   (mêmes chiffres exacts, l'API ne fait que les relire) — **les chiffres
   changent bien selon le profil**, poids croissant -> TDEE croissant.
2. **`disclaimer` present et nonvide sur les 3 requetes** (>50 caracteres
   a chaque fois), texte identique (source unique cote API, jamais
   personnalise/tronque par utilisateur).
3. **8 aliments suggeres retournes a chaque appel**, tries par
   `protein_g_per_100g` decroissant confirme (ex. Tofu dried-frozen 52.5g,
   Muscle Milk powder 45.7g, cheddar nonfat 32.1g, ... jusqu'a peanut
   butter 24.0g).
4. **Verification structurelle de la visibilite de l'avertissement**
   (navigateur Chrome indisponible, voir limite ci-dessous) : confirme par
   lecture directe du code que `#nutrition-content` (qui contient
   `#nutrition-disclaimer` en premier enfant) **n'a AUCUNE classe `hidden`
   par defaut** dans `index.html` — seul `#nutrition-unavailable` (message
   "mode demo") demarre cache. Aucune regle CSS `display:none`/`visibility:hidden`
   trouvee sur `.nutrition-disclaimer` ou `.nutrition-content`. Le
   basculement demo/reel est le SEUL mecanisme qui masque cette section
   (`applyNutritionAvailability()`), jamais un clic supplementaire de
   l'utilisateur.
5. **Verification structurelle complete du frontend** (Node.js) :
   `node --check dashboard.js` (syntaxe valide), 51 references
   `getElementById(...)` toutes resolues dans `index.html` (0 manquante),
   balises `<section>` (7/7) et `<div>` (34/34) equilibrees, accolades CSS
   equilibrees (117/117).
6. **Non-regression** : `/`, `/static/dashboard.js`, `/static/dashboard.css`
   tous `200` avec le nouveau contenu confirme present ; dashboard
   `healthy` apres rebuild.

### Limite assumee

**Verification VISUELLE (capture d'ecran) du rendu et de la visibilite
effective de l'avertissement dans le navigateur NON effectuee par Claude
Code** — extension Chrome indisponible sur cette session comme sur toutes
les precedentes du projet. Les verifications ci-dessus (donnees reelles
via l'API, structure DOM/CSS sans classe `hidden`/regle `display:none` sur
l'avertissement) constituent une preuve forte mais indirecte que
l'avertissement s'affiche bien en permanence — **a confirmer visuellement
par Moulaye sur http://localhost:18000, section "Nutrition"**, avant de
considerer cette contrainte (explicitement non negociable) entierement
verifiee.

### Prochaine action

Sous-etape 3/6 (preparation des donnees ML) traitee — voir ci-dessous.

## Sous-etape 3/6 — Preparation des donnees ML (pas d'entrainement) — ✅ fait

**Date** : 2026-07-09 (meme jour que les sous-etapes 1/6 et 2/6).

**Perimetre explicitement borne** : agregation hebdomadaire + feature
engineering + split train/test + documentation. **Aucun entrainement de
modele a ce stade** (sous-etape ulterieure). Voir
[data/ml/ML_DATA_PREP.md](./data/ml/ML_DATA_PREP.md) pour le detail
complet (grain, formules, chiffres reels, verification anti-fuite).

### ⚠️ Rappel du cadre (deja tranche, non renegocie ici)

Bloc 4 deja valide par GeoPort Intelligence — ce ML est un **BONUS** qui
raffine `risk_score`, **pas une exigence de certification**. Objectif :
predire le `risk_score` de la SEMAINE SUIVANTE par (utilisateur, zone),
jamais reapprendre la formule deterministe actuelle de
`fact_risk_score.sql`.

### Livre

- **`scripts/prepare_ml_features.py`** : script standalone (pas un modele
  dbt), lit `gold.fact_risk_score` via psycopg2 (curseur brut, pas
  `pandas.read_sql`), agrege par `(user_id, muscle_group,
  week_start_date)`, construit les lags calendaires exacts
  (`get_value_at_offset`, lookup pandas `MultiIndex`) et la cible, filtre
  au jeu labelise, effectue le split temporel, ecrit 3 fichiers Parquet.
- **`data/ml/weekly_features_full.parquet`** (814 lignes, toutes les
  agregations hebdomadaires y compris celles sans lag/cible — transparence
  totale), **`data/ml/train.parquet`** (491 lignes) et
  **`data/ml/test.parquet`** (161 lignes).
- **`data/ml/ML_DATA_PREP.md`** : documentation complete (grain, features,
  cible, chiffres reels, limite mono-utilisateur, ecart de 8 ans,
  verification anti-fuite, apercu des fichiers produits).
- **`.gitignore`** : `data/ml/*` ignore, exceptions `!data/ml/.gitkeep` et
  `!data/ml/ML_DATA_PREP.md` (meme pattern que bronze/silver/gold).

### Verifications effectuees (toutes reelles, aucun dry-run)

1. **Execution reelle du script** (dans `airflow-webserver`, dependances
   deja presentes — pandas/pyarrow/psycopg2, aucune nouvelle dependance
   installee) :
   ```
   [prepare_ml_features] 814 lignes (user, zone, semaine) agregees depuis gold.fact_risk_score.
   [prepare_ml_features] Utilisateurs distincts : 1 -- Zones distinctes : 8 -- Plage de semaines : 2015-10-19 -> 2026-07-06
   [prepare_ml_features] Lignes avec lag_1 disponible : 652/814 -- Lignes avec cible disponible : 652/814
   [prepare_ml_features] Jeu labelise (cible connue) : 652 lignes sur 814 (162 exclues, pas de semaine suivante connue).
   [prepare_ml_features] Split temporel : 134 semaines distinctes labelisees, 27 en test (>= 2018-03-19), 107 en train.
   [prepare_ml_features] Train : 491 lignes -- Test : 161 lignes
   ```
2. **Aucun chevauchement train/test** : `max(train.week_start_date) =
   2018-03-12` < `min(test.week_start_date) = 2018-03-19` (verifie
   programmatiquement, pas seulement par construction du code).
3. **Verification manuelle d'une ligne precise** (`arms`, semaine
   2016-01-18) contre une requete independante sur les donnees brutes :
   `lag_1_risk_score` attendu `NULL` (aucune ligne a 2016-01-11) —
   confirme ; `target_next_week_risk_score` attendu = valeur reelle a
   2016-01-25 (`5.06`) — confirme exact.
4. **Les 2 lignes orphelines `week_start_date=2026-07-06`** (issues des
   tests reels du formulaire "Logger une seance", Jalon 2) confirmees
   `NULL` sur `lag_1`/`target` dans `weekly_features_full.parquet` et
   **absentes** de `train.parquet`/`test.parquet` — exclusion naturelle
   du lookup calendaire exact, aucun filtre special-case ecrit pour ce
   cas precis.
5. **Correction d'un avertissement pandas** (`pd.read_sql` avec connexion
   psycopg2 brute) : refactorise en curseur + `fetchall()` manuel,
   re-execute apres correction, **chiffres numeriques identiques** (814
   lignes, memes plages de dates, memes comptes lag/cible) — confirme que
   le correctif etait purement cosmetique.
6. **Apercu des fichiers Parquet produits** (premieres lignes de
   `train.parquet`/`test.parquet`) : colonnes attendues presentes
   (`user_id`, `muscle_group`, `week_start_date`, `risk_score_avg`,
   4 facteurs moyens, `session_count`, 3 lags, `trend_vs_previous_week`,
   `target_next_week_risk_score`), motif `NaN` attendu en debut de serie
   par zone (moins de 1/2/3 semaines precedentes disponibles) confirme
   visuellement.

### Limite assumee

**Jeu de donnees MONO-UTILISATEUR** (`user_id=9` uniquement) — aucune
generalisation inter-utilisateurs possible, a rappeler explicitement a la
prochaine sous-etape (entrainement) comme contrainte structurante, pas a
minimiser. Volume tres petit pour du ML (491 lignes train / 161 lignes
test, 8 zones tres inegalement representees, `legs` = 18 lignes labelisees
au total) — quantifie honnetement dans `ML_DATA_PREP.md`, pas dissimule.
Ecart de ~8 ans dans l'historique (2018-09-24 -> 2026-07-06) constate et
documente, pas comble artificiellement.

### Prochaine action

Sous-etape 4/6 (entrainement + evaluation du modele ML) traitee — voir
ci-dessous.

## Sous-etape 4/6 — Entrainement + evaluation du modele ML — ✅ fait

**Date** : 2026-07-09 (meme jour que les sous-etapes 1/6, 2/6, 3/6).

**Perimetre explicitement borne** : entrainement + evaluation
UNIQUEMENT, sur `train.parquet`/`test.parquet` deja produits en
sous-etape 3/6. Aucune integration au dashboard/API a ce stade. Voir
[data/ml/ML_TRAINING_RESULTS.md](./data/ml/ML_TRAINING_RESULTS.md) pour
le detail complet (tableau comparatif, importances, conclusion honnete).

### ⚠️ Rappel du cadre (deja tranche, non renegocie ici)

**Decision deja actee** : UN SEUL modele poole sur les 8 zones
musculaires, `muscle_group` en feature categorique (one-hot) — PAS 8
modeles independants. `legs` (18 lignes labelisees au total) et `unknown`
(50, categorie fourre-tout non anatomique) sont structurellement trop
petits pour un entrainement par zone (voir `ML_DATA_PREP.md` section 6,
verdict de viabilite deja etabli).

### Livre

- **`scikit-learn==1.5.2` ajoute a `airflow/requirements.txt`** — image
  `safelift-airflow:local` reconstruite (`docker compose build
  airflow-init`, PAS `airflow-webserver` : c'est `airflow-init` qui
  possede le `build: ./airflow` du Dockerfile, `airflow-webserver`/
  `airflow-scheduler` ne font que referencer l'image par tag — une
  premiere tentative de `docker compose build airflow-webserver` a
  echoue silencieusement, exit 0 sans rien reconstruire, bug constate en
  verifiant la date de creation de l'image restee inchangee). Conteneurs
  `airflow-webserver`/`airflow-scheduler` recrees (`docker compose up -d
  --no-deps ...`) pour utiliser la nouvelle image ; import
  `sklearn`/`joblib` verifie reellement dans le conteneur avant de lancer
  le script.
- **`scripts/train_risk_trend_model.py`** : charge `train.parquet`/
  `test.parquet`, impute les NULL des lags par 0 (documente, pas de
  suppression de lignes), encode `muscle_group` en one-hot via un
  `ColumnTransformer` scikit-learn (encapsule dans le pipeline
  sauvegarde, pas de logique d'encodage dupliquee cote consommateur
  futur), entraine et compare 3 approches sur le TEST set uniquement :
  - **Baseline naive** : `risk_score_avg` (semaine courante) predit tel
    quel comme valeur de la semaine suivante.
  - **Ridge** (`alpha=1.0`, `random_state=42`).
  - **RandomForest** (`max_depth=4`, `n_estimators=100`,
    `random_state=42`) — profondeur volontairement limitee vu le petit
    volume (491 lignes train), pour eviter le sur-apprentissage.
  - Aucun hyperparametre cherche par validation croisee sur le test set
    (volume trop faible pour un split train/validation supplementaire
    fiable) : valeurs par defaut fixees et documentees dans le code AVANT
    toute evaluation.
- **`data/ml/model.pkl`** (joblib, 230 Ko) : pipeline scikit-learn complet
  (preprocessing one-hot + `RandomForestRegressor` retenu) + metadonnees
  (date d'entrainement, features, tailles train/test, les 3 jeux de
  metriques, `beats_naive_baseline`). **`data/ml/training_metrics.json`** :
  copie complete et lisible des memes informations (metriques + coefficients
  Ridge + importances RandomForest), pour inspection sans desserialiser le
  `.pkl`. Les deux fichiers restent NON versionnes (`data/ml/*` deja
  ignore, artefacts regenerables par re-execution du script).
- **`data/ml/ML_TRAINING_RESULTS.md`** (nouveau, exception versionnee du
  `.gitignore`) : documentation complete (tableau comparatif, imputation,
  analyse d'importance des features des DEUX modeles, modele retenu et
  pourquoi, conclusion honnete sur la valeur ajoutee reelle du ML ici).

### Verifications effectuees (toutes reelles, aucun dry-run)

1. **Execution reelle du script** dans `airflow-webserver` (apres
   reconstruction/verification de l'image) :
   ```
   [train_risk_trend_model] Train : 491 lignes -- Test : 161 lignes
   [train_risk_trend_model] Evaluation de la baseline naive...
     -> RMSE=14.6866  MAE=11.5016
   [train_risk_trend_model] Entrainement du modele lineaire (Ridge)...
     -> RMSE=9.2689  MAE=7.5479
   [train_risk_trend_model] Entrainement du modele arbre (RandomForest, max_depth=4)...
     -> RMSE=9.1152  MAE=7.5362
   [train_risk_trend_model] Meilleur modele ML (RMSE test le plus bas) : random_forest (RMSE=9.1152)
   [train_risk_trend_model] Bat la baseline naive ? OUI (baseline RMSE=14.6866)
   ```
2. **Les deux modeles ML battent nettement la baseline naive** (RMSE
   reduit de ~37-38%, MAE reduit de ~34%) — resultat obtenu AVANT toute
   comparaison/ajustement, rapporte tel quel (le script aurait rapporte
   l'inverse a l'identique si la baseline avait gagne, aucune tentative
   de "forcer" un meilleur chiffre en modifiant les hyperparametres apres
   coup).
3. **Analyse d'importance des features confirmee sensee, pas du bruit** :
   `lag_1_risk_score` (0.2563) + `lag_2_risk_score` (0.2004) = 45.7% de
   l'importance RandomForest — le modele retenu s'appuie principalement
   sur l'historique recent, coherent avec l'objectif de predire une
   tendance. **Constat different et documente honnetement pour Ridge** :
   ses plus gros coefficients sont les dummies `muscle_group` (ex.
   `shoulder` +6.61, `chest` -5.33), pas les lags (`lag_1_risk_score`
   seulement +0.099) — Ridge capture surtout le niveau de risque de base
   propre a chaque zone plutot qu'une vraie dynamique temporelle.
4. **`duree_factor_avg` : coefficient ET importance a 0 dans les DEUX
   modeles**, confirme coherent (pas un bug) avec le constat deja
   documente en Jalon 1 : `duration_seconds` quasi toujours 0 sur ce
   dataset, quasi aucune variance a exploiter pour ce facteur.
5. **Fichiers de sortie confirmes presents et coherents** :
   `data/ml/model.pkl` (230 Ko) et `data/ml/training_metrics.json` (les
   memes 3 jeux de metriques que les logs, coefficients/importances
   complets pour les 18 features, metadonnees horodatees) verifies sur
   le systeme de fichiers hote via le bind mount `./data`.

### Bug reel rencontre et corrige pendant les tests

- **`docker compose build airflow-webserver` echoue silencieusement (exit
  0, aucune sortie, image non reconstruite)** : ce service ne declare pas
  de `build:` dans `docker-compose.yml`, seulement `image:
  safelift-airflow:local` (reference a l'image deja construite par
  `airflow-init`, qui possede seul le `build: ./airflow`). Constate en
  verifiant que la date de creation de l'image (`docker inspect
  --format '{{.Created}}'`) restait a une session precedente (2026-07-03)
  malgre un exit code 0. Corrige en ciblant `docker compose build
  airflow-init` (le bon service), puis en recreant explicitement
  `airflow-webserver`/`airflow-scheduler` (`docker compose up -d
  --no-deps ...`) pour qu'ils repartent sur la nouvelle image — sinon les
  conteneurs deja demarres continuent de tourner sur l'ancienne image en
  memoire meme apres un rebuild reussi de l'image sous-jacente.

### Limite assumee

Memes limites structurantes deja quantifiees en sous-etape 3/6
(mono-utilisateur, volume test de 161 lignes, ecart de 8 ans dans
l'historique) — voir `ML_DATA_PREP.md`. Limite supplementaire propre a
cette sous-etape : **l'ecart RMSE entre Ridge et RandomForest est modeste
(9.27 vs 9.12)**, le choix de RandomForest est defendable (RMSE
legerement meilleur + signal d'importance plus aligne sur l'objectif de
tendance) mais ne doit pas etre presente comme une victoire ecrasante
d'une approche sur l'autre. Le modele reste une **preuve de concept**,
pas un remplacement de la formule deterministe `risk_score` utilisee par
le dashboard (aucune integration prevue a ce stade — hors perimetre de
cette sous-etape).

### Prochaine action

Sous-etape 5/6 (integration pipeline + dashboard de la prediction ML)
traitee — voir ci-dessous.

## Sous-etape 5/6 — Integration pipeline + dashboard de la prediction ML — ✅ fait

**Date** : 2026-07-09 (meme jour que les sous-etapes 1/6 a 4/6).

**Perimetre explicitement borne** : scoring batch + orchestration Airflow +
endpoint API + encart dashboard UNIQUEMENT. Le modele lui-meme
(`data/ml/model.pkl`) n'est pas reentraine ici (sous-etape 4/6, deja faite).

### ⚠️ Rappel du cadre (deja tranche, non renegocie ici)

**Decision deja actee** : la prediction ML est un encart CLAIREMENT
DISTINCT du risk_score deterministe existant — jamais fusionne
visuellement, toujours accompagne d'un rappel de ses limites
(mono-utilisateur, preuve de concept, ne remplace pas la formule
deterministe).

### Livre

- **`scripts/score_risk_trend.py`** : charge `data/ml/model.pkl`, REUTILISE
  (importe, ne duplique jamais) `fetch_weekly_aggregates`/`build_features`
  de `prepare_ml_features.py` et `impute_lag_nulls`/`NUMERIC_FEATURES`/
  `CATEGORICAL_FEATURES` de `train_risk_trend_model.py`. Pour chaque
  `(user_id, muscle_group)`, garde la ligne de la semaine la PLUS RECENTE
  deja observee (`latest_rows_per_zone`) et predit la semaine suivante.
  Ecrit `gold.ml_risk_prediction` (table creee directement en psycopg2,
  cle primaire `(user_id, muscle_group)`, `TRUNCATE`+reinsertion complete a
  chaque execution — etat courant, pas un historique preserve).
  Colonnes : `user_id`, `muscle_group`, `week_predicted_for`,
  `predicted_risk_score`, `based_on_week` (semaine source, tracabilite),
  `model_version`, `model_trained_at`, `scored_at`.
- **`airflow/dags/ml_scoring.py`** (nouveau DAG, 1 task
  `score_risk_trend`) : `schedule=None`, declenche par `gold_dbt_run`.
- **`airflow/dags/gold_dbt_run.py`** modifie : `trigger_ml_scoring`
  (`TriggerDagRunOperator`) ajoute apres `dbt_test`, meme mecanisme que le
  reste de la cascade (`bronze_ingestion -> silver_transformation ->
  gold_dbt_run`).
- **`GET /users/{user_id}/risk/prediction`** (`dashboard/main.py`) :
  lecture seule de `gold.ml_risk_prediction`, aucun calcul. 2 cas
  "non disponible" distingues explicitement (table absente vs table
  presente sans ligne pour cet utilisateur), toujours `disclaimer` present
  (`ML_PREDICTION_DISCLAIMER`), jamais de 500.
- **Dashboard** (`index.html`/`dashboard.js`/`dashboard.css`) : encart
  "Tendance prédictive" dans la section risque existante (`side-panel`,
  apres le panneau `history-panel-wrapper`), bordure pointillee violette +
  badge "EXPERIMENTAL" (jamais fusionne visuellement avec le
  risk_score deterministe), rappel de limite toujours affiche
  (`data.disclaimer`), message explicite "Non disponible pour ce profil —
  {reason}" si aucune prediction, desactive en mode demo (meme pattern que
  simulateur what-if/nutrition).

### Verifications effectuees (toutes reelles, aucun dry-run)

1. **`scripts/score_risk_trend.py` execute isolement** (avant integration
   Airflow) : `8 ligne(s) a scorer, pour 1 utilisateur(s) distinct(s)`,
   `8 prediction(s) ecrite(s) dans gold.ml_risk_prediction` — confirme par
   requete SQL directe (8 lignes, toutes `user_id=9`, une par zone).
2. **Test end-to-end REEL via la cascade Airflow complete** (pas juste le
   script isole) : `airflow dags trigger gold_dbt_run` -> les 10 tasks
   (`load_silver_to_postgres`, `dbt_seed`, `dbt_run_staging`,
   `fuzzy_match_exercises`, `dbt_run`, `dbt_test`, `trigger_ml_scoring`)
   confirmees `success` via `airflow tasks states-for-dag-run` ; run
   `ml_scoring` (`manual__2026-07-09T23:22:17...`) confirme `success` en
   ~1.6s (23:22:18 -> 23:22:19) ; **8 lignes fraiches confirmees en base**
   (`scored_at = 2026-07-09 23:22:19`, identique pour les 8 lignes —
   preuve d'un seul run coherent, pas de melange d'executions).
3. **⚠️ Bug reel rencontre et corrige (pas un echec du script, un delai
   d'enregistrement Airflow normal)** : `airflow dags unpause ml_scoring`
   a d'abord echoue avec `No paused DAGs were found` — le DAG venait
   d'etre ajoute au dossier `dags/` et le scheduler ne l'avait pas encore
   serialise dans la table `dag` (`airflow dags details ml_scoring` :
   `does not exist in 'dag' table`). Resolu avec
   `airflow dags reserialize` (force la synchronisation immediate) puis
   `airflow dags unpause ml_scoring` a reussi. Rappel operationnel deja
   documente (CLAUDE.md, "Comment reprendre une session") reste valable :
   tout nouveau DAG doit etre depause explicitement — ce cas ajoute une
   nuance : si `unpause` echoue juste apres la creation du fichier, il
   peut falloir laisser le scheduler le temps de le parser (ou forcer avec
   `airflow dags reserialize`) avant de reessayer.
4. **Endpoint teste sur 3 cas reels** :
   - `GET /users/9/risk/prediction` -> `available:true`, 8 predictions
     (toutes les zones), `disclaimer` present.
   - `GET /users/4/risk/prediction` (profil sans donnee de seance) ->
     `available:false`, `reason` explicite ("Pas assez d'historique reel
     pour ce profil..."), **PAS d'erreur**.
   - `GET /users/999999/risk/prediction` (utilisateur inexistant) -> HTTP
     `404` propre (`user_id 999999 introuvable`), pas de 500.
5. **Frontend verifie structurellement** (extension Chrome indisponible,
   meme limite que le reste du dashboard) : `node --check dashboard.js`
   (syntaxe valide), balises `section`/`div` equilibrees (7/7, 39/39 apres
   ajout du nouvel encart), accolades CSS equilibrees (128/128), tous les
   nouveaux `id` (`ml-prediction-unavailable`, `ml-prediction-content`,
   `ml-prediction-disclaimer`, `ml-prediction-list`) references par
   `dashboard.js` confirmes presents dans `index.html`. Contenu confirme
   reellement servi par le conteneur reconstruit (`curl` sur `/`,
   `/static/dashboard.js`, `/static/dashboard.css`).

### Limite assumee

**Constat honnete non masque** : les predictions des zones `arms`/`back`
s'appuient sur la semaine la plus recente disponible pour ces zones, qui se
trouve etre un point isole (`week_start_date=2026-07-06`, issu des tests
reels du formulaire temps reel de Jalon 2) sans aucun contexte de lags
reels (`lag_1`/`lag_2`/`lag_3` tous imputes a 0, faute de semaine
calendaire adjacente dans l'historique — voir `ML_DATA_PREP.md` section 5
pour le detail de cet ecart de ~8 ans). La prediction est techniquement
produite (le pipeline ne plante pas, aucune erreur) mais structurellement
MOINS FIABLE que les 6 autres zones, ancrees sur l'historique dense reel de
2018. Ce n'est pas corrige par un filtre special-case (qui masquerait le
comportement reel du modele) : le champ `based_on_week` reste visible tel
quel dans l'API et le dashboard, permettant a un observateur attentif de
voir la difference (2026 vs 2018).

**Verification visuelle du rendu (bordure pointillee, badge EXPERIMENTAL,
lisibilite de l'encart) NON effectuee dans un navigateur reel** — meme
limite deja documentee pour tout le reste du dashboard depuis le debut du
projet (extension Chrome indisponible sur toutes les sessions a ce jour).

### Prochaine action

Sous-etape 6/6 (refonte UX/UI par onglets) traitee — voir ci-dessous.

## Sous-etape 6/6 — Refonte UX/UI par onglets + corrections de débordement — ✅ fait

**Date** : 2026-07-09 (meme jour que les sous-etapes 1/6 a 5/6).

**Perimetre explicitement borne** : reorganisation VISUELLE et ergonomique
de l'existant UNIQUEMENT — aucune nouvelle fonctionnalite, aucun nouveau
backend. Demande initiale : le dashboard etait devenu dense (tout empile
verticalement sur une seule page) avec des debordements de texte constates
visuellement par Moulaye (capture jointe).

### Livre

- **Navigation par 3 onglets** (SPA-style, changement de vue 100% en JS,
  aucun rechargement de page, aucune route serveur separee) :
  - **"Risque & Entraînement"** : silhouette + panneau de detail, KPIs,
    zones sensibles, simulateur what-if, "Logger une seance", "Tendance
    predictive" (ML). (Le simulateur what-if n'etait pas explicitement
    assigne a un onglet dans la demande — regroupe ici, seul endroit
    logique, pour ne perdre aucune fonctionnalite existante.)
  - **"Affluence"** : cards de salles SSE + recommandation de creneau.
  - **"Nutrition"** : TDEE/BMR, jauge proteique, aliments suggeres.
  - Implementation : `.tab-panel { display:none; }` /
    `.tab-panel.active { display:flex; ...; animation: tab-fade-in 0.2s; }`
    — changement de vue PUREMENT par toggle de classes CSS, **aucun
    element jamais retire du DOM**. `switchTab()` (dashboard.js) ne touche
    QUE `classList` des boutons/panneaux, jamais l'etat JS des
    fonctionnalites sous-jacentes (SSE, polling, etc.).
  - Selecteur d'utilisateur + toggle demo regroupes avec le bandeau demo
    et la nav d'onglets dans un wrapper unique `.app-header-sticky`
    (`position: sticky; top:0;`) — reste visible en permanence quel que
    soit l'onglet actif et le defilement.
- **Corrections de debordement de texte**, deux familles de correctifs
  distincts selon le type d'element :
  - **Elements HTML normaux** (span/div/h3) : `overflow:hidden;
    text-overflow:ellipsis; white-space:nowrap;` + attribut `title` (tooltip
    natif au survol) sur : `.kpi-value`/`.kpi-sub` (derniere seance),
    `.zone-name` (zones sensibles), `.nutrition-food-name` (aliments),
    `.ml-prediction-zone` (tendance predictive), `.gym-card h3` (salles).
    **Piege flexbox/grid corrige explicitement** (`min-width:0` ajoute sur
    `.kpi-card`, `.zone-name`, `.gym-card`, `.nutrition-food-item`,
    `.ml-prediction-info`) : un item flex/grid refuse par defaut de
    retrecir sous la largeur intrinseque de son texte, ce qui aurait
    silencieusement desactive l'ellipsis meme avec la regle CSS en place.
  - **`<select>` natifs** (exercices, scenarios demo) : CSS
    `text-overflow:ellipsis` seul est **peu fiable sur l'etat FERME d'un
    select natif** selon les navigateurs (souvent une coupure brutale
    SANS "…"). Correctif principal cote JS : `truncateForSelect()` coupe
    le texte AVANT de l'inserer comme `<option>`, texte complet toujours
    disponible via l'attribut `title` (liste ouverte ET boite fermee,
    cette derniere synchronisee par `updateSelectTitle()` sur l'evenement
    `change`, attache UNE SEULE FOIS dans `init()` pour ne jamais empiler
    de listeners dupliques sur un select reutilise entre changements
    d'utilisateur).
- **Harmonisation des espacements** : `margin-top: 20px` redondant retire
  de `.log-session-panel`/`.occupancy-panel`/`.nutrition-panel` — ces
  panneaux cumulaient cette marge AVEC le `gap` du conteneur flex parent,
  creant un ecart ~44px la ou les autres panneaux (ex.
  `.simulator-panel`, jamais eu de `margin-top`) n'avaient que 24px.
  Desormais `.tab-panel.active { gap: 24px }` gere l'espacement de
  maniere uniforme pour tous les panneaux d'un meme onglet.
- **Hierarchie typographique verifiee** : `h2` (titres de section, 1rem/600)
  vs `h3` (sous-titres de card, 0.95rem) coherents partout, aucune
  correction necessaire (deja homogene avant cette passe).
- **Transition douce** au changement d'onglet : `@keyframes tab-fade-in`
  (opacity 0->1, 0.2s ease) applique via l'animation CSS declenchee par
  l'ajout de la classe `.active` — pas de `transition` classique sur
  `display` (qui ne s'anime pas nativement), contournement volontaire via
  animation CSS plutot que par un jeu `opacity`+`setTimeout` en JS.

### Verifications effectuees (toutes reelles, aucun dry-run)

1. **Bug reel trouve et corrige en verifiant l'equilibre des balises** :
   un premier controle automatique (comptage `<div>`/`</div>`) a signale
   44 ouvrantes / 43 fermantes — **faux positif cause par le propre
   commentaire HTML de cette passe**, qui citait litteralement
   `<div class="tab-panel">` en exemple dans son texte explicatif (capture
   par le regex de verification comme une vraie balise). Corrige en
   reformulant le commentaire pour ne plus contenir de balise litterale.
   Recompte final : 43/43 (`<div>`/`</div>`), 7/7 (`<section>`), 1/1
   (`<nav>`), 5/5 (`<button>`).
2. **Tous les `getElementById(...)` de `dashboard.js` resolus** (verifie
   programmatiquement) : 55 references, 0 manquante parmi les 67 `id`
   presents dans `index.html`.
3. **Seuil de troncature des `<select>` calibre sur des donnees reelles,
   pas suppose** : un premier seuil de 55 caracteres ne declenchait
   JAMAIS sur les 81 exercices reels de `user_id=9` (le plus long,
   "Seated Military Press (Dumbbell) (Épaules / deltoïdes)", fait 54
   caracteres) — alors que cette chaine deborde deja visuellement d'une
   colonne de 320px a ~0.9rem (largeur en PIXELS, pas en nombre de
   caracteres). Seuil resserre a 42 caracteres : **25 des 81 labels reels
   sont desormais effectivement tronques**, verifie par un script Node
   independant interrogeant `GET /users/9/exercises`.
4. **Connexion SSE confirmee vivante independamment du frontend** :
   `curl -N --max-time 35 http://localhost:18000/gyms/occupancy/stream`
   -> **5 evenements reels recus** sur la fenetre de 35s (coherent avec
   le rythme d'emission du simulateur, 5-10s/salle + deduplication cote
   serveur). Preuve COMPLEMENTAIRE (pas suffisante seule) a la preuve par
   construction : `occupancyEventSource` (variable JS) n'est reference
   QUE dans `connectOccupancyStream()` (appelee UNE SEULE FOIS dans
   `init()`) — confirme par recherche exhaustive dans `dashboard.js`,
   `switchTab()` ne touche JAMAIS cette variable ni n'appelle
   `.close()`/ne recree jamais l'`EventSource`. Le changement d'onglet
   est un pur toggle de classes CSS sur des elements qui restent presents
   dans le DOM — rien ne peut donc interrompre le flux SSE en changeant
   d'onglet, par construction du code.
5. **Non-regression confirmee** : les 12 services restent `healthy` apres
   reconstruction/redemarrage du conteneur `dashboard` (2 rebuilds : un
   premier avec la restructuration, un second apres resserrement du
   seuil de troncature). Contenu reellement servi verifie via `curl` sur
   `/` (les 3 `data-tab`/`data-tab-panel` presents), `/static/dashboard.js`
   et `/static/dashboard.css` (nouvelles regles/fonctions confirmees
   presentes).
6. **Absence de chevauchement a 1920x1080/1366x768 verifiee par le calcul
   de mise en page CSS, PAS par un navigateur reel** (extension Chrome
   indisponible, meme limite que le reste du projet) : le conteneur
   principal (`max-width:1200px` + `padding:32px`, soit 1264px) est
   inferieur aux deux largeurs cibles -> contenu toujours centre avec
   marges, aucun risque de debordement horizontal duplique par le
   viewport. Hauteur du wrapper sticky (bandeau+header+nav) estimee a
   ~131-171px selon que le bandeau demo est visible, tres inferieure aux
   768px de hauteur minimale visee -> le reste de la page defile
   normalement sous ce bandeau, aucun chevauchement possible (aucun
   positionnement absolu utilise ailleurs dans le CSS, uniquement
   Grid/Flexbox + SVG a coordonnees relatives au viewBox, donc
   independantes de la taille d'ecran reelle).

### Limite assumee

**Verification VISUELLE reelle (captures d'ecran des 3 onglets, rendu
effectif des transitions/de la mise en page a l'ecran) NON effectuee dans
un navigateur** — extension Chrome indisponible sur cette session comme
sur toutes les precedentes du projet (meme limite documentee depuis
l'etape 5 du Jalon 1). Les verifications ci-dessus (structure DOM/CSS/JS
bien formee, calcul de mise en page, tests reseau reels sur le flux SSE,
calibration du seuil de troncature sur des donnees reelles) constituent
une preuve forte mais indirecte — **la confirmation visuelle finale
(esthetique des onglets, lisibilite effective, absence de chevauchement
constate a l'oeil) reste a faire par Moulaye sur
http://localhost:18000**, notamment en comparant a la capture qui a
motive cette passe.

### Prochaine action

Aucune sous-etape supplementaire definie pour le Jalon 3 a ce stade (6/6
sous-etapes traitees) — ne pas anticiper la suite sans demande explicite.

## Passe de style "holographique/neon" (post-sous-étape 6/6) — ✅ fait

**Date** : 2026-07-09 (meme jour que le reste du Jalon 3).

**Perimetre explicitement borne** : passe VISUELLE pure sur le dashboard
deja reorganise en onglets (sous-etape 6/6) — aucune nouvelle
fonctionnalite, aucun changement backend. Direction demandee : garder le
theme sombre existant, ajouter une ambiance "futuriste/holographique"
(lueurs neon cyan/turquoise, liseres lumineux) + langage de card epure
"gros chiffre + label discret", en restant strictement en SVG/CSS 2D (pas
de 3D/particules — coherent avec la consigne initiale du projet).

### Livre

- **Couleur de marque cyan/turquoise** : nouvelle variable `--accent-cyan`
  (`#2fe0e8`, distincte de `--accent-teal` deja utilise ailleurs, pour ne
  rien casser d'existant) + `--accent-cyan-soft`/`--accent-cyan-glow`
  (variantes rgba pour fonds/lueurs). Reservee aux elements NEUTRES (nav
  d'onglets active, bordures de card sans code couleur de risque propre,
  chiffres "hero" generiques) — jamais utilisee pour les codes couleur de
  risque existants (Faible/Modere/Eleve, inchanges).
- **Lueur holographique sur la silhouette** : `colorZones()` (dashboard.js)
  injecte desormais une variable CSS `--zone-glow-color` (identique a la
  couleur de remplissage, `transparent` si pas de donnee) plutot qu'un
  `style.filter` direct — le VRAI effet `filter:drop-shadow(...)` reste
  defini en CSS (`.zone`), ce qui permet aux effets `:hover`/`.selected`
  (lueur blanche existante) de s'ADDITIONNER a la lueur de couleur au lieu
  de l'ecraser (un `style.filter` direct depuis JS aurait eu une
  specificite superieure a toute regle CSS de classe, cassant les etats
  hover/selection). Meme mecanisme applique a l'anneau radial du score
  global (`renderGauge()` -> `--gauge-glow`).
- **Silhouette enrichie (sans toucher aux courbes existantes, deja
  eprouvees)** : 2 nouvelles zones "trapezes" ajoutees (mappees sur
  `data-muscle="shoulder"`, MEME muscle_group que les deltoides — aucune
  nouvelle donnee necessaire) a la jonction cou/epaules, + 2 lignes
  decoratives de clavicules (non cliquables). Total de zones cliquables
  passe de 16 a 18. Aucune modification des courbes de contour du corps ni
  des zones existantes (chest/abs/legs/knee/calves/back/lower_back
  inchangees) : risque de distordre une geometrie deja validee juge trop
  eleve sans verification visuelle possible.
- **Grille de scan en arriere-plan de la carte corporelle** (bonus demande
  optionnel) : degrade radial cyan + grille `repeating-linear-gradient`
  tres faible opacite (0.05) sur `.bodymap-panel`, purement decoratif,
  jamais superpose a du texte.
- **Langage "gros chiffre + label discret" generalise** via une paire de
  classes CSS reutilisables (`.hero-stat-value`/`.hero-stat-unit`),
  appliquee a : TDEE (nutrition, chiffre principal, BMR relegue en
  ligne secondaire sous un separateur), besoin proteique (nutrition),
  pourcentage d'occupation (affluence, couleur/lueur suivant la
  categorie de charge), score de zone (`scoreBadge()` remplace l'ancien
  badge "score — niveau" par un vrai chiffre "hero" + pastille de niveau
  discrete a cote).
- **⚠️ Bug reel trouve et corrige pendant cette passe (pas dans le nouveau
  code, dans l'existant depuis la sous-etape 5/6)** : le selecteur partage
  `.bodymap-panel, .side-panel > div` (specificite CSS (0,1,1) : 1 classe
  + 1 element) etait TOUJOURS plus specifique que `.ml-prediction-panel`
  seul (specificite (0,1,0)). Ce panneau etant un `div` enfant direct de
  `.side-panel`, il correspond aux deux selecteurs — et en CSS c'est la
  specificite qui tranche, jamais l'ordre d'apparition dans le fichier
  quand les specificites different. **Consequence : la bordure pointillee
  violette et l'ombre distinctive de l'encart "Tendance prédictive"
  n'avaient JAMAIS reellement rendu depuis leur creation** (sous-etape
  5/6) — le panneau affichait silencieusement le style par defaut partage
  avec les autres panneaux de `.side-panel`, jamais detecte faute de
  verification visuelle reelle dans un navigateur. Corrige en utilisant
  un selecteur combine ID+classe (`#ml-prediction-panel.ml-prediction-panel`),
  qui gagne face a n'importe quelle combinaison de classes seules.
- **Anti-debordement verifie et RENFORCE, pas seulement preserve** : en
  concevant le pattern "gros chiffre", un risque de regression reel a ete
  identifie et corrige AVANT mise en production (pas apres coup) — une
  chaine combinee "2446 kcal/jour" a `font-size:2rem` depasserait la
  largeur utile d'une card de 260px. Corrige en separant systematiquement
  le CHIFFRE (grande taille, `.hero-stat-value`) de son UNITE (petite
  taille discrete, `.hero-stat-unit`, span imbrique) — seul le nombre
  (toujours court, 3-5 caracteres) beneficie du traitement "hero", jamais
  une chaine combinee longue.
- **Lisibilite verifiee par construction** : tous les effets `text-shadow`
  ajoutes sont des halos flous DERRIERE un glyphe reste 100% opaque au
  premier plan (technique standard "neon text" CSS) — ne floute jamais le
  texte lui-meme, contrairement a un `filter:blur()` qui affecterait le
  glyphe entier. Blur radius volontairement modeste (12-18px) pour rester
  un liseret discret, pas un halo qui mangerait le contraste.

### Verifications effectuees (toutes reelles, aucun dry-run)

1. **SVG re-verifie bien forme** apres ajout des trapezes/clavicules
   (parseur XML, commentaires HTML retires au prealable — ceux-ci
   contiennent legitimement des "--" internes, valides en HTML mais pas en
   XML strict, meme convention deja utilisee dans tout le reste du
   fichier) : 18 zones `.zone` confirmees, reparties sur les 9
   `muscle_group` valides existants (aucun nouveau muscle_group invente).
2. **Tags HTML/CSS re-verifies equilibres** apres la passe : `<div>` 42/42,
   `<section>` 7/7, `<nav>` 1/1, `<button>` 5/5, accolades CSS 147/147.
3. **Tous les `getElementById(...)` de `dashboard.js` resolus** apres la
   restructuration des cards nutrition/affluence (aucun `id` renomme,
   uniquement la structure/le style autour ont change).
4. **Non-regression backend confirmee reellement** (cette passe est
   frontend pur, mais verifie explicitement plutot que suppose) :
   `/health`, `/users/9/risk`, `/users/9/nutrition`,
   `/users/9/risk/prediction`, `/users`, et `POST /api/simulate-risk`
   tous testes en conditions reelles apres reconstruction du conteneur —
   reponses identiques en substance a avant la passe de style.
5. **SSE affluence re-confirmee vivante apres la passe** :
   `curl -N --max-time 35` sur `/gyms/occupancy/stream` -> 5 evenements
   reels recus (identique au resultat de la sous-etape 6/6, aucune
   regression introduite par les changements CSS/JS de cette passe).
6. **12 services confirmes `healthy`** apres reconstruction/redemarrage du
   conteneur `dashboard`.

### Limite assumee

**Verification VISUELLE reelle (captures d'ecran, rendu effectif des
lueurs/degrades/animations a l'ecran, esthetique du resultat final) NON
effectuee dans un navigateur** — extension Chrome indisponible, meme
limite documentee sur toutes les sessions precedentes du projet. Les
verifications ci-dessus (structure DOM/CSS/JS/SVG bien formee, non-
regression fonctionnelle reelle, analyse de specificite CSS ayant permis
de detecter et corriger un bug de rendu jamais visible) constituent une
preuve de correction structurelle forte, mais **la confirmation
esthetique finale (l'ambiance "holographique" rend-elle bien visuellement
comme decrit, les lueurs sont-elles equilibrees, rien ne parait-il
surcharge) reste a faire par Moulaye sur http://localhost:18000**.

## Suite : migration dashboard-v2 (React)

A partir du 2026-07-10, l'effort frontend se poursuit sur un NOUVEAU
projet `dashboard-v2/` (React, en parallele de l'ancien dashboard
ci-dessus, qui reste le filet de securite pour la soutenance du
2026-07-13). Ce n'est plus a proprement parler une sous-etape du Jalon 3
(nutrition/ML) mais une migration transverse du serving existant —
**suivi detaille dans CLAUDE.md, section "Migration dashboard-v2 (React)"**
(sous-etapes 1/N a 5/N a ce jour : scaffolding, correctif wireframe,
tous les widgets branches, selecteur allege, correctif toggle
demo + graphique tendance recharts, simulateur what-if porte). Ce
fichier-ci (`PROGRESS_JALON3.md`) reste la reference pour le travail
Jalon 3 sur l'ancien dashboard (nutrition, ML, style holographique
ci-dessus), inchange et clos.
