# PROGRESS_JALON3.md — Suivi d'avancement SafeLift, Jalon 3 (nutrition + ML bonus)

> Meme legende/regle que PROGRESS.md/PROGRESS_JALON2.md (✅ fait ·
> 🔄 en cours · ⏳ a faire, toujours a jour AVANT de considerer une
> sous-etape terminee). Voir CLAUDE.md pour le pointeur vers ce fichier et
> le contexte global du projet.

## Contexte du Jalon 3

Le Jalon 1 (pipeline batch complet) et le Jalon 2 (streaming temps reel)
sont clos. Le Jalon 3 ajoute la **nutrition** (API USDA FoodData Central)
et une **couche ML bonus** (sous-etapes suivantes, pas celle-ci).

Decoupage prevu (6 sous-etapes, seule la 1/6 est traitee a ce stade — ne
pas anticiper la suite sans demande explicite) :
1. Ingestion nutrition + dimension + calculs deterministes (ce document)
2. (a definir)
3. (a definir)
4. (a definir)
5. (a definir)
6. (a definir)

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

Sous-etape 2/6 non definie a ce stade — ne pas anticiper sans demande
explicite (meme regle que les Jalons 1 et 2).
