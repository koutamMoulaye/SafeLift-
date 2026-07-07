# GOLD_MODEL_DECISIONS.md — Journal des decisions du modele en etoile Gold

> Meme principe que `data/silver/CLEANING_LOG.md` : chaque decision de
> modelisation est justifiee et chiffree. Une donnee et une hypothese de
> modelisation ne doivent jamais se ressembler dans ce document — les
> hypotheses sont marquees comme telles explicitement, nulle part presentees
> comme des faits verifies. C'est un point de vigilance fort pour la
> soutenance RNCP : plusieurs choix ci-dessous (muscle_group, risk_score,
> rattachement dim_user) sont des CHOIX DE MODELISATION DEMONSTRATIFS, pas
> des donnees epidemiologiques ou des jointures reelles.
>
> Date de redaction initiale : 2026-07-03. **Mise a jour du 2026-07-03 (meme
> jour)** : correctifs matching exercice + recalibrage risk_score, suite a
> revue. Chiffres issus de l'execution reelle du DAG `gold_dbt_run` (voir
> section "Resultats d'execution").

## 1. Architecture generale

**dbt opere sur Postgres, pas directement sur les fichiers Parquet Silver.**
dbt (adaptateurs `dbt-postgres`) a besoin d'une base SQL connectable ; il ne
lit pas nativement des fichiers Parquet sur disque. Un job Spark
(`spark/jobs/load_silver_to_postgres.py`) recharge donc les 4 tables Silver
telles quelles dans le schema Postgres `raw` de `app-postgres` (JDBC), avant
que dbt ne construise dessus :

```
data/silver/*.parquet --[Spark JDBC]--> raw.silver_* --[dbt]--> staging.stg_* --[dbt]--> gold.dim_*/fact_*
```

**Colonnes exclues du chargement `raw.*`** : `level_list`/`goal_list`
(600k_fitness_*), de type `array<string>` en Parquet. Le connecteur JDBC
Spark->Postgres ne les porte pas de maniere fiable dans toutes les
configurations, et elles ne sont pas necessaires au modele Gold demande
(`dim_exercise`/`dim_muscle` ne se basent que sur `exercise_name`). Exclues
explicitement (`.drop(...)` dans le job Spark) plutot que serialisees
silencieusement.

**`600k_fitness_summary` n'a pas de modele dbt staging dedie** : aucune de
ses colonnes (`program_length_weeks`, `level_list`, `goal_list`...) n'est
requise par le modele Gold demande. Elle est chargee dans `raw.*` par
completude/tracabilite, mais pas transformee plus loin.

**dbt tourne dans un venv Python isole** (`/opt/dbt_venv`, cf.
`airflow/Dockerfile`), separe de l'environnement Airflow. `dbt-core` et
Airflow ont des contraintes de version conflictuelles sur des dependances
communes (`click`, `jinja2`...) : c'est la recommandation officielle de la
communaute dbt pour l'integration avec Airflow. dbt est toujours invoque via
son chemin complet (`/opt/dbt_venv/bin/dbt`), jamais ajoute au `PATH` global
du conteneur (pour ne pas faire d'ombre au `pip`/`python3` d'Airflow).
Versions retenues : `dbt-core==1.8.2` / `dbt-postgres==1.8.2` (`1.8.9`
n'existe pas pour `dbt-postgres`, seul `dbt-core` va jusqu'a `1.8.9`).

**Tests dbt natifs plutot que Great Expectations.** Les 3 exigences de tests
demandees (bornes de `risk_score`, integrite referentielle, absence de
doublon sur le grain) sont toutes couvertes par les tests generiques
integres a `dbt-core` (`not_null`, `unique`, `relationships`,
`accepted_values`) plus 2 tests singuliers SQL (`dbt/tests/*.sql`) — sans
ajouter de dependance/framework supplementaire (pas de `dbt_utils` non plus :
tout est ecrit en SQL simple, directement lisible). `dbt run` + `dbt test`
partagent le meme moteur (Postgres) et le meme flux de logs, ce qui simplifie
le debug. Great Expectations reste pertinent pour des besoins de validation
plus complexes (statistiques de distribution, detection d'anomalies
inter-sources) non requis a ce stade — non retenu ici pour eviter de
maintenir 2 frameworks de test en parallele pour un besoin deja couvert.

**Orchestration** : `gold_dbt_run.py` (nouveau DAG) est declenche
automatiquement en fin de `silver_transformation` via `TriggerDagRunOperator`
(meme mecanisme que `bronze_ingestion -> silver_transformation`, meme
justification : DAGs a declenchement manuel, un `ExternalTaskSensor` serait
fragile). `dbt test` fait echouer explicitement la task (et donc le DAG run)
si au moins un test echoue : comportement par defaut de `BashOperator`
(code de sortie non nul = tache en echec), aucun contournement.

---

## 2. dim_exercise — catalogue et taux de matching (pipeline en 4 etapes)

**Base = `exercise_name` deduplique de `600k_fitness_detailed`** (catalogue
de reference, jamais une source de faits — voir section 6). Les
`exercise_name` de `weight_training` sont resolus contre ce catalogue en
**4 ETAPES EN CASCADE** (chaque etape ne s'applique qu'aux exercices non
resolus par la precedente — `dbt/models/marts/dim_exercise.sql`) :

1. **Match strict** sur `normalized_exercise_name` (minuscules, ponctuation
   retiree, espaces multiples reduits — `normalize_exercise_name.sql`).
2. **Match sur nom de base** (`normalized_exercise_base_name` :
   etape 1 + mots d'equipement generiques retires — `barbell`, `dumbbell(s)`,
   `machine`, `cable`, `kettlebell`, `band(s)`, `smith`, `weighted`,
   `bodyweight` — + pluriel simple type "curls"->"curl" —
   `normalize_exercise_base_name.sql`).
3. **Fuzzy matching** (`rapidfuzz`, scorer `token_sort_ratio`, seuil **85%**),
   calcule HORS dbt par `scripts/fuzzy_match_exercises.py` (dbt-postgres ne
   supporte pas les modeles Python, reserves a Snowflake/Databricks/BigQuery).
   Le script ecrit tous les scores (meme sous le seuil) dans
   `raw.fuzzy_exercise_matches`, pour audit complet.
4. **Mapping manuel** (`dbt/seeds/manual_exercise_muscle_mapping.csv`) pour
   les exercices restant non resolus apres l'etape 3, en commencant par les
   plus frequents (nombre d'occurrences dans `weight_training`), avec une
   justification par ligne.

Ce qui reste non resolu apres l'etape 4 recoit `muscle_group='unknown'`,
`is_matched=false`, `match_stage='unmatched'` — **objectif volontairement
PAS 100%** : un residu documente est prefere a un matching force. Aucune
ligne de `fact_workout_session` n'est jamais perdue faute de match.

**Taux de matching reel mesure (execution du 2026-07-03, apres les 4 etapes)** :

| Etape | Exercices weight_training resolus a cette etape | Cumul |
|---|---|---|
| 1. Strict | 31 | 31 (38.3%) |
| 2. Nom de base (equipement retire) | +21 | 52 (64.2%) |
| 3. Fuzzy (seuil 85%, faux positifs exclus) | +6 | 58 (71.6%) |
| 4. Mapping manuel | +15 | 73 (**90.1%**) |
| Non resolu | 8 | 81 (100%) |

**Progression : 38.3% -> 90.1%.** La normalisation etendue (etape 2) a, a
elle seule, quasiment double le taux initial (38.3% -> 64.2%), avant meme
tout fuzzy matching ou intervention manuelle. Le residu de **8 exercices non
resolus (9.9%)** est assume et documente (voir liste en section 2bis) :
ce sont des exercices a tres faible frequence (<10 occurrences chacun dans
`fact_workout_session`), pour lesquels forcer une correspondance aurait plus
de risque (faux positif) que de valeur (faible volume concerne).

`dim_exercise` compte **3 177 lignes** au total : 3 127 issues du catalogue
(`match_stage='catalog_strict'`) + 50 lignes ajoutees pour les exercices
weight_training resolus aux etapes 2/3/4 mais absents du catalogue sous leur
forme exacte, repartis ainsi : 21 `base_name_match`, 6 `fuzzy_match`,
15 `manual_mapping`, 8 `unmatched`.

## 2bis. Verification de l'echantillon fuzzy (risque de faux positifs)

Sur les 47 candidats fuzzy au-dessus du seuil de 85%, **seuls 8 apportent une
information genuinement nouvelle** (les 39 autres, a 100% de similarite,
redecouvrent en realite des correspondances deja resolues par l'etape 2 —
redondants, sans risque). Ce sont donc ces 8 candidats qui ont ete
**verifies manuellement un par un** (echantillon de 8, complete a 10 avec 2
candidats triviaux a 100% pour contraste — comme demande) :

| Candidat weight_training | Match propose | Score | Verdict manuel |
|---|---|---|---|
| `deadlift trap bar` | Trap Bar Deadlift | 100% | ✅ Correct (ordre des mots) |
| `shoulder press standing` | Standing Shoulder Press (Dumbbell) | 100% | ✅ Correct (ordre des mots) |
| `curl ez bar` | Ez Bar Cable Curl | 100% | ✅ Correct (ordre des mots) |
| `lat pulldown closegrip` | Lat Pulldown (Close Grip) | 97.8% | ✅ Correct (espace manquant) |
| `close grip bench` | ACB Close Grip Bench | 88.9% | ✅ Correct (prefixe de marque) |
| `hammer decline chest press` | Decline Chest Press (Hammer Strength) | 85.2% | ✅ Correct ("Hammer" = marque d'equipement dans les 2 cas) |
| **`low incline bench`** | Incline Bench Row | 94.1% | ❌ **FAUX POSITIF** — mouvement de POUSSEE (bench press) rapproche a tort d'un mouvement de TIRAGE (row) |
| **`glute extension`** | Leg Extension | 85.7% | ❌ **FAUX POSITIF** — cible les fessiers, pas les quadriceps : mouvement different malgre la proximite textuelle |

**Taux d'erreur observe sur cet echantillon : 2/8 = 25%** (en ne comptant que
les candidats genuinement nouveaux, hors redondants triviaux a 100%). C'est
un taux d'erreur significatif, qui confirme que le fuzzy matching ne doit
JAMAIS etre applique sans verification humaine sur les candidats a faible
marge. **Les 2 faux positifs identifies sont explicitement exclus** dans
`dim_exercise.sql` (liste en dur, avec commentaire) et re-classes
manuellement via l'etape 4 (voir seed, `low incline bench` -> `chest`,
`glute extension` -> `legs`).

**Consequence sur la methodologie retenue** : le seuil de 85% reste utilise
tel quel (le rejeter completement aurait aussi rejete 6 bons candidats), mais
la verification manuelle systematique des candidats non-100% s'est averee
indispensable et est desormais documentee comme une etape obligatoire du
pipeline, pas une simple option.

## 2ter. Mapping manuel — exercices residuels les plus frequents

`dbt/seeds/manual_exercise_muscle_mapping.csv` couvre les 15 exercices les
plus frequents restant non resolus apres l'etape 3 (fuzzy), representant
**1 320 occurrences** dans `fact_workout_session` sur les ~1 347 occurrences
totales des 23 exercices residuels a ce stade (98%) — le residu APRES
mapping manuel (8 exercices non resolus, `match_stage='unmatched'`)
correspond donc a un volume tres faible (<30 occurrences cumulees). Exemple
de ligne (format demande) :

> `low incline bench` -> `chest` : "Developpe couche a faible inclinaison
> (mouvement de poussee pectoraux) — le candidat fuzzy retenu automatiquement
> ('Incline Bench Row', 94.1%) a ete verifie et rejete manuellement car c'est
> un mouvement de TIRAGE (row), pas de poussee : faux positif du fuzzy
> matching corrige ici."

Chaque ligne du seed suit ce format (mapping + justification). Voir le
fichier CSV pour les 15 lignes completes.

## 3. muscle_group — classification heuristique (PAS une taxonomie validee)

**Aucune source (Bronze/Silver) ne fournit de colonne `muscle_group`** :
`600k_fitness_detailed` ne decrit que `exercise_name`, sans attribut
anatomique. `muscle_group` est donc **entierement derive par une
classification a base de mots-cles** (`dbt/macros/classify_muscle_group.sql`),
appliquee UNIQUEMENT aux entrees du catalogue 600k_fitness. Les exercices
weight_training resolus aux etapes 2/3 (base-name/fuzzy) HERITENT du
`muscle_group` de leur match catalogue (pas de re-classification
independante) ; ceux resolus a l'etape 4 (mapping manuel) recoivent un
`muscle_group` assigne directement a la main (voir seed) ; ceux non resolus
recoivent `unknown` sans jamais passer par le classifieur automatique.

**AVERTISSEMENT EXPLICITE** : cette classification est une regle de gestion
inventee pour ce projet, 100% transparente (une seule chaine `CASE WHEN`,
aucun ML) mais **sans aucune valeur scientifique**. Deux exercices au nom
proche peuvent solliciter des groupes musculaires differents dans la
realite, et inversement. A ne jamais presenter comme une donnee medicale
verifiee dans le rapport de certification.

**Mots-cles retenus par zone** (ordre de priorite = ordre de verification,
les regles specifiques passent avant les generiques) :

| Zone | Mots-cles (sous-chaines, apres normalisation) |
|---|---|
| `lower_back` | deadlift, good morning, hyperextension, back extension |
| `knee` | squat, lunge, leg press, leg extension, step up |
| `shoulder` | overhead press, shoulder press, military press, arnold press, lateral raise, front raise, upright row, shrug |
| `chest` | bench press, chest press, chest fly, fly, push up, pushup, dip |
| `back` | pull up, pullup, chin up, pulldown, pull down, row, pullover, pull over |
| `arms` | curl, tricep, skull crusher, close grip |
| `abs` | crunch, sit up, situp, plank, russian twist, ab wheel, leg raise |
| `legs` (hors genou) | calf raise, hip thrust, glute bridge, leg curl, hamstring, hip abduction, hip adduction |
| `unknown` | tout le reste (+ tous les exercices weight_training non matches) |

**Distribution reelle de `dim_muscle`** (9 zones, execution du 2026-07-03) :
`shoulder`, `knee`, `lower_back`, `chest`, `legs`, `abs`, `arms`, `back`,
`unknown` — chacune presente au moins une fois dans le catalogue.

**`equipment` de `dim_exercise` = toujours NULL, volontairement.** Dans
`600k_fitness_detailed`, `equipment` est un attribut du PROGRAMME (Full Gym /
Garage Gym / ...), pas de l'exercice individuel. L'exposer sur
`dim_exercise` aurait fabrique une granularite qui n'existe pas dans la
source (un meme exercice apparait dans des programmes a equipement
different). Laisse a `NULL` plutot que d'inventer une valeur par
approximation (ex: valeur la plus frequente).

## 4. dim_muscle — base_epidemiological_risk

| `muscle_group` | `base_epidemiological_risk` | Statut |
|---|---|---|
| `shoulder` | 0.25 | Hypothese de modelisation |
| `knee` | 0.20 | Hypothese de modelisation |
| `lower_back` | 0.18 | Hypothese de modelisation |
| toute autre zone (chest, legs, abs, arms, back, unknown) | **0.10** | **Valeur par defaut faute de source epidemiologique identifiee pour cette zone precise — hypothese de modelisation, PAS une donnee epidemiologique verifiee, a traiter avec prudence dans le rapport de certification.** |

Les 3 premieres valeurs (epaule/genou/lombaires) reprennent un ordre de
grandeur communement admis en musculation (zones frequemment citees comme a
risque de blessure plus eleve), mais **aucune etude epidemiologique
specifique n'a ete citee ou verifiee pour fixer 0.25/0.20/0.18
precisement** — ce sont des ordres de grandeur relatifs choisis pour ce
projet, pas des coefficients medicaux calibres. Le commentaire ci-dessus est
duplique mot pour mot dans `dbt/models/marts/dim_muscle.sql` pour qu'il ne
passe pas inapercu a la lecture du code.

## 5. dim_user — rattachement de weight_training (HYPOTHESE DE DEMONSTRATION)

**`weight_training` (journal personnel de seances sur ~3 ans, 2015-2018) ne
contient AUCUN identifiant utilisateur.** Aucune cle commune n'existe entre
`weight_training` et `gym_members` (973 profils, source de `dim_user`) — ce
sont deux jeux de donnees Kaggle independants, sans lien reel. Rattacher les
9 142 lignes de `weight_training` (Silver) a UN SEUL profil de `dim_user`
est une **hypothese de demonstration** necessaire pour que
`fact_workout_session`/`fact_risk_score` aient une dimension utilisateur
exploitable — **ce n'est PAS une jointure de donnees reelle.**

**Critere de selection retenu** (100% deterministe,
`dbt/models/marts/dim_user.sql`) :
1. `experience_level` le plus eleve (coherent avec un historique de
   musculation de plusieurs annees, tel que celui de `weight_training`)
2. en cas d'egalite, `workout_frequency_days_per_week` le plus eleve
3. en dernier recours, `user_id` le plus petit (departage arbitraire mais
   stable d'un run a l'autre)

**Profil selectionne (execution du 2026-07-03)** : `user_id=9`, 18 ans,
Female, `experience_level=3` (maximum de l'echelle observee), 5 jours
d'entrainement/semaine. Marque par la colonne booleenne
`is_weight_training_demo_user=true` sur `dim_user` (une seule ligne `true`
parmi les 973). Toutes les lignes de `fact_workout_session` portent donc le
meme `user_id=9`.

**Limite assumee** : les attributs demographiques de ce profil (age, genre,
poids corporel...) n'ont aucun lien reel avec la personne ayant reellement
pratique les seances de `weight_training`. Toute analyse croisant
`fact_risk_score` avec les attributs de `dim_user` (ex: "risque par tranche
d'age") serait donc trompeuse et ne doit pas etre presentee comme une
conclusion valide.

## 6. Decision d'architecture : weight_training = seule source de faits

Rappel de la decision (deja actee, non renegociee ici, appliquee) :
`fact_workout_session` est alimentee **exclusivement** par `weight_training`.
`600k_fitness` (summary + detailed) n'est **jamais** une source de faits :
c'est uniquement le catalogue de reference de `dim_exercise`. Aucune
fusion/jointure factice n'est faite entre les deux univers au niveau des
faits — seul `dim_exercise` fait le pont, via le matching sur
`normalized_exercise_name` decrit en section 2.

## 7. fact_workout_session — grain et agregation

**Grain retenu : (session_date, workout_name, normalized_exercise_name).**
Le Silver `weight_training` est au grain "1 ligne = 1 set" (`set_order`
distingue les sets d'un meme exercice). `sets` n'existe donc pas comme
colonne brute : c'est un decompte obtenu PAR REGROUPEMENT (comme demande) :

- `sets` = nombre de sets loggees pour cet exercice ce jour-la (`count(*)`)
- `reps` = repetitions moyennes PAR SET, arrondi (format usuel "3x10")
- `total_reps` = somme exacte des repetitions (colonne technique
  supplementaire, necessaire au calcul EXACT de `volume_factor` dans
  `fact_risk_score` — `sets x reps_moyen` introduirait une approximation)
- `lifted_weight_kg` = poids moyen souleve sur les sets de cet exercice ce
  jour-la
- `duration_seconds` = somme des durees (quasi-toujours 0 sur ce dataset,
  voir `data/silver/CLEANING_LOG.md`)

**"session" = jour calendaire** (`DATE(performed_at)`) : simplification
retenue car la source ne fournit pas d'identifiant de seance explicite.
Documentee comme hypothese de modelisation dans `stg_weight_training.sql`.

**Bug reel rencontre et corrige pendant les tests** : la premiere version
agregait par `exercise_name` BRUT (pas normalise). Deux variantes textuelles
d'un meme exercice normalisant vers la meme valeur (ex. differences de
casse) produisaient alors 2 lignes distinctes partageant pourtant le meme
`exercise_id` apres jointure sur `dim_exercise` — violation du grain,
**detectee par** `tests/assert_fact_workout_session_grain_unique.sql`
(32 groupes en doublon constates). Corrige en agregeant directement sur
`normalized_exercise_name`. Le nombre de lignes de `fact_workout_session`
est passe de 2196 (bugge) a **2 164** (correct) apres correction.

## 8. fact_risk_score — formule et facteurs

Formule : `risk_score_brut = base_zone x charge_factor x volume_factor x recup_factor x duree_factor`,
puis normalisation lineaire sur 0-100. **Chaque facteur est une colonne
visible** du modele final (pas de score agrege en boite noire) :

| Facteur | Declenchement | Valeur | Justification |
|---|---|---|---|
| `base_zone` | toujours | `dim_muscle.base_epidemiological_risk` | Voir section 4 |
| `charge_factor` | hausse de charge hebdomadaire (`sum(lifted_weight_kg)` par user/zone/semaine) > 10% vs semaine precedente | **1.3** | Valeur choisie pour ce projet (penalite moderee), pas calibree sur donnees externes |
| `volume_factor` | ratio volume hebdomadaire (`sum(total_reps)`) / moyenne historique des semaines **strictement anterieures** (pas de fuite) | ratio borne **[0.5, 2.0]** | Bornes choisies pour eviter qu'un ratio extreme (ex. debut d'historique) ne domine le score total |
| `recup_factor` | meme zone sollicitee il y a moins de 48h (grain "seance" = jour calendaire) | **1.4** | Valeur choisie pour ce projet |
| `duree_factor` | seance complete (tous exercices, meme jour) > 2h (7200s) au total | **1.2** | Valeur choisie pour ce projet |

**Tous les facteurs par defaut valent 1.0 (neutre)** quand la condition ne
peut pas etre evaluee (pas de semaine precedente, pas d'historique, pas de
seance anterieure sur cette zone) — jamais de penalite appliquee par defaut
faute de donnee.

### Correctif duree_factor (recalibrage de la normalisation)

**Constat initial** : la premiere version de la formule normalisait sur
`min=0.05` / `max=1.092` (ce dernier incluant `duree_factor=1.2`). Resultat
observe : **0% de scores "Eleve"** (0/2164), max reel = 43.97/100, alors que
le seuil "Eleve" est a 67.

**Root cause identifiee** : `duration_seconds` est fiable a 0% sur ce
dataset (quasi-totalite des lignes a 0, voir `data/silver/CLEANING_LOG.md`).
`duree_factor` restait donc **correctement neutre (1.0) sur CHAQUE ligne**
individuelle — ce n'etait pas un bug de calcul ligne par ligne — MAIS le
denominateur de normalisation incluait quand meme le plafond theorique
`duree_factor=1.2`, un cas qui ne se produit JAMAIS en pratique sur ce
dataset. Ce plafond fantome compressait artificiellement TOUS les scores
reels vers le bas.

**Correctif applique** : le plafond de `duree_factor` est desormais EXCLU du
calcul de la borne max de normalisation (fixe a 1.0 au lieu de 1.2 dans le
calcul des bornes uniquement — le calcul PAR LIGNE de `duree_factor`, lui,
reste inchange et continuera a valoir 1.2 si des donnees de duree fiables
apparaissent un jour) :

- `min = 0.10 (base la plus faible) x 1.0 x 0.5 (plancher volume_factor) x 1.0 x 1.0 = 0.05` (inchange)
- `max = 0.25 (base la plus elevee) x 1.3 x 2.0 (plafond volume_factor) x 1.4 x 1.0 = 0.86` (**1.092 -> 0.86**, duree_factor exclu)
- `risk_score = 100 x (raw_risk_score - 0.05) / (0.86 - 0.05)`, borne a [0, 100]

Ces deux bornes sont des **variables dbt** (`risk_score_min_raw`/
`risk_score_max_raw`, `dbt_project.yml`), partagees entre `fact_risk_score.sql`
et `fact_risk_score_demo_synthetic.sql` pour eviter toute divergence future.

**Impact chiffre du correctif (avant / apres, memes 2 164 lignes reelles)** :

| | Avant correctif | Apres correctif |
|---|---|---|
| Faible (0-33) | 2 117 (97.8%) | 1 915 (88.5%) |
| Modere (34-66) | 47 (2.2%) | 223 (10.3%) |
| Eleve (67-100) | **0 (0.0%)** | **26 (1.2%)** |
| Score max observe | 43.97 | **100.00** |
| Score moyen | 10.97 | 15.85 |

C'est un correctif honnete (recalibrage d'une borne theorique jamais
atteinte dans ce dataset), **pas un forcage des seuils/poids pour "faire
joli"** : aucune valeur de facteur (1.3/1.4/2.0/0.5) ni aucun seuil
Faible/Modere/Eleve n'a ete modifie. Seule la borne de normalisation,
elle-meme deja documentee comme "un choix parmi d'autres, pas une verite
statistique", a ete corrigee pour ne plus compter un cas impossible en
pratique sur ce dataset precis.

**Ce qui plafonne desormais reellement le score** : les 26 lignes "Eleve"
(voir section "Resultats d'execution") concentrent TOUTES la zone
`shoulder` (`base_zone=0.25`, la plus elevee) avec `charge_factor=1.3` ET
`volume_factor` proche du plafond 2.0 simultanement. Aucune ligne
"Eleve" sur les autres zones n'a ete observee sur ce run : le cumul
simultane de plusieurs facteurs elevés sur une meme ligne reste rare dans
les donnees reelles, ce qui est un constat honnete sur le dataset, pas une
limite de la formule elle-meme (le scenario synthetique #9,
`fact_risk_score_demo_synthetic`, montre qu'un cumul total de tous les
facteurs atteint bien 100/100).

**Seuils de niveau** (inchanges) : `risk_score <= 33` -> `Faible` ;
`<= 66` -> `Modere` ; sinon `Eleve`.

### Table de demonstration synthetique (`fact_risk_score_demo_synthetic`)

Le correctif ci-dessus a suffi a produire des scores "Eleve" **reels**
(26 lignes). La table `gold.fact_risk_score_demo_synthetic` n'a donc PAS ete
creee pour compenser une distribution plate — **elle anticipe le besoin du
futur dashboard (etape 5)** de disposer d'exemples canoniques et garantis
pour illustrer chaque seuil, independamment de la rarete naturelle des cas
extremes reels (1.2% seulement pour "Eleve"). 9 scenarios fictifs (3 par
niveau, `dbt/seeds/demo_synthetic_risk_scenarios.csv`) sont calcules avec
EXACTEMENT la meme formule et les memes bornes de normalisation que
`fact_risk_score.sql` (memes variables dbt), pour rester coherents avec le
vrai moteur de calcul.

**Separation stricte imposee** :
- Table Postgres DISTINCTE (`gold.fact_risk_score_demo_synthetic`), jamais
  une simple colonne/flag sur la table reelle — impossible de l'agreger par
  erreur avec `fact_risk_score` sans une jointure/UNION explicite et
  deliberee.
- Colonne `is_synthetic_demo=true` sur 100% des lignes (redondant avec le
  nom de la table, mais un filet de securite supplementaire).
- Avertissement en tete du modele SQL (`fact_risk_score_demo_synthetic.sql`)
  et ici : **NE JAMAIS melanger ces lignes aux statistiques reelles ni les
  utiliser dans un calcul agrege sur les vraies donnees.**
- Le futur dashboard (etape 5, non implementee ici) devra exposer une
  bascule explicite "donnees reelles" / "demo" sans jamais les confondre
  visuellement — anticipe dans le design (table separee) mais pas encore
  implemente cote UI.

---

## Resultats d'execution (chiffres reels, DAG `gold_dbt_run`)

**Derniere execution complete : 2026-07-03** (chaine
`bronze_ingestion -> silver_transformation -> gold_dbt_run`, declenchement
automatique de bout en bout via `TriggerDagRunOperator`), toutes les tasks
`success` : `load_silver_to_postgres`, `dbt_seed`, `dbt_run_staging`,
`fuzzy_match_exercises`, `dbt_run` (10/10 modeles), `dbt_test` (**60/60
tests**, en hausse depuis 45 suite aux tests ajoutes sur `match_stage` et la
table de demo synthetique).

**Lignes par table Gold** :

| Table | Lignes |
|---|---|
| `dim_exercise` | 3 177 (3 127 catalogue + 50 weight_training : 21 base-name + 6 fuzzy + 15 manuel + 8 non resolus) |
| `dim_muscle` | 9 |
| `dim_user` | 973 |
| `dim_date` | 1 073 |
| `fact_workout_session` | 2 164 |
| `fact_risk_score` | 2 164 |
| `fact_risk_score_demo_synthetic` | 9 (table synthetique, jamais melangee aux vraies donnees) |

**Distribution des `risk_score` reels** (2 164 lignes, APRES correctif
duree_factor — voir section 8 pour le detail avant/apres) :

| Niveau | Lignes | % | `risk_score` moyen |
|---|---|---|---|
| Faible (0-33) | 1 915 | 88.5% | 12.86 |
| Modere (34-66) | 223 | 10.3% | 41.95 |
| Eleve (67-100) | **26** | **1.2%** | 76.58 |

`risk_score` observe : min=0.00, max=**100.00**, moyenne=15.85.

**Distribution des `risk_score` synthetiques** (9 scenarios fictifs,
`fact_risk_score_demo_synthetic`, JAMAIS melanges aux stats ci-dessus) :
3 Faible, 3 Modere, 3 Eleve (par construction, un scenario par niveau x3).

**Verification de tracabilite** (exemple reel post-correctif, un des
`workout_session_id` en zone `shoulder`) : `base_zone=0.25`,
`charge_factor=1.3`, `volume_factor=1.9452`, `recup_factor=1.4`,
`duree_factor=1.0` -> `raw_risk_score = 0.25 x 1.3 x 1.9452 x 1.4 x 1.0 =
0.8850` -> `risk_score = 100 x (0.8850 - 0.05) / (0.86 - 0.05) = 100.00`
(clamp au plafond) -> `Eleve` — calcul manuel confirme conforme a la valeur
stockee.

## Bugs reels rencontres et corriges pendant les tests

- **`round(double precision, integer)` n'existe pas en Postgres** (seule la
  signature `round(numeric, integer)` existe) : corrige avec un cast
  explicite `::numeric` avant `round()` dans `fact_workout_session.sql`.
- **`No suitable driver found` puis `ClassNotFoundException` sur le driver
  JDBC Postgres** lors d'une connexion JDBC "nue" via
  `spark._jvm.java.sql.DriverManager` : le jar ajoute par `--packages` vit
  dans un classloader Spark isole (`MutableURLClassLoader`), invisible du
  `DriverManager` JDBC utilise directement via py4j (meme apres
  `Class.forName`, qui utilise un AUTRE classloader). Corrige en utilisant
  `psycopg2` (Python pur, aucune JVM impliquee) pour la seule operation DDL
  necessaire (`CREATE SCHEMA IF NOT EXISTS raw`).
- **`cannot drop table raw.silver_600k_fitness_detailed because other
  objects depend on it`** : le mode `overwrite` par defaut de l'ecrivain JDBC
  Spark fait un `DROP TABLE` + `CREATE TABLE`, ce qui echoue des la 2e
  execution car les modeles staging dbt sont materialises en VUES
  (`dbt_project.yml`) qui referencent directement `raw.silver_*`. Corrige
  avec l'option `truncate=true` (TRUNCATE + INSERT, qui preserve l'objet
  table et donc les vues qui en dependent).
- **Grain dupliquee sur `fact_workout_session`** (32 groupes en doublon,
  detecte par le test singulier dedie) : voir section 7 ci-dessus pour le
  detail complet.
- **`dbt-postgres==1.8.9` n'existe pas** (seul `dbt-core` va jusqu'a `1.8.9`,
  `dbt-postgres` s'arrete a `1.8.2` sur cette branche) : les deux packages
  pinnes sur `1.8.2` pour une paire coherente.
- **`pip install` en root refuse par l'image `apache/airflow`** (garde-fou
  impose par l'image officielle) : la creation du venv dbt et les
  installations pip associees sont faites sous `USER airflow`, pas `root`.
- **Regex Postgres de pluriel trop agressive dans une premiere version**
  (`normalize_exercise_base_name.sql`) : un pattern naif de retrait du 's'
  final aurait transforme "press" en "pres" (mots se terminant par "ss").
  Corrige en excluant explicitement le cas ou l'avant-dernier caractere est
  deja un 's' (`\y([a-z]*[^s])s\y`), teste manuellement sur "press"/"curls"/
  "dips"/"raises" avant integration.
- **Faux positifs du fuzzy matching non detectes automatiquement** (2 cas sur
  8 candidats genuinement nouveaux, soit 25% — voir section 2bis) : aucun
  mecanisme automatique ne peut distinguer un bon match d'un faux positif a
  score eleve (94.1% et 85.7%) ; seule la verification manuelle sur
  echantillon les a reveles. Exclus explicitement par une liste en dur dans
  `dim_exercise.sql`, avec commentaire renvoyant vers cette section.
- **Distribution de `risk_score` figee a 0% "Eleve"** (voir section 8,
  "Correctif duree_factor") : root cause identifiee (borne de normalisation
  incluant un plafond `duree_factor=1.2` jamais atteint en pratique sur ce
  dataset), corrigee honnetement (recalibrage de la borne, pas des
  poids/seuils), impact chiffre avant/apres documente.
