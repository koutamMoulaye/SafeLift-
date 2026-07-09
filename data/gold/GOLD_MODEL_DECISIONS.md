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

## 9. dim_gym — salles FICTIVES pour le Jalon 2 (streaming affluence)

**Contexte** : le Jalon 2 de la certification (Bloc 2, pipelines temps
reel) ajoute un flux de streaming d'affluence par salle de sport, destine a
etre consomme en Spark Structured Streaming (sous-etape suivante, non
traitee ici). Aucun dataset Kaggle d'affluence de salle de sport en temps
reel n'existe pour un projet de ce type — le cahier des charges du Jalon 2
a explicitement retenu un **simulateur custom** plutot qu'un dataset
synthetique externe pour ce flux (a la difference de Bronze/Silver/Gold du
Jalon 1, entierement bases sur de vrais datasets Kaggle).

**`dim_gym` (`dbt/models/marts/dim_gym.sql`, alimentee par le seed
`dbt/seeds/dim_gym_seed.csv`) contient 5 salles 100% FICTIVES** : nom,
ville, quartier et capacite maximale theorique inventes pour ce projet,
choisis pour rester credibles (grandes villes francaises, capacites
plausibles pour une salle de sport urbaine, 90 a 180 personnes) mais **ne
correspondent a aucun etablissement reel.**

| gym_id | gym_name | Ville | Quartier | capacity_max |
|---|---|---|---|---|
| 1 | SafeLift Bastille | Paris | Bastille (11e) | 140 |
| 2 | SafeLift Part-Dieu | Lyon | Part-Dieu | 180 |
| 3 | SafeLift Vieux-Port | Marseille | Vieux-Port | 100 |
| 4 | SafeLift Capitole | Toulouse | Capitole | 130 |
| 5 | SafeLift Chartrons | Bordeaux | Chartrons | 90 |

**Pourquoi un seed dbt et pas un modele Bronze/Silver** : il n'existe aucune
donnee source a nettoyer/transformer ici (contrairement aux 4 tables
Bronze du Jalon 1) — le seed EST directement la donnee de reference, meme
pattern que `dbt/seeds/demo_synthetic_risk_scenarios.csv` pour
`fact_risk_score_demo_synthetic.sql` (etape 4/6).

**`scripts/simulate_gym_occupancy.py` lit `gold.dim_gym` une seule fois au
demarrage** (table statique, pas besoin de la relire a chaque cycle) pour
connaitre la liste des salles et leur `capacity_max`, puis publie un
message JSON par salle toutes les 5 a 10 secondes (configurable) sur le
topic Kafka `safelift-gym-occupancy` — voir le docstring du script pour le
detail du pattern d'affluence simule (heures de pointe 7h-9h/18h-21h en
semaine, plateau plus etale le week-end, nuit quasi vide). **Ce pattern est
FICTIF/illustratif, pas une vraie donnee d'affluence mesuree** — documente
dans le script lui-meme.

**Meme logique de documentation que `dim_muscle`/`fact_risk_score_demo_synthetic`** :
les donnees fictives sont marquees comme telles a la fois dans le code
(commentaire en tete de `dim_gym.sql` et de `simulate_gym_occupancy.py`) et
dans ce document, pour qu'elles ne soient jamais presentees par erreur
comme des donnees reelles dans le rapport de certification.

## 10. gym_occupancy_live — consumer Spark Structured Streaming (Jalon 2, sous-etape 2/5)

**Contexte** : suite de la section 9 — `scripts/simulate_gym_occupancy.py`
publie en continu sur le topic Kafka `safelift-gym-occupancy`
(`spark/jobs/stream_gym_occupancy.py` en est le consumer). Decision deja
actee : approche simple, Spark Structured Streaming maintient l'**etat
courant** (dernier message connu par salle) dans une table Postgres,
**PAS de fenetre temporelle/agregation historique a ce stade** — la table
`gold.gym_occupancy_live` contient donc toujours exactement une ligne par
salle de `dim_gym` (3 a 5 lignes), ecrasee a chaque nouveau message.

**Cette table n'est PAS geree par dbt** (contrairement au reste du schema
`gold`) : elle est creee et maintenue directement par le job Spark
(`CREATE TABLE IF NOT EXISTS` via psycopg2 au demarrage, puis upsert a
chaque micro-batch) — un run dbt complet (`dbt run`) ne la touche jamais et
ne la supprime jamais.

### `startingOffsets=latest`

Au (re)demarrage du job, seuls les NOUVEAUX messages sont consommes ;
l'historique deja publie sur le topic avant le demarrage est ignore.
**Justification** : ce job maintient un etat courant, pas un historique a
preserver — rejouer tout l'historique a chaque redemarrage n'apporterait
rien de plus (les vieux messages seraient de toute facon ecrases par les
plus recents des le premier micro-batch concernant chaque salle) et
retarderait inutilement la fraicheur de la table pendant tout le
rattrapage. **Consequence assumee** : les messages publies pendant que ce
job est arrete (redemarrage volontaire, crash, deploiement) sont
**definitivement perdus** — juge acceptable ici puisqu'aucune valeur n'est
tiree de connaitre precisement l'occupation a un instant passe non observe
pour un etat courant (a la difference d'un historique, hors perimetre de
cette sous-etape).

### Upsert : `INSERT ... ON CONFLICT (gym_id) DO UPDATE`, pas DELETE+INSERT

Le writer JDBC natif de Spark (`DataFrameWriter.jdbc`) ne supporte que
`append`/`overwrite`/`ignore`/`errorifexists` — aucune notion d'upsert.
Deux options envisagees :
1. **DELETE puis INSERT** (option initialement suggeree) : necessiterait 2
   instructions SQL distinctes explicitement enveloppees dans la meme
   transaction pour garantir l'atomicite (pas de fenetre ou la ligne
   n'existe plus).
2. **`INSERT ... ON CONFLICT (gym_id) DO UPDATE SET ...`** (retenue) :
   primitive Postgres atomique **en une seule instruction**, strictement
   equivalente en resultat final a l'option 1, mais plus simple a lire et a
   maintenir.

Execution : chaque micro-batch est minuscule (au plus une ligne par salle
de `dim_gym`, donc 3 a 5 lignes) — `foreachBatch` s'execute de toute facon
sur le driver Spark, donc un simple `.collect()` suffit largement, pas
besoin de repasser par un ecrivain JDBC distribue pour un tel volume.
L'upsert lui-meme est fait via `psycopg2` (pas via JDBC/py4j), meme
raisonnement que `ensure_raw_schema_exists()` dans
`spark/jobs/load_silver_to_postgres.py` (driver JDBC ajoute par
`--packages` invisible d'un `DriverManager` JDBC "nu").

### `charge_category` — seuils et justification

| `occupancy_rate` | `charge_category` |
|---|---|
| < 40% | Faible |
| 40% a 70% | Moderee |
| > 70% | Elevee |

Seuils simples et documentes (dans le meme esprit que les seuils
`risk_level` 33/66 de `fact_risk_score.sql`, section 8) — **pas issus d'une
etude/norme citee**, choisis pour rester credibles : sous 40% une salle de
sport est generalement consideree confortable (peu d'attente sur les
equipements), entre 40% et 70% elle se remplit mais reste utilisable, au-dela
de 70% le seuil de "quasi-sature" est couramment retenu pour un lieu
recevant du public (file d'attente probable sur les equipements les plus
demandes). **Hypothese de modelisation, pas une donnee mesuree/verifiee** —
meme statut que `base_epidemiological_risk` (section 4) : a traiter avec la
meme prudence dans le rapport de certification si ces seuils sont
presentes comme une recommandation.

### Absence d'operateur stateful — impact sur le checkpoint

Le job ne fait ni agregation, ni watermark, ni jointure : chaque message
est traite independamment (parse -> calcul -> upsert), aucun etat
intermediaire n'est accumule entre micro-batches par Spark lui-meme (l'etat
"courant" vit dans Postgres, pas dans le checkpoint). Le checkpoint Spark
ne contient donc que les offsets Kafka consommes et le log de commit du
sink `foreachBatch`, **geres entierement par le driver**. Consequence
pratique : contrairement a `silver_transformation.py` (ecriture Parquet
partagee entre driver et executeurs, necessitant
`spark.hadoop.fs.permissions.umask-mode=000` pour contourner un mismatch
d'UID), **aucun repertoire n'a besoin d'etre partage entre le conteneur
driver (`spark-streaming-gym`) et `spark-worker`** ici — le volume de
checkpoint (`spark_streaming_checkpoints`, `docker-compose.yml`) n'est
monte que sur le conteneur driver.

### Gestion des messages JSON malformes

`from_json` (schema Spark explicite, pas d'inference) est en mode
PERMISSIVE par defaut et **ne leve jamais d'exception** — un message
malforme produit un struct entierement `null` plutot que de faire planter
le job. Chaque micro-batch filtre explicitement les lignes avec un champ
requis `null` (`gym_id`, `current_occupancy`, `capacity`,
`parsed_timestamp`, `occupancy_rate`, `charge_category`) avant l'upsert, et
logue un avertissement avec le compte de messages ignores — **le stream
continue sans interruption**, aucune exception n'est laissee remonter
jusqu'a faire planter la query Structured Streaming. Meme philosophie pour
une erreur de connexion/ecriture Postgres au sein d'un micro-batch : logue
(`logger.exception`, stack complete) mais jamais relancee, le micro-batch
suivant retentera naturellement une ecriture a jour quelques secondes plus
tard.

## 11. Inputs utilisateur temps reel — union weight_training + realtime_user_sessions (Jalon 2, sous-etape 3/5)

**Contexte** : ajout d'un flux d'ecriture temps reel (l'utilisateur logue
une seance via le dashboard) en plus des deux flux de lecture deja en place
(Jalon 1 batch, Jalon 2 streaming affluence). **Decision deja actee, non
renegociee** : UNE SEULE formule de calcul de `risk_score` (celle deja en
dbt, `fact_risk_score.sql`) — aucun recalcul cote consumer, qui se contente
de faire entrer la donnee dans le pipeline puis de declencher le run dbt
existant.

### `raw.realtime_user_sessions` — table et grain

Creee par `scripts/consume_user_inputs.py` (psycopg2, `CREATE TABLE IF NOT
EXISTS`, PAS un modele dbt ni un job Spark) :

```sql
CREATE TABLE raw.realtime_user_sessions (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    exercise_name TEXT NOT NULL,
    lifted_weight_kg DOUBLE PRECISION NOT NULL,
    reps INTEGER NOT NULL,
    sets INTEGER NOT NULL,
    duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
    performed_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
```

**Grain = 1 ligne par exercice complet d'une seance** (sets/reps DEJA
agreges, le formulaire ne capture pas chaque repetition individuellement)
— DIFFERENT du grain source de `silver_weight_training` (1 ligne par SET).
`user_id` est ici **OBLIGATOIRE** (contrairement a `weight_training`, qui
n'en a nativement aucun) : l'API `POST /users/{user_id}/sessions` verifie
que l'utilisateur existe dans `gold.dim_user` AVANT de publier sur Kafka.

### Decouplage evenementiel : l'API ne write JAMAIS en base

`POST /users/{user_id}/sessions` (dashboard/main.py) **PUBLIE** l'evenement
sur Kafka (`safelift-user-inputs`) et s'arrete la — c'est le point
architectural explicitement demande a demontrer. L'API ne connait meme pas
le schema de `raw.realtime_user_sessions` (aucun `import psycopg2`
supplementaire pour cette route, uniquement le producteur Kafka deja
present pour le simulateur d'affluence). Erreurs Kafka gerees explicitement
(callback de livraison + `flush(timeout=5)` verifie) : file d'attente
pleine ou broker injoignable -> **HTTP 503** avec message clair, jamais un
succes trompeur.

### Union des sources : ou et comment

Le modele staging concerne par l'union est **`stg_workout_sessions_unified.sql`**
(nouveau, pas une modification de `stg_weight_training.sql`) :

- `stg_weight_training.sql` reste **INCHANGE** (grain per-set original) —
  choix deliberer : il est aussi consomme par `dim_exercise.sql` (comptage
  d'occurrences pour choisir la graphie representative d'un
  `normalized_exercise_name`) et `dim_date.sql`. Agreger `stg_weight_training`
  directement aurait modifie l'`occurrence_count` utilise par
  `dim_exercise.sql` pour departager les graphies (moins de lignes ->
  comptages differents -> risque de deplacer silencieusement le
  "gagnant" des ex-aequo), remettant en cause le taux de matching deja
  verifie et documente (38.3%->90.1%, section 2). **Risque identifie et
  evite explicitement**, pas par accident.
- `stg_realtime_user_sessions.sql` (nouveau) : lit `raw.realtime_user_sessions`,
  applique les MEMES macros de normalisation
  (`normalize_exercise_name`/`normalize_exercise_base_name`) que
  `stg_weight_training.sql` — garantit que le matching vers `dim_exercise`
  (base sur `normalized_exercise_name`) s'applique de facon identique aux
  deux sources, sans code duplique.
- `stg_workout_sessions_unified.sql` (nouveau) : agrege `stg_weight_training`
  au grain par-exercice (meme logique qu'avant, simplement deplacee depuis
  `fact_workout_session.sql`) avec `user_id = NULL`, UNION ALL avec
  `stg_realtime_user_sessions` (deja au bon grain, `user_id` reel).
- `fact_workout_session.sql` : simplifie (n'agrege plus rien lui-meme,
  selectionne directement depuis le modele unifie),
  `coalesce(u.user_id, demo_user.user_id) as user_id` remplace l'ancien
  `cross join demo_user` inconditionnel — **memes resultats pour les 2164
  lignes weight_training existantes** (coalesce(NULL, demo_id) =
  demo_id), verifie par re-execution complete du pipeline (0 changement de
  row count).
- `dim_exercise.sql` continue de lire `stg_weight_training` et
  `stg_600k_fitness_detailed` **DIRECTEMENT** (pas le modele unifie) —
  meme raison que ci-dessus.

### Grain de fact_workout_session : `user_id` ajoute

**Grain change** : `(session_date, workout_name, exercise_id)` ->
`(user_id, session_date, workout_name, exercise_id)`. Necessaire des lors
que plusieurs utilisateurs REELS distincts peuvent contribuer des lignes
(avant cette sous-etape, tous les faits appartenaient au meme demo_user,
le grain sans `user_id` etait suffisant par construction).
`tests/assert_fact_workout_session_grain_unique.sql` mis a jour en
consequence. **Limite assumee, non corrigee** : si un meme utilisateur
soumet 2 seances temps reel sur le meme exercice le meme jour calendaire,
les 2 lignes restent DISTINCTES dans le modele unifie (contrairement a
`weight_training`, qui agrege par jour) — violerait potentiellement le
test de grain si `workout_name` coincidait aussi (tres improbable :
`workout_name='Séance temps réel'` pour toute ligne realtime, namespace
disjoint des vrais libelles de programme weight_training). Non traite ici
(scenario de test = une seule seance soumise), a corriger si le besoin
reel de plusieurs seances/jour emerge.

### BUG REEL trouve et corrige : dim_date trop etroite

`dim_date.sql` generait un calendrier borne par le min/max de
`weight_training.performed_at` UNIQUEMENT (2015-2018). Une seance saisie
en 2026 tombe hors de cette plage -> `date_id` NULL apres le LEFT JOIN dans
`fact_workout_session.sql` -> `week_start_date` NULL dans
`fact_risk_score.sql` -> le LEFT JOIN sur `week_start_date` (deux NULL ne
sont JAMAIS egaux en SQL) ne retrouve plus la ligne agregee correspondante
-> `charge_factor`/`volume_factor` NULL -> `risk_score` NULL,
**silencieusement**, pour TOUTE seance temps reel. Corrige en unionnant
`stg_weight_training` et `stg_realtime_user_sessions` avant de calculer
min/max dans `dim_date.sql`. **Trouve en testant reellement** (pas en
relecture de code) : la premiere execution de bout en bout avec une vraie
seance aurait sinon produit un `risk_score` NULL, qui aurait probablement
echoue silencieusement le test `not_null` sur `fact_risk_score.risk_score`
— verifie a posteriori que le fix elimine bien le probleme (voir
"Resultats d'execution" ci-dessous, `dbt test` 72/72 avec la ligne reelle
en base).

### Formulaire du dashboard restreint a un `<select>`, pas un champ texte libre

`exercise_name` DOIT correspondre a un `normalized_exercise_name` deja
connu de `gold.dim_exercise`, sans quoi le LEFT JOIN dans
`fact_workout_session.sql` laisse `exercise_id`/`muscle_id` NULL pour
cette ligne (orpheline) — `risk_score` ne pourrait pas etre calcule pour
cette seance (meme classe de probleme que le bug dim_date ci-dessus, mais
sur le matching exercice plutot que la date). **Mitigation cote UI, pas
cote serveur** : le formulaire "Logger une seance" (`dashboard/static/index.html`/`dashboard.js`)
reutilise `GET /users/{user_id}/exercises` (deja utilise par le simulateur
what-if, Feature A) pour peupler un `<select>` limite aux exercices DEJA
pratiques par cet utilisateur — garantit par construction un match. **Ni
l'API (`POST /users/{user_id}/sessions`) ni le consumer
(`scripts/consume_user_inputs.py`) ne valident `exercise_name` contre
`dim_exercise`** — limite assumee et documentee : un appel direct a l'API
(hors dashboard) avec un `exercise_name` inconnu produirait une ligne
orpheline silencieuse plutot qu'une erreur explicite. Pas corrige ici (hors
perimetre), a traiter si l'API doit un jour etre exposee/utilisee
hors du dashboard.

### Declenchement dbt : run COMPLET, PAS partiel par utilisateur

`scripts/consume_user_inputs.py` declenche le DAG `gold_dbt_run` EXISTANT
dans son integralite via l'API REST Airflow
(`POST /api/v1/dags/gold_dbt_run/dagRuns`), avec
`conf={"triggered_by": "realtime_user_input", "user_id": ...}` transmis
**uniquement a des fins de tracabilite/logging** dans l'UI Airflow (visible
sur la page du DAG run) — `gold_dbt_run.py` ne lit pas cette conf, aucune
branche de code conditionnelle dessus.

**Run partiel par utilisateur explicitement ENVISAGE puis ECARTE** (documente,
pas simplement omis) : `fact_risk_score.sql` calcule `charge_factor`/`volume_factor`
par FENETRE GLISSANTE (`lag()`/`avg() over (...)` sur les semaines de
CHAQUE utilisateur x zone musculaire). dbt `--select` filtre des MODELES,
pas des lignes — il n'existe pas de mecanisme natif dbt pour "ne
recalculer que les lignes d'un utilisateur" au sein d'un modele `table`.
Un run partiel correct necessiterait soit un modele **incremental** dbt
(refonte du materialization strategy, hors perimetre de cette sous-etape),
soit de recalculer quand meme TOUTE la fenetre pour rester juste (auquel
cas le "partiel" ne gagnerait rien). Le run complet reste donc la seule
option a la fois simple et garantie correcte pour ce volume de donnees.

### Authentification API Airflow : `basic_auth` ajoute

Le backend d'auth PAR DEFAUT de l'API REST Airflow
(`airflow.api.auth.backend.session`, confirme via `airflow config
get-value api auth_backends` avant modification) exige un cookie de
session obtenu via le formulaire de login web — inutilisable pour un appel
HTTP programmatique simple. `AIRFLOW__API__AUTH_BACKENDS` etendu a
`"airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"`
(les deux, `session` reste utile pour l'UI web elle-meme). Identifiants
admin existants reutilises (`AIRFLOW_ADMIN_USERNAME`/`PASSWORD`) —
**simplification assumee** : un compte de service dedie avec un role IAM
Airflow restreint (ex. lecture/trigger seul, pas admin complet) serait plus
propre en production, hors perimetre de cette sous-etape.

### BUG REEL trouve et corrige : contention de ressources Spark

**Le plus significatif des bugs de cette sous-etape.** `spark-streaming-gym`
(sous-etape 2/5, job Structured Streaming LONG-COURANT) capturait les 2
coeurs du worker (`SPARK_WORKER_CORES=2`) **des son demarrage et les
gardait indefiniment** (une application Spark standalone ne relache ses
coeurs qu'a la fin de son execution — un job de streaming continu ne se
termine jamais). Consequence : `load_silver_to_postgres` (premiere task du
DAG `gold_dbt_run`, un job Spark **batch**) restait bloque a l'etat
`WAITING` avec **0 coeur alloue**, indefiniment — confirme concretement via
l'API JSON du master Spark (`http://spark-master:8080/json/`,
`coresused: 2`/`coresfree: 0` en continu, l'app batch affichant
`"state": "WAITING", "cores": 0"` sans jamais progresser, un premier test
de declenchement etant reste bloque plus de 3 minutes sans que la task
`load_silver_to_postgres` ne demarre). **Corrige** en ajoutant
`--conf spark.cores.max=1` a la commande `spark-submit` de
`spark-streaming-gym` (`docker-compose.yml`) : le volume du topic
`safelift-gym-occupancy` (1 partition, 5 messages/5-10s) ne justifie de
toute facon qu'un seul coeur, liberant l'autre pour les jobs batch soumis
ponctuellement. Verifie concretement apres correctif : `coresused: 1/2`,
la task `load_silver_to_postgres` demarre et se termine en ~20s comme
attendu.

### Test end-to-end reel (user_id=9, exercice "Bicep Curl (Barbell)")

Voir PROGRESS_JALON2.md pour le detail complet (logs, requetes, chiffres).
Resume : soumission via `POST /users/9/sessions` (70kg x 5 series x 15
reps) -> message Kafka confirme -> ligne inseree dans
`raw.realtime_user_sessions` (id=1) -> DAG `gold_dbt_run` declenche
automatiquement (`realtime_input_user9_...`) -> 6/6 tasks `success` -> **nouvelle
ligne reelle dans `gold.fact_risk_score`** (`session_date=2026-07-08`,
`workout_session_id=1`, `risk_score=7.81`, recalcule par la formule dbt
EXISTANTE, aucun code duplique) -> `GET /users/9/risk` (dashboard) reflete
immediatement la nouvelle valeur. **Delai reel mesure entre la soumission
et la fin du run dbt : ~70 secondes** (`load_silver_to_postgres` ~20s a
lui seul, le reste du pipeline dbt ~50s) — le timeout de polling cote
frontend (`dashboard.js`) a ete ajuste de 60s (valeur initiale supposee,
non mesuree) a **120s** suite a cette mesure reelle, avec marge.
`dbt test` : 72/72 PASS avec la ligne reelle en base (pas de regression
sur les 2164 lignes historiques, `coalesce` verifie neutre pour elles).

## 12. Affluence en direct (SSE) et recommandation de creneau (Jalon 2, sous-etape 4/5)

**Contexte** : le dashboard affiche desormais l'etat de `gold.gym_occupancy_live`
en direct (mise a jour continue par `spark/jobs/stream_gym_occupancy.py`,
sous-etape 2/5), via Server-Sent Events (**decision deja actee** : SSE,
pas de WebSocket, pas de polling pour cette fonctionnalite precise — le
polling existant du recalcul de risque, sous-etape 3/5, est totalement
independant et reste inchange).

### `GET /gyms/occupancy/stream` — SSE avec deduplication

Interroge `gold.gym_occupancy_live` toutes les 3 secondes cote serveur
(`GYM_OCCUPANCY_SSE_POLL_SECONDS`, cf. `dashboard/main.py`) — coherent avec
le rythme d'emission du simulateur (5-10s/salle) et le trigger du job Spark
(5s) : interroger plus vite n'apporterait aucune fraicheur supplementaire.
Un evenement n'est envoye au client QUE si les donnees ont reellement
change depuis le dernier envoi.

**BUG REEL rencontre et corrige en testant** : la comparaison de
deduplication portait initialement sur le PAYLOAD COMPLET envoye au
client, qui inclut `server_time` (recalcule a *chaque* iteration). Comme
`server_time` change systematiquement, la comparaison ne detectait jamais
une "absence de changement" — un evenement partait toutes les 3s meme sans
changement reel des donnees, a l'oppose du comportement demande ("pas
besoin de repousser si rien n'a changé"). Corrige en comparant uniquement
les LIGNES de donnees (`gym_id`/`occupancy`/...), `server_time` etant
ajoute seulement au payload effectivement envoye, apres la comparaison.
**Verifie concretement apres correctif** : capture reelle de 40s via
`curl -N`, 7 evenements distincts recus, tous correspondant a un
changement reel de donnees (aucun doublon) — voir PROGRESS_JALON2.md pour
le detail complet de la capture.

**Deconnexion propre** : `await request.is_disconnected()` verifie a chaque
iteration de la boucle — des que le client ferme l'onglet, la boucle
s'arrete et le generateur se termine. Chaque requete SQL passe par
`get_cursor()` (context manager qui rend toujours sa connexion au pool
immediatement apres usage), donc **aucune connexion Postgres n'est jamais
retenue entre deux iterations**, meme pour une connexion SSE de plusieurs
minutes. Verifie concretement : nombre de connexions Postgres actives
(`pg_stat_activity`) identique avant/apres une coupure brutale de la
connexion SSE (3 dans les deux cas).

**Simplification assumee** : psycopg2 synchrone (pas asyncpg) reutilise
ici comme partout ailleurs dans `dashboard/main.py` — chaque requete est
tres rapide (5 lignes maximum) et le nombre de clients SSE simultanes
attendu pour une demo est faible ; un driver Postgres asynchrone
introduirait une complexite non justifiee a ce stade.

### `GET /gyms/{gym_id}/best_slot` — recommandation de creneau

**LIMITATION ASSUMEE ET EXPLICITE, cœur de cette fonctionnalite** :
`gold.gym_occupancy_live` vient tout juste d'etre mise en service
(sous-etape 2/5) — son historique reel est bien trop court pour constituer
une base statistiquement valable pour une vraie prediction basee sur de la
donnee observee. La recommandation lit donc le **PATTERN THEORIQUE** code
en dur dans le simulateur (`peak_ratio()`, `scripts/simulate_gym_occupancy.py`)
plutot qu'un historique reel — **ce n'est PAS un modele predictif appris
sur de la donnee, c'est une lecture directe des regles du generateur**.
Champ `is_theoretical_pattern_based: true` renvoye explicitement dans
chaque reponse pour que le frontend (et tout consommateur futur de l'API)
puisse afficher cette nuance sans ambiguite — jamais presente comme une
vraie prediction basee sur l'historique observe. Le champ `methodology`
(texte libre, affiche tel quel dans le panneau du dashboard) porte la
meme explication en langage clair.

**`_theoretical_occupancy_ratio()` dans `dashboard/main.py` : DUPLIQUEE
DELIBEREMENT** de `peak_ratio()` (`scripts/simulate_gym_occupancy.py`),
memes seuils horaires exacts — meme raisonnement deja documente pour
`MUSCLE_LABELS_FR` (section conventions, CLAUDE.md) : ces deux services
tournent dans des conteneurs/processus Python separes, sans mecanisme de
partage de code entre les images Docker de ce projet. **Si `peak_ratio()`
change un jour cote simulateur, cette fonction DOIT etre mise a jour en
miroir** — pas de synchronisation automatique entre les deux.

**Granularite d'evaluation** : toutes les 30 minutes sur une fenetre de 4h
(9 points) — suffisant puisque le pattern est une fonction en PALIERS
(aucun segment horaire ne dure moins d'1h dans `_theoretical_occupancy_ratio`),
une granularite plus fine n'apporterait aucune information supplementaire.
En cas d'egalite entre plusieurs creneaux au meme ratio minimal (frequent :
toute une plage horaire partage le meme palier), le PREMIER creneau
atteignable est retenu — recommandation la plus actionnable ("des que
possible"), pas la plus tardive.

**Verifie concretement sur les 5 salles** (test reel a ~09:48 UTC un
jeudi, donc dans le palier "modere" 9h-18h, ratio 0.35) : les 5 endpoints
`GET /gyms/{1..5}/best_slot` renvoient tous `expected_occupancy_rate=0.35`
avec `recommended_slot_utc` == l'instant present (a quelques secondes
pres) — coherent avec le pattern (aucun palier plus bas que 0.35
n'apparait dans les 4h suivantes a cette heure-la un jour de semaine, donc
"maintenant" est bien le meilleur creneau atteignable, comportement
attendu et non un bug).

## 13. Nutrition — dim_nutrition + fact_nutrition_target (Jalon 3, sous-etape 1/6)

**Contexte** : demarrage du Jalon 3 (nutrition + ML bonus). Cette
sous-etape couvre l'ingestion d'un catalogue d'aliments (API USDA
FoodData Central) et le calcul de besoins nutritionnels cibles par
utilisateur (BMR/TDEE/proteines), a partir des donnees deja presentes
dans `dim_user` (source `gym_members`, Jalon 1). Pas de dashboard a ce
stade.

### ⚠️ Rappel du cadre ethique (a repeter a chaque usage de ces donnees)

**Les formules ci-dessous sont des formules STANDARD de la litterature
sportive generaliste, appliquees de facon deterministe — CE NE SONT PAS
des recommandations medicales ou nutritionnelles personnalisees.**
SafeLift ne remplace ni un coach sportif diplome, ni un medecin, ni un
dieteticien — ce rappel etait deja pose au lancement du projet et reste
valable ici a l'identique. Les valeurs de `fact_nutrition_target` sont
des ESTIMATIONS a but pedagogique/demo (certification RNCP36739), issues
d'equations publiees mais appliquees sans aucun avis professionnel
individualise (pas d'historique medical, pas de pathologie prise en
compte, pas de grossesse/allaitement pris en compte, etc.). Ne jamais
presenter ces chiffres comme un plan nutritionnel pret a suivre.

### 13.1 Collecte USDA FoodData Central (`airflow/dags/nutrition_ingestion.py`)

- **Endpoint utilise : `/foods/search`**, pas `/food/{fdcId}` — choisi
  pour le rapport simplicite/couverture demande : une seule requete par
  mot-cle renvoie directement plusieurs aliments AVEC leurs nutriments
  deja inclus (`foodNutrients`), alors que `/food/{fdcId}` aurait exige un
  appel individuel par aliment deja identifie (donc un aller-retour
  supplementaire pour d'abord obtenir les `fdcId`).
- **~31 mots-cles de recherche interroges** (proteines, feculents/glucides,
  legumes, fruits, matieres grasses/divers — voir `FOOD_KEYWORDS` dans le
  DAG), 4 resultats maximum par mot-cle, deduplication par `fdcId` ->
  cible 50-100 aliments distincts (PAS une aspiration complete du
  catalogue USDA, qui compte ~300k entrees — hors sujet et inutilement
  lent pour ce projet).
- **`dataType` restreint a `Foundation,SR Legacy`** : jeux de donnees de
  reference USDA (aliments bruts/peu transformes), valeurs nutritionnelles
  fiables et **systematiquement exprimees par 100g** — exclut
  volontairement `Branded` (produits industriels de marque, tres nombreux
  et bruyants pour une recherche par mot-cle generique, avec des portions
  variables) et `Survey (FNDDS)`.
- **IDs de nutriments USDA utilises** (identifiants stables du referentiel
  FoodData Central, verifies sur des reponses reelles de l'API) :
  `1008` = Energy (kcal), `1003` = Protein (g), `1004` = Total lipid/fat
  (g), `1005` = Carbohydrate by difference (g).
- **Aucune conversion d'unite appliquee** : les 4 macro-nutriments sont
  deja exprimes par 100g dans les dataType interroges — verifie a la
  source (documentation USDA FoodData Central) et confirme sur les
  echantillons reels recuperes (voir PROGRESS_JALON3.md pour les valeurs
  obtenues).
- **Aliment ignore (pas d'imputation inventee) si un des 4 macro-nutriments
  centraux est absent** de la reponse API pour cet aliment — compte des
  aliments ignores loggue explicitement, jamais une table Bronze
  silencieusement incomplete.
- **Cle API (`USDA_API_KEY`) jamais en dur, jamais loggee meme
  partiellement** : variable d'environnement uniquement
  (`docker-compose.yml`, `x-airflow-common-env`), toute chaine d'erreur
  passee au logger est systematiquement filtree par `_redact_secret()`
  avant d'etre ecrite (une exception `requests` peut contenir l'URL
  complete avec la cle en parametre de requete — jamais loggee telle
  quelle).
- **Rate limit / indisponibilite API** : retry avec backoff fixe (3
  tentatives, 5s d'attente) sur HTTP 429 (rate limit) et erreurs reseau
  transitoires ; **echec EXPLICITE de la task Airflow** (exception levee,
  pas de table Bronze partielle presentee comme complete) si l'API reste
  indisponible apres epuisement des tentatives pour un mot-cle donne.

### 13.2 `dim_nutrition` — pipeline Bronze -> Silver -> Gold

Meme pattern que le pipeline Kaggle existant (Jalon 1), DAG
`nutrition_ingestion` **self-contained** (independant de
`bronze_ingestion`/`silver_transformation`/`gold_dbt_run` — domaine
different) :
1. `ingest_usda_nutrition` (PythonOperator) : appel API -> Bronze
   (`data/bronze/usda_nutrition/ingestion_date={{ ds }}/`, idempotent,
   meme convention que `bronze_ingestion.py`).
2. `silver_usda_nutrition` (spark-submit) : dedup par `fdc_id`, trim
   `food_name` -> Silver.
3. `load_usda_nutrition_to_postgres` (spark-submit) : **reutilise
   `spark/jobs/load_silver_to_postgres.py` existant**, `usda_nutrition`
   simplement ajoutee au dictionnaire `TABLES` — recharge au passage les 4
   tables Kaggle existantes (leger surcout de quelques secondes accepte
   pour ne pas dupliquer ce script de chargement).
4. `dbt_run_nutrition`/`dbt_test_nutrition` : **`dbt run --select
   stg_usda_nutrition dim_nutrition fact_nutrition_target`** (scope
   restreint, PAS tout `gold_dbt_run`) — `fact_nutrition_target` ne
   depend que de `dim_user` deja construite, inutile de retraiter tout le
   pipeline Kaggle (matching d'exercices, etc.) pour une ingestion
   nutrition.

### 13.3 `fact_nutrition_target` — formules et hypotheses

**BMR (Basal Metabolic Rate) — equation de Mifflin-St Jeor (1990)**,
formule standard la plus citee dans la litterature pour estimer le
metabolisme de base a partir du poids/taille/age/sexe (a l'origine
publiee dans *A new predictive equation for resting energy expenditure
in healthy individuals*, American Journal of Clinical Nutrition) :

```
Homme  : BMR = 10 x poids(kg) + 6.25 x taille(cm) - 5 x age(annees) + 5
Femme  : BMR = 10 x poids(kg) + 6.25 x taille(cm) - 5 x age(annees) - 161
```

`gender` de `gym_members` ne contient que 2 valeurs (`Male`/`Female`,
verifie sur les 973 lignes) — aucune branche de repli codee : un `gender`
inattendu produirait un `bmr_kcal` NULL, detecte explicitement par le
test `not_null` sur `fact_nutrition_target.bmr_kcal` plutot que suppose
silencieusement.

**TDEE (Total Daily Energy Expenditure) = BMR x facteur d'activite.**
Facteur deduit de `workout_frequency_days_per_week` (`gym_members`),
mapping standard (tables Harris-Benedict/Mifflin usuelles) :

| Jours d'entrainement/semaine | Facteur d'activite | Categorie |
|---|---|---|
| ≤ 1 (jamais observe dans ce dataset) | 1.2 | Sedentaire |
| 2 | 1.375 | Legerement actif |
| 3 | 1.55 | Moderement actif |
| 4 | 1.725 | Actif |
| ≥ 5 | 1.9 | Tres actif |

**`gym_members` ne couvre que 2 a 5 jours/semaine** (verifie : 197
utilisateurs a 2j, 368 a 3j, 306 a 4j, 102 a 5j — aucun a 0, 1, 6 ou 7).
Les paliers ≤1 et ≥6 restent codes (valeurs 1.2/1.9 reprises des tables
standard) par robustesse si un profil futur en sortait, mais ne sont
**jamais exerces par les donnees actuelles** — constat honnete, pas
masque.

**Besoin proteique cible = `protein_g_per_kg_target` x poids(kg)`.**
`protein_g_per_kg_target` deduit de `experience_level` (`gym_members`,
valeurs 1/2/3), dans la fourchette **1.6 a 2.2 g/kg** couramment citee
pour les pratiquants de musculation/fitness (ordre de grandeur repris de
positions de societes savantes en nutrition sportive, ex. ISSN — voir
limite ci-dessous) :

| `experience_level` | `protein_g_per_kg_target` | Justification |
|---|---|---|
| 1 (debutant) | 1.6 g/kg | Borne basse de la fourchette usuelle — historique d'entrainement/masse musculaire a construire plus limites |
| 2 (intermediaire) | 1.9 g/kg | Milieu de fourchette |
| 3 (avance) | 2.2 g/kg | Borne haute — volume d'entrainement et masse musculaire a soutenir generalement plus eleves |

**Limite assumee** : ce mapping `experience_level -> g/kg` est une
**simplification deliberee** du critere demande (task : "proposer un
critere simple et documente") — `experience_level` est un proxy indirect
de l'anciennete/l'intensite d'entrainement, PAS une mesure directe de
masse musculaire ou d'objectif (prise de masse/seche/maintien, non
disponible dans `gym_members`). Documente comme telle, pas presente comme
une methode de calcul validee scientifiquement pour CE mapping precis
(contrairement a la fourchette 1.6-2.2 g/kg elle-meme, qui est bien
issue de la litterature).

### 13.4 Tests dbt (garde-fous, pas des bornes medicales exactes)

- `assert_tdee_within_plausible_range.sql` : `tdee_kcal` toujours defini,
  entre 1000 et 6000 kcal/jour — detecte un bug de formule (ex. conversion
  d'unite oubliee), pas une verification medicale.
- `assert_protein_target_plausible.sql` : `protein_target_g_per_day`
  toujours defini, strictement positif, jamais > 4g/kg de poids corporel
  (marge large au-dela de toute recommandation serieuse, meme pour un
  athlete de haut niveau).
- Tests generiques (`_marts__models.yml`) : `fact_nutrition_target.user_id`
  unique + not_null + relationship vers `dim_user` (grain = 1 ligne par
  utilisateur, aucun utilisateur sans cible nutritionnelle).

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
