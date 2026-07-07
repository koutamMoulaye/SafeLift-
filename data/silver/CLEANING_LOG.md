# CLEANING_LOG.md — Journal des decisions de nettoyage Bronze -> Silver

> Chaque decision de nettoyage ci-dessous est justifiee et chiffree, comme
> exige pour cette etape (un jury RNCP peut demander pourquoi telle ligne a
> ete droppee ou telle valeur modifiee — rien n'est fait "silencieusement").
> Source des anomalies : `data/bronze/SCHEMA_NOTES.md`. Code source des
> transformations : `spark/jobs/silver_*.py` (+ helpers dans
> `spark/jobs/silver_common.py`).
>
> Date de redaction initiale : 2026-07-01. Les chiffres "avant/apres" exacts
> issus de l'execution reelle du DAG `silver_transformation` sont reportes
> plus bas (section "Resultats d'execution").

## Principes generaux

- **Aucune jointure entre tables** a ce stade : chaque table Silver reste
  independante, un seul job = une seule table source.
- **Aucune valeur n'est corrigee "par supposition"** sans verification ou
  justification explicite ci-dessous. Quand une hypothese est necessaire
  (unite non documentee, semantique de signe ambigue...), elle est verifiee
  empiriquement quand c'est possible, et le raisonnement est trace ici.
- **Bronze reste la source de verite brute** : Silver ne supprime jamais
  d'information sans tracabilite — les colonnes de metadonnees Bronze
  (`ingestion_timestamp`, `source_file`, `source_dataset`) sont conservees
  dans Silver, avec ajout d'un `silver_processed_at` marquant le passage de
  transformation.

---

## 1. `600k_fitness_summary`

Aucune anomalie majeure identifiee en Bronze pour cette table (0 doublon).
Nettoyage = normalisation uniquement :

| Decision | Detail | Impact chiffre |
|---|---|---|
| Parsing `level`/`goal` | Chaines "liste Python stringifiee" (ex. `"['Beginner', 'Intermediate']"`) parsees via `ast.literal_eval` en vraies colonnes `array<string>` : `level_list`, `goal_list`. **Choix rejete : explode en plusieurs lignes** (multiplierait le nombre de lignes, notamment en produit croise level × goal — une table Silver perdrait le grain "1 ligne = 1 programme"). **Choix rejete : colonnes one-hot** (figerait un vocabulaire de valeurs a l'avance, moins robuste si de nouvelles valeurs de level/goal apparaissent). Le type array Parquet natif est requetable directement (ex. `array_contains`) sans ces inconvenients. | 2598/2598 lignes concernees (100%), pas de perte de ligne. |
| `program_length` -> `program_length_weeks` | Hypothese "l'unite est la semaine" **confirmee empiriquement** : jointure interne (hors Silver, controle ponctuel) entre `program_length` (summary) et `max(week)` du grain detaille pour les titres communs -> **2583/2597 titres comparables (99.5%) ont `program_length == max(week)` exactement**. | Renommage seul, aucune ligne modifiee. |
| `created`/`last_edit` -> `created_at`/`last_edited_at` | Cast chaine -> timestamp reel (`to_timestamp`, format `yyyy-MM-dd HH:mm:ss`). Les valeurs deja nulles en Bronze (`created` : 1/2598 soit 0.04% ; `last_edit` : 2/2598 soit 0.1%) restent nulles apres cast (pas d'imputation). | 1 valeur nulle sur `created_at`, 2 sur `last_edited_at` — inchangees. |
| `description`, `equipment` | Laissees telles quelles (nulls : 4/2598 soit 0.2% et 1/2598 soit 0.04%). Sous tout seuil raisonnable, pas d'action. | 0 ligne modifiee. |
| Deduplication | Aucune (0 doublon strict constate en Bronze sur cette table). | 0 ligne supprimee. |

## 2. `600k_fitness_detailed`

Table avec le plus d'anomalies (605 033 lignes en Bronze).

| Decision | Detail | Impact chiffre |
|---|---|---|
| **Deduplication** | Suppression des lignes strictement dupliquees (toutes colonnes identiques), constatees en Bronze via scan complet. | **904 lignes supprimees / 605 033 (0.15%)** -> 604 129 lignes apres dedup. |
| Parsing `level`/`goal` | Identique a `600k_fitness_summary` (voir ci-dessus pour le raisonnement complet). | 100% des lignes concernees, pas de perte. |
| `program_length` -> `program_length_weeks` | Meme renommage/justification que `600k_fitness_summary`. | Renommage seul. |
| **`reps` : valeurs negatives** | **25 967 valeurs negatives constatees sur le Bronze complet (~4.3% des 605 033 lignes)**, sans metadonnee source documentant une convention de signe. Deux options etaient envisagees : (a) valeur absolue, (b) nullifier + flag. **Decision retenue : (b) nullifier + flag booleen `reps_anomaly_flag`.** Raisonnement : prendre la valeur absolue transformerait par exemple `-180` en `180` repetitions — physiologiquement peu plausible pour les exercices de mobilite/etirement ou l'anomalie est concentree (ex. observe : "Knee-to-wall ankle dorsiflexion test" avec `reps=-180`, plus coherent avec 180 **secondes** de maintien qu'avec 180 repetitions). Sans confirmation externe de cette hypothese de "secondes deguisees en reps negatifs", fabriquer une valeur positive serait une correction non fondee. Nullifier + flaguer garde la donnee absente visible (exclue des agregations par defaut en aval) plutot que de la biaiser silencieusement. | **25 908 valeurs mises a `null` sur la colonne `reps`, `reps_anomaly_flag=true` sur ces memes 25 908 lignes**, mesurees APRES deduplication (604 129 lignes) — l'ecart avec les 25 967 constatees sur le Bronze brut (605 033 lignes, doublons inclus) s'explique par les 59 lignes a `reps` negatif qui faisaient partie des 904 doublons stricts supprimes juste avant. Aucune ligne supprimee par cette etape : c'est une nullification de valeur, pas un drop. |
| `week`, `day`, `number_of_exercises`, `sets` | Cast `float` -> `int` (aucune valeur manquante constatee sur ces colonnes en Bronze, cast donc sans perte). | 0 ligne affectee par une perte de donnee (cast pur). |
| `created`/`last_edit` -> `created_at`/`last_edited_at` | Cast chaine -> timestamp reel (aucun null constate en Bronze sur ces 2 colonnes pour cette table). | 0 valeur nulle introduite. |

## 3. `gym_members`

Table la plus propre (973 lignes, 0 null, 0 doublon en Bronze). Nettoyage =
normalisation des noms de colonnes uniquement, aucune donnee modifiee :

| Colonne source (Bronze) | Colonne Silver | Justification |
|---|---|---|
| `Age` | `age` | snake_case |
| `Gender` | `gender` | snake_case |
| `Weight (kg)` | `body_weight_kg` | Unite deja explicite en Bronze (kg). Nom **volontairement different** de `lifted_weight_kg` (weight_training) — voir section "Harmonisation inter-tables" ci-dessous. |
| `Height (m)` | `height_m` | snake_case + unite |
| `Max_BPM`, `Avg_BPM`, `Resting_BPM` | `max_bpm`, `avg_bpm`, `resting_bpm` | snake_case |
| `Session_Duration (hours)` | `session_duration_hours` | Unite deja explicite en Bronze |
| `Calories_Burned` | `calories_burned` | snake_case |
| `Workout_Type` | `workout_type` | snake_case |
| `Fat_Percentage` | `fat_percentage` | snake_case |
| `Water_Intake (liters)` | `water_intake_liters` | Unite deja explicite en Bronze |
| `Workout_Frequency (days/week)` | `workout_frequency_days_per_week` | snake_case + unite |
| `Experience_Level` | `experience_level` | snake_case |
| `BMI` | `bmi` | snake_case |

**Impact chiffre : 0 ligne supprimee, 0 valeur modifiee** — renommage pur.

## 4. `weight_training`

| Decision | Detail | Impact chiffre |
|---|---|---|
| **Deduplication** | Suppression des lignes strictement dupliquees, constatees en Bronze via scan complet. | **790 lignes supprimees / 9 932 (7.95%)** -> 9 142 lignes apres dedup. |
| **Drop `Notes`/`Workout Notes`** | Seuil retenu : une colonne est droppee si son taux de remplissage est **inferieur a 5%**. Taux reels constates en Bronze : `Notes` = 0.1% (7/9932 lignes non vides), `Workout Notes` = 0.0% (3/9932 lignes non vides). Les deux sont tres largement sous le seuil -> droppees. | 2 colonnes supprimees ; aucune perte de ligne (le drop porte sur des colonnes, pas des lignes). |
| **`Weight` -> `lifted_weight_kg`** | Conversion **livres -> kilogrammes** (facteur 0.45359237). Hypothese "l'unite source est la livre (lb)" **confirmee empiriquement** : les valeurs les plus frequentes de `Weight` sont 185, 225, 135, 275, 235, 230 — des reperes caracteristiques du chargement de disques en livres dans la culture americaine de musculation (225 lbs = "deux plaques de 45 lbs" par cote, un seuil tres reconnaissable ; en kg ces memes valeurs seraient physiologiquement extremes pour la majorite des pratiquants). | Conversion appliquee aux 9 142 lignes restantes apres dedup (100%). Aucune ligne supprimee par cette etape. |
| `Seconds` -> `duration_seconds` | Renommage seul, unite deja explicite dans le nom source. | 0 ligne modifiee. |
| `Date` -> `performed_at` | Cast chaine -> timestamp reel (`to_timestamp`, format `yyyy-MM-dd HH:mm:ss`). Aucun null constate en Bronze sur cette colonne. | 0 valeur nulle introduite. |
| `Distance` | Conservee telle quelle (renommage snake_case implicite car deja en minuscule). Unite non documentee dans les metadonnees source, valeur a 0 pour la quasi-totalite des lignes (dataset de musculation, pas de cardio) : pas de conversion tentee faute d'unite fiable a confirmer. | 0 ligne modifiee. |

## Harmonisation inter-tables (convention de nommage)

- **`weight_kg` a ete volontairement EVITE comme nom unique commun**, malgre
  la consigne initiale de nommer de maniere identique les concepts partages
  entre tables. `gym_members."Weight (kg)"` et `weight_training."Weight"`
  mesurent deux grandeurs physiques **differentes** : la premiere est le poids
  corporel de la personne, la seconde le poids souleve lors d'une serie. Les
  fusionner sous un nom identique aurait ete trompeur (un jury ou un futur
  utilisateur du warehouse aurait pu croire a tort qu'il s'agit de la meme
  mesure). Decision retenue : conserver la **meme convention d'unite**
  (suffixe `_kg`, valeurs en kilogrammes dans les deux cas) mais des noms
  distincts et explicites : `body_weight_kg` (gym_members) vs
  `lifted_weight_kg` (weight_training).
- **`reps` et `exercise_name`** apparaissent dans `600k_fitness_detailed`
  (plan d'entrainement prescrit) et `weight_training` (seance loggee
  reellement effectuee) avec le meme nom des deux cotes : ce sont
  conceptuellement le meme type de mesure (nombre de repetitions, nom
  d'exercice), donc meme nom conserve. **Aucune jointure n'est faite entre
  ces deux tables a ce stade** (rappel : hors perimetre Silver), le nom
  commun sert uniquement la lisibilite/coherence du data lake.
- **Convention timestamp** : toute colonne de type date/heure se termine par
  `_at` (`created_at`, `last_edited_at`, `performed_at`, `silver_processed_at`).

---

## Resultats d'execution (chiffres reels, DAG `silver_transformation`)

Execution reelle du 2026-07-01 (run `manual__2026-07-01T14:53:22`, declenche
automatiquement par `bronze_ingestion` via `TriggerDagRunOperator`), 4/4
tasks `success`. Chiffres extraits directement des logs de chaque task Spark
et confirmes par relecture des fichiers Parquet Silver (script de controle
pandas : shape, colonnes, echantillon de 5 lignes par table).

| Table | Lignes avant (Bronze) | Lignes apres dedup | Lignes finales (Silver) | Colonnes finales |
|---|---|---|---|---|
| `600k_fitness_summary` | 2 598 | 2 598 (0 doublon) | **2 598** | 14 |
| `600k_fitness_detailed` | 605 033 | 604 129 (-904 doublons) | **604 129** (25 908 `reps` nullifies + flagges) | 21 |
| `gym_members` | 973 | 973 (0 doublon) | **973** | 19 |
| `weight_training` | 9 932 | 9 142 (-790 doublons) | **9 142** | 12 |

Colonnes finales detaillees par table :
- `600k_fitness_summary` : `title`, `description`, `equipment`, `program_length_weeks`, `time_per_workout`, `total_exercises`, `level_list`, `goal_list`, `created_at`, `last_edited_at`, `ingestion_timestamp`, `source_file`, `source_dataset`, `silver_processed_at`.
- `600k_fitness_detailed` : idem + `week`, `day`, `number_of_exercises`, `exercise_name`, `sets`, `reps`, `intensity`, `reps_anomaly_flag` (sans `total_exercises`).
- `gym_members` : `age`, `gender`, `body_weight_kg`, `height_m`, `max_bpm`, `avg_bpm`, `resting_bpm`, `session_duration_hours`, `calories_burned`, `workout_type`, `fat_percentage`, `water_intake_liters`, `workout_frequency_days_per_week`, `experience_level`, `bmi`, `ingestion_timestamp`, `source_file`, `source_dataset`, `silver_processed_at`.
- `weight_training` : `workout_name`, `exercise_name`, `set_order`, `reps`, `distance`, `duration_seconds`, `lifted_weight_kg`, `performed_at`, `ingestion_timestamp`, `source_file`, `source_dataset`, `silver_processed_at`.

Verification manuelle de coherence : sur l'echantillon relu, une ligne de
`weight_training` avec `Weight=135` (lbs) en Bronze donne bien
`lifted_weight_kg=61.23` en Silver (135 × 0.45359237 = 61.235, arrondi a 2
decimales) ; une ligne de `600k_fitness_detailed` avec `reps` negatif en
Bronze apparait bien avec `reps=NaN` et `reps_anomaly_flag=True` en Silver.
