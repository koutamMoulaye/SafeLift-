# SCHEMA_NOTES.md — Schéma réel des datasets bruts (Bronze)

> Schéma constaté en inspectant directement les fichiers CSV sous
> `data/bronze/raw/` (module `csv` de la stdlib Python, pas de supposition sur
> les noms/types de colonnes). Aucune correction n'est appliquée ici : c'est le
> rôle de l'étape Silver. Ce document sert de référence pour l'écriture du DAG
> `airflow/dags/bronze_ingestion.py`.
>
> Date d'inspection : 2026-07-01.

## Regroupement des sources en tables Bronze

3 datasets Kaggle sources sont regroupés en **3 tables Bronze** (une table =
un dataset source, conformément à la contrainte "pas de jointure en Bronze") :

| Table Bronze      | Fichier(s) source                                                    |
|--------------------|-----------------------------------------------------------------------|
| `600k_fitness`     | `program_summary.csv` **+** `programs_detailed_boostcamp_kaggle.csv` |
| `gym_members`      | `gym_members_exercise_tracking.csv`                                   |
| `weight_training`  | `weightlifting_721_workouts.csv`                                      |

**Choix documenté** : les deux fichiers `600k_fitness` ne sont **pas fusionnés
en une seule table** malgré le regroupement sous un même dossier `600k_fitness/`.
Ce sont deux grains différents (un programme vs le détail exercice/semaine/jour
d'un programme) avec des schémas différents — les fusionner nécessiterait une
jointure sur `title`, ce qui est explicitement hors périmètre du Bronze. Le DAG
les écrit donc comme **deux tables Parquet distinctes** :
`data/bronze/600k_fitness_summary/` et `data/bronze/600k_fitness_detailed/`.
Au total, le DAG comporte donc **4 tasks** (une par fichier CSV source), et non 3.

---

## 1. `600k_fitness_summary` (source : `600k_fitness/program_summary.csv`)

- **Lignes** : 2 598 (hors en-tête)
- **Encodage** : UTF-8
- **Doublons stricts** : 0

| Colonne             | Type observé            | Nulls (sur 2598) |
|----------------------|-------------------------|-------------------|
| `title`              | str                      | 0                 |
| `description`        | str                      | 4 (0.2%)          |
| `level`              | str — **liste Python stringifiée**, ex. `"['Intermediate']"`, `"['Beginner', 'Novice', 'Intermediate']"` | 0 |
| `goal`               | str — **liste Python stringifiée**, ex. `"['Bodybuilding']"` | 0 |
| `equipment`          | str                      | 1 (0.04%)         |
| `program_length`     | float                    | 1 (0.04%)         |
| `time_per_workout`   | float                    | 0                 |
| `total_exercises`    | int                      | 0                 |
| `created`            | str, format `YYYY-MM-DD HH:MM:SS` | 1 (0.04%) |
| `last_edit`          | str, format `YYYY-MM-DD HH:MM:SS` | 2 (0.1%)  |

**Anomalies constatées (non corrigées ici)** :
- `level` et `goal` sont des chaînes représentant des listes Python
  (`"['A', 'B']"`), pas des valeurs scalaires ni du JSON valide — nécessitera
  un parsing dédié (`ast.literal_eval` ou équivalent) en étape Silver.
- `created`/`last_edit` sont des chaînes, pas un type date natif du CSV — à
  caster en Silver.

## 2. `600k_fitness_detailed` (source : `600k_fitness/programs_detailed_boostcamp_kaggle.csv`)

- **Lignes** : 605 033 (hors en-tête) — fichier de 282 MB
- **Encodage** : UTF-8
- **Doublons stricts** : 904 (scan complet)

| Colonne                | Type observé | Nulls |
|--------------------------|--------------|-------|
| `title`                  | str          | 0     |
| `description`            | str          | ~0.3% (576/200k échantillonnées) |
| `level`                  | str — liste Python stringifiée (idem `600k_fitness_summary`) | 0 |
| `goal`                   | str — liste Python stringifiée | 0 |
| `equipment`              | str          | 0     |
| `program_length`         | float        | 0     |
| `time_per_workout`       | float        | 0     |
| `week`                   | float (entier stocké en flottant, ex. `1.0`) | 0 |
| `day`                    | float (idem) | 0     |
| `number_of_exercises`    | float (idem) | 0     |
| `exercise_name`          | str          | 0     |
| `sets`                   | float (idem) | 0     |
| `reps`                   | float — **contient des valeurs négatives** | 0 |
| `intensity`              | float        | 0     |
| `created`                | str, format `YYYY-MM-DD HH:MM:SS` | 0 |
| `last_edit`              | str, format `YYYY-MM-DD HH:MM:SS` | 0 |

**Anomalies constatées (non corrigées ici)** :
- **904 lignes strictement dupliquées** (toutes colonnes identiques) sur les
  605 033 — à dédupliquer en Silver.
- **25 967 valeurs négatives dans `reps`** (ex. `-180.0`, `-18.0` observées sur
  les tout premiers exercices du fichier) — sémantique incertaine (pourrait
  encoder une variante d'exercice, un temps de maintien, ou être une erreur de
  saisie) : à investiguer et clarifier en Silver, ne pas supposer que c'est une
  erreur pure.
- `level`/`goal` : même remarque que `600k_fitness_summary` (listes Python
  stringifiées).
- Colonnes numériques (`week`, `day`, `number_of_exercises`, `sets`) stockées
  en `float` alors que ce sont conceptuellement des entiers — probablement dû à
  la présence de valeurs manquantes ailleurs dans le fichier complet qui a
  forcé pandas/l'export CSV original à passer la colonne en float.

## 3. `gym_members` (source : `gym_members/gym_members_exercise_tracking.csv`)

- **Lignes** : 973 (hors en-tête)
- **Encodage** : UTF-8
- **Doublons stricts** : 0

| Colonne                            | Type observé | Nulls |
|--------------------------------------|--------------|-------|
| `Age`                                 | int          | 0     |
| `Gender`                              | str (`Male`/`Female`) | 0 |
| `Weight (kg)`                         | float        | 0     |
| `Height (m)`                          | float        | 0     |
| `Max_BPM`                             | int          | 0     |
| `Avg_BPM`                             | int          | 0     |
| `Resting_BPM`                         | int          | 0     |
| `Session_Duration (hours)`            | float        | 0     |
| `Calories_Burned`                     | float        | 0     |
| `Workout_Type`                        | str          | 0     |
| `Fat_Percentage`                      | float        | 0     |
| `Water_Intake (liters)`               | float        | 0     |
| `Workout_Frequency (days/week)`       | int          | 0     |
| `Experience_Level`                    | int (échelle 1-3 observée) | 0 |
| `BMI`                                 | float        | 0     |

**Anomalies constatées** : aucune valeur nulle, aucun doublon strict sur
l'ensemble du fichier (dataset le plus "propre" des 3). Noms de colonnes
contenant espaces et parenthèses (ex. `Weight (kg)`) — à normaliser en Silver
pour un usage SQL/dbt plus simple, pas touché ici.

## 4. `weight_training` (source : `weight_training/weightlifting_721_workouts.csv`)

- **Lignes** : 9 932 (hors en-tête)
- **Encodage** : UTF-8
- **Doublons stricts** : 790 (scan complet)

| Colonne          | Type observé | Nulls |
|--------------------|--------------|-------|
| `Date`              | str, format `YYYY-MM-DD HH:MM:SS` | 0 |
| `Workout Name`      | str          | 0     |
| `Exercise Name`     | str          | 0     |
| `Set Order`         | int          | 0     |
| `Weight`            | int          | 0     |
| `Reps`              | int          | 0     |
| `Distance`          | int (0 pour la quasi-totalité des lignes — exercices de force, pas de cardio) | 0 |
| `Seconds`           | int (0 pour la quasi-totalité des lignes) | 0 |
| `Notes`             | str          | 9 925 / 9 932 (**99.9%**) |
| `Workout Notes`     | str          | 9 929 / 9 932 (**100.0%**, en pratique inexploitable) |

**Anomalies constatées (non corrigées ici)** :
- **790 lignes strictement dupliquées** sur 9 932 (≈8%) — à dédupliquer en
  Silver.
- `Notes` et `Workout Notes` sont quasi-intégralement vides : colonnes
  probablement inutilisables telles quelles, à évaluer (drop ou conservation)
  en Silver.
- `Distance`/`Seconds` valent 0 pour la quasi-totalité des lignes (dataset de
  musculation, pas d'endurance) — colonnes à faible valeur informative pour ce
  dataset mais pas nulles au sens strict, donc pas signalées comme anomalie
  "valeurs manquantes".

---

## Statut de licence (info disponible via l'API Kaggle)

| Dataset Kaggle | Slug | Licence déclarée (API `kaggle datasets metadata`) |
|---|---|---|
| 600K+ Fitness Exercise & Workout Program | `adnanelouardi/600k-fitness-exercise-and-workout-program-dataset` | **ODbL-1.0** (Open Data Commons Open Database License) |
| 721 Weight Training Workouts | `joep89/weightlifting` | **`unknown`** — Kaggle ne renvoie aucune licence déclarée par l'auteur du dataset. **À vérifier manuellement sur https://www.kaggle.com/datasets/joep89/weightlifting avant tout usage au-delà d'un contexte pédagogique/certification.** |

(Le dataset `gym_members` n'était pas dans le périmètre de vérification de
licence demandé pour cette étape.)
