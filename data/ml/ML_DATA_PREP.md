# ML_DATA_PREP.md — Preparation des donnees ML (Jalon 3, sous-etape 3/6)

> Bloc 4 deja valide par GeoPort Intelligence — ce ML est un **BONUS** qui
> raffine `risk_score`, **PAS une exigence de certification**. Ce document
> couvre UNIQUEMENT la preparation des donnees (`scripts/prepare_ml_features.py`)
> — **aucun entrainement de modele ici**, ce sera une sous-etape ulterieure.
> Voir [PROGRESS_JALON3.md](../../PROGRESS_JALON3.md) pour le suivi
> d'avancement et `data/gold/GOLD_MODEL_DECISIONS.md` pour le detail de la
> formule `risk_score` deterministe deja en place (Jalon 1).

## 1. Objectif (deja tranche, rappel)

Predire le `risk_score` de la **semaine suivante**, par (utilisateur, zone
musculaire), a partir de l'historique des semaines precedentes — **PAS**
reapprendre la formule deterministe actuelle de `fact_risk_score.sql`. Un
modele qui ne ferait que recopier `base_zone x charge_factor x
volume_factor x recup_factor x duree_factor` n'aurait rien appris (fuite
de donnees triviale) : la cible est deliberement decalee d'une semaine
dans le futur, une information que la formule deterministe elle-meme ne
connait pas au moment de la semaine courante.

## 2. Choix : script Python plutot qu'un modele dbt

`scripts/prepare_ml_features.py` (pas un modele dbt) — raisons :
- La sortie attendue est des **fichiers Parquet train/test** (convention
  data science standard pour la suite du pipeline ML), pas une table
  Postgres consommee par le dashboard — dbt materialise des tables SQL,
  pas des artefacts fichier.
- Le **split temporel train/test** est une decision de consommation des
  donnees (pas une transformation de schema) : dbt n'a pas de notion
  native de "separer les lignes les plus recentes dans un fichier
  different".
- Les **lags a la semaine calendaire EXACTE** (voir section 4) sont plus
  naturels a exprimer et a verifier ligne par ligne en pandas qu'en SQL
  pur pour ce volume de donnees (quelques centaines de lignes, aucun
  besoin de la scalabilite d'un moteur SQL).

Le script tourne **hors dbt**, lit `gold.fact_risk_score` (deja calculee
par dbt) et n'effectue **aucun calcul de risque** — uniquement de
l'agregation temporelle et du decalage (lag/target). Execute
manuellement a ce stade (pas encore orchestre par un DAG Airflow — hors
perimetre de cette sous-etape) :

```bash
docker compose exec airflow-webserver python3 /opt/airflow/scripts/prepare_ml_features.py
```

## 3. Grain et agregation hebdomadaire

**Grain de la table de base** : `(user_id, muscle_group, week_start_date)`
— une ligne par combinaison ayant EU REELLEMENT au moins une seance
(`GROUP BY` sur des lignes existantes de `fact_risk_score` : par
construction, aucune semaine "a zero" n'est inventee, une semaine sans
seance ne produit tout simplement aucune ligne).

`week_start_date` = **la MEME colonne `gold.dim_date.week_start_date`**
deja utilisee en interne par `fact_risk_score.sql` pour calculer
`charge_factor`/`volume_factor` (voir `GOLD_MODEL_DECISIONS.md` section
8) — coherence garantie avec la definition de "semaine" deja actee dans
le projet (semaine ISO, lundi au dimanche).

Colonnes agregees (moyenne de la semaine) :

| Colonne | Calcul |
|---|---|
| `risk_score_avg` | `AVG(risk_score)` |
| `charge_factor_avg` | `AVG(charge_factor)` |
| `volume_factor_avg` | `AVG(volume_factor)` |
| `recup_factor_avg` | `AVG(recup_factor)` |
| `duree_factor_avg` | `AVG(duree_factor)` |
| `session_count` | `COUNT(*)` (nombre de lignes `fact_risk_score` agregees cette semaine-la) |

## 4. Feature engineering — AUCUNE fuite temporelle

**Principe central, verifie explicitement (section 7)** : chaque
`lag_N_risk_score` recherche la valeur a la semaine calendaire **EXACTE**
`week_start_date - N semaines`, meme utilisateur/zone. Si cette semaine
precise n'a **aucune** ligne dans les donnees (aucune seance ce jour-la
pour cette zone), la feature est `NULL` — **jamais interpolee, jamais
remplacee par la derniere valeur observee plus loin dans le passe** (ce
qui inventerait une regularite hebdomadaire qui n'existe pas dans un
historique reel, forcement irregulier).

| Feature | Definition | Regarde le futur ? |
|---|---|---|
| `lag_1_risk_score` | `risk_score_avg` a `semaine - 1` (exact) | Non |
| `lag_2_risk_score` | `risk_score_avg` a `semaine - 2` (exact) | Non |
| `lag_3_risk_score` | `risk_score_avg` a `semaine - 3` (exact) | Non |
| `trend_vs_previous_week` | `risk_score_avg` (courant) `-` `lag_1_risk_score` | Non (NULL si `lag_1` est NULL) |
| `target_next_week_risk_score` | `risk_score_avg` a `semaine + 1` (exact) | **OUI — c'est la CIBLE, jamais une feature d'entree** |

`target_next_week_risk_score` est **exclue de tout calcul de feature de la
semaine courante** — elle n'est utilisee QUE comme colonne cible (label),
jamais comme entree d'un autre calcul.

## 5. Taille reelle du jeu de donnees — TRANSPARENCE COMPLETE

**⚠️ Limitation majeure, a rappeler explicitement a l'etape suivante
(entrainement) : un SEUL utilisateur (`user_id=9`, le "demo user" deja
documente en Jalon 1) possede un historique `fact_risk_score` exploitable.**
Les 972 autres profils `dim_user` n'ont aucune seance reelle rattachee
(voir `GOLD_MODEL_DECISIONS.md` section 5) — **aucune generalisation
inter-utilisateurs n'est possible avec ces donnees**, le modele de
l'etape suivante ne pourra apprendre qu'un pattern **specifique a cet
unique utilisateur**.

**Chiffres reels obtenus (execution du 2026-07-09)** :

| Metrique | Valeur |
|---|---|
| Lignes source (`gold.fact_risk_score`) | 2166 |
| Utilisateurs distincts | **1** (`user_id=9`) |
| Zones musculaires distinctes | 8 (`arms`, `back`, `chest`, `knee`, `legs`, `lower_back`, `shoulder`, `unknown`) |
| Lignes agregees (user, zone, semaine) — table complete | **814** |
| Plage de semaines (table complete) | 2015-10-19 -> 2026-07-06 |
| Lignes avec `lag_1` disponible | 652 / 814 |
| Lignes avec cible connue (labelisees) | **652 / 814** (162 exclues, pas de semaine suivante connue) |

**Repartition des 652 lignes labelisees par zone** (tres inegale — a
rappeler comme limite) :

| Zone | Lignes labelisees |
|---|---|
| chest | 134 |
| back | 123 |
| knee | 105 |
| shoulder | 93 |
| arms | 77 |
| lower_back | 52 |
| unknown | 50 |
| **legs** | **18** (zone la plus rare, tres peu de signal pour l'entrainement) |

### ⚠️ Ecart de 8 ans dans l'historique — constat honnete, pas masque

L'historique reel **ininterrompu** couvre **2015-10-19 a 2018-09-24**
(donnees Kaggle `weight_training`, rattachees au demo user — voir Jalon 1).
Les 2 lignes a `week_start_date=2026-07-06` proviennent des tests reels du
formulaire "Logger une seance" (Jalon 2, sous-etapes 3/5 et 5/5) — un
ecart de **~8 ans** separe ces 2 lignes du reste de l'historique. Ces 2
lignes **n'ont ni `lag_1` ni cible disponible** (aucune semaine
calendaire adjacente dans les donnees) : elles apparaissent dans
`weekly_features_full.parquet` (transparence totale) mais sont
**automatiquement exclues** du jeu labelise et de `train.parquet`/
`test.parquet` — **confirme programmatiquement**, pas juste suppose (voir
section 7).

## 6. Split train/test TEMPOREL

Split sur les **semaines distinctes du jeu LABELISE** (pas sur le nombre
brut de lignes, qui serait deforme par les zones a beaucoup de lignes) :
les 20% de semaines les plus recentes vont en test, tout ce qui precede va
en train. **Aucun melange aleatoire.**

| | Valeur reelle |
|---|---|
| Semaines distinctes labelisees | 134 |
| Ratio test vise | 20% |
| Semaines en test | 27 (>= **2018-03-19**) |
| Semaines en train | 107 |
| **Lignes train** | **491** |
| **Lignes test** | **161** |
| Plage train | 2015-10-19 -> 2018-03-12 |
| Plage test | 2018-03-19 -> 2018-09-17 |

**Date de coupure exacte : `2018-03-19`.** Verifie explicitement :
`max(train.week_start_date) = 2018-03-12` < `min(test.week_start_date) =
2018-03-19` (ecart d'exactement 1 semaine, aucun chevauchement).

### Repartition train/test par zone (calcule directement sur les Parquet)

| Zone | Train | Test | Total |
|---|---|---|---|
| chest | 107 | 27 | 134 |
| back | 96 | 27 | 123 |
| knee | 78 | 27 | 105 |
| shoulder | 72 | 21 | 93 |
| arms | 57 | 20 | 77 |
| unknown | 32 | 18 | 50 |
| lower_back | 36 | 16 | 52 |
| **legs** | **13** | **5** | **18** |

### Verdict de viabilite (seuil retenu : 20-30 lignes minimum pour qu'un entrainement ait un sens)

- **Modele UNIQUE poole sur les 8 zones** (`muscle_group` comme feature
  categorique) : **652 lignes labelisees au total (491 train / 161 test)**
  — largement au-dessus du seuil, viable.
- **Un modele PAR zone** (si c'est l'approche retenue a la sous-etape
  suivante) : `chest`/`back`/`knee`/`shoulder`/`arms` restent tous
  au-dessus du seuil (>= 77 lignes labelisees, test >= 20). **`legs` (13
  train / 5 test, 18 au total) est EN DESSOUS du seuil** — un test set de
  5 lignes ne permet aucune evaluation fiable, ce volume est **trop
  faible pour qu'un entrainement par zone ait un sens sur `legs`**.
  `unknown` (32 train / 18 test) est limite (juste au-dessus du seuil en
  train, en dessous en test) — a traiter avec prudence d'autant que
  `unknown` est une categorie fourre-tout (exercices non matches par le
  pipeline fuzzy matching de Jalon 1, pas une vraie zone anatomique), pas
  un signal biomecanique coherent a modeliser en soi.
- **Consequence pour la sous-etape suivante** : privilegier un modele
  unique poole (avec `muscle_group` en feature) plutot que 8 modeles
  independants, sauf a accepter d'exclure/fusionner `legs` et `unknown`
  d'une approche par-zone.

## 7. Verification anti-fuite (execution reelle, pas une relecture de code)

Trois verifications executees pour de vrai sur les fichiers Parquet
produits (voir aussi PROGRESS_JALON3.md pour le detail de la commande) :

1. **Aucun chevauchement de dates entre train et test** : confirme
   `max(train.week_start_date) < min(test.week_start_date)`.
2. **Verification manuelle d'une ligne** (`arms`, semaine 2016-01-18) :
   `lag_1_risk_score` attendu `NULL` (aucune ligne a 2016-01-11 dans les
   donnees brutes — confirme, requete directe renvoie un resultat vide) ;
   `target_next_week_risk_score` attendu = valeur reelle a 2016-01-25
   (`5.06`) — confirme, correspond exactement a la ligne brute trouvee a
   cette date.
3. **Les 2 lignes 2026-07-06 (test Jalon 2) absentes de train/test** :
   confirme (`lag_1`/`target` tous deux `NULL` pour ces 2 lignes dans
   `weekly_features_full.parquet`, et absence totale dans `train.parquet`/
   `test.parquet`).

## 8. Fichiers produits

| Fichier | Contenu | Lignes | Taille |
|---|---|---|---|
| `data/ml/weekly_features_full.parquet` | Table complete (avec `NULL`), transparence totale | 814 | 54 Ko |
| `data/ml/train.parquet` | Jeu d'entrainement (labelise, semaines < 2018-03-19) | 491 | 39 Ko |
| `data/ml/test.parquet` | Jeu de test (labelise, semaines >= 2018-03-19) | 161 | 21 Ko |

Colonnes de `train.parquet`/`test.parquet` : `user_id`, `muscle_group`,
`week_start_date`, `risk_score_avg`, `charge_factor_avg`,
`volume_factor_avg`, `recup_factor_avg`, `duree_factor_avg`,
`session_count`, `lag_1_risk_score`, `lag_2_risk_score`,
`lag_3_risk_score`, `trend_vs_previous_week`,
`target_next_week_risk_score` (colonne cible — a exclure des features
d'entree au moment de l'entrainement, sous-etape suivante).

## 9. Limites a rappeler explicitement a la sous-etape suivante (entrainement)

- **Mono-utilisateur** : le modele n'apprendra un pattern QUE pour
  `user_id=9`. Aucune preuve de generalisation a d'autres profils n'est
  possible avec ces donnees — a presenter comme une preuve de concept
  (Bloc 4 bonus), pas comme un modele pret pour production multi-utilisateurs.
- **Volume tres petit pour un modele ML** : 491 lignes d'entrainement,
  161 de test, reparties sur 8 zones tres inegalement (`legs` = 18 lignes
  labelisees au total train+test). Un modele complexe (ex. deep learning)
  serait deraisonnable ici — un modele simple (regression lineaire,
  arbre peu profond) est plus adapte a ce volume, a trancher a l'etape
  suivante.
- **Ecart de 8 ans non comble** : aucune tentative de "combler" ce trou
  par interpolation — les 2 lignes 2026 restent presentes dans la table
  complete pour transparence, mais structurellement inutilisables pour
  cette sous-etape (documente, pas masque).
- **Historique reel confine a 2015-2018** : train/test portent tous les
  deux sur cette meme fenetre historique — aucune notion de "donnees
  recentes" au sens calendaire actuel, uniquement au sens relatif de
  l'historique disponible.
