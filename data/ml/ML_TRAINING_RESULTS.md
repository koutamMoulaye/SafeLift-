# ML_TRAINING_RESULTS.md — Entrainement/evaluation (Jalon 3, sous-etape 4/6)

> Bloc 4 deja valide par GeoPort Intelligence — ce ML est un **BONUS** qui
> raffine `risk_score`, **PAS une exigence de certification**. Ce document
> couvre l'entrainement et l'evaluation d'un modele predisant le
> `risk_score` de la semaine suivante. Voir
> [ML_DATA_PREP.md](./ML_DATA_PREP.md) pour la preparation des donnees
> (sous-etape 3/6) et `data/gold/GOLD_MODEL_DECISIONS.md` pour la formule
> `risk_score` deterministe actuelle (Jalon 1).

## 1. Rappel du cadre (deja tranche, non renegocie ici)

Objectif : predire `target_next_week_risk_score` a partir de l'historique
(voir ML_DATA_PREP.md). **Decision deja actee** : un **SEUL modele poole**
sur les 8 zones musculaires (`muscle_group` encode en one-hot), **pas 8
modeles independants** — `legs` (18 lignes labelisees au total) et
`unknown` (50) sont structurellement trop petits pour un entrainement par
zone (voir ML_DATA_PREP.md section 6, verdict de viabilite).

Script : `scripts/train_risk_trend_model.py`, execute reellement dans le
conteneur `airflow-webserver` (memes dependances que le reste du projet,
`scikit-learn==1.5.2` ajoute a `airflow/requirements.txt`, image
reconstruite et verifiee).

## 2. Features, imputation et fuite de donnees

**Features d'entree** (11, identiques a `feature_columns` dans
`training_metrics.json`) : `muscle_group` (categorique, one-hot),
`risk_score_avg`, `charge_factor_avg`, `volume_factor_avg`,
`recup_factor_avg`, `duree_factor_avg`, `session_count`,
`lag_1_risk_score`, `lag_2_risk_score`, `lag_3_risk_score`,
`trend_vs_previous_week`. **Cible** : `target_next_week_risk_score`.

**Imputation des NULL des lags** : remplacement par **0**, appliquee
identiquement sur train et test — **aucune ligne supprimee** (aurait
reduit encore le train, deja a 491 lignes). 0 est un choix neutre
documente dans le code (`impute_lag_nulls()`) : une moyenne calculee sur
le train aurait ete une alternative valable, mais ajoute une complexite
(risque de fuite train->test si mal implementee) non justifiee ici vu le
volume de NULL concerne.

**Aucune fuite de donnees** : toutes les features viennent de la semaine
COURANTE ou de semaines STRICTEMENT PASSEES (lags), jamais de la semaine
cible elle-meme — heritage direct des garanties deja verifiees en
sous-etape 3/6 (`ML_DATA_PREP.md` section 7). Le **TEST set n'a servi
qu'a l'evaluation finale**, jamais a choisir un hyperparametre : aucune
recherche par validation croisee n'a ete effectuee (volume trop faible
pour un split train/validation supplementaire fiable), les
hyperparametres (`alpha=1.0` pour Ridge, `max_depth=4`/`n_estimators=100`
pour RandomForest) sont des valeurs par defaut raisonnables fixees et
documentees dans le code AVANT toute evaluation sur le test set.

## 3. Tableau comparatif (TEST set uniquement, 161 lignes)

Execution reelle du 2026-07-09 (`training_metrics.json`) :

| Modele | RMSE | MAE |
|---|---|---|
| **Baseline naive** (predit `risk_score_avg` courant tel quel) | 14.6866 | 11.5016 |
| **Ridge (lineaire)** | 9.2689 | 7.5479 |
| **RandomForest** (max_depth=4) | **9.1152** | **7.5362** |

**Les deux modeles ML battent nettement la baseline naive** (RMSE reduit
de ~37-38%, MAE reduit de ~34%) : predire "la semaine suivante ressemble a
la semaine courante" est une hypothese mesurablement moins bonne
qu'un modele qui integre l'historique recent (lags) et le contexte de la
zone musculaire.

**RandomForest est retenu** (`best_model` = `random_forest` dans
`training_metrics.json`) : RMSE legerement meilleur que Ridge (9.1152 vs
9.2689), MAE quasi identique (7.5362 vs 7.5479). L'ecart entre les deux
modeles ML reste toutefois **modeste** — pas une victoire ecrasante de
l'un sur l'autre, les deux constituent une amelioration reelle sur la
baseline.

## 4. Analyse d'importance des features — resultat honnete et nuance

### RandomForest (modele retenu) — `feature_importances_`

| Feature | Importance |
|---|---|
| **`lag_1_risk_score`** | **0.2563** |
| **`lag_2_risk_score`** | **0.2004** |
| `trend_vs_previous_week` | 0.0953 |
| `muscle_group_shoulder` | 0.0875 |
| `risk_score_avg` | 0.0791 |
| `volume_factor_avg` | 0.0521 |
| `muscle_group_chest` | 0.0512 |
| `muscle_group_knee` | 0.0427 |
| `lag_3_risk_score` | 0.0682 |
| (autres zones + facteurs) | < 0.03 chacun |

**`lag_1_risk_score` + `lag_2_risk_score` = 45.7% de l'importance totale**
— c'est **coherent avec l'hypothese attendue** : le risque de la semaine
suivante est avant tout proche de l'historique recent (semaines -1/-2),
pas du bruit. Le modele a appris un signal sense, pas une correlation
fortuite. `trend_vs_previous_week` (9.5%) renforce ce constat : la
DIRECTION du changement recent compte aussi, pas seulement le niveau.

### Ridge (lineaire) — coefficients (valeur absolue, top 5)

| Feature | Coefficient |
|---|---|
| `muscle_group_shoulder` | +6.6085 |
| `recup_factor_avg` | +5.7740 |
| `muscle_group_chest` | -5.3325 |
| `muscle_group_knee` | +5.1857 |
| `muscle_group_lower_back` | +5.1149 |

**⚠️ Constat different et documente honnetement** : pour Ridge, ce sont
les dummies **`muscle_group`** (l'identite de la zone) qui dominent, PAS
les lags (`lag_1_risk_score` = +0.099, `lag_2_risk_score` = +0.166 —
quasi negligeables face aux coefficients de zone). Ridge capture surtout
un **decalage moyen propre a chaque zone** (coherent avec
`dim_muscle.base_epidemiological_risk`, deja documente comme plus eleve
pour `shoulder`/`knee`/`lower_back` en Jalon 1) plutot qu'une vraie
dynamique temporelle. **`duree_factor_avg` a un coefficient et une
importance a 0 dans les DEUX modeles** — coherent avec un constat deja
documente en Jalon 1 (`GOLD_MODEL_DECISIONS.md` section 8) : ce facteur
est quasi toujours neutre sur ce dataset (`duration_seconds` fiable a 0%),
donc quasi sans variance a exploiter.

**Interpretation** : les deux modeles ont appris des choses differentes
mais toutes deux sensees — Ridge capture principalement "quelle zone
est-ce" (proxy du risque de base), RandomForest capture principalement
"qu'est-ce qui s'est passe recemment" (dynamique temporelle). Le second
est **plus proche de l'objectif reellement vise** (predire une TENDANCE,
pas juste re-deviner le niveau de base d'une zone) — un argument
supplementaire, au-dela du RMSE legerement meilleur, en faveur du choix
RandomForest.

## 5. Modele sauvegarde

`data/ml/model.pkl` (230 Ko, `joblib.dump`) contient :
- `pipeline` : le pipeline scikit-learn complet (`ColumnTransformer`
  one-hot + `RandomForestRegressor`), directement utilisable sur des
  donnees brutes (pas besoin de dupliquer l'encodage cote consommateur).
- `metadata` : date d'entrainement (`2026-07-09T22:06:11 UTC`), nom du
  modele (`random_forest`), colonnes de features/cible, tailles
  train/test, les 3 jeux de metriques, `beats_naive_baseline: true`.

`data/ml/training_metrics.json` : copie complete et lisible (metriques,
coefficients Ridge, importances RandomForest, metadonnees) — pour
inspection/reprise sans devoir desserialiser le `.pkl`.

## 6. Conclusion honnete sur la valeur ajoutee reelle de ce ML bonus

**Le ML apporte une amelioration mesurable et reelle sur ce jeu de
donnees** : RMSE reduit de ~37% par rapport a la baseline naive
(14.69 -> 9.12), avec un signal dominant (`lag_1`/`lag_2` a 45.7%
d'importance dans le modele retenu) qui est **exactement celui attendu**
d'un modele de tendance — pas une amelioration artificielle ou un
artefact.

**Mais cette conclusion doit etre lue avec les limites deja quantifiees
en sous-etape 3/6, qui restent pleinement valables ici** :
- **Mono-utilisateur** (`user_id=9` uniquement) : ce resultat prouve que
  le modele apprend un signal reel POUR CET UTILISATEUR PRECIS. Il ne
  prouve RIEN sur la capacite du modele a generaliser a d'autres profils
  — aucune donnee d'un autre utilisateur n'existe pour le verifier.
- **Volume test limite** (161 lignes) : un RMSE de 9.12 sur 161
  observations reste une estimation avec une marge d'incertitude non
  negligeable, pas une mesure de precision au sens d'un jeu de test a
  grande echelle.
- **Le gain Ridge vs RandomForest est modeste** (RMSE 9.27 vs 9.12) : le
  choix de RandomForest est defendable (RMSE legerement meilleur + signal
  plus interpretable/aligne sur l'objectif) mais **pas un ecart massif** —
  un jury attentif ne devrait pas y voir une demonstration de superiorite
  ecrasante d'un modele sur l'autre, plutot deux approches raisonnables
  aboutissant a un resultat proche.
- **Ce que ce resultat NE dit PAS** : que ce modele est pret pour un
  usage en production multi-utilisateurs, ni qu'il remplace la formule
  deterministe `risk_score` (qui reste la seule source de verite pour
  l'affichage temps reel du dashboard — voir CLAUDE.md, "toute evolution
  future vers du ML devra etre une etape explicitement identifiee, pas une
  modification silencieuse de `fact_risk_score.sql`"). C'est une preuve de
  concept honnete : sur les donnees disponibles, un modele simple
  entraine sur l'historique recent bat une heuristique naive, avec un
  signal appris qui a du sens.

**Verdict final : le ML bonus a une valeur reelle et demontrable ICI, mais
strictement bornee au perimetre de donnees disponible (un utilisateur, un
historique de 2015-2018).** Aucun chiffre n'a ete ajuste ou choisi apres
coup pour ameliorer artificiellement ce constat — les hyperparametres
etaient fixes avant l'evaluation sur le test set, et le resultat aurait
ete rapporte tel quel meme si la baseline avait gagne.
