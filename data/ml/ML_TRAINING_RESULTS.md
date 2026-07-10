# ML_TRAINING_RESULTS.md — Entrainement/evaluation (Jalon 3, sous-etape 4/6)

> Bloc 4 deja valide par GeoPort Intelligence — ce ML est un **BONUS** qui
> raffine `risk_score`, **PAS une exigence de certification**. Ce document
> couvre l'entrainement et l'evaluation d'un modele predisant le
> `risk_score` de la semaine suivante. Voir
> [ML_DATA_PREP.md](./ML_DATA_PREP.md) pour la preparation des donnees
> (sous-etape 3/6) et `data/gold/GOLD_MODEL_DECISIONS.md` pour la formule
> `risk_score` deterministe actuelle (Jalon 1).
>
> ⚠️ **RE-ENTRAINE le 2026-07-11** suite a l'extension multi-profils de la
> demonstration (5 profils `dim_user` reels au lieu d'1 seul — voir
> `GOLD_MODEL_DECISIONS.md` section 5 et `ML_DATA_PREP.md` section 5). Tous
> les chiffres ci-dessous sont ceux du RE-entrainement ; les valeurs de
> l'entrainement precedent (mono-utilisateur, 2026-07-09) sont conservees
> en colonne de comparaison la ou c'est pertinent, **jamais masquees**.

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
reduit encore le train, deja a 487 lignes). 0 est un choix neutre
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

## 3. Tableau comparatif (TEST set uniquement, 150 lignes)

**⚠️ RE-execute le 2026-07-11** (`training_metrics.json`, apres l'extension
multi-profils) — comparaison honnete avec l'execution precedente
(mono-utilisateur, 161 lignes de test) :

| Modele | RMSE (2026-07-11, 5 profils) | MAE (2026-07-11) | RMSE (2026-07-09, 1 profil) | MAE (2026-07-09) |
|---|---|---|---|---|
| **Baseline naive** (predit `risk_score_avg` courant tel quel) | 17.1276 | 13.4344 | 14.6866 | 11.5016 |
| **Ridge (lineaire)** | 11.3157 | 9.0642 | 9.2689 | 7.5479 |
| **RandomForest** (max_depth=4) | **11.1687** | **9.0308** | 9.1152 | 7.5362 |

**Constat honnete, pas masque : les 3 RMSE/MAE se DEGRADENT (augmentent)
apres l'extension multi-profils** — RandomForest passe de 9.12 a 11.17
(+22.5%), Ridge de 9.27 a 11.32 (+22.1%), et meme la baseline naive se
degrade (14.69 -> 17.13, +16.6%). **Ceci est coherent avec l'hypothese
anticipee dans `ML_DATA_PREP.md`** : chaque profil ne recoit plus qu'un
bloc chronologique plus court (~1/5e de l'historique total contre
l'integralite avant), donc moins de contexte/lags reels disponibles par
sequence, ET une variance inter-individus desormais presente (5 personnes
physiologiquement differentes) que le mono-utilisateur ne pouvait pas
produire.

**Les deux modeles ML battent TOUJOURS nettement la baseline naive**
(RMSE reduit de ~34-35%, MAE reduit de ~33-33.5%) — legerement MOINS bien
qu'avant (~37-38% de reduction RMSE), mais l'amelioration relative reste
substantielle et de meme nature :

| Modele | Reduction RMSE vs baseline (2026-07-11) | Reduction RMSE vs baseline (2026-07-09) |
|---|---|---|
| Ridge | 33.9% | 36.9% |
| RandomForest | 34.8% | 37.9% |

**RandomForest est de nouveau retenu** (`best_model` = `random_forest`
dans `training_metrics.json`) : RMSE legerement meilleur que Ridge
(11.1687 vs 11.3157), MAE quasi identique (9.0308 vs 9.0642) — memes
proportions relatives qu'avant l'extension, l'ecart entre les deux
modeles ML reste **modeste**, ni plus ni moins qu'avant.

## 4. Analyse d'importance des features — resultat honnete et nuance

### RandomForest (modele retenu) — `feature_importances_`

**⚠️ Changement notable par rapport a l'entrainement mono-utilisateur** :

| Feature | Importance (2026-07-11) | Importance (2026-07-09) |
|---|---|---|
| `muscle_group_shoulder` | **0.1807** | 0.0875 |
| **`lag_1_risk_score`** | **0.1600** | 0.2563 |
| `muscle_group_knee` | 0.1322 | (< 0.043) |
| **`lag_2_risk_score`** | **0.1141** | 0.2004 |
| `risk_score_avg` | 0.0976 | 0.0791 |
| `trend_vs_previous_week` | 0.0898 | 0.0953 |
| `muscle_group_lower_back` | 0.0688 | (< 0.03) |
| `session_count` | 0.0537 | (< 0.03) |
| `lag_3_risk_score` | 0.0478 | 0.0682 |
| `volume_factor_avg` | 0.0477 | 0.0521 |
| (autres zones + facteurs) | < 0.01 chacun | < 0.03 chacun |

**Constat honnete : le signal temporel pese desormais MOINS lourd, en
proportion, que le signal "identite de la zone"** — `lag_1 + lag_2 =
27.4%` (contre **45.7%** avant l'extension), alors que
`muscle_group_shoulder + muscle_group_knee + muscle_group_lower_back =
38.2%` a lui seul depasse desormais la somme des 2 principaux lags. En
elargissant a TOUTES les features temporelles (`lag_1` + `lag_2` +
`lag_3` + `trend_vs_previous_week` = **41.2%**), le signal temporel
reste malgre tout **legerement superieur** au signal de zone statique
(38.2%) — mais l'ecart s'est nettement resserre par rapport a
l'entrainement precedent.

**Interpretation honnete de ce changement** : avec 5 profils reels
distincts au lieu d'1 seul, le modele dispose desormais d'une vraie
variance INTER-INDIVIDUS a exploiter (5 corps physiologiquement
differents, 5 blocs chronologiques distincts) — il est donc logique
qu'une partie du pouvoir predictif se deplace vers "quelle zone/quel
contexte de base" plutot que de reposer presque exclusivement sur "quelle
etait la valeur i semaines avant POUR CET UNIQUE UTILISATEUR". Ce n'est
pas necessairement une degradation qualitative du modele — c'est un
signal different, tout aussi legitime, mais qui **affaiblit legerement
l'argument** "RandomForest est prefere parce qu'il capture surtout une
dynamique temporelle" deja avance pour le precedent entrainement (voir
plus bas, conclusion revisee en consequence).

### Ridge (lineaire) — coefficients (valeur absolue, top 5)

| Feature | Coefficient (2026-07-11) | Coefficient (2026-07-09) |
|---|---|---|
| `muscle_group_shoulder` | +10.4531 | +6.6085 |
| `muscle_group_knee` | +7.5989 | +5.1857 |
| `muscle_group_unknown` | -7.2112 | (non classe dans le top 5 avant) |
| `muscle_group_legs` | -6.6471 | (non classe dans le top 5 avant) |
| `muscle_group_lower_back` | +4.9930 | +5.1149 |

**Constat inchange par rapport a l'entrainement precedent** : pour Ridge,
ce sont toujours les dummies **`muscle_group`** (l'identite de la zone)
qui dominent, PAS les lags (`lag_1_risk_score` = +0.026,
`lag_2_risk_score` = +0.135 — quasi negligeables face aux coefficients de
zone, memes ordres de grandeur qu'avant). Ridge capture surtout un
**decalage moyen propre a chaque zone** (coherent avec
`dim_muscle.base_epidemiological_risk`, deja documente comme plus eleve
pour `shoulder`/`knee`/`lower_back` en Jalon 1) plutot qu'une vraie
dynamique temporelle — ce constat, lui, **n'a pas change** avec
l'extension multi-profils. **`duree_factor_avg` a un coefficient et une
importance a 0 dans les DEUX modeles** — coherent avec un constat deja
documente en Jalon 1 (`GOLD_MODEL_DECISIONS.md` section 8) : ce facteur
est quasi toujours neutre sur ce dataset (`duration_seconds` fiable a 0%),
donc quasi sans variance a exploiter.

**Interpretation, revisee honnetement suite a ce re-entrainement** : les
deux modeles ont toujours appris des choses differentes — Ridge capture
principalement "quelle zone est-ce" (proxy du risque de base),
RandomForest capture un **melange plus equilibre** entre "quelle
zone/quel contexte" et "qu'est-ce qui s'est passe recemment" (contre une
dominance nette du second avant cette extension). L'argument
"RandomForest est plus proche de l'objectif reellement vise (predire une
TENDANCE)" **reste valable mais moins tranche qu'avant** — le choix de
RandomForest demeure defendable (RMSE legerement meilleur + signal
temporel toujours legerement majoritaire, 41.2% vs 38.2%), mais ce
n'est plus l'argument aussi net qu'avec 45.7% face a une poignee de
coefficients de zone epars.

## 5. Modele sauvegarde

`data/ml/model.pkl` (`joblib.dump`) contient :
- `pipeline` : le pipeline scikit-learn complet (`ColumnTransformer`
  one-hot + `RandomForestRegressor`), directement utilisable sur des
  donnees brutes (pas besoin de dupliquer l'encodage cote consommateur).
- `metadata` : date d'entrainement (**`2026-07-10T23:07:51 UTC`, re-entraine
  suite a l'extension multi-profils** — `2026-07-09T22:06:11 UTC`
  precedemment), nom du modele (`random_forest`), colonnes de
  features/cible, tailles train/test (487/150, contre 491/161 avant), les
  3 jeux de metriques, `beats_naive_baseline: true` (inchange).

`data/ml/training_metrics.json` : copie complete et lisible (metriques,
coefficients Ridge, importances RandomForest, metadonnees) — pour
inspection/reprise sans devoir desserialiser le `.pkl`.

## 6. Conclusion honnete sur la valeur ajoutee reelle de ce ML bonus

**⚠️ Conclusion revisee le 2026-07-11 suite au re-entrainement
multi-profils — comparee honnetement a la conclusion precedente, rien
n'est cache.**

**Le ML continue d'apporter une amelioration mesurable et reelle sur ce
jeu de donnees, mais un peu moins marquee qu'avant** : RMSE reduit de
~34.8% par rapport a la baseline naive (17.13 -> 11.17), contre ~37.9%
avant l'extension (14.69 -> 9.12). Le signal temporel
(`lag_1`+`lag_2`+`lag_3`+`trend` = 41.2% d'importance) reste le plus
important dans le modele retenu, mais talonne de pres par le signal
"identite de zone" (38.2%) — l'ecart entre les deux s'est nettement
resserre (voir section 4). **Ce n'est toujours pas une amelioration
artificielle ou un artefact**, mais l'argument en faveur de RandomForest
est objectivement **moins tranche** qu'avant cette extension.

**Cette conclusion doit etre lue avec les limites suivantes, mises a jour
suite a l'extension multi-profils** :
- **PLUS mono-utilisateur, mais toujours limite a 5 profils demo**
  (`user_id` 9, 21, 34, 46, 83) : ce resultat prouve desormais que le
  modele apprend un signal reel PARTAGE ENTRE CES 5 PROFILS PRECIS — un
  progres reel par rapport a l'ancienne preuve mono-individu, mais qui ne
  prouve toujours RIEN sur la capacite du modele a generaliser AU-DELA de
  ces 5 profils (les 968 autres profils `dim_user` n'ont aucune donnee
  exploitable).
- **Volume test LEGEREMENT plus petit** (150 lignes contre 161 avant) :
  un RMSE de 11.17 sur 150 observations reste une estimation avec une
  marge d'incertitude non negligeable, encore moins negligeable qu'avant
  vu le volume en (leger) recul.
- **RMSE/MAE ABSOLUS se degradent sur les 3 modeles** (baseline incluse) :
  +22.5% pour RandomForest, +22.1% pour Ridge, +16.6% pour la baseline —
  **attribue avec confiance raisonnable a la reduction de la longueur de
  sequence disponible par profil** (chaque profil ne recoit plus qu'un
  bloc chronologique ~5x plus court), pas a une erreur d'implementation
  (memes hyperparametres, meme methodologie, seule la donnee source a
  change — verifie par la re-execution identique du pipeline).
- **Le gain Ridge vs RandomForest reste modeste** (RMSE 11.32 vs 11.17,
  ecart quasi identique en proportion a avant) : le choix de RandomForest
  reste defendable mais, comme deja note en section 4, l'argument
  qualitatif ("capture surtout une dynamique temporelle") s'est affaibli.
- **Ce que ce resultat NE dit TOUJOURS PAS** : que ce modele est pret pour
  un usage en production multi-utilisateurs generale, ni qu'il remplace
  la formule deterministe `risk_score` (qui reste la seule source de
  verite pour l'affichage temps reel du dashboard — voir CLAUDE.md,
  "toute evolution future vers du ML devra etre une etape explicitement
  identifiee, pas une modification silencieuse de `fact_risk_score.sql`").
  C'est toujours une preuve de concept honnete, desormais un cran plus
  large (5 individus reels plutot qu'1) mais avec un signal legerement
  plus bruite en consequence.

**Verdict final, revise : le ML bonus conserve une valeur reelle et
demontrable, desormais sur 5 profils demo reels distincts plutot qu'un
seul — un progres net en termes de perimetre de preuve, mais avec des
metriques absolues legerement moins bonnes, honnetement rapportees comme
telles (pas dissimulees derriere le progres de perimetre). Toujours
strictement borne au perimetre de donnees disponible (5 profils demo, un
historique de 2015-2018 reparti entre eux).** Aucun chiffre n'a ete
ajuste ou choisi apres coup pour ameliorer artificiellement ce constat —
les hyperparametres etaient fixes avant l'evaluation sur le test set
(inchanges par rapport a l'entrainement precedent), et le resultat aurait
ete rapporte tel quel meme si la baseline avait gagne, ou si les
metriques s'etaient degradees plus fortement.
