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

**⚠️ MISE A JOUR (2026-07-11) : extension multi-profils.** Jusqu'au
2026-07-10, un SEUL utilisateur (`user_id=9`) possedait un historique
`fact_risk_score` exploitable — voir l'historique git de ce fichier pour
les chiffres exacts de cette periode. Depuis l'extension multi-profils
(`data/gold/GOLD_MODEL_DECISIONS.md` section 5), **5 profils `dim_user`
distincts** (`user_id` 9, 21, 34, 46, 83) possedent chacun un historique
reel exploitable, chacun sur un bloc chronologique CONTIGU distinct de
l'historique `weight_training` (aucun chevauchement de dates entre
profils). Les 968 autres profils `dim_user` restent sans seance reelle
rattachee — **toujours aucune generalisation A CE QUI EST HORS DE CES 5
PROFILS n'est possible**, mais le modele peut desormais apprendre sur un
signal partage entre PLUSIEURS individus reels distincts (poids corporel,
age, genre differents), ce qui n'etait pas le cas avant.

**Chiffres reels obtenus (execution du 2026-07-11, apres l'extension
multi-profils)** :

| Metrique | Valeur (2026-07-11) | Valeur avant l'extension (2026-07-09) |
|---|---|---|
| Lignes source (`gold.fact_risk_score`) | 2169 | 2166 |
| Utilisateurs distincts | **5** (`user_id` 9, 21, 34, 46, 83) | 1 (`user_id=9`) |
| Zones musculaires distinctes | 8 (`arms`, `back`, `chest`, `knee`, `legs`, `lower_back`, `shoulder`, `unknown`) | 8 (identique) |
| Lignes agregees (user, zone, semaine) — table complete | **821** | 814 |
| Plage de semaines (table complete) | 2015-10-19 -> 2026-07-06 (identique — les blocs par profil couvrent la MEME fenetre globale, repartie differemment) | 2015-10-19 -> 2026-07-06 |
| Lignes avec `lag_1` disponible | 637 / 821 | 652 / 814 |
| Lignes avec cible connue (labelisees) | **637 / 821** (184 exclues) | 652 / 814 (162 exclues) |

**Constat honnete** : le volume TOTAL de lignes labelisees baisse
legerement (637 vs 652, -2.3%) malgre 5x plus d'utilisateurs — chaque
profil ne recoit desormais qu'1/5e environ de l'historique calendaire
(un bloc plus court a moins de semaines consecutives disponibles pour
calculer des `lag_N` sans trou), alors qu'avant, `user_id=9` a lui seul
concentrait tout l'historique 2015-2018 en continu. C'est un compromis
EXPLICITE de cette extension (diversite de profils contre longueur de
sequence par profil), pas un effet de bord cache.

**Repartition des 637 lignes labelisees par zone** (tres inegale — a
rappeler comme limite, comparee a l'ancien etat mono-profil) :

| Zone | Lignes labelisees (2026-07-11) | Lignes labelisees (avant, mono-profil) |
|---|---|---|
| chest | 131 | 134 |
| back | 120 | 123 |
| knee | 103 | 105 |
| shoulder | 92 | 93 |
| arms | 75 | 77 |
| unknown | 48 | 50 |
| lower_back | 50 | 52 |
| **legs** | **18** | 18 (inchange, toujours la zone la plus rare) |

**Repartition des 637 lignes labelisees par profil demo** (nouveau,
n'existait pas avant l'extension — reparti relativement equitablement,
comme attendu vu la taille comparable des 5 blocs chronologiques) :

| `user_id` | Lignes labelisees |
|---|---|
| 9 | 137 |
| 21 | 132 |
| 34 | 110 |
| 46 | 127 |
| 83 | 131 |

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

⚠️ **MIS A JOUR (2026-07-11) apres l'extension multi-profils** :

| | Valeur reelle (2026-07-11) | Valeur avant l'extension |
|---|---|---|
| Semaines distinctes labelisees | 132 | 134 |
| Ratio test vise | 20% | 20% |
| Semaines en test | 26 (>= **2018-03-26**) | 27 (>= 2018-03-19) |
| Semaines en train | 106 | 107 |
| **Lignes train** | **487** | 491 |
| **Lignes test** | **150** | 161 |
| Plage train | 2015-10-19 -> 2018-03-19 | 2015-10-19 -> 2018-03-12 |
| Plage test | 2018-03-26 -> 2018-09-17 | 2018-03-19 -> 2018-09-17 |

**Date de coupure exacte : `2018-03-26`** (`2018-03-19` avant l'extension).
Verifie explicitement : `max(train.week_start_date) = 2018-03-19` <
`min(test.week_start_date) = 2018-03-26` (ecart d'exactement 1 semaine,
aucun chevauchement) — **le decalage de la coupure d'une semaine par
rapport a l'ancienne valeur est un simple effet de bord du split par
NOMBRE de semaines distinctes (20% de 132 semaines != 20% de 134
semaines), pas une anomalie.**

### Repartition train/test par zone (calcule directement sur les Parquet)

| Zone | Train (2026-07-11) | Test (2026-07-11) | Total | Total avant l'extension |
|---|---|---|---|---|
| chest | 105 | 26 | 131 | 134 |
| back | 95 | 25 | 120 | 123 |
| knee | 77 | 26 | 103 | 105 |
| shoulder | 72 | 20 | 92 | 93 |
| arms | 57 | 18 | 75 | 77 |
| unknown | 32 | 16 | 48 | 50 |
| lower_back | 35 | 15 | 50 | 52 |
| **legs** | **14** | **4** | **18** | 18 |

### Verdict de viabilite (seuil retenu : 20-30 lignes minimum pour qu'un entrainement ait un sens)

- **Modele UNIQUE poole sur les 8 zones** (`muscle_group` comme feature
  categorique) : **637 lignes labelisees au total (487 train / 150 test)**
  — legerement moins qu'avant (652/491/161) mais toujours largement
  au-dessus du seuil, viable.
- **Un modele PAR zone** (si c'etait l'approche retenue) : le constat
  reste **inchange par cette extension** — `chest`/`back`/`knee`/
  `shoulder`/`arms` restent tous au-dessus du seuil (>= 75 lignes
  labelisees, test >= 18). **`legs` (14 train / 4 test, 18 au total)
  reste EN DESSOUS du seuil**, exactement comme avant. `unknown` (32
  train / 16 test) reste limite, meme constat qu'avant (categorie
  fourre-tout, pas une vraie zone anatomique).
- **Consequence pour la sous-etape suivante** : le choix deja acte
  (modele unique poole, `muscle_group` en feature) reste valide et n'a
  pas besoin d'etre reconsidere suite a cette extension.

## 7. Verification anti-fuite (execution reelle, pas une relecture de code)

**RE-VERIFIE reellement le 2026-07-11** apres l'extension multi-profils
(pas juste suppose stable) :

1. **Aucun chevauchement de dates entre train et test** : confirme
   `max(train.week_start_date) = 2018-03-19` < `min(test.week_start_date)
   = 2018-03-26` (re-execute sur les nouveaux fichiers Parquet).
2. **Verification manuelle d'une ligne** (`arms`, semaine 2016-01-18,
   desormais rattachee a `user_id=9`, dans le bloc chronologique 1) :
   memes valeurs qu'avant l'extension (cette semaine tombe dans le bloc
   toujours assigne a `user_id=9`) — `lag_1_risk_score` `NULL`,
   `target_next_week_risk_score = 5.06`, inchange.
3. **Les lignes `week_start_date=2026-07-06` absentes de train/test** :
   **desormais 5 lignes** orphelines a cette date (vs 2 avant cette
   extension — plus de tests reels du formulaire "Logger une seance" ont
   ete effectues entre-temps, sur `user_id=9`, zones `arms`/`back`/
   `chest`/`knee`/`lower_back`), toutes confirmees `lag_1`/`target` =
   `NULL` dans `weekly_features_full.parquet`, et confirmees **absentes**
   de `train.parquet`/`test.parquet` (0 ligne >= 2020-01-01 trouvee dans
   les 2 fichiers) — exclues naturellement par la logique de lookup exact,
   sans filtre special-case, exactement comme avant l'extension.

## 8. Fichiers produits

**Chiffres mis a jour le 2026-07-11** (extension multi-profils) :

| Fichier | Contenu | Lignes (2026-07-11) | Lignes (avant) |
|---|---|---|---|
| `data/ml/weekly_features_full.parquet` | Table complete (avec `NULL`), transparence totale | 821 | 814 |
| `data/ml/train.parquet` | Jeu d'entrainement (labelise, semaines < 2018-03-26) | 487 | 491 |
| `data/ml/test.parquet` | Jeu de test (labelise, semaines >= 2018-03-26) | 150 | 161 |

Colonnes de `train.parquet`/`test.parquet` : `user_id`, `muscle_group`,
`week_start_date`, `risk_score_avg`, `charge_factor_avg`,
`volume_factor_avg`, `recup_factor_avg`, `duree_factor_avg`,
`session_count`, `lag_1_risk_score`, `lag_2_risk_score`,
`lag_3_risk_score`, `trend_vs_previous_week`,
`target_next_week_risk_score` (colonne cible — a exclure des features
d'entree au moment de l'entrainement, sous-etape suivante).

## 9. Limites a rappeler explicitement a la sous-etape suivante (entrainement)

**⚠️ Mises a jour le 2026-07-11 suite a l'extension multi-profils :**

- **PLUS mono-utilisateur, mais toujours limite a 5 profils de
  demonstration** : le modele apprend desormais sur `user_id` 9, 21, 34,
  46, 83 (au lieu du seul `user_id=9`) — un progres reel (signal partage
  entre plusieurs individus reels, poids/age/genre distincts), mais
  **aucune preuve de generalisation au-dela de ces 5 profils demo n'est
  possible** avec ces donnees (les 968 autres profils `dim_user` n'ont
  aucune seance reelle). Toujours a presenter comme une preuve de concept
  (Bloc 4 bonus) enrichie, pas comme un modele pret pour une production
  multi-utilisateurs generale.
- **Volume LEGEREMENT plus petit qu'avant** (compromis assume de cette
  extension, voir section 5) : 487 lignes d'entrainement (491 avant), 150
  de test (161 avant), reparties sur 8 zones tres inegalement (`legs` =
  18 lignes labelisees au total train+test, inchange). Un modele complexe
  (ex. deep learning) reste deraisonnable ici — un modele simple
  (regression lineaire, arbre peu profond) reste plus adapte a ce volume,
  voir `ML_TRAINING_RESULTS.md` pour la reevaluation complete.
- **Ecart de ~8 ans toujours non comble** : aucune tentative de "combler"
  ce trou par interpolation — desormais **5 lignes** a
  `week_start_date=2026-07-06` (contre 2 avant, plus de tests reels
  effectues entre-temps) restent presentes dans la table complete pour
  transparence, mais structurellement inutilisables pour cette
  sous-etape (documente, pas masque).
- **Historique reel confine a 2015-2018** (inchange) : train/test
  portent tous les deux sur cette meme fenetre historique globale (repartie
  differemment entre 5 profils desormais) — aucune notion de "donnees
  recentes" au sens calendaire actuel, uniquement au sens relatif de
  l'historique disponible.
