# PROGRESS_FINAL.md — Résumé de clôture GLOBAL du projet SafeLift (3 jalons)

> Document de synthèse pour la soutenance RNCP36739 — récapitule les 3
> jalons (clos), pointe vers le détail technique complet de chacun
> ([PROGRESS.md](./PROGRESS.md), [PROGRESS_JALON2.md](./PROGRESS_JALON2.md),
> [PROGRESS_JALON3.md](./PROGRESS_JALON3.md), [CLAUDE.md](./CLAUDE.md)), et
> liste **explicitement tous les points encore ouverts** avant le jour J
> (section 4) — objectif : qu'aucun ne soit découvert en surprise pendant
> la soutenance. Rédigé le 2026-07-11, dernier commit à cette date :
> `9924542`.

## 1. Vue d'ensemble : ce que le projet démontre

SafeLift est un pipeline Data Engineering de bout en bout (Kaggle → Bronze
→ Silver → Gold → Serving) enrichi d'un flux temps réel (Kafka/Spark
Structured Streaming) et d'un bonus ML, réalisé pour la certification
RNCP36739 (M2 Data Engineering & IA). Cas d'usage : détection de zones à
risque de blessure musculo-squelettique à partir de l'historique
d'entraînement, avec un score de risque déterministe et documenté à
chaque étape.

## 2. Statut des 3 jalons

| Jalon | Contenu | Statut | Détail |
|---|---|---|---|
| **1** | Pipeline batch (Kaggle→Bronze→Silver→Gold→Serving) + Terraform/AWS S3/Athena + RGPD + CI/CD | ✅ **Clos** (2026-07-08) | [PROGRESS.md](./PROGRESS.md) |
| **2** | Streaming temps réel (Kafka/Spark : affluence salles + inputs utilisateur) | ✅ **Clos** (2026-07-09) | [PROGRESS_JALON2.md](./PROGRESS_JALON2.md) |
| **3** | Nutrition (API USDA) + ML bonus (tendance prédictive) + extension multi-profils | ✅ **Clos** (2026-07-11) | [PROGRESS_JALON3.md](./PROGRESS_JALON3.md) |

**Chaîne complète opérationnelle et testée en conditions réelles** :
Kaggle → Bronze → Silver → Gold (étoile + `risk_score` + nutrition + ML) →
Serving (2 dashboards) → AWS S3/Athena (pseudonymisé), avec déclenchement
en cascade automatique Bronze→Silver→Gold→ML en local (Airflow), et une
boucle événementielle temps réel complète (Kafka→consumer→dbt→ML) testée
sous charge concurrente réelle le 2026-07-11 (voir section 3).

## 3. Travail post-Jalon 3 "officiel" — extension et vérification finale (2026-07-11)

Réalisé le même jour, avant la clôture complète :

1. **Migration dashboard-v2 (React)** — 5 sous-étapes + correctif
   silhouette figée (bug réel trouvé et corrigé le 2026-07-11 : la
   silhouette n'était pas branchée sur le contexte partagé du sélecteur
   d'utilisateur). Parité fonctionnelle confirmée avec l'ancien dashboard.
2. **Extension multi-profils demo** — historique réel réparti sur 5
   profils `dim_user` distincts (au lieu d'1 seul), via 5 blocs
   chronologiques contigus. RGPD et ML re-vérifiés/recalculés réellement
   sur cette nouvelle réalité.
3. **Resynchronisation export AWS S3/Athena** — relancé réellement,
   validé via requêtes Athena réelles.
4. **Vérification finale de clôture** :
   - **Stabilité sous charge complète** : 17.7 minutes (> 15 requises),
     tous les services actifs simultanément, 2 séances réelles
     déclenchées sur 2 profils différents (21 et 34) quasi simultanément
     — `RestartCount` stable sur toute la fenêtre (0 nouveau redémarrage),
     allocation de cœurs Spark cohérente (`coresused=1` en régime
     stable), `dbt test` 99/99 avant/après. Détail complet :
     [PROGRESS_JALON3.md](./PROGRESS_JALON3.md#verification-finale--stabilite-sous-charge-complete-177-minutes).
   - **Parité des 5 profils confirmée sur les 2 dashboards** (silhouette,
     zones sensibles, what-if, nutrition, tendance ML) — scores
     réellement distincts par profil (preuve de données non mises en
     cache).
   - **Script de démo consolidé** : [docs/DEMO_SCRIPT_FINAL.md](./docs/DEMO_SCRIPT_FINAL.md)
     (couvre les 3 jalons, recommandation de dashboard à présenter,
     métriques ML actuelles, questions probables du jury Jalon 3).

## 4. Points encore ouverts avant la soutenance — liste exhaustive

**Aucun bloquant pour la certification** — tous documentés honnêtement
plutôt que masqués. À vérifier/traiter par Moulaye avant le 2026-07-13 :

| # | Point ouvert | Action requise | Bloquant ? |
|---|---|---|---|
| 1 | **Licence Kaggle du dataset `weight_training`** (`721 Weight Training Workouts`, `joep89/weightlifting`) reste `unknown` (aucune licence déclarée par l'auteur) | Vérifier manuellement avant la soutenance — voir `data/bronze/SCHEMA_NOTES.md` section "Statut de licence" | Non, mais à clarifier si le jury demande |
| 2 | **`user_id` (`dim_user`) reste un surrogate `row_number()` non stable** — toute pseudonymisation/effacement RGPD reste structurellement fragile face à un changement futur de volumétrie de `gym_members` | Amélioration future proposée (pas implémentée) : remplacer par une clé stable (hash d'un identifiant naturel, ou UUID persisté). Limite déjà documentée depuis le Jalon 1, non aggravée par l'extension multi-profils | Non |
| 3 | **Postgres local (`app-postgres`) non chiffré en transit** (`SHOW ssl` → `off`), connexions `psycopg2` non chiffrées | Limitation assumée (trafic confiné au réseau Docker interne/localhost) — proposition production déjà documentée : AWS RDS + `rds.force_ssl=1` (`docs/RGPD_GOVERNANCE.md`) | Non |
| 4 | **Purge automatique RGPD non implémentée** (politique de rétention écrite, pas de job de purge programmé) | Hors périmètre explicitement acté — à implémenter dans une itération future si demandé | Non |
| 5 | **Commentaire automatique de résumé de plan Terraform sur PR** (CI/CD) codé mais jamais déclenché par une vraie pull request (tests faits via `workflow_dispatch`/re-run uniquement) | S'exécutera réellement à la prochaine PR touchant `terraform/**` — comportement `skipped` observé cohérent avec la condition | Non |
| 6 | **Producteur Kafka "générique" de l'étape 2/6 originale (Jalon 1)** jamais traité en tant que tel | **Effectivement superseded** par le travail du Jalon 2 (simulateur d'affluence + inputs utilisateur temps réel, architecture équivalente ou plus riche) — item historique sans action supplémentaire nécessaire | Non |
| 7 | **`dashboard-v2` tourne via `npm run dev`** (pas géré par Docker, pas de `restart: unless-stopped`) | Recommandation actée : présenter `dashboard-v2` en principal, ancien dashboard (Docker, robuste) en filet de sécurité dans un 2e onglet — voir `docs/DEMO_SCRIPT_FINAL.md` section 0 | Non, mitigé par le plan de secours |
| 8 | **Vérification visuelle du dashboard (ancien, thème sombre/holographique)** | **Résolu** — captures d'écran réelles obtenues via Chrome local (playwright-core) sur plusieurs sessions récentes (ex. `parity-evidence-old-user34.png`, captures de la migration dashboard-v2), confirmant le rendu correct. L'ancienne mention "extension Chrome indisponible, vérification non faite" dans CLAUDE.md/PROGRESS_JALON3.md concernait uniquement l'outil d'automatisation navigateur intégré (toujours indisponible), pas la vérification visuelle elle-même (contournée avec succès via Chrome local) | Non, déjà traité |
| 9 | **RMSE ML absolu se dégrade avec l'extension multi-profils** (9.12→11.17 pour RandomForest) | Pas une action à mener — **honnêtement documenté et expliqué** (séquences plus courtes par profil), le ML continue de battre nettement la baseline. Réponse prête pour le jury, voir `docs/DEMO_SCRIPT_FINAL.md` section 9 | Non |
| 10 | **Bug historique de grain `fact_workout_session`** (3 runs `gold_dbt_run` échoués le 2026-07-10, avant l'extension multi-profils) | **Résolu** — cause root-causée (2 soumissions du même exercice le même jour) et corrigée pendant l'extension multi-profils, confirmé non-récurrent par les 2 runs réussis du test de stabilité du 2026-07-11 | Non, déjà traité |

## 5. Où trouver le détail technique complet

| Sujet | Fichier |
|---|---|
| Jalon 1 (pipeline batch, AWS, RGPD, CI/CD) | `PROGRESS.md`, `data/gold/GOLD_MODEL_DECISIONS.md`, `docs/RGPD_GOVERNANCE.md`, `docs/DATA_CATALOG.md`, `terraform/AWS_LAB_CONSTRAINTS.md` |
| Jalon 2 (streaming temps réel) | `PROGRESS_JALON2.md`, `docs/DEMO_SCRIPT_JALON2.md` |
| Jalon 3 (nutrition, ML, extension multi-profils) | `PROGRESS_JALON3.md`, `data/ml/ML_DATA_PREP.md`, `data/ml/ML_TRAINING_RESULTS.md` |
| Migration dashboard-v2 (React) | `CLAUDE.md` section "Migration dashboard-v2 (React)" |
| Décisions techniques transverses (toutes, datées, justifiées) | `CLAUDE.md` section "Decisions techniques prises (et pourquoi)" |
| **Script de démo pour la soutenance** | **`docs/DEMO_SCRIPT_FINAL.md`** |

## 6. Chiffres clés à retenir pour la soutenance

- **973 profils `dim_user`**, **5 avec données de séance réelles** (9, 21,
  34, 46, 83), **~2170 lignes** `fact_workout_session`/`fact_risk_score`.
- **Distribution `risk_level`** : Faible ~84%, Modéré ~15%, Élevé ~1.5%
  (32 lignes) — sur les données actuelles.
- **ML bonus** : RandomForest RMSE **11.17** vs baseline naïve **17.13**
  (**~35% de réduction d'erreur**), entraîné sur les 5 profils demo.
- **Délai de recalcul temps réel** : **1 à 2 minutes** (mesures réelles :
  52s, 70s, 91s, 110s selon la charge du système).
- **99/99 tests dbt PASS**, **12/12 services Docker `healthy`**,
  stabilité confirmée sur **17.7 minutes** sous charge complète avec 2
  déclenchements concurrents réels.
