# Script de démonstration FINAL — SafeLift (Jalons 1, 2 et 3)

> Ce document **remplace/consolide** [DEMO_SCRIPT_JALON2.md](./DEMO_SCRIPT_JALON2.md)
> (conservé tel quel pour l'historique, ne pas le supprimer) : il couvre
> l'intégralité du projet pour la soutenance finale — Jalon 1 (pipeline
> batch), Jalon 2 (streaming temps réel), Jalon 3 (nutrition + ML bonus +
> extension multi-profils). Voir [PROGRESS.md](../PROGRESS.md),
> [PROGRESS_JALON2.md](../PROGRESS_JALON2.md),
> [PROGRESS_JALON3.md](../PROGRESS_JALON3.md) et
> [PROGRESS_FINAL.md](../PROGRESS_FINAL.md) pour le détail technique
> complet. Toutes les durées/métriques indiquées ici sont des **mesures
> réelles**, jamais une estimation devinée — sources citées à chaque fois.
>
> **Comment utiliser ce document le jour J** : les phrases entre
> guillemets sont des EXEMPLES à adapter avec ses propres mots, pas un
> texte à réciter. Retenir les idées listées, pas la formulation exacte.

## 0. Quel dashboard présenter ? (recommandation explicite)

**Recommandation : présenter `dashboard-v2` (React, port 5173) en
principal, garder l'ancien dashboard (port 18000) ouvert dans un 2e onglet
comme filet de sécurité.**

Justification :
- `dashboard-v2` a désormais une **parité fonctionnelle confirmée** avec
  l'ancien (tous les widgets : silhouette, zones sensibles, logger séance,
  simulateur what-if, tendance, affluence SSE, nutrition, tendance
  prédictive ML) — vérifié réellement sur les 5 profils le 2026-07-11.
- Rendu visuel plus abouti (wireframe holographique, animations Framer
  Motion) — meilleur impact visuel pour un jury.
- **MAIS** `dashboard-v2` tourne via `npm run dev` (serveur Vite), un
  processus **non géré par Docker** (pas de `restart: unless-stopped`) —
  plus fragile qu'un service Docker si le terminal se ferme
  accidentellement. L'ancien dashboard, lui, tourne dans le stack Docker
  Compose (`docker compose ps` le montre, redémarre automatiquement en cas
  de crash).
- **D'où la recommandation "les deux ouverts, v2 en principal"** : bascule
  immédiate vers l'ancien dashboard (mêmes données, même API backend) si
  jamais le serveur Vite plante en pleine démo — aucune perte de contenu,
  juste une esthétique légèrement moins soignée en secours.

**Montrer explicitement les deux, brièvement, est aussi une option
valable** ("voici l'ancien dashboard qui reste la référence stable,
voici la migration React en cours vers un rendu plus poussé") — valorise
le travail d'itération si le temps de la soutenance le permet.

## 1. Ce que le projet démontre dans son ensemble (intro, ~30s)

- **Jalon 1** : pipeline batch complet Kaggle → Bronze → Silver → Gold
  (modèle en étoile dbt + `risk_score` déterministe) → Serving (API +
  dashboard), + Terraform/AWS S3/Athena + gouvernance RGPD + CI/CD.
- **Jalon 2 (Bloc 2, temps réel)** : streaming Kafka/Spark (affluence des
  salles) + boucle événementielle utilisateur complète (formulaire →
  Kafka → consumer → Postgres → recalcul dbt).
- **Jalon 3 (bonus)** : nutrition (API USDA réelle, formules standard
  documentées) + ML bonus (prédiction de tendance) + **extension récente à
  5 profils de démonstration réels** (au lieu d'1 seul), pour une
  démonstration plus riche.

## 2. Script chronométré (dashboard-v2 en principal)

| # | Action | Durée | Ce que ça montre |
|---|---|---|---|
| 1 | Ouvrir http://localhost:5173, montrer le sélecteur d'utilisateur en haut ("973 profils au total — 5 avec données réelles") | ~15s | **Nouveauté Jalon 3** : 5 profils réels distincts sélectionnables (9, 21, 34, 46, 83), pas un seul comme avant — historique réel réparti en blocs chronologiques. Changer 1-2 fois de profil, montrer que la silhouette/le score suivent à chaque fois (bug corrigé récemment, voir section 6). |
| 2 | Silhouette centrale + panneau "Zones sensibles" | ~15s | Score de risque déterministe (formule Jalon 1), zones colorées par niveau, wireframe holographique. |
| 3 | Panneau **"Simulateur what-if"** (colonne gauche) : choisir un exercice, charge/durée/reps/séries, cliquer "Simuler" | ~20s | **Réponse INSTANTANÉE** (pas de Kafka, pas d'attente) — bien insister sur ce contraste avec "Logger une séance" (étape 5). Montrer les 5 facteurs de la formule affichés en clair (transparence). |
| 4 | Section **"Affluence en direct"** | ~15s | Jauges qui bougent seules (SSE), preuve visuelle d'un flux Kafka → Spark → SSE actif en arrière-plan, indépendant du profil sélectionné. |
| 5 | **"Logger une séance"** : choisir un exercice, valeurs nettement différentes de d'habitude, annoncer AVANT de cliquer : *"Ça publie sur Kafka, un consumer va l'insérer puis déclencher un run dbt complet — entre 1 et 2 minutes selon la charge."* | ~15s + attente | Contraste explicite avec le what-if instantané (étape 3) : ici c'est une VRAIE écriture qui déclenche le pipeline complet. |
| 6 | Pendant l'attente : transition (voir section 3, réutilisée du Jalon 2) | ~1-2 min | Ne jamais rester silencieux. |
| 7 | Score mis à jour automatiquement, silhouette recolorée | — | Preuve que ce n'est pas un mock. |
| 8 | Section **Nutrition** | ~20s | **Avertissement éthique affiché en premier, avant les chiffres** — le lire ou le paraphraser explicitement (voir section 4). BMR/TDEE/besoin protéique, 8 aliments suggérés. |
| 9 | Section **"Tendance prédictive"** (bordure pointillée violette, badge EXPÉRIMENTAL) | ~20s | Bonus ML, jamais fusionné visuellement avec le risk_score déterministe. Mentionner les vraies métriques (voir section 4). |

**Durée totale (hors questions) : environ 4-5 minutes**, dont 1-2 minutes
d'attente productive.

## 3. Que faire pendant l'attente du recalcul (1 à 2 minutes)

**Inchangé depuis le Jalon 2** (voir DEMO_SCRIPT_JALON2.md section 3 pour
le détail complet) — Option A (expliquer l'architecture Kafka → consumer
→ dbt à l'oral) ou Option B (montrer le DAG `gold_dbt_run` dans l'UI
Airflow, `http://localhost:18089`, si un 2e écran est prêt).

**Complément Jalon 3** : si la question "et le score ML aussi ?" vient
naturellement, mentionner que `gold_dbt_run` déclenche automatiquement le
DAG `ml_scoring` juste après (`trigger_ml_scoring`, dernière task) — la
tendance prédictive se met donc aussi à jour avec le même recalcul, sans
mécanisme séparé.

## 4. Points à mentionner à l'oral sans attendre la question

- **Avertissement éthique nutrition (étape 8)** — à dire explicitement,
  ne jamais laisser sous-entendre que c'est un conseil personnalisé :
  > *"Ces chiffres sont des formules standard (Mifflin-St Jeor), pas une
  > recommandation médicale personnalisée — SafeLift ne remplace ni un
  > coach, ni un médecin, ni un diététicien. C'est écrit noir sur blanc
  > dans l'interface, avant même les chiffres."*
- **Tendance prédictive ML (étape 9)** — annoncer les vraies métriques
  actuelles avant qu'on les demande :
  > *"Le modèle retenu (RandomForest) a un RMSE de 11.17 sur le jeu de
  > test, contre 17.13 pour une baseline naïve qui se contenterait de
  > recopier le score actuel — soit environ 35% de réduction d'erreur.
  > C'est un bonus hors périmètre de certification, entraîné sur les 5
  > profils de démonstration disponibles, pas prêt pour une généralisation
  > à grande échelle."*
- **5 profils au lieu d'1 (étape 1)** — si l'occasion se présente
  naturellement :
  > *"On a étendu récemment la démo à 5 profils réels plutôt qu'un seul,
  > en répartissant l'historique réel en 5 blocs chronologiques distincts
  > — une démo plus riche, toujours documentée comme une hypothèse de
  > démonstration, pas une vraie identité liée aux données."*

## 5. Rappel du délai de recalcul (inchangé, toujours 1 à 2 minutes)

**Mesures réelles historiques : 70s et 110s** (Jalon 2). **Re-confirmé le
2026-07-11** sur 2 profils différents pendant le test de stabilité de
clôture du Jalon 3 : `user_id=21` en **52s**, `user_id=34` en **91s**
(2 déclenchements quasi simultanés, sans contention observée) — la
fourchette "1 à 2 minutes" reste la bonne formulation à donner au jury.

## 6. Rappel : bug de la silhouette figée (dashboard-v2), déjà corrigé

Si le jury a suivi le projet ou pose la question directement : la
silhouette de `dashboard-v2` restait figée sur `user_id=9` même en
changeant de profil (limite assumée puis bug une fois plusieurs profils
réels disponibles) — corrigé le 2026-07-11 (branchement sur le même
contexte partagé que les autres widgets), vérifié sur 7 changements de
profil consécutifs + confirmé de nouveau lors du test de parité de
clôture sur les 5 profils.

## 7. Plan de secours (fallback)

**Inchangé depuis le Jalon 2** (voir DEMO_SCRIPT_JALON2.md section 5,
grille de lecture `docker compose ps` complète) — **complément
dashboard-v2** : si `http://localhost:5173` ne répond pas (serveur Vite
arrêté), basculer immédiatement sur l'onglet `http://localhost:18000`
déjà ouvert (même backend, mêmes données, juste un rendu visuel
différent) — ne jamais essayer de relancer `npm run dev` en direct
devant le jury, ça prend du temps et n'apporte rien à ce stade.

## 8. Questions probables du jury — Jalon 1/2 (rappel, déjà répondues au Jalon 2)

Voir DEMO_SCRIPT_JALON2.md section 6 pour le détail complet (SSE vs
WebSocket, délai de recalcul, recommandation de créneau théorique,
résilience Kafka/Spark, correctif cœurs Spark, utilisateurs concurrents).

**Mise à jour de la réponse "Est-ce que ça marcherait avec plusieurs
utilisateurs simultanés ?"** — **désormais testé réellement**, pas
seulement une hypothèse d'architecture : 2 séances soumises sur 2 profils
différents (21 et 34) à ~1s d'intervalle le 2026-07-11, les 2 runs
`gold_dbt_run` se sont exécutés sans blocage ni erreur (52s et 91s),
`dbt_test` 99/99 après coup, `ml_scoring` a rafraîchi les prédictions des
5 profils en une seule passe consécutive. La limite structurelle
(dbt_run complet, pas incrémental) reste la même qu'avant, mais la
non-régression sous 2 déclenchements concurrents est désormais
**démontrée**, pas supposée.

## 9. Questions probables du jury — Jalon 3 spécifiquement

**"Pourquoi la baseline naïve reste-t-elle si compétitive (RMSE 17.13 vs
11.17) ?"**
> Parce que le risque de la semaine suivante est structurellement très
> corrélé au risque de la semaine courante — une heuristique "ça ressemble
> à la semaine d'avant" n'est pas absurde en soi pour ce type de série
> temporelle courte. Le ML gagne quand même ~35% de réduction d'erreur en
> exploitant en plus l'historique récent (lags) et le contexte de zone —
> un gain réel, mesuré honnêtement, pas artificiellement gonflé (les
> hyperparamètres étaient fixés avant l'évaluation sur le test set).

**"La section nutrition remplace-t-elle un vrai nutritionniste ?"**
> Non, explicitement pas — l'avertissement est affiché en premier élément
> de la section, jamais masqué, texte source unique de vérité renvoyé par
> l'API (`disclaimer`), jamais reformulé côté frontend. Ce sont des
> formules standard de la littérature sportive (Mifflin-St Jeor + facteur
> d'activité), appliquées automatiquement, pas une évaluation clinique.

**"Pourquoi avoir étendu à 5 profils plutôt que garder 1 seul ?"**
> Décision de démonstration : montrer que le pipeline fonctionne sur
> plusieurs individus réels distincts (poids/âge/genre différents), pas
> seulement un cas particulier. Toujours une hypothèse de démonstration
> assumée (aucune vraie clé de jointure entre `gym_members` et
> `weight_training`) — juste répartie sur 5 profils plutôt que concentrée
> sur 1, via un découpage chronologique CONTIGU (pas de mélange de dates,
> pour préserver la cohérence des facteurs de charge/récupération et des
> features ML).

**"Les métriques ML se sont-elles dégradées avec l'extension à 5
profils ? Pourquoi ?"**
> Oui, honnêtement rapporté : RMSE RandomForest passe de 9.12 (1 profil)
> à 11.17 (5 profils), soit +22.5% en absolu. Cause bien identifiée :
> chaque profil ne reçoit plus qu'environ 1/5e de l'historique calendaire
> continu, donc moins de contexte/lags disponibles par séquence — un
> compromis assumé entre richesse de la démo (5 individus réels) et
> longueur de séquence par individu. Le ML continue néanmoins de battre
> nettement la baseline (~35% de réduction contre ~38% avant l'extension)
> — la dégradation est réelle mais modérée, pas une régression qui
> invaliderait le résultat.

**"Le risque de fuite de données a-t-il été revérifié après
l'extension ?"**
> Oui, réellement, pas supposé stable : les 3 vérifications anti-fuite
> (chevauchement train/test, cohérence d'une ligne calculée manuellement,
> exclusion des lignes hors historique adjacent) ont toutes été
> ré-exécutées sur les nouveaux fichiers Parquet après l'extension,
> mêmes résultats qualitatifs qu'avant (voir `data/ml/ML_DATA_PREP.md`
> section 7).

**"Le droit à l'effacement RGPD fonctionne-t-il toujours avec 5 profils
demo au lieu d'1 ?"**
> Oui, re-testé réellement sur un des NOUVEAUX profils (`user_id=21`, pas
> le profil principal `user_id=9`) : dry-run puis exécution réelle avec
> le nouveau flag requis (`--i-understand-this-breaks-the-demo`, car 5
> profils portent désormais `is_weight_training_demo_user=true` au lieu
> d'1), suppression confirmée sur les 4 couches physiques, puis restaurée
> à partir d'une sauvegarde pour ne pas altérer le jeu de démo — 99/99
> tests dbt confirmés après restauration. Voir
> `docs/RGPD_GOVERNANCE.md` section 5.

**"L'export AWS S3/Athena est-il à jour avec ces 5 profils ?"**
> Oui, resynchronisé réellement le 2026-07-11 (7 tables, 12 422 lignes),
> requêtes Athena de validation confirmant `cnt=2170`/`distinct_users=5`
> sur `gold.fact_risk_score`, cohérent avec Postgres. Voir
> `terraform/AWS_LAB_CONSTRAINTS.md`.

## 10. Stabilité sous charge complète — vérifiée le 2026-07-11

Test réel de clôture du Jalon 3 (pas un dry-run) : tous les services
tournant simultanément (`gym-simulator`, `spark-streaming-gym`,
`dashboard`, `dashboard-v2`, streaming SSE actif) pendant **16 minutes**
(32 mesures/30s), avec 2 déclenchements réels de `gold_dbt_run` +
`ml_scoring` au milieu de la fenêtre (profils 21 et 34) :

- **`RestartCount` de tous les services stable sur toute la fenêtre**
  (aucune augmentation par rapport à la mesure de référence prise au
  début du test) — aucun crash en cascade sous charge combinée.
- **Allocation de cœurs Spark cohérente** (`coresused=1` en régime
  stable, `spark-streaming-gym` continue de tenir sa limite
  `spark.cores.max=1` fixée au Jalon 2) — les 2 runs `gold_dbt_run`
  déclenchés pendant la fenêtre se sont terminés sans blocage
  (52s et 91s).
- **`dbt test` : 99/99 PASS** avant et après les 2 déclenchements.

Voir [PROGRESS_FINAL.md](../PROGRESS_FINAL.md) pour le détail complet des
mesures brutes.
