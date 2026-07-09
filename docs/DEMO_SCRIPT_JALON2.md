# Script de démonstration — Jalon 2 (streaming temps réel)

> Ce document est un support de soutenance pour le Jalon 2 (Kafka + Spark
> Structured Streaming + inputs utilisateur temps réel + dashboard
> SSE/polling), voir [PROGRESS_JALON2.md](../PROGRESS_JALON2.md) pour le
> détail technique complet et [CLAUDE.md](../CLAUDE.md) pour le contexte
> global du projet. Toutes les durées indiquées ici sont des **mesures
> réelles** effectuées en conditions réelles pendant le développement
> (voir PROGRESS_JALON2.md pour chaque mesure d'origine) — **jamais une
> estimation devinée**.
>
> **Prérequis avant de commencer la démo** : `docker compose up -d` lancé
> depuis au moins 2-3 minutes (le temps que tous les services passent
> `healthy`), dashboard accessible sur http://localhost:18000. Voir la
> checklist de la section 5 pour une vérification rapide juste avant
> d'ouvrir la démo devant le jury.
>
> **Délai de recalcul du score : 2 mesures réelles distinctes, 70s et
> 110s** (voir PROGRESS_JALON2.md sous-étape 3/5 et 5/5) — la variance
> vient de la charge du système au moment du test (ex. le job Spark de
> streaming d'affluence tourne en continu en parallèle). **Annoncer
> "environ 1 à 2 minutes" au jury plutôt qu'un chiffre unique trop
> précis** — mieux vaut annoncer large et faire plus vite que l'inverse.
>
> **Comment utiliser ce document le jour J** : les phrases entre guillemets
> ci-dessous sont des EXEMPLES, pas un texte à réciter par cœur — les
> réciter mot pour mot au jury sonnerait artificiel, et un trou de mémoire
> en plein milieu serait pire qu'une phrase plus simple dite avec ses
> propres mots. Le plus important est de retenir les 2-3 idées listées
> juste avant/après chaque citation, pas la formulation exacte.

## 1. Ce que ce Jalon démontre (à dire en une phrase d'intro)

Le Jalon 1 couvrait le pipeline batch (Kaggle → Bronze → Silver → Gold →
dashboard). Le Jalon 2 ajoute le **Bloc 2 (pipelines temps réel)** :
- un flux de streaming continu (affluence des salles, simulé faute de
  dataset réel disponible, mais publié/consommé en Kafka/Spark réels) ;
- une boucle événementielle complète déclenchée par l'utilisateur
  (formulaire → Kafka → consumer → Postgres → déclenchement dbt → score
  recalculé) — la même architecture événementielle que le flux d'affluence,
  appliquée cette fois à une écriture réelle plutôt qu'à un état courant.

## 2. Script chronométré

| # | Action | Durée estimée | Ce que ça montre |
|---|---|---|---|
| 1 | Ouvrir http://localhost:18000, scroller jusqu'à la section **"Affluence en direct"** en bas de page | ~10s | La pastille **🔴 EN DIRECT** pulse, les jauges de remplissage des 5 salles bougent **toutes seules, sans aucune action du présentateur** — preuve visuelle immédiate qu'un flux Kafka → Spark Structured Streaming → SSE tourne réellement en arrière-plan. Si possible, laisser l'écran 15-20s avant de parler pour que le jury voie au moins un changement de valeur de ses propres yeux. |
| 2 | Cliquer sur une carte de salle (ex. "SafeLift Bastille") | ~5s | Le panneau sous la grille affiche la **recommandation de créneau** ("Moins de monde prévu vers XXhXX..."). Enchaîner immédiatement avec la phrase de la section 4 ci-dessous (limitation assumée) — ne pas attendre que le jury pose la question. |
| 3 | Remonter en haut, sélectionner **l'utilisateur de démonstration** (celui qui a déjà des données réelles, pré-sélectionné par défaut dans le menu déroulant) | ~5s | Rappel rapide : c'est le même utilisateur que le Jalon 1 (score de risque déjà affiché, silhouette colorée). |
| 4 | Descendre jusqu'à la section **"Logger une séance"**, choisir un exercice dans le menu déroulant, remplir poids/répétitions/séries/durée avec des valeurs nettement supérieures à l'habitude (ex. poids x2) | ~15s | Le menu déroulant ne propose QUE des exercices déjà pratiqués par cet utilisateur — mentionner en une phrase que c'est voulu (voir section 4). |
| 5 | **AVANT de cliquer sur "Enregistrer la séance"**, annoncer à voix haute : *"Cette action va publier l'événement sur Kafka, un consumer va l'insérer en base puis déclencher un run dbt complet — ça prend entre 1 et 2 minutes selon la charge du système, le temps que le pipeline recalcule le score."* | ~5s | Évite un silence gênant pendant l'attente — le jury sait déjà à quoi s'attendre, et une fourchette large évite l'effet "raté" si c'est plus lent que prévu. |
| 6 | Cliquer sur "Enregistrer la séance" | instantané | Le statut passe à *"Envoi de la séance vers Kafka..."* puis *"Séance envoyée — recalcul dbt en cours..."* — le bouton est désactivé pendant l'attente (empêche un double-clic accidentel). |
| 7 | **Pendant l'attente (~70-110s selon la charge)** : transition prévue (voir section 3) | ~1-2 min | Ne pas rester silencieux face au jury — voir la section 3 pour le contenu exact de cette transition. |
| 8 | Le statut passe automatiquement à *"Score mis à jour en Xs — [zone] : ancien_score → nouveau_score"* | — | **Le score a réellement changé** — pas une valeur statique, pas un mock. Montrer que la silhouette/le panneau de détail se sont aussi mis à jour automatiquement (pas de rechargement de page). |

**Durée totale de la démo (hors questions) : environ 3 à 4 minutes**, dont
1 à 2 minutes d'attente "productive" (section 3).

## 3. Que faire pendant l'attente du recalcul (1 à 2 minutes)

Ne jamais laisser un silence de 70s face au jury. Deux options, à choisir
selon le matériel disponible le jour J :

**Option A — expliquer l'architecture à l'oral (aucun matériel
supplémentaire requis)** : pas besoin d'un texte appris par cœur, juste
retenir ces **4 idées, dans l'ordre**, et les dire avec ses propres mots
pendant que le jury regarde l'indicateur "Recalcul en cours..." :

1. La séance saisie dans le formulaire part d'abord sur Kafka (le même
   principe que pour l'affluence en direct, montrée juste avant).
2. Un programme séparé du dashboard lit ce message et l'enregistre dans
   la base de données.
3. Ce même programme relance ensuite tout le calcul dbt vu au Jalon 1
   (celui qui construit le tableau de bord de risque).
4. **Le point important à faire ressortir** : c'est exactement le même
   calcul que celui déjà montré au Jalon 1 — rien n'est recalculé "à la
   main" ou en double, c'est le même moteur qui tourne à nouveau.

Exemple de formulation si besoin d'un point de départ (à adapter, pas à
réciter) :
> *"Là, le message vient d'être envoyé sur Kafka, un programme en
> arrière-plan va l'enregistrer puis relancer le même calcul dbt que celui
> du Jalon 1 — rien n'est dupliqué, c'est le même moteur."*

**Option B — montrer le DAG en cours d'exécution dans l'UI Airflow (si un
2e écran/onglet est prêt à l'avance)** : ouvrir
`http://localhost:18089` (port `AIRFLOW_WEBSERVER_PORT_EXPOSED`, voir
`.env` — identifiants `AIRFLOW_ADMIN_USERNAME`/`AIRFLOW_ADMIN_PASSWORD`,
`admin` par défaut), page du DAG
`gold_dbt_run` → onglet "Grid" ou "Graph", montrer les tasks qui passent
au vert une par une en temps réel (`load_silver_to_postgres` →
`dbt_seed` → `dbt_run_staging` → `fuzzy_match_exercises` → `dbt_run` →
`dbt_test`). **Plus impressionnant visuellement**, mais nécessite d'avoir
l'onglet déjà ouvert et loggé AVANT de cliquer sur "Enregistrer la
séance" (se logger en direct devant le jury ferait perdre du temps).
**Recommandé si le temps de préparation le permet.**

Dans les deux cas, terminer par une phrase de transition vers l'étape 8 :
*"Et voilà, le score vient de se mettre à jour tout seul, sans recharger
la page."*

## 4. Points à mentionner à l'oral sans attendre la question (anticiper le jury)

- **Recommandation de créneau (étape 2)** : dire explicitement *"Cette
  recommandation est basée sur le pattern théorique codé dans le
  simulateur — heures de pointe/creuses — PAS sur un historique observé
  réel, qui vient tout juste de démarrer et serait encore trop court pour
  être statistiquement valable. C'est documenté comme une limitation
  temporaire assumée."* Le dire avant que le jury le demande évite l'effet
  "on a été pris en faute".
- **Formulaire limité à un menu déroulant (étape 4)** : *"Le formulaire ne
  propose que des exercices déjà pratiqués par l'utilisateur — c'est
  voulu, ça garantit que l'exercice matche toujours notre catalogue
  `dim_exercise`, sinon la séance resterait orpheline et le score ne
  pourrait pas être calculé."*

## 5. Plan de secours (fallback) si un service ne répond pas le jour J

**Ne jamais paniquer devant le jury — dire calmement "je vérifie l'état
des services" et exécuter la commande ci-dessous.** Un terminal avec ce
répertoire déjà ouvert doit être prêt AVANT la démo.

```bash
docker compose ps
```

**Grille de lecture rapide** :

| Symptôme observé | Cause probable | Action |
|---|---|---|
| Un service affiche `Restarting` ou `Exited` | Crash, souvent transitoire au démarrage (dépendance pas encore prête) | `docker compose up -d <service>` — patienter 30s, revérifier `docker compose ps` |
| Un service `Up` mais `(unhealthy)` depuis plus d'1-2 minutes | Healthcheck en échec réel (pas juste `start_period`) | Regarder les logs : `docker compose logs --tail 30 <service>` |
| La section "Affluence en direct" reste figée (pastille grise, pas verte) | `gym-simulator` ou `spark-streaming-gym` down, ou connexion SSE coupée | Rafraîchir la page (EventSource se reconnecte tout seul sinon) ; si toujours figé, `docker compose ps gym-simulator spark-streaming-gym` |
| Le score ne se met jamais à jour après >2 minutes | `user-inputs-consumer` ou `airflow-webserver` down, ou run dbt en échec | `docker compose logs --tail 30 user-inputs-consumer` — chercher "DAG gold_dbt_run declenche" ; si absent, Kafka ou Postgres est probablement en cause |
| Tout semble bloqué / rien ne répond | Rare, mais possible si la machine a manqué de ressources | **Fallback ultime : montrer les captures/logs déjà enregistrés dans PROGRESS_JALON2.md** (mesures réelles déjà faites, dire clairement "voici la preuve déjà obtenue en conditions réelles avant la soutenance") plutôt que d'insister sur une démo live qui ne fonctionne pas |

**Règle d'or** : si un service ne répond pas, ne PAS improviser de
correctif compliqué devant le jury. Un simple `docker compose up -d`
(sans argument, relance tout ce qui manque) suivi d'un `docker compose ps`
30s plus tard résout la grande majorité des cas — et donne l'occasion de
dire *"le `restart: unless-stopped` configuré sur chaque service gère
justement ce cas"*, ce qui est en soi une réponse technique valable.

## 6. Questions probables du jury + pistes de réponse courtes

**"Pourquoi Server-Sent Events et pas WebSocket pour l'affluence ?"**
> Le flux est unidirectionnel (serveur → client uniquement, jamais l'
> inverse) : SSE est le protocole le plus simple pour ce cas précis
> (`EventSource` natif du navigateur, reconnexion automatique intégrée,
> pas de librairie externe nécessaire côté client). WebSocket serait
> justifié si le client devait aussi envoyer des données sur le même
> canal, ce qui n'est pas le cas ici — le formulaire "Logger une séance"
> passe par un POST HTTP classique, pas par le canal SSE.

**"Pourquoi 1 à 2 minutes et pas un recalcul instantané ?"**
> Décision d'architecture assumée : on ne duplique JAMAIS la formule de
> calcul du risque. Le recalcul passe donc par le run dbt complet du
> Jalon 1 (chargement Spark + 5 modèles dbt + tests), qui recalcule tout
> le schéma Gold, pas seulement la ligne concernée — un recalcul partiel
> aurait nécessité soit un modèle dbt incrémental (refonte hors périmètre
> de cette sous-étape), soit de recalculer de toute façon toute la fenêtre
> glissante utilisée par `charge_factor`/`volume_factor`. Le délai a été
> **mesuré réellement à 2 reprises (70s puis 110s, variance liée à la
> charge du système)** plutôt que deviné, et le frontend informe
> l'utilisateur pendant l'attente plutôt que de le laisser dans le flou.

**"La recommandation de créneau est-elle une vraie prédiction basée sur du
machine learning ?"**
> Non, explicitement pas. C'est une lecture directe des règles horaires
> codées dans le simulateur d'affluence (heures de pointe/creuses), pas un
> modèle appris sur de la donnée observée — l'historique réel de
> `gold.gym_occupancy_live` vient tout juste de démarrer et serait
> largement insuffisant statistiquement. La réponse de l'API porte
> d'ailleurs un champ explicite `is_theoretical_pattern_based: true` pour
> qu'aucun consommateur de l'API ne puisse la confondre avec une vraie
> prédiction.

**"Que se passe-t-il si un service Kafka/Spark plante en pleine démo ?"**
> `restart: unless-stopped` est configuré sur tous les services Docker du
> projet — un crash déclenche un redémarrage automatique. Pour Kafka
> précisément, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` est un filet de
> sécurité supplémentaire. Un test réel de résilience a été fait
> volontairement pendant le développement (arrêt puis redémarrage d'un
> service en conditions réelles, voir PROGRESS_JALON2.md sous-étape 5/5) :
> le reste du système continue de fonctionner sans crash en cascade.

**"Qu'est-ce qui garantit que le simulateur de streaming ne bloque pas le
reste du pipeline batch ?"**
> Bug réel rencontré et corrigé pendant le développement : le job Spark
> Structured Streaming (long-courant) capturait tous les cœurs du worker
> Spark et bloquait indéfiniment tout job batch soumis ensuite
> (`load_silver_to_postgres`, déclenché par chaque nouvelle séance
> utilisateur). Corrigé en limitant ce job à 1 seul cœur
> (`spark.cores.max=1`), laissant le reste disponible pour les jobs
> batch. Ce correctif a été re-vérifié en conditions réelles sur une
> fenêtre de 10+ minutes avec les deux flux actifs simultanément (voir
> section 7 de PROGRESS_JALON2.md, sous-étape 5/5).

**"Est-ce que ça marcherait avec plusieurs utilisateurs simultanés ?"**
> Le grain de `fact_workout_session` a été explicitement étendu pour
> inclure `user_id` lors de l'ajout des saisies temps réel (Jalon 2,
> sous-étape 3/5), précisément pour distinguer plusieurs utilisateurs
> réels. Non testé en charge concurrente à ce stade (hors périmètre d'une
> démo pédagogique), mais l'architecture (Kafka + consumer découplé) ne
> pose pas de blocage structurel à la montée en charge côté ingestion —
> la limite actuelle est plutôt le run dbt complet (pas incrémental), qui
> resterait le goulot d'étranglement si plusieurs utilisateurs
> déclenchaient des runs simultanément (des dag runs Airflow concurrents
> sur le même DAG sont possibles mais non testés ici).

**(rappel, concerne plutôt le Jalon 1) "Que se passe-t-il si le lab AWS
Academy expire pendant la démo ?"**
> Le Jalon 2 ne dépend d'AUCUNE ressource AWS (tout tourne en local via
> Docker Compose) — une expiration du lab AWS Academy n'affecte pas cette
> partie de la démo. Pour le Jalon 1 (Terraform/S3/Athena), voir le pipeline
> CI/CD qui gère justement ce cas gracieusement (`terraform plan` échoue
> proprement sans bloquer le reste, voir `.github/workflows/README.md`).

