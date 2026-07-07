# SafeLift

Projet Data Engineering (certification RNCP36739, M2 Data Engineering & IA).

> Ce README couvre uniquement le lancement de la stack locale (etape 1/6).
> Pour le contexte complet du projet, l'architecture cible et l'avancement des
> etapes, voir [CLAUDE.md](./CLAUDE.md) et [PROGRESS.md](./PROGRESS.md).

## Prerequis

- Docker Desktop (avec Docker Compose v2) installe et demarre
- `make` disponible dans le PATH (optionnel, sinon utiliser directement `docker compose`)
- Ports par defaut libres sur la machine (voir tableau ci-dessous, tous personnalisables via `.env`)

## Demarrage

1. Copier le fichier d'exemple et l'adapter si besoin (mots de passe, ports) :

   ```bash
   cp .env.example .env
   ```

2. Lancer la stack :

   ```bash
   make up
   # equivalent : docker compose up -d --build
   ```

3. Suivre les logs si besoin :

   ```bash
   make logs
   ```

4. Arreter la stack (les volumes/donnees sont conserves) :

   ```bash
   make down
   ```

5. Tout arreter ET supprimer les volumes (reset complet, destructif) :

   ```bash
   make clean
   ```

## Services et ports exposes

Tous les ports sont configurables via `.env` (utile pour eviter les conflits si
plusieurs projets Docker tournent en parallele sur la machine).

| Service                    | Port hote par defaut | Description                                   |
|----------------------------|-----------------------|------------------------------------------------|
| Zookeeper                  | 12181                 | Coordination Kafka                              |
| Kafka (listener externe)   | 19092                 | Broker Kafka accessible depuis l'hote           |
| Spark Master (UI)          | 18080                 | http://localhost:18080                          |
| Spark Master (RPC)         | 17077                 | `spark://localhost:17077`                       |
| Spark Worker (UI)          | 18081                 | http://localhost:18081                          |
| PostgreSQL applicatif      | 15432                 | Futur data warehouse (schema en etoile)         |
| PostgreSQL Airflow         | 15433                 | Metadata DB Airflow uniquement (base separee)   |
| Airflow Webserver          | 18089                 | http://localhost:18089                          |
| Dashboard (FastAPI)        | 18000                 | http://localhost:18000/health                   |

Identifiants Airflow par defaut (definis dans `.env`, a changer en production) :
utilisateur `admin`, mot de passe defini par `AIRFLOW_ADMIN_PASSWORD`.

## Verifier que chaque service tourne

Attendre que tous les conteneurs soient `healthy` :

```bash
docker compose ps
```

Puis, individuellement :

- **Kafka** — lister les topics (le topic de test doit apparaitre, cree automatiquement
  par le conteneur `kafka-init` au demarrage) :

  ```bash
  docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
  ```

- **Spark** — ouvrir l'UI du master dans un navigateur : http://localhost:18080
  (le worker doit apparaitre dans la liste "Workers").

- **Airflow** — ouvrir l'UI web : http://localhost:18089 (login avec les identifiants
  admin definis dans `.env`).

- **Dashboard** — verifier l'endpoint de sante :

  ```bash
  curl http://localhost:18000/health
  # -> {"status":"ok"}
  ```

- **PostgreSQL applicatif** :

  ```bash
  docker compose exec app-postgres pg_isready -U safelift_app
  ```

- **PostgreSQL Airflow** :

  ```bash
  docker compose exec airflow-postgres pg_isready -U airflow
  ```

## Structure du repo

```
safelift/
├── CLAUDE.md           # Memoire de projet (contexte, decisions, avancement)
├── PROGRESS.md         # Suivi detaille des 6 etapes du projet
├── docker-compose.yml  # Stack locale complete
├── .env.example        # Variables d'environnement (a copier en .env)
├── airflow/
│   ├── Dockerfile      # Image Airflow + providers Kafka/Spark
│   ├── dags/           # DAGs Airflow (vide pour l'instant)
│   └── requirements.txt
├── spark/
│   └── jobs/           # Jobs Spark (vide pour l'instant)
├── dashboard/           # Placeholder FastAPI (/health)
├── dbt/                 # Warehouse dbt (peuple a l'etape 4)
└── data/
    ├── bronze/          # Data lake medaillon - donnees brutes
    ├── silver/          # Data lake medaillon - donnees nettoyees
    └── gold/            # Data lake medaillon - donnees agregees
```
