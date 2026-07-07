# SafeLift - Raccourcis pour piloter la stack Docker locale

.PHONY: up down logs ps build restart clean

# Demarre tous les services en arriere-plan (construit les images si besoin)
up:
	docker compose up -d --build

# Arrete et supprime les conteneurs (les volumes de donnees sont conserves)
down:
	docker compose down

# Affiche les logs de tous les services en continu
logs:
	docker compose logs -f

# Affiche l'etat des services (running / healthy / exited)
ps:
	docker compose ps

# Reconstruit les images sans cache
build:
	docker compose build --no-cache

# Redemarre tous les services
restart:
	docker compose restart

# Arrete les services ET supprime les volumes (destructif : perte des donnees locales)
clean:
	docker compose down -v
