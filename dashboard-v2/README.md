# SafeLift dashboard-v2 (prototype React)

Migration du dashboard SafeLift vers React + Tailwind CSS + Framer Motion,
construite EN PARALLELE du dashboard existant (`dashboard/`, vanilla
JS/CSS servi par FastAPI). **Le dashboard existant n'est ni modifie ni
remplace** — voir `CLAUDE.md` (racine du projet) pour le contexte complet
de cette migration, le point de controle du 11 juillet 2026, et l'etat
d'avancement precis.

## Demarrer en local

Prerequis : le dashboard existant doit deja tourner (`docker compose up`,
depuis la racine du projet) — dashboard-v2 consomme son API FastAPI
existante (`http://localhost:18000` par defaut), aucune donnee/backend
propre.

```bash
cd dashboard-v2
cp .env.example .env   # ajuster VITE_API_BASE_URL si besoin
npm install
npm run dev             # http://localhost:5173
```

## Etat d'avancement

Voir `CLAUDE.md` (racine du projet), section dashboard-v2, pour le detail
complet et a jour. Resume tres court : scaffolding + silhouette centrale
branchee sur `GET /users/{id}/risk` (sous-etape 1) — reste : widgets des
colonnes laterales, barre superieure, zone de tendances, selecteur
d'utilisateur (sous-etapes suivantes, pas commencees).

## Stack

- React 19 + Vite 8
- Tailwind CSS v4 (config CSS-first via `@theme` dans `src/index.css`,
  pas de `tailwind.config.js` separe)
- Framer Motion (animation de "respiration" de la silhouette)
- Aucun state manager/routeur ajoute a ce stade (pas necessaire pour une
  seule page de scaffolding)
