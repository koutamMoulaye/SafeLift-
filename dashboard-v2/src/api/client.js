// SafeLift dashboard-v2 -- client HTTP minimal vers l'API FastAPI EXISTANTE
// (dashboard/main.py, servie sur DASHBOARD_PORT_EXPOSED=18000 en local).
// Ce projet ne cree AUCUN nouvel endpoint : il consomme exactement les
// memes routes que l'ancien dashboard (dashboard/static/dashboard.js).
//
// VITE_API_BASE_URL est definie dans .env (non versionne, voir
// .env.example) -- valeur par defaut ci-dessous alignee sur le port local
// standard du projet (docker-compose.yml / .env.example racine) pour que
// `npm run dev` fonctionne sans configuration prealable sur la machine de
// developpement habituelle de ce projet.
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:18000";

// Erreur explicite (jamais un objet vide silencieux) si l'API repond en
// dehors de la plage 2xx -- coherent avec la philosophie "pas d'echec
// silencieux" deja appliquee dans le reste du projet (voir dashboard.js
// existant, ex. gestion d'erreur de submitUserSession).
async function apiGet(path) {
  const res = await fetch(`${API_BASE_URL}${path}`);
  if (!res.ok) {
    throw new Error(`SafeLift API ${path} -> HTTP ${res.status}`);
  }
  return res.json();
}

/** GET /users/{user_id}/risk -- meme endpoint que l'ancien dashboard. */
export function fetchUserRisk(userId) {
  return apiGet(`/users/${userId}/risk`);
}

/** GET /users -- profils groupes {users_with_data, users_without_data}. */
export function fetchUsers() {
  return apiGet(`/users`);
}

/** GET /users/{user_id}/risk/history -- serie risk_score moyen par date. */
export function fetchUserRiskHistory(userId) {
  return apiGet(`/users/${userId}/risk/history`);
}

/** GET /users/{user_id}/exercises -- exercices reellement deja logues (simulateur/logger). */
export function fetchUserExercises(userId) {
  return apiGet(`/users/${userId}/exercises`);
}

/** GET /users/{user_id}/nutrition -- BMR/TDEE/proteines + aliments suggeres. */
export function fetchNutrition(userId) {
  return apiGet(`/users/${userId}/nutrition`);
}

/** GET /users/{user_id}/risk/prediction -- tendance predictive ML (bonus). */
export function fetchMlPrediction(userId) {
  return apiGet(`/users/${userId}/risk/prediction`);
}

/** GET /demo/scenarios -- les 9 scenarios synthetiques du mode demo. */
export function fetchDemoScenarios() {
  return apiGet(`/demo/scenarios`);
}

/** GET /gyms/{gym_id}/best_slot -- creneau recommande (pattern theorique). */
export function fetchGymBestSlot(gymId) {
  return apiGet(`/gyms/${gymId}/best_slot`);
}

/** URL du flux SSE d'affluence -- consomme directement via `new EventSource(...)`. */
export function occupancyStreamUrl() {
  return `${API_BASE_URL}/gyms/occupancy/stream`;
}

/**
 * POST /users/{user_id}/sessions -- publie sur Kafka (jamais d'ecriture DB
 * directe, voir dashboard/main.py). Erreur explicite avec le detail renvoye
 * par l'API (jamais un succes trompeur), meme philosophie que apiGet.
 */
export async function submitUserSession(userId, payload) {
  const res = await fetch(`${API_BASE_URL}/users/${userId}/sessions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    let detail = `HTTP ${res.status}`;
    try {
      const body = await res.json();
      if (body?.detail) detail = body.detail;
    } catch {
      // reponse non-JSON -- on garde le detail HTTP par defaut
    }
    throw new Error(detail);
  }
  return res.json();
}

/**
 * POST /api/simulate-risk -- simulateur what-if (Feature A, deja existant
 * sur l'ancien dashboard, reutilise TEL QUEL ici -- aucun nouvel endpoint
 * cree). Calcul pur, ne persiste rien en base, ne publie rien sur Kafka --
 * reponse immediate, contrairement a submitUserSession (Kafka + polling).
 */
export async function simulateRisk(payload) {
  const res = await fetch(`${API_BASE_URL}/api/simulate-risk`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    let detail = `HTTP ${res.status}`;
    try {
      const body = await res.json();
      if (body?.detail) detail = body.detail;
    } catch {
      // reponse non-JSON -- on garde le detail HTTP par defaut
    }
    throw new Error(detail);
  }
  return res.json();
}

export { API_BASE_URL };
