// SafeLift dashboard-v2 -- helpers de domaine "risque" PORTES depuis
// dashboard/static/dashboard.js (deja valides sur l'ancien dashboard).
// Duplication ASSUMEE (meme raisonnement documente pour MUSCLE_LABELS_FR
// cote backend/frontend existant, voir CLAUDE.md) : ce sont deux
// frontends distincts sans mecanisme de partage de code entre eux. Si ces
// constantes changent d'un cote, l'autre doit etre mis a jour en miroir.

export const COLOR_BY_LEVEL = {
  Faible: "var(--color-risk-faible)",
  Modere: "var(--color-risk-modere)",
  Eleve: "var(--color-risk-eleve)",
};
export const COLOR_NONE = "var(--color-risk-none)";

// Bornes min/max de chaque facteur (data/gold/GOLD_MODEL_DECISIONS.md
// section 8) -- une barre pleine = facteur a sa valeur maximale possible
// dans le modele.
export const FACTOR_BOUNDS = {
  base_zone: [0.1, 0.25],
  charge_factor: [1.0, 1.3],
  volume_factor: [0.5, 2.0],
  recup_factor: [1.0, 1.4],
  duree_factor: [1.0, 1.2],
};

export const FACTOR_LABELS = {
  base_zone: "base_zone (risque de base de la zone)",
  charge_factor: "charge_factor (hausse charge >10%/sem.)",
  volume_factor: "volume_factor (volume vs historique)",
  recup_factor: "recup_factor (même zone <48h)",
  duree_factor: "duree_factor (séance >2h)",
};

export const FACTOR_PLAIN_LABELS = {
  charge_factor: "Hausse de charge importante (>10% vs semaine précédente)",
  volume_factor: "Volume d'entraînement au-dessus de la moyenne habituelle",
  recup_factor: "Récupération insuffisante (même zone sollicitée il y a <48h)",
  duree_factor: "Séance particulièrement longue (>2h)",
};

export const MUSCLE_LABELS_FR = {
  shoulder: "Épaules / deltoïdes",
  chest: "Pectoraux",
  abs: "Abdominaux",
  arms: "Bras (biceps/triceps)",
  legs: "Cuisses (quadriceps)",
  knee: "Genoux",
  back: "Haut du dos",
  lower_back: "Bas du dos / lombaires",
  unknown: "Zone non classifiée",
  calves: "Mollets",
};

export function muscleLabel(group) {
  return MUSCLE_LABELS_FR[group] || group;
}

export function levelColor(level) {
  return COLOR_BY_LEVEL[level] || COLOR_NONE;
}

// Facteurs "actionnables" au-dessus de leur valeur neutre (1.0) -- base_zone
// exclu (caracteristique fixe de la zone, pas un comportement recent).
export function getDominantFactors(entry, maxCount = 2) {
  const keys = ["charge_factor", "volume_factor", "recup_factor", "duree_factor"];
  const elevated = keys
    .map((key) => ({ key, value: parseFloat(entry[key]) }))
    .filter((f) => f.value > 1.001)
    .sort((a, b) => b.value - a.value)
    .slice(0, maxCount);
  return elevated.map((f) => FACTOR_PLAIN_LABELS[f.key]);
}

// Score global agrege = MAX des risk_score par zone (pas la moyenne) --
// signale la zone la plus a risque plutot que de la diluer.
export function computeGlobalScore(entries) {
  if (!entries || entries.length === 0) return null;
  return entries.reduce((worst, e) => {
    const score = parseFloat(e.risk_score);
    return worst === null || score > worst.risk_score ? { risk_score: score, risk_level: e.risk_level } : worst;
  }, null);
}

// Tendance : 2e moitie chronologique de l'historique vs 1re moitie (pas un
// decoupage calendaire strict -- seances trop irregulierement espacees).
export function computeTrend(history) {
  if (!history || history.length < 2) return null;
  const mid = Math.floor(history.length / 2);
  const firstHalf = history.slice(0, mid);
  const secondHalf = history.slice(mid);
  const avg = (arr) => arr.reduce((sum, p) => sum + parseFloat(p.avg_risk_score), 0) / arr.length;
  const delta = avg(secondHalf) - avg(firstHalf);
  return { delta, firstHalfAvg: avg(firstHalf), secondHalfAvg: avg(secondHalf) };
}

// Troncature explicite des <select> natifs -- CSS text-overflow:ellipsis
// est peu fiable sur leur etat FERME selon les navigateurs (meme constat
// deja documente pour l'ancien dashboard). Texte complet toujours
// disponible via l'attribut title.
export function truncateForSelect(text, maxLen = 42) {
  if (!text || text.length <= maxLen) return text;
  return `${text.slice(0, maxLen - 1)}…`;
}
