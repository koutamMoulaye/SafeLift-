// SafeLift Dashboard — logique cote client (pas de framework, fetch natif,
// pas de librairie de graphiques : les graphiques (jauge, tendance) sont de
// simples SVG traces a la main, suffisants pour les besoins de cette etape).
// Deux modes strictement separes : "reel" (utilisateur -> gold.fact_risk_score)
// et "demo" (scenario synthetique -> gold.fact_risk_score_demo_synthetic).
// Ne jamais melanger les deux sources sur un meme ecran (cf. consigne).

const COLOR_BY_LEVEL = {
  Faible: "#2ecc71",
  Modere: "#f5a623",
  Eleve: "#f0483e",
};
const COLOR_NO_DATA = "#3a4152";

// Bornes min/max de chaque facteur, telles que documentees dans
// data/gold/GOLD_MODEL_DECISIONS.md (section 8, calcul des bornes de
// normalisation de risk_score). Reutilisees pour dimensionner les barres du
// panneau de detail : une barre pleine = facteur a sa valeur maximale
// possible dans le modele, pas une echelle arbitraire.
const FACTOR_BOUNDS = {
  base_zone: [0.10, 0.25],
  charge_factor: [1.0, 1.3],
  volume_factor: [0.5, 2.0],
  recup_factor: [1.0, 1.4],
  duree_factor: [1.0, 1.2],
};
const FACTOR_LABELS = {
  base_zone: "base_zone (risque de base de la zone)",
  charge_factor: "charge_factor (hausse charge >10%/sem.)",
  volume_factor: "volume_factor (volume vs historique)",
  recup_factor: "recup_factor (même zone <48h)",
  duree_factor: "duree_factor (séance >2h)",
};
// Libelle "langage clair" utilise dans le panneau Zones sensibles pour le
// facteur dominant (different du libelle technique ci-dessus).
const FACTOR_PLAIN_LABELS = {
  charge_factor: "Hausse de charge importante (>10% vs semaine précédente)",
  volume_factor: "Volume d'entraînement au-dessus de la moyenne habituelle",
  recup_factor: "Récupération insuffisante (même zone sollicitée il y a <48h)",
  duree_factor: "Séance particulièrement longue (>2h)",
};
// Libelles anatomiques francais affiches dans le panneau "Zones sensibles"
// et les info-bulles -- correspondance directe avec gold.dim_muscle.muscle_group.
const MUSCLE_LABELS_FR = {
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

let isDemoMode = false;
let currentRealMuscles = []; // dernieres donnees /users/{id}/risk.muscles
let currentDemoScenarios = []; // donnees /demo/scenarios

const zoneElements = () => document.querySelectorAll(".zone");

function colorZones(byMuscleGroup) {
  // byMuscleGroup : { muscle_group: {risk_level, ...} }
  // "calves" n'a jamais de donnee (aucune valeur muscle_group='calves' dans
  // dim_muscle) : reste toujours gris, ce qui est le comportement correct
  // ici (cle absente de byMuscleGroup -> COLOR_NO_DATA).
  zoneElements().forEach((el) => {
    const muscle = el.dataset.muscle;
    const entry = byMuscleGroup[muscle];
    el.style.fill = entry ? COLOR_BY_LEVEL[entry.risk_level] || COLOR_NO_DATA : COLOR_NO_DATA;
  });
}

function clearZoneSelection() {
  zoneElements().forEach((el) => el.classList.remove("selected"));
}

function scoreBadge(level, score) {
  const color = COLOR_BY_LEVEL[level] || COLOR_NO_DATA;
  return `<span class="score-badge" style="background:${color}">${score} — ${level}</span>`;
}

function renderFactorBars(factors) {
  const rows = Object.keys(FACTOR_BOUNDS)
    .map((key) => {
      const value = parseFloat(factors[key]);
      const [min, max] = FACTOR_BOUNDS[key];
      const pct = Math.max(0, Math.min(100, ((value - min) / (max - min)) * 100));
      return `
        <div class="factor-row">
          <span>${FACTOR_LABELS[key]}</span>
          <span class="factor-track"><span class="factor-fill" style="width:${pct}%"></span></span>
          <span class="factor-value">${value.toFixed(2)}</span>
        </div>`;
    })
    .join("");
  return `<div class="factor-bars">${rows}</div>`;
}

// Facteurs "actionnables" elevés (au-dessus de leur valeur neutre 1.0) --
// base_zone est exclu ici car c'est une caracteristique fixe de la zone
// (pas un comportement recent de l'utilisateur), donc pas une "explication"
// utile dans le panneau Zones sensibles. Renvoie les 1-2 facteurs les plus
// elevés (tries par ecart a la neutralite), ou une liste vide si aucun
// facteur ne depasse la neutralite (le score vient alors uniquement de
// base_zone).
function getDominantFactors(entry, maxCount = 2) {
  const keys = ["charge_factor", "volume_factor", "recup_factor", "duree_factor"];
  const elevated = keys
    .map((key) => ({ key, value: parseFloat(entry[key]) }))
    .filter((f) => f.value > 1.001)
    .sort((a, b) => b.value - a.value)
    .slice(0, maxCount);
  return elevated.map((f) => FACTOR_PLAIN_LABELS[f.key]);
}

function renderZoneDetail(entry, { isSynthetic = false, metaLabel = "" } = {}) {
  const panel = document.getElementById("zone-detail");
  if (!entry) {
    panel.innerHTML = '<p class="placeholder">Aucune donnée disponible pour cette zone.</p>';
    return;
  }
  const muscleLabel = MUSCLE_LABELS_FR[entry.muscle_group] || entry.muscle_group;
  panel.innerHTML = `
    <p><strong>Zone :</strong> ${muscleLabel}${isSynthetic ? " <em>(scénario synthétique)</em>" : ""}</p>
    <p>${scoreBadge(entry.risk_level, entry.risk_score)}</p>
    ${renderFactorBars(entry)}
    <p class="meta-note">${metaLabel}</p>
  `;
}

// --- Panneau "Zones sensibles" : explication en langage clair AVANT le clic ---

function renderSensitiveZones(entries) {
  const container = document.getElementById("sensitive-zones-list");
  const sensitive = (entries || []).filter((e) => e.risk_level === "Modere" || e.risk_level === "Eleve");

  if (sensitive.length === 0) {
    container.innerHTML = '<p class="placeholder">Aucune zone en Modéré ou Élevé actuellement — tout est sous contrôle.</p>';
    return;
  }

  // Zones les plus a risque en premier
  sensitive.sort((a, b) => parseFloat(b.risk_score) - parseFloat(a.risk_score));

  container.innerHTML = sensitive
    .map((entry) => {
      const muscleLabel = MUSCLE_LABELS_FR[entry.muscle_group] || entry.muscle_group;
      const reasons = getDominantFactors(entry);
      const reasonText = reasons.length > 0 ? reasons.join(" · ") : "Risque de base de la zone (aucun facteur comportemental élevé)";
      const color = COLOR_BY_LEVEL[entry.risk_level] || COLOR_NO_DATA;
      return `
        <div class="sensitive-zone-row">
          <span class="zone-name">${muscleLabel}</span>
          <span class="zone-reason">${reasonText}</span>
          <span class="level-badge" style="background:${color}">${entry.risk_level} (${entry.risk_score})</span>
        </div>`;
    })
    .join("");
}

// --- KPI : score global (jauge radiale) ---

// Score global agrege : on retient le MAX des risk_score par zone (pas la
// moyenne). Choix documente dans data/gold/GOLD_MODEL_DECISIONS.md et ici :
// un outil de suivi de risque doit signaler la zone la PLUS a risque, pas
// la masquer dans une moyenne (7 zones Faible + 1 Eleve doit rester visible
// comme "Eleve" globalement, pas dilue par une moyenne).
function computeGlobalScore(entries) {
  if (!entries || entries.length === 0) return null;
  return entries.reduce((worst, e) => {
    const score = parseFloat(e.risk_score);
    return worst === null || score > worst.risk_score ? { risk_score: score, risk_level: e.risk_level } : worst;
  }, null);
}

const GAUGE_RADIUS = 50;
const GAUGE_CIRCUMFERENCE = 2 * Math.PI * GAUGE_RADIUS;

function renderGauge(entries) {
  const arc = document.getElementById("gauge-arc");
  const number = document.getElementById("gauge-number");
  const levelLabel = document.getElementById("gauge-level-label");
  const global = computeGlobalScore(entries);

  arc.setAttribute("stroke-dasharray", GAUGE_CIRCUMFERENCE.toFixed(1));

  if (!global) {
    arc.setAttribute("stroke-dashoffset", GAUGE_CIRCUMFERENCE.toFixed(1));
    arc.setAttribute("stroke", COLOR_NO_DATA);
    number.textContent = "—";
    levelLabel.textContent = "Aucune donnée";
    return global;
  }

  const pct = Math.max(0, Math.min(100, global.risk_score)) / 100;
  arc.setAttribute("stroke-dashoffset", (GAUGE_CIRCUMFERENCE * (1 - pct)).toFixed(1));
  arc.setAttribute("stroke", COLOR_BY_LEVEL[global.risk_level] || COLOR_NO_DATA);
  number.textContent = global.risk_score.toFixed(0);
  levelLabel.textContent = `Score global (max des zones) — ${global.risk_level}`;
  return global;
}

// --- KPI : cards (derniere seance, tendance, zones en alerte) ---

function renderLastSessionKpi(entries, fallbackLabel) {
  const valueEl = document.getElementById("kpi-last-session-value");
  const subEl = document.getElementById("kpi-last-session-sub");
  if (!entries || entries.length === 0) {
    valueEl.textContent = fallbackLabel || "—";
    subEl.textContent = "";
    return;
  }
  // La ligne avec la session_date la plus recente, tous zones confondues
  const latest = entries.reduce((acc, e) => (!acc || e.session_date > acc.session_date ? e : acc), null);
  valueEl.textContent = latest.exercise_name || fallbackLabel;
  subEl.textContent = latest.session_date ? `${MUSCLE_LABELS_FR[latest.muscle_group] || latest.muscle_group} — ${latest.session_date}` : "";
}

function renderAlertsKpi(entries) {
  const valueEl = document.getElementById("kpi-alerts-value");
  const count = (entries || []).filter((e) => e.risk_level === "Modere" || e.risk_level === "Eleve").length;
  valueEl.textContent = count;
  valueEl.style.color = count > 0 ? COLOR_BY_LEVEL.Modere : COLOR_BY_LEVEL.Faible;
}

// Tendance : compare la moyenne de la 2e moitie de l'historique a la
// moyenne de la 1re moitie (plutot que "vs semaine derniere" au sens
// calendaire strict) -- les seances de weight_training sont espacees de
// facon irreguliere sur ~3 ans, une comparaison "semaine calendaire vs
// semaine calendaire" donnerait souvent des echantillons vides. Comparer
// les 2 moities chronologiques de l'historique disponible est plus robuste
// et reste simple a comprendre. Choix documente ici et dans
// data/gold/GOLD_MODEL_DECISIONS.md si reference y est faite.
function computeTrend(history) {
  if (!history || history.length < 2) return null;
  const mid = Math.floor(history.length / 2);
  const firstHalf = history.slice(0, mid);
  const secondHalf = history.slice(mid);
  const avg = (arr) => arr.reduce((sum, p) => sum + parseFloat(p.avg_risk_score), 0) / arr.length;
  const delta = avg(secondHalf) - avg(firstHalf);
  return { delta, firstHalfAvg: avg(firstHalf), secondHalfAvg: avg(secondHalf) };
}

function renderTrendKpi(history) {
  const valueEl = document.getElementById("kpi-trend-value");
  const subEl = document.getElementById("kpi-trend-sub");
  const trend = computeTrend(history);
  if (!trend) {
    valueEl.textContent = "—";
    subEl.textContent = "Historique insuffisant";
    return;
  }
  const sign = trend.delta >= 0 ? "+" : "";
  valueEl.textContent = `${sign}${trend.delta.toFixed(1)} pts`;
  valueEl.style.color = trend.delta > 0 ? COLOR_BY_LEVEL.Eleve : trend.delta < 0 ? COLOR_BY_LEVEL.Faible : "inherit";
  subEl.textContent = "2e moitié vs 1re moitié de l'historique";
}

function drawHistoryChart(history) {
  const svg = document.getElementById("history-chart");
  const emptyMsg = document.getElementById("history-empty");
  svg.innerHTML = "";

  if (!history || history.length === 0) {
    emptyMsg.classList.remove("hidden");
    return;
  }
  emptyMsg.classList.add("hidden");

  const width = 400;
  const height = 130;
  const padding = 20;
  const maxScore = 100;

  const points = history.map((point, i) => {
    const x = padding + (i / Math.max(history.length - 1, 1)) * (width - 2 * padding);
    const y = height - padding - (point.avg_risk_score / maxScore) * (height - 2 * padding);
    return `${x},${y}`;
  });

  const polyline = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
  polyline.setAttribute("points", points.join(" "));
  polyline.setAttribute("class", "history-line");
  svg.appendChild(polyline);

  history.forEach((point, i) => {
    const [x, y] = points[i].split(",");
    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("cx", x);
    circle.setAttribute("cy", y);
    circle.setAttribute("r", "3");
    circle.setAttribute("fill", COLOR_BY_LEVEL[
      point.avg_risk_score <= 33 ? "Faible" : point.avg_risk_score <= 66 ? "Modere" : "Eleve"
    ]);
    svg.appendChild(circle);
  });
}

// --- Chargement utilisateurs reels ---

async function loadUsers() {
  const res = await fetch("/users");
  const { users_with_data, users_without_data } = await res.json();
  const select = document.getElementById("user-select");
  select.innerHTML = "";

  // Groupe principal : utilisateurs AVEC donnees reelles -- toujours en
  // premier et pre-selectionne, pour que le premier coup d'oeil sur le
  // dashboard ne tombe jamais sur un ecran vide.
  if (users_with_data.length > 0) {
    const groupWithData = document.createElement("optgroup");
    groupWithData.label = "Profils avec données de séance réelles";
    users_with_data.forEach((u) => {
      const opt = document.createElement("option");
      opt.value = u.user_id;
      opt.textContent = `Utilisateur ${u.user_id} (${u.age} ans, ${u.gender})`;
      groupWithData.appendChild(opt);
    });
    select.appendChild(groupWithData);
  }

  // Groupe secondaire : profils gym_members sans seance reelle rattachee.
  if (users_without_data.length > 0) {
    const groupWithoutData = document.createElement("optgroup");
    groupWithoutData.label =
      "Profils sans séance réelle (1 seul profil de weight_training a pu être rattaché à dim_user — hypothèse de démonstration documentée)";
    users_without_data.forEach((u) => {
      const opt = document.createElement("option");
      opt.value = u.user_id;
      opt.textContent = `Utilisateur ${u.user_id} — pas de séance loggée`;
      groupWithoutData.appendChild(opt);
    });
    select.appendChild(groupWithoutData);
  }

  if (users_with_data.length > 0) {
    select.value = users_with_data[0].user_id;
  } else if (users_without_data.length > 0) {
    select.value = users_without_data[0].user_id;
  }
  await onUserChange();
}

async function onUserChange() {
  const userId = document.getElementById("user-select").value;
  if (!userId) return;
  currentRealUserId = userId;

  const [riskRes, historyRes] = await Promise.all([
    fetch(`/users/${userId}/risk`),
    fetch(`/users/${userId}/risk/history`),
  ]);
  const risk = await riskRes.json();
  const history = await historyRes.json();

  currentRealMuscles = risk.muscles;
  const byMuscle = {};
  risk.muscles.forEach((m) => (byMuscle[m.muscle_group] = m));
  colorZones(byMuscle);
  clearZoneSelection();
  document.getElementById("zone-detail").innerHTML =
    '<p class="placeholder">Survolez ou cliquez une zone de la carte corporelle pour voir le détail du calcul.</p>';

  renderGauge(risk.muscles);
  renderLastSessionKpi(risk.muscles, "Aucune séance");
  renderAlertsKpi(risk.muscles);
  renderTrendKpi(history.history);
  renderSensitiveZones(risk.muscles);
  drawHistoryChart(history.history);

  clearSimHighlight();
  document.getElementById("sim-result").innerHTML =
    '<p class="placeholder">Choisissez un exercice et des paramètres, puis cliquez sur « Simuler ».</p>';
  stopLogSessionPolling();
  document.getElementById("log-session-status").textContent = "";
  document.getElementById("log-session-status").className = "log-session-status";
  if (!isDemoMode) {
    await loadSimulatorExercises(userId);
    await loadLogSessionExercises(userId);
  }
}

// --- Chargement scenarios demo ---

async function loadDemoScenarios() {
  const res = await fetch("/demo/scenarios");
  currentDemoScenarios = await res.json();
  const select = document.getElementById("scenario-select");
  select.innerHTML = "";
  currentDemoScenarios.forEach((s) => {
    const opt = document.createElement("option");
    opt.value = s.scenario_id;
    opt.textContent = `#${s.scenario_id} — ${s.scenario_label} (${s.risk_level})`;
    select.appendChild(opt);
  });
  if (currentDemoScenarios.length > 0) {
    select.value = currentDemoScenarios[0].scenario_id;
    onScenarioChange();
  }
}

function onScenarioChange() {
  const scenarioId = parseInt(document.getElementById("scenario-select").value, 10);
  const scenario = currentDemoScenarios.find((s) => s.scenario_id === scenarioId);
  if (!scenario) return;

  // En mode demo, un seul scenario = une seule zone coloree a la fois ;
  // les autres zones restent grises (pas de donnee "demo" pour elles).
  const byMuscle = { [scenario.muscle_group]: scenario };
  colorZones(byMuscle);
  clearZoneSelection();
  renderZoneDetail(scenario, { isSynthetic: true, metaLabel: `${scenario.scenario_label} — ${scenario.notes}` });

  renderGauge([scenario]);
  renderLastSessionKpi(null, scenario.scenario_label);
  renderAlertsKpi([scenario]);

  const trendValueEl = document.getElementById("kpi-trend-value");
  const trendSubEl = document.getElementById("kpi-trend-sub");
  trendValueEl.textContent = "—";
  trendValueEl.style.color = "inherit";
  trendSubEl.textContent = "Non applicable en mode démo";

  renderSensitiveZones([scenario]);

  document.getElementById("history-chart").innerHTML = "";
  document.getElementById("history-empty").classList.remove("hidden");
  document.getElementById("history-empty").textContent =
    "Pas d'historique pour les scénarios de démonstration (données ponctuelles fictives).";
}

// --- Interaction avec les zones de la carte corporelle ---

function onZoneClick(event) {
  const muscle = event.target.dataset.muscle;
  if (!muscle) return;

  clearZoneSelection();
  event.target.classList.add("selected");

  if (isDemoMode) {
    const scenarioId = parseInt(document.getElementById("scenario-select").value, 10);
    const scenario = currentDemoScenarios.find((s) => s.scenario_id === scenarioId);
    if (scenario && scenario.muscle_group === muscle) {
      renderZoneDetail(scenario, { isSynthetic: true, metaLabel: `${scenario.scenario_label} — ${scenario.notes}` });
    } else if (muscle === "calves") {
      renderCalvesExplanation();
    } else {
      renderZoneDetail(null);
    }
  } else if (muscle === "calves") {
    renderCalvesExplanation();
  } else {
    const entry = currentRealMuscles.find((m) => m.muscle_group === muscle);
    if (entry) {
      renderZoneDetail(entry, { metaLabel: `Dernier exercice : ${entry.exercise_name} — ${entry.session_date}` });
    } else {
      renderZoneDetail(null);
    }
  }
}

// Les mollets n'ont aucune correspondance dans gold.dim_muscle.muscle_group
// (le pipeline de classification -- dbt/macros/classify_muscle_group.sql --
// ne produit jamais la valeur "calves") : plutot que de masquer cette zone
// anatomique, elle reste affichee (toujours grise) avec une explication
// honnete au clic, conformement a l'esprit "pas de boite noire" du projet.
function renderCalvesExplanation() {
  document.getElementById("zone-detail").innerHTML = `
    <p><strong>Zone :</strong> Mollets</p>
    <p class="placeholder">Cette zone n'est pas couverte par le modèle de classification actuel
    (aucune valeur "calves" produite par la heuristique muscle_group, voir
    data/gold/GOLD_MODEL_DECISIONS.md) — affichée à titre indicatif, toujours grisée.</p>
  `;
}

// --- Simulateur what-if (Feature A) ---
// Hypothese ponctuelle calculee a la volee par POST /api/simulate-risk,
// RIEN n'est ecrit en base. Desactive en mode demo (les scenarios
// synthetiques n'ont pas d'user_id/exercise_id reels a simuler).

let currentSimExercises = [];
let currentRealUserId = null;

async function loadSimulatorExercises(userId) {
  const res = await fetch(`/users/${userId}/exercises`);
  const data = await res.json();
  currentSimExercises = data.exercises;

  const select = document.getElementById("sim-exercise-select");
  select.innerHTML = "";
  currentSimExercises.forEach((ex) => {
    const opt = document.createElement("option");
    opt.value = ex.exercise_id;
    opt.textContent = `${ex.exercise_name} (${MUSCLE_LABELS_FR[ex.muscle_group] || ex.muscle_group})`;
    select.appendChild(opt);
  });
}

function clearSimHighlight() {
  zoneElements().forEach((el) => {
    el.classList.remove("sim-highlight");
    el.style.stroke = "";
  });
}

function applySimHighlight(muscleGroup, riskLevel) {
  clearSimHighlight();
  document.querySelectorAll(`.zone[data-muscle="${muscleGroup}"]`).forEach((el) => {
    el.classList.add("sim-highlight");
    el.style.stroke = COLOR_BY_LEVEL[riskLevel] || COLOR_NO_DATA;
  });
}

// Facteur "actionnable" le plus eloigne de sa valeur neutre (1.0), pour
// expliquer en langage clair la cause principale de l'ecart -- meme esprit
// que getDominantFactors() (zones sensibles), adapte a la forme de reponse
// {facteurs: {cle: {valeur, explication}}} de /api/simulate-risk. base_zone
// exclu (caracteristique fixe de la zone, pas un parametre de l'hypothese).
function pickDominantSimFactor(facteurs) {
  const keys = ["charge_factor", "volume_factor", "recup_factor", "duree_factor"];
  const candidates = keys
    .map((key) => ({ key, ecart: Math.abs(facteurs[key].valeur - 1.0), entry: facteurs[key] }))
    .filter((f) => f.ecart > 0.001)
    .sort((a, b) => b.ecart - a.ecart);
  return candidates.length > 0 ? candidates[0].entry : null;
}

function buildSimMessage(data) {
  const zone = data.muscle_zone;
  const exerciseName = data.exercise_name;

  if (data.risk_score_actuel === null) {
    return `Aucune donnée réelle existante pour « ${exerciseName} » ou la zone ${zone} — risque simulé estimé à
      <strong>${data.risk_level_simule} (${data.risk_score_simule.toFixed(0)})</strong>.`;
  }

  const delta = data.delta;
  const sign = delta > 0 ? "+" : "";
  const deltaClass = delta > 0.5 ? "sim-delta-up" : delta < -0.5 ? "sim-delta-down" : "sim-delta-flat";
  const dominant = pickDominantSimFactor(data.facteurs);
  const reason = dominant
    ? `principalement à cause de : ${dominant.explication}`
    : "principalement en raison du risque de base de cette zone (aucun facteur comportemental élevé)";

  return `Sur « ${exerciseName} », ces paramètres hypothétiques feraient passer le risque
    <strong>${zone}</strong> de <strong>${data.risk_level_actuel}</strong> (${data.risk_score_actuel.toFixed(0)})
    à <strong>${data.risk_level_simule}</strong> (${data.risk_score_simule.toFixed(0)})
    (<span class="${deltaClass}">${sign}${delta.toFixed(1)} pts</span>), ${reason}.`;
}

function renderSimResult(data) {
  const panel = document.getElementById("sim-result");
  const factorsFlat = {
    base_zone: data.facteurs.base_zone.valeur,
    charge_factor: data.facteurs.charge_factor.valeur,
    volume_factor: data.facteurs.volume_factor.valeur,
    recup_factor: data.facteurs.recup_factor.valeur,
    duree_factor: data.facteurs.duree_factor.valeur,
  };
  panel.innerHTML = `
    <p class="sim-message">${buildSimMessage(data)}</p>
    ${renderFactorBars(factorsFlat)}
  `;
  applySimHighlight(data.muscle_group, data.risk_level_simule);
}

async function onSimSubmit() {
  if (isDemoMode || !currentRealUserId) return;

  const exerciseId = parseInt(document.getElementById("sim-exercise-select").value, 10);
  if (!exerciseId) return;

  const payload = {
    user_id: parseInt(currentRealUserId, 10),
    exercise_id: exerciseId,
    charge_kg: parseFloat(document.getElementById("sim-charge").value),
    reps: parseInt(document.getElementById("sim-reps").value, 10),
    sets: parseInt(document.getElementById("sim-sets").value, 10),
    duration_minutes: parseFloat(document.getElementById("sim-duration").value),
  };

  const res = await fetch("/api/simulate-risk", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    document.getElementById("sim-result").innerHTML =
      '<p class="placeholder">Erreur lors de la simulation — vérifiez les paramètres.</p>';
    return;
  }

  renderSimResult(await res.json());
}

function bindSimSlider(inputId, valueId) {
  const input = document.getElementById(inputId);
  const valueEl = document.getElementById(valueId);
  input.addEventListener("input", () => (valueEl.textContent = input.value));
}

function applySimulatorAvailability() {
  document.getElementById("simulator-unavailable").classList.toggle("hidden", !isDemoMode);
  document.getElementById("simulator-form-wrapper").classList.toggle("hidden", isDemoMode);
  if (isDemoMode) {
    clearSimHighlight();
    document.getElementById("sim-result").innerHTML =
      '<p class="placeholder">Choisissez un exercice et des paramètres, puis cliquez sur « Simuler ».</p>';
  }
}

// --- Logger une seance temps reelle (Jalon 2, sous-etape 3/5) ---
// POST /users/{user_id}/sessions PUBLIE l'evenement sur Kafka -- l'API
// n'ecrit JAMAIS en base directement (voir dashboard/main.py). Le score
// n'est PAS mis a jour immediatement : un consumer (scripts/consume_user_inputs.py)
// insere la seance puis declenche un run dbt COMPLET (memes constantes que
// fact_risk_score.sql, aucune duplication de la formule -- le run complet
// recalcule tout le schema gold, pas seulement l'utilisateur concerne, cf.
// data/gold/GOLD_MODEL_DECISIONS.md section 11). On re-fetch donc
// GET /users/{id}/risk toutes les LOG_SESSION_POLL_INTERVAL_MS jusqu'a
// detecter un changement du risk_score sur la zone concernee, ou jusqu'a
// LOG_SESSION_POLL_TIMEOUT_MS.
//
// Delai REEL mesure en conditions reelles, 2 mesures INDEPENDANTES (voir
// PROGRESS_JALON2.md sous-etapes 3/5 et 5/5, tests end-to-end sur
// user_id=9) : 70s puis 110s entre la soumission et la fin du run dbt --
// la variance vient de la charge concurrente du systeme (ex. le job Spark
// Structured Streaming de l'affluence tourne en continu en parallele).
// Timeout fixe a 180s (marge x1.6 par rapport a la PIRE mesure observee,
// 110s), releve de 120s (marge insuffisante face a la 2e mesure) -- ne
// JAMAIS deviner un delai sans le mesurer sur ce projet, et ne jamais
// fixer une marge sur une seule mesure quand plusieurs sont disponibles.
const LOG_SESSION_POLL_INTERVAL_MS = 3000;
const LOG_SESSION_POLL_TIMEOUT_MS = 180000;
let logSessionPollTimer = null;

function stopLogSessionPolling() {
  if (logSessionPollTimer) {
    clearTimeout(logSessionPollTimer);
    logSessionPollTimer = null;
  }
}

async function loadLogSessionExercises(userId) {
  // Reutilise la MEME source que le simulateur what-if (exercices
  // REELLEMENT deja pratiques par cet utilisateur) : un <select> plutot
  // qu'un champ texte libre, precisement pour garantir que exercise_name
  // correspond toujours a un normalized_exercise_name deja connu de
  // gold.dim_exercise -- sinon la seance resterait orpheline (exercise_id
  // NULL) et le risk_score ne pourrait pas etre calcule. Voir
  // data/gold/GOLD_MODEL_DECISIONS.md section 11.
  const res = await fetch(`/users/${userId}/exercises`);
  const data = await res.json();

  const select = document.getElementById("log-exercise-select");
  select.innerHTML = "";
  data.exercises.forEach((ex) => {
    const opt = document.createElement("option");
    opt.value = ex.exercise_name;
    opt.dataset.muscleGroup = ex.muscle_group;
    opt.textContent = `${ex.exercise_name} (${MUSCLE_LABELS_FR[ex.muscle_group] || ex.muscle_group})`;
    select.appendChild(opt);
  });
}

function applyLogSessionAvailability() {
  document.getElementById("log-session-unavailable").classList.toggle("hidden", !isDemoMode);
  document.getElementById("log-session-form-wrapper").classList.toggle("hidden", isDemoMode);
  if (isDemoMode) {
    stopLogSessionPolling();
    document.getElementById("log-session-status").textContent = "";
    document.getElementById("log-session-status").className = "log-session-status";
  }
}

function setLogSessionStatus(text, variant) {
  const el = document.getElementById("log-session-status");
  el.textContent = text;
  el.className = variant ? `log-session-status log-session-${variant}` : "log-session-status";
}

async function onLogSessionSubmit() {
  if (isDemoMode || !currentRealUserId) return;

  const exerciseSelect = document.getElementById("log-exercise-select");
  const exerciseName = exerciseSelect.value;
  if (!exerciseName) return;
  const muscleGroup = exerciseSelect.selectedOptions[0]?.dataset.muscleGroup || null;

  const payload = {
    exercise_name: exerciseName,
    lifted_weight_kg: parseFloat(document.getElementById("log-weight").value),
    reps: parseInt(document.getElementById("log-reps").value, 10),
    sets: parseInt(document.getElementById("log-sets").value, 10),
    duration_seconds: (parseFloat(document.getElementById("log-duration").value) || 0) * 60,
  };

  const submitBtn = document.getElementById("log-session-submit");
  submitBtn.disabled = true;
  stopLogSessionPolling();
  setLogSessionStatus("Envoi de la séance vers Kafka...", "pending");

  let res;
  try {
    res = await fetch(`/users/${currentRealUserId}/sessions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    setLogSessionStatus("Erreur réseau lors de l'envoi — le dashboard est peut-être injoignable.", "error");
    submitBtn.disabled = false;
    return;
  }

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    setLogSessionStatus(`Échec de l'envoi (Kafka injoignable ou requête invalide) : ${body.detail || res.status}`, "error");
    submitBtn.disabled = false;
    return;
  }

  // Score de reference AVANT le recalcul, pour detecter un vrai changement
  // (et pas juste "une reponse est revenue") -- comparaison sur la zone
  // musculaire concernee par l'exercice logue.
  const before = currentRealMuscles.find((m) => m.muscle_group === muscleGroup);
  const beforeScore = before ? before.risk_score : null;
  const startedAt = Date.now();

  setLogSessionStatus("Séance envoyée — recalcul dbt en cours...", "pending");

  const poll = async () => {
    const elapsedMs = Date.now() - startedAt;
    if (elapsedMs > LOG_SESSION_POLL_TIMEOUT_MS) {
      setLogSessionStatus(
        `Toujours en attente après ${Math.round(elapsedMs / 1000)}s — le score se mettra à jour automatiquement au prochain rafraîchissement de la page.`,
        "timeout"
      );
      submitBtn.disabled = false;
      return;
    }

    let risk;
    try {
      const riskRes = await fetch(`/users/${currentRealUserId}/risk`);
      risk = await riskRes.json();
    } catch (err) {
      logSessionPollTimer = setTimeout(poll, LOG_SESSION_POLL_INTERVAL_MS);
      return;
    }

    const after = risk.muscles.find((m) => m.muscle_group === muscleGroup);
    const changed = after && (beforeScore === null || after.risk_score !== beforeScore);

    if (changed) {
      const elapsedSeconds = (elapsedMs / 1000).toFixed(1);
      const zoneLabel = MUSCLE_LABELS_FR[muscleGroup] || muscleGroup || "zone";
      submitBtn.disabled = false;
      currentRealMuscles = risk.muscles;
      // onUserChange() re-fetch tout (silhouette, KPIs...) et REINITIALISE
      // log-session-status au passage (voir onUserChange) -- le message de
      // succes doit donc etre pose APRES cet appel, sans quoi il serait
      // efface immediatement.
      await onUserChange();
      setLogSessionStatus(
        `Score mis à jour en ${elapsedSeconds}s — ${zoneLabel} : ${beforeScore ?? "—"} → ${after.risk_score} (${after.risk_level}).`,
        "success"
      );
      return;
    }

    logSessionPollTimer = setTimeout(poll, LOG_SESSION_POLL_INTERVAL_MS);
  };

  logSessionPollTimer = setTimeout(poll, LOG_SESSION_POLL_INTERVAL_MS);
}

// --- Toggle reel / demo ---

function applyModeToUI() {
  document.getElementById("demo-banner").classList.toggle("hidden", !isDemoMode);
  document.getElementById("real-controls").classList.toggle("hidden", isDemoMode);
  document.getElementById("demo-controls").classList.toggle("hidden", !isDemoMode);
  document.getElementById("history-panel").classList.toggle("hidden", isDemoMode);
  applySimulatorAvailability();
  applyLogSessionAvailability();

  clearZoneSelection();
  if (isDemoMode) {
    if (currentDemoScenarios.length === 0) {
      loadDemoScenarios();
    } else {
      onScenarioChange();
    }
  } else {
    onUserChange();
  }
}

// --- Affluence en direct (Jalon 2, sous-etape 4/5) ---
// Server-Sent Events (decision deja actee : PAS de WebSocket, PAS de
// polling pour cette fonctionnalite) -- totalement INDEPENDANT du
// mecanisme de polling deja en place pour le recalcul de risque
// (sous-etape 3/5, onLogSessionSubmit ci-dessus), qui reste inchange.

// Couleurs par charge_category -- volontairement UN DICTIONNAIRE DISTINCT
// de COLOR_BY_LEVEL (risque musculo-squelettique) : gold.gym_occupancy_live
// produit "Faible"/"Moderee"/"Elevee" (spark/jobs/stream_gym_occupancy.py),
// AVEC un e final double sur "Moderee"/"Elevee" -- alors que risk_level
// produit "Faible"/"Modere"/"Eleve" (fact_risk_score.sql, sans le e final
// double). Reutiliser COLOR_BY_LEVEL ici aurait fait rater le lookup pour
// "Moderee"/"Elevee" (orthographe differente -> cle absente -> gris par
// defaut, silencieusement) : bug potentiel evite en verifiant l'orthographe
// exacte cote serveur avant d'ecrire ce dictionnaire.
const OCCUPANCY_COLOR_BY_CATEGORY = {
  Faible: "#2ecc71",
  Moderee: "#f5a623",
  Elevee: "#f0483e",
};

let occupancyEventSource = null;
let selectedGymId = null;

function connectOccupancyStream() {
  if (occupancyEventSource) return; // deja connecte, ne pas dupliquer

  occupancyEventSource = new EventSource("/gyms/occupancy/stream");
  const badge = document.getElementById("live-badge");

  occupancyEventSource.onopen = () => {
    badge.classList.remove("live-badge-disconnected");
  };

  occupancyEventSource.onmessage = (event) => {
    const data = JSON.parse(event.data);
    renderOccupancyGrid(data.gyms);
  };

  // EventSource se RECONNECTE AUTOMATIQUEMENT (comportement natif du
  // navigateur, jamais desactive/court-circuite ici) -- onerror se contente
  // de refleter visuellement l'etat "deconnexion en cours" pendant la
  // tentative de reconnexion.
  occupancyEventSource.onerror = () => {
    badge.classList.add("live-badge-disconnected");
  };
}

function renderOccupancyGrid(gyms) {
  const grid = document.getElementById("occupancy-grid");
  const loading = document.getElementById("occupancy-loading");
  if (loading) loading.remove();

  gyms.forEach((g) => {
    let card = grid.querySelector(`[data-gym-id="${g.gym_id}"]`);
    if (!card) {
      card = document.createElement("div");
      card.className = "gym-card";
      card.dataset.gymId = g.gym_id;
      card.addEventListener("click", () => selectGym(g.gym_id));
      grid.appendChild(card);
    }

    const pct = Math.round(parseFloat(g.occupancy_rate) * 100);
    const color = OCCUPANCY_COLOR_BY_CATEGORY[g.charge_category] || COLOR_NO_DATA;
    card.classList.toggle("selected", g.gym_id === selectedGymId);
    card.innerHTML = `
      <h3>${g.gym_name}</h3>
      <div class="gym-gauge-track"><div class="gym-gauge-fill" style="width:${pct}%;background:${color}"></div></div>
      <p class="gym-occupancy-text">${g.current_occupancy} / ${g.capacity} personnes (${pct}%) — <span class="gym-charge-badge" style="color:${color}">${g.charge_category}</span></p>
      <p class="gym-updated-at">Dernier message : ${new Date(g.last_message_timestamp).toLocaleTimeString("fr-FR")}</p>
    `;
  });

  // Auto-selection de la 1re salle au tout premier evenement recu, pour
  // que le panneau de recommandation ne reste jamais vide par defaut.
  if (selectedGymId === null && gyms.length > 0) {
    selectGym(gyms[0].gym_id);
  }
}

function selectGym(gymId) {
  selectedGymId = gymId;
  document.querySelectorAll(".gym-card").forEach((el) => {
    el.classList.toggle("selected", parseInt(el.dataset.gymId, 10) === gymId);
  });
  loadBestSlot(gymId);
}

async function loadBestSlot(gymId) {
  const panel = document.getElementById("occupancy-best-slot");
  panel.innerHTML = '<p class="placeholder">Calcul de la recommandation...</p>';

  let data;
  try {
    const res = await fetch(`/gyms/${gymId}/best_slot`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    data = await res.json();
  } catch (err) {
    panel.innerHTML = '<p class="placeholder">Recommandation indisponible pour le moment.</p>';
    return;
  }

  const slotDate = new Date(data.recommended_slot_utc);
  const timeLabel = slotDate.toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" });
  const isToday = slotDate.toDateString() === new Date().toDateString();
  const dayLabel = isToday ? "aujourd'hui" : "demain";
  const pct = Math.round(data.expected_occupancy_rate * 100);

  panel.innerHTML = `
    <p class="occupancy-recommendation">
      Moins de monde prévu <strong>${dayLabel} vers ${timeLabel}</strong> à ${data.gym_name}
      (environ <strong>${pct}%</strong> de la capacité, soit ~${data.expected_occupancy_count} personnes).
    </p>
    <p class="occupancy-methodology-note">${data.methodology}</p>
  `;
}

function init() {
  document.getElementById("user-select").addEventListener("change", onUserChange);
  document.getElementById("scenario-select").addEventListener("change", onScenarioChange);
  document.getElementById("demo-toggle").addEventListener("change", (e) => {
    isDemoMode = e.target.checked;
    applyModeToUI();
  });
  zoneElements().forEach((el) => el.addEventListener("click", onZoneClick));

  bindSimSlider("sim-charge", "sim-charge-value");
  bindSimSlider("sim-reps", "sim-reps-value");
  bindSimSlider("sim-sets", "sim-sets-value");
  bindSimSlider("sim-duration", "sim-duration-value");
  document.getElementById("sim-submit").addEventListener("click", onSimSubmit);
  applySimulatorAvailability();

  document.getElementById("log-session-submit").addEventListener("click", onLogSessionSubmit);
  applyLogSessionAvailability();

  connectOccupancyStream();

  loadUsers();
}

document.addEventListener("DOMContentLoaded", init);
