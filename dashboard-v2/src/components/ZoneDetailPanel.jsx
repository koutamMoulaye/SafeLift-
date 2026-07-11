import { useDashboard } from "../hooks/useDashboard";
import { FACTOR_BOUNDS, FACTOR_LABELS, levelColor, muscleLabel } from "../lib/riskHelpers";

// Panneau "Detail de la zone selectionnee" -- PORTE depuis l'ancien
// dashboard (renderZoneDetail()/onZoneClick() de dashboard.js), au clic
// sur une zone de la silhouette (voir Silhouette.jsx). Contrairement aux
// barres pleines "app tracker" de l'ancien dashboard (.factor-fill,
// fond uni), les barres ici sont en WIREFRAME (trait fin + lueur), pour
// rester coherent avec le reste de dashboard-v2 -- AUCUN remplissage
// plat, meme principe que la silhouette elle-meme.
//
// Couleur neutre pour les barres (pas codee par niveau de risque) : la
// couleur --color-cyan (marque, reservee aux elements neutres depuis la
// passe de style holographique du 2026-07-09, jamais utilisee pour les
// codes couleur Faible/Modere/Eleve) -- meme choix deja fait pour les
// points d'articulation de la silhouette.
const BAR_COLOR = "var(--color-cyan)";
const TRACK_COLOR = "#3d4f6b"; // meme couleur neutre que l'armature structurelle de la silhouette

function FactorBar({ label, value, min, max }) {
  const pct = Math.max(0, Math.min(100, ((value - min) / (max - min)) * 100));
  return (
    <div className="grid grid-cols-[1fr_auto] items-center gap-3 text-xs">
      <div className="min-w-0">
        <div className="mb-1 truncate text-slate-400" title={label}>
          {label}
        </div>
        <svg viewBox="0 0 100 8" preserveAspectRatio="none" className="h-2 w-full overflow-visible">
          <rect x="0.5" y="3" width="99" height="2" rx="1" fill="none" stroke={TRACK_COLOR} strokeWidth="1" />
          {pct > 0 && (
            <rect
              x="0.5"
              y="3"
              width={Math.max(pct - 0.5, 1)}
              height="2"
              rx="1"
              fill="none"
              stroke={BAR_COLOR}
              strokeWidth="1.5"
              style={{ filter: `drop-shadow(0 0 3px ${BAR_COLOR})` }}
            />
          )}
        </svg>
      </div>
      <span className="w-10 shrink-0 text-right font-mono text-slate-300">{value.toFixed(2)}</span>
    </div>
  );
}

// Les mollets n'ont AUCUNE correspondance dans gold.dim_muscle.muscle_group
// (dbt/macros/classify_muscle_group.sql ne produit jamais "calves") --
// plutot que de masquer cette zone cliquable, une explication honnete est
// affichee, meme principe que renderCalvesExplanation() de l'ancien
// dashboard (esprit "pas de boite noire" du projet).
function CalvesExplanation() {
  return (
    <p className="text-xs text-slate-400">
      Les mollets sont affichés à titre anatomique mais ne reçoivent{" "}
      <strong className="text-slate-300">jamais</strong> de donnée réelle : le pipeline de classification
      des exercices (<code className="text-[11px]">classify_muscle_group.sql</code>) ne produit aucune
      catégorie « mollets » à partir des données sources. Zone cliquable volontairement, pour l'expliquer
      plutôt que de la masquer.
    </p>
  );
}

export default function ZoneDetailPanel() {
  const { muscles, selectedMuscle, isDemoMode, selectedScenario } = useDashboard();

  const entry = selectedMuscle ? muscles.find((m) => m.muscle_group === selectedMuscle) : null;

  const metaLabel = entry
    ? isDemoMode
      ? `${selectedScenario?.scenario_label ?? ""} — ${selectedScenario?.notes ?? ""}`
      : `Dernier exercice : ${entry.exercise_name} — ${entry.session_date}`
    : null;

  return (
    <div className="flex h-full min-h-[220px] flex-col rounded-2xl border border-line bg-panel/60 p-5">
      <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">
        Détail de la zone sélectionnée
      </h2>

      {!selectedMuscle && (
        <p className="flex-1 text-xs text-slate-500">
          Cliquez une zone de la silhouette pour voir le détail du calcul (score, facteurs, dernier
          exercice).
        </p>
      )}

      {selectedMuscle === "calves" && <CalvesExplanation />}

      {selectedMuscle && selectedMuscle !== "calves" && !entry && (
        <p className="flex-1 text-xs text-slate-500">
          Aucune donnée disponible pour « {muscleLabel(selectedMuscle)} » sur ce profil.
        </p>
      )}

      {entry && (
        <div className="flex flex-col gap-4">
          <div className="flex items-center justify-between gap-2">
            <span className="min-w-0 truncate text-sm font-medium text-slate-200" title={muscleLabel(entry.muscle_group)}>
              {muscleLabel(entry.muscle_group)}
              {isDemoMode && <em className="ml-1 text-slate-500">(scénario synthétique)</em>}
            </span>
            <span
              className="shrink-0 rounded-full px-2 py-0.5 text-[11px] font-semibold text-slate-900"
              style={{ background: levelColor(entry.risk_level), boxShadow: `0 0 8px ${levelColor(entry.risk_level)}` }}
            >
              {entry.risk_level} ({parseFloat(entry.risk_score).toFixed(0)})
            </span>
          </div>

          <div className="flex flex-col gap-2.5">
            {Object.keys(FACTOR_BOUNDS).map((key) => {
              const [min, max] = FACTOR_BOUNDS[key];
              return (
                <FactorBar key={key} label={FACTOR_LABELS[key]} value={parseFloat(entry[key])} min={min} max={max} />
              );
            })}
          </div>

          {metaLabel && <p className="truncate text-[11px] text-slate-500" title={metaLabel}>{metaLabel}</p>}
        </div>
      )}
    </div>
  );
}
