import { useDashboard } from "../hooks/useDashboard";
import { getDominantFactors, levelColor, muscleLabel } from "../lib/riskHelpers";

// Panneau "Zones sensibles" : explique en langage clair, AVANT tout clic,
// quelles zones sont Modere/Eleve et pourquoi (facteur dominant) -- porte
// depuis renderSensitiveZones() de l'ancien dashboard.js.
export default function SensitiveZones() {
  const { muscles, musclesStatus, isDemoMode } = useDashboard();

  const sensitive = (muscles || [])
    .filter((e) => e.risk_level === "Modere" || e.risk_level === "Eleve")
    .sort((a, b) => parseFloat(b.risk_score) - parseFloat(a.risk_score));

  return (
    <div className="flex h-full min-h-[420px] flex-col rounded-2xl border border-line bg-panel/60 p-5">
      <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">Zones sensibles</h2>

      {musclesStatus === "loading" && <p className="text-xs text-slate-500">Chargement…</p>}
      {musclesStatus === "error" && <p className="text-xs text-red-400">Erreur de chargement.</p>}

      {musclesStatus === "ready" && sensitive.length === 0 && (
        <p className="flex-1 text-xs text-slate-500">
          Aucune zone en Modéré ou Élevé actuellement — tout est sous contrôle.
        </p>
      )}

      {musclesStatus === "ready" && sensitive.length > 0 && (
        <ul className="flex flex-col gap-2">
          {sensitive.map((entry, i) => {
            const label = muscleLabel(entry.muscle_group);
            const color = levelColor(entry.risk_level);
            const reasons = getDominantFactors(entry);
            const reasonText =
              reasons.length > 0
                ? reasons.join(" · ")
                : "Risque de base de la zone (aucun facteur comportemental élevé)";
            return (
              <li key={`${entry.muscle_group}-${i}`} className="min-w-0 rounded-lg border border-line bg-panel-alt/60 p-3">
                <div className="mb-1 flex min-w-0 items-center justify-between gap-2">
                  <span className="min-w-0 truncate text-sm font-medium text-slate-200" title={label}>
                    {label}
                  </span>
                  <span
                    className="shrink-0 rounded-full px-2 py-0.5 text-[11px] font-semibold text-slate-900"
                    style={{ background: color, boxShadow: `0 0 8px ${color}` }}
                  >
                    {entry.risk_level} ({parseFloat(entry.risk_score).toFixed(0)})
                  </span>
                </div>
                <p className="truncate text-xs text-slate-400" title={reasonText}>
                  {reasonText}
                </p>
              </li>
            );
          })}
        </ul>
      )}

      {isDemoMode && (
        <p className="mt-3 text-[11px] text-slate-600">Scénario synthétique — jamais mélangé aux vraies données.</p>
      )}
    </div>
  );
}
