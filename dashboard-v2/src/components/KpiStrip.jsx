import { useDashboard } from "../hooks/useDashboard";
import { muscleLabel } from "../lib/riskHelpers";

// Comble un ecart trouve lors de l'audit de parite v1/v2 (2026-07-11) :
// l'ancien dashboard affiche 4 cards KPI (derniere seance / jauge radiale /
// tendance / zones en alerte) -- "jauge" et "tendance" ont deja un
// equivalent v2 (score global dans TopBar, BottomTrends), mais "derniere
// seance" et "zones en alerte" n'avaient AUCUN equivalent nulle part dans
// dashboard-v2. Meme logique que renderLastSessionKpi()/renderAlertsKpi()
// de dashboard/static/dashboard.js, portee ici -- pas de nouvel appel API
// (reutilise `muscles`, deja charge par le contexte pour TopBar/SensitiveZones).
export default function KpiStrip() {
  const { muscles, musclesStatus, isDemoMode, selectedScenario } = useDashboard();

  const alertsCount = muscles.filter((m) => m.risk_level === "Modere" || m.risk_level === "Eleve").length;

  // En mode demo, les scenarios synthetiques n'ont pas de session_date ni
  // d'exercise_name (voir gold.fact_risk_score_demo_synthetic) -- meme
  // repli que v1 (renderLastSessionKpi(null, scenario.scenario_label)) :
  // on affiche le libelle du scenario a la place.
  const latestReal = !isDemoMode
    ? muscles.reduce((acc, m) => (!acc || m.session_date > acc.session_date ? m : acc), null)
    : null;

  return (
    <div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
      <div className="rounded-2xl border border-line bg-panel/60 p-4">
        <h3 className="mb-1 text-[11px] font-semibold uppercase tracking-wide text-slate-500">Dernière séance</h3>
        {isDemoMode ? (
          <p className="truncate text-lg font-semibold text-slate-200" title={selectedScenario?.scenario_label}>
            {selectedScenario?.scenario_label || "—"}
          </p>
        ) : musclesStatus === "loading" ? (
          <p className="text-lg text-slate-600">…</p>
        ) : latestReal ? (
          <>
            <p className="truncate text-lg font-semibold text-slate-200" title={latestReal.exercise_name}>
              {latestReal.exercise_name}
            </p>
            <p className="truncate text-xs text-slate-500">
              {muscleLabel(latestReal.muscle_group)} — {latestReal.session_date}
            </p>
          </>
        ) : (
          <p className="text-lg text-slate-600">—</p>
        )}
      </div>

      <div className="rounded-2xl border border-line bg-panel/60 p-4">
        <h3 className="mb-1 text-[11px] font-semibold uppercase tracking-wide text-slate-500">Zones en alerte</h3>
        <p
          className="text-3xl font-bold"
          style={{
            color: alertsCount > 0 ? "var(--color-risk-modere)" : "var(--color-risk-faible)",
            textShadow: `0 0 10px ${alertsCount > 0 ? "var(--color-risk-modere)" : "var(--color-risk-faible)"}`,
          }}
        >
          {musclesStatus === "loading" ? "…" : alertsCount}
        </p>
        <p className="text-xs text-slate-500">Modéré ou Élevé, {isDemoMode ? "scénario en cours" : "profil actuel"}</p>
      </div>
    </div>
  );
}
