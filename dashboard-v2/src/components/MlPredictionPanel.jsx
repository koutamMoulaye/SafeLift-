import { useEffect, useState } from "react";
import { useDashboard } from "../hooks/useDashboard";
import { fetchMlPrediction } from "../api/client";
import { muscleLabel } from "../lib/riskHelpers";

// Tendance prédictive (bonus ML, Jalon 3 sous-étape 5/6) : lecture directe
// de gold.ml_risk_prediction, AUCUN calcul côté frontend. DÉCISION DÉJÀ
// ACTÉE : reste TOUJOURS visuellement distinct du risk_score déterministe
// (bordure pointillée violette + badge EXPERIMENTAL), jamais fusionné,
// jamais présenté comme plus fiable. Le champ `disclaimer` de l'API est
// TOUJOURS affiché, disponible ou non.
export default function MlPredictionPanel() {
  const { selectedUserId, isDemoMode } = useDashboard();
  const [data, setData] = useState(null);
  const [status, setStatus] = useState("idle");

  useEffect(() => {
    if (isDemoMode || !selectedUserId) {
      setData(null);
      return;
    }
    let cancelled = false;
    setStatus("loading");
    fetchMlPrediction(selectedUserId)
      .then((d) => {
        if (cancelled) return;
        setData(d);
        setStatus("ready");
      })
      .catch(() => {
        if (cancelled) return;
        setStatus("error");
      });
    return () => {
      cancelled = true;
    };
  }, [isDemoMode, selectedUserId]);

  return (
    <div
      className="rounded-2xl border border-dashed p-5"
      style={{ borderColor: "var(--color-violet)", background: "rgba(139,92,246,0.06)" }}
    >
      <div className="mb-3 flex items-center justify-between gap-2">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-slate-400">Tendance prédictive</h2>
        <span
          className="shrink-0 rounded-full px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide text-slate-900"
          style={{ background: "var(--color-violet)", boxShadow: "0 0 8px var(--color-violet)" }}
        >
          Expérimental
        </span>
      </div>

      {isDemoMode ? (
        <p className="text-xs text-slate-500">
          Indisponible en mode démo — les scénarios synthétiques n'ont pas d'utilisateur réel associé.
        </p>
      ) : (
        <>
          {data?.disclaimer && <p className="mb-3 text-[11px] leading-snug text-slate-500">⚠️ {data.disclaimer}</p>}

          {status === "loading" && <p className="text-xs text-slate-500">Chargement…</p>}
          {status === "error" && <p className="text-xs text-red-400">Communication impossible avec le serveur.</p>}

          {status === "ready" && data && !data.available && (
            <p className="text-xs text-slate-500">Non disponible pour ce profil — {data.reason}</p>
          )}

          {status === "ready" && data?.available && (
            <ul className="flex flex-col gap-2">
              {data.predictions.map((p) => (
                <li key={p.muscle_group} className="flex min-w-0 items-center justify-between gap-2 rounded-lg border border-line/60 bg-panel-alt/50 p-2">
                  <div className="min-w-0">
                    <span className="block truncate text-xs font-medium text-slate-300" title={muscleLabel(p.muscle_group)}>
                      {muscleLabel(p.muscle_group)}
                    </span>
                    <span className="block truncate text-[10px] text-slate-500">
                      Semaine du {p.week_predicted_for} — basé sur {p.based_on_week}
                    </span>
                  </div>
                  <span className="shrink-0 text-lg font-bold" style={{ color: "var(--color-violet)" }}>
                    {parseFloat(p.predicted_risk_score).toFixed(1)}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </>
      )}
    </div>
  );
}
