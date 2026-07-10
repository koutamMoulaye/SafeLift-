import { useEffect, useState } from "react";
import { useDashboard } from "../hooks/useDashboard";
import { fetchNutrition } from "../api/client";
import HeroStat from "./HeroStat";

// Nutrition (Jalon 3, sous-étape 2/6) : lecture directe de
// gold.fact_nutrition_target / gold.dim_nutrition, AUCUN recalcul côté
// frontend. L'AVERTISSEMENT ÉTHIQUE est une contrainte NON NÉGOCIABLE :
// affiché en tout premier élément de la section, jamais masqué (le champ
// `disclaimer` de la réponse API est la source unique de vérité du texte).
const PROTEIN_GAUGE_MAX_G = 300;

export default function NutritionPanel() {
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
    fetchNutrition(selectedUserId)
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

  const proteinPct = data ? Math.max(0, Math.min(100, (data.protein_target_g_per_day / PROTEIN_GAUGE_MAX_G) * 100)) : 0;

  return (
    <div className="rounded-2xl border border-line bg-panel/60 p-5">
      <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">Nutrition</h2>

      {isDemoMode ? (
        <p className="text-xs text-slate-500">
          Indisponible en mode démo — les scénarios synthétiques n'ont pas d'utilisateur réel associé.
        </p>
      ) : (
        <>
          {/* Avertissement ethique : TOUJOURS affiche en premier, jamais conditionne. */}
          <p className="mb-3 rounded-lg border border-amber-500/30 bg-amber-500/10 p-2 text-[11px] leading-snug text-amber-300">
            ⚠️{" "}
            {data?.disclaimer ||
              "Ces chiffres sont des estimations génériques, pas une recommandation médicale personnalisée."}
          </p>

          {status === "loading" && <p className="text-xs text-slate-500">Chargement…</p>}
          {status === "error" && <p className="text-xs text-red-400">Données indisponibles pour cet utilisateur.</p>}

          {status === "ready" && data && (
            <>
              <div className="mb-3 grid grid-cols-2 gap-3">
                <div>
                  <div className="text-[10px] uppercase tracking-wide text-slate-500">BMR</div>
                  <HeroStat value={Math.round(data.bmr_kcal)} unit="kcal/j" />
                </div>
                <div>
                  <div className="text-[10px] uppercase tracking-wide text-slate-500">TDEE</div>
                  <HeroStat value={Math.round(data.tdee_kcal)} unit="kcal/j" color="var(--color-cyan)" />
                </div>
              </div>
              <p className="mb-3 text-[11px] text-slate-500">
                Facteur d'activité {parseFloat(data.activity_factor).toFixed(2)} —{" "}
                {data.workout_frequency_days_per_week} j/sem., poids {data.body_weight_kg}kg.
              </p>

              <div className="mb-3">
                <div className="mb-1 flex items-baseline justify-between">
                  <span className="text-[10px] uppercase tracking-wide text-slate-500">Besoin protéique</span>
                  <HeroStat value={parseFloat(data.protein_target_g_per_day).toFixed(1)} unit="g/j" color="var(--color-violet)" />
                </div>
                <div className="h-1.5 w-full overflow-hidden rounded-full bg-panel">
                  <div
                    className="h-full rounded-full bg-violet"
                    style={{ width: `${proteinPct}%`, boxShadow: "0 0 8px var(--color-violet)" }}
                  />
                </div>
                <p className="mt-1 text-[11px] text-slate-500">
                  {parseFloat(data.protein_g_per_kg_target).toFixed(1)} g/kg × {data.body_weight_kg}kg
                </p>
              </div>

              <div className="text-[10px] uppercase tracking-wide text-slate-500">Aliments suggérés</div>
              <ul className="mt-1 flex flex-col gap-1">
                {data.suggested_foods.map((food) => (
                  <li key={food.fdc_id} className="flex min-w-0 items-center justify-between gap-2 text-xs">
                    <span className="min-w-0 truncate text-slate-300" title={food.food_name}>
                      {food.food_name}
                    </span>
                    <span className="shrink-0 text-slate-500">
                      {Math.round(food.kcal_per_100g)} kcal · {food.protein_g_per_100g.toFixed(1)}g/100g
                    </span>
                  </li>
                ))}
              </ul>
            </>
          )}
        </>
      )}
    </div>
  );
}
