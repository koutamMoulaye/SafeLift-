import { useMemo } from "react";
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
} from "recharts";
import { useDashboard } from "../hooks/useDashboard";
import { computeTrend } from "../lib/riskHelpers";

// Tendance : jauge KPI (2e moitié vs 1re moitié de l'historique, pas un
// découpage calendaire strict) + graphique en aire (recharts). Non
// applicable en mode démo (données synthétiques ponctuelles, sans série
// temporelle).
//
// ⚠️ Remplace l'ancien rendu SVG fait main (polyline + un cercle colore par
// point, porte tel quel depuis drawHistoryChart() de l'ancien dashboard) :
// avec les 573 points reels de l'historique de user_id=9 compresses dans
// une largeur de 400px, cette approche produisait un enchevetrement de
// pics colores en haut de traits verticaux -- visuellement une "pelouse",
// pas un graphique de tendance lisible. Corrige en (1) supprimant les
// marqueurs par point (aucun cercle par vertex), (2) une seule aire/ligne
// continue avec degrade cyan->bleu, (3) un axe X a VRAIES dates mais avec
// seulement une poignee de ticks affiches (calcules explicitement, pas les
// 573 labels), (4) deux lignes de repere discretes aux seuils
// Faible/Modere/Eleve (33/66) deja utilises partout ailleurs dans le
// dashboard pour donner un contexte de lecture sans surcharger.
//
// `recharts` choisi plutot que du SVG fait main (deja disponible dans
// l'ecosysteme React, ajoute comme dependance dashboard-v2) : gere nativement
// le redimensionnement responsive, le tooltip au survol et l'espacement des
// ticks d'axe -- reecrire cette logique a la main aurait pris plus de temps
// pour un resultat moins robuste, sans gain de coherence avec le reste du
// projet (ce composant n'a pas d'equivalent partage avec l'ancien dashboard
// vanilla JS, contrairement a MUSCLE_LABELS_FR par exemple).
const CHART_HEIGHT = 220;
const TICK_COUNT = 6;

function formatTickDate(dateStr) {
  const d = new Date(dateStr);
  if (Number.isNaN(d.getTime())) return dateStr;
  return d.toLocaleDateString("fr-FR", { month: "short", year: "numeric" });
}

function formatTooltipDate(dateStr) {
  const d = new Date(dateStr);
  if (Number.isNaN(d.getTime())) return dateStr;
  return d.toLocaleDateString("fr-FR", { day: "numeric", month: "long", year: "numeric" });
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload || payload.length === 0) return null;
  const { score, dataPoints } = payload[0].payload;
  return (
    <div className="rounded-lg border border-line bg-panel-alt px-3 py-2 text-xs text-slate-200 shadow-lg">
      <p className="font-semibold text-cyan">{formatTooltipDate(label)}</p>
      <p>risk_score moyen : {score.toFixed(1)}</p>
      <p className="text-slate-500">{dataPoints} séance(s) ce jour-là</p>
    </div>
  );
}

export default function BottomTrends() {
  const { isDemoMode, history, historyStatus } = useDashboard();

  const trend = computeTrend(history);

  // Conversion en nombres explicite : l'API renvoie `avg_risk_score` comme
  // une chaine ("13.85") -- recharts a besoin de valeurs numeriques pour
  // calculer les positions des points.
  const chartData = useMemo(
    () =>
      history.map((point) => ({
        date: point.session_date,
        score: Number(point.avg_risk_score),
        dataPoints: point.data_points,
      })),
    [history]
  );

  // Poignee de ticks d'axe X calcules explicitement (dates reelles de
  // l'historique) -- afficher les 573 labels un par un serait illisible,
  // et laisser recharts deviner l'espacement sur un axe de type "category"
  // n'evite pas le chevauchement par defaut.
  const tickValues = useMemo(() => {
    if (chartData.length === 0) return [];
    const count = Math.min(TICK_COUNT, chartData.length);
    const indices = Array.from({ length: count }, (_, i) =>
      Math.round((i * (chartData.length - 1)) / (count - 1 || 1))
    );
    return [...new Set(indices)].map((i) => chartData[i].date);
  }, [chartData]);

  return (
    <div className="rounded-2xl border border-line bg-panel/60 p-5">
      <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">Tendance</h2>

      {isDemoMode ? (
        <p className="text-xs text-slate-500">Non applicable en mode démo (données ponctuelles fictives).</p>
      ) : historyStatus === "loading" ? (
        <p className="text-xs text-slate-500">Chargement…</p>
      ) : history.length === 0 ? (
        <p className="text-xs text-slate-500">Historique insuffisant pour tracer une tendance.</p>
      ) : (
        <>
          <div className="mb-3">
            <p
              className="text-2xl font-bold"
              style={{
                color: trend ? (trend.delta > 0 ? "var(--color-risk-eleve)" : "var(--color-risk-faible)") : undefined,
              }}
            >
              {trend ? `${trend.delta >= 0 ? "+" : ""}${trend.delta.toFixed(1)} pts` : "—"}
            </p>
            <p className="text-[11px] text-slate-500">2e moitié vs 1re moitié de l'historique</p>
          </div>

          <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
            <AreaChart data={chartData} margin={{ top: 8, right: 8, left: -16, bottom: 0 }}>
              <defs>
                <linearGradient id="trendFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="var(--color-cyan)" stopOpacity={0.35} />
                  <stop offset="100%" stopColor="var(--color-blue)" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="var(--color-line)" strokeDasharray="3 3" vertical={false} />
              <XAxis
                dataKey="date"
                ticks={tickValues}
                tickFormatter={formatTickDate}
                stroke="var(--color-line)"
                tick={{ fill: "#64748b", fontSize: 11 }}
                tickLine={false}
              />
              <YAxis
                domain={[0, 100]}
                stroke="var(--color-line)"
                tick={{ fill: "#64748b", fontSize: 11 }}
                tickLine={false}
                width={28}
              />
              <ReferenceLine y={33} stroke="var(--color-risk-faible)" strokeDasharray="4 4" strokeOpacity={0.4} />
              <ReferenceLine y={66} stroke="var(--color-risk-modere)" strokeDasharray="4 4" strokeOpacity={0.4} />
              <Tooltip content={<CustomTooltip />} />
              <Area
                type="monotone"
                dataKey="score"
                stroke="var(--color-cyan)"
                strokeWidth={2}
                fill="url(#trendFill)"
                dot={false}
                activeDot={{ r: 4, fill: "var(--color-cyan)" }}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </>
      )}
    </div>
  );
}
