import { useEffect, useRef, useState } from "react";
import { fetchGymBestSlot, occupancyStreamUrl } from "../api/client";

// Affluence en direct (Jalon 2, sous-étape 4/5) : Server-Sent Events, pas
// de WebSocket ni de polling -- une seule connexion EventSource ouverte au
// montage du composant (jamais recréée), fermée proprement au démontage.
// Couleurs par charge_category volontairement DISTINCTES de COLOR_BY_LEVEL
// (risque musculo-squelettique) : gold.gym_occupancy_live produit
// "Faible"/"Moderee"/"Elevee" (orthographe différente de "Modere"/"Eleve"),
// voir dashboard.js pour le même constat déjà documenté.
const OCCUPANCY_COLOR_BY_CATEGORY = {
  Faible: "#2ecc71",
  Moderee: "#f5a623",
  Elevee: "#f0483e",
};
const COLOR_NONE = "#3a4152";

export default function OccupancyPanel() {
  const [gyms, setGyms] = useState([]);
  const [connected, setConnected] = useState(false);
  const [selectedGymId, setSelectedGymId] = useState(null);
  const [bestSlot, setBestSlot] = useState(null);
  const [bestSlotStatus, setBestSlotStatus] = useState("idle");
  const autoSelected = useRef(false);

  useEffect(() => {
    const source = new EventSource(occupancyStreamUrl());
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(false);
    source.onmessage = (event) => {
      const data = JSON.parse(event.data);
      setGyms(data.gyms || []);
    };
    return () => source.close();
  }, []);

  useEffect(() => {
    if (!autoSelected.current && gyms.length > 0) {
      autoSelected.current = true;
      setSelectedGymId(gyms[0].gym_id);
    }
  }, [gyms]);

  useEffect(() => {
    if (!selectedGymId) return;
    let cancelled = false;
    setBestSlotStatus("loading");
    fetchGymBestSlot(selectedGymId)
      .then((data) => {
        if (cancelled) return;
        setBestSlot(data);
        setBestSlotStatus("ready");
      })
      .catch(() => {
        if (cancelled) return;
        setBestSlotStatus("error");
      });
    return () => {
      cancelled = true;
    };
  }, [selectedGymId]);

  return (
    <div className="rounded-2xl border border-line bg-panel/60 p-5">
      <div className="mb-3 flex items-center justify-between">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-slate-500">Affluence en direct</h2>
        <span
          className={`flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase ${
            connected ? "bg-emerald-500/15 text-emerald-400" : "bg-slate-600/20 text-slate-500"
          }`}
        >
          <span className={`h-1.5 w-1.5 rounded-full ${connected ? "animate-pulse bg-emerald-400" : "bg-slate-500"}`} />
          {connected ? "En direct" : "Déconnecté"}
        </span>
      </div>

      {gyms.length === 0 ? (
        <p className="text-xs text-slate-500">En attente du flux…</p>
      ) : (
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
          {gyms.map((g) => {
            const pct = Math.round(parseFloat(g.occupancy_rate) * 100);
            const color = OCCUPANCY_COLOR_BY_CATEGORY[g.charge_category] || COLOR_NONE;
            const selected = g.gym_id === selectedGymId;
            return (
              <button
                key={g.gym_id}
                onClick={() => setSelectedGymId(g.gym_id)}
                className={`min-w-0 rounded-lg border p-3 text-left transition ${
                  selected ? "border-cyan bg-panel-alt" : "border-line bg-panel-alt/60 hover:border-slate-600"
                }`}
              >
                <h3 className="truncate text-xs font-medium text-slate-300" title={g.gym_name}>
                  {g.gym_name}
                </h3>
                <p className="text-2xl font-bold" style={{ color, textShadow: `0 0 12px ${color}` }}>
                  {pct}
                  <span className="ml-0.5 text-sm font-normal text-slate-400">%</span>
                </p>
                <div className="mt-1 h-1.5 w-full overflow-hidden rounded-full bg-panel">
                  <div className="h-full rounded-full" style={{ width: `${pct}%`, background: color, boxShadow: `0 0 8px ${color}` }} />
                </div>
                <p className="mt-1 truncate text-[11px] text-slate-500">
                  {g.current_occupancy} / {g.capacity} pers. — <span style={{ color }}>{g.charge_category}</span>
                </p>
              </button>
            );
          })}
        </div>
      )}

      <div className="mt-4 rounded-lg border border-dashed border-line bg-panel-alt/40 p-3">
        {bestSlotStatus === "loading" && <p className="text-xs text-slate-500">Calcul de la recommandation…</p>}
        {bestSlotStatus === "ready" && bestSlot && (
          <>
            <p className="text-xs text-slate-300">
              Moins de monde prévu vers{" "}
              <strong>
                {new Date(bestSlot.recommended_slot_utc).toLocaleTimeString("fr-FR", {
                  hour: "2-digit",
                  minute: "2-digit",
                })}
              </strong>{" "}
              à {bestSlot.gym_name} (~{Math.round(bestSlot.expected_occupancy_rate * 100)}%,{" "}
              {bestSlot.expected_occupancy_count} pers.)
            </p>
            <p className="mt-1 text-[10px] text-slate-600">{bestSlot.methodology}</p>
          </>
        )}
        {bestSlotStatus === "error" && <p className="text-xs text-slate-500">Recommandation indisponible.</p>}
      </div>
    </div>
  );
}
