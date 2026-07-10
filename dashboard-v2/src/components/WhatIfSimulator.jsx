import { useEffect, useState } from "react";
import { useDashboard } from "../hooks/useDashboard";
import { fetchUserExercises, simulateRisk } from "../api/client";
import { muscleLabel, truncateForSelect, levelColor } from "../lib/riskHelpers";

// Simulateur what-if : REUTILISE TEL QUEL l'endpoint POST /api/simulate-risk
// deja existant sur l'ancien dashboard (Feature A, 2026-07-06) -- aucun
// nouvel endpoint cree, aucune logique metier dupliquee cote frontend.
// Difference cle avec "Logger une séance" : calcul PUR (aucune ecriture en
// base, aucune publication Kafka), reponse INSTANTANEE -- pas de polling.
//
// Coherence formule verifiee (2026-07-11, voir CLAUDE.md/PROGRESS_JALON3.md) :
// risk_formula.compute_risk_score() (le module Python que cet endpoint
// appelle) reproduit EXACTEMENT le risk_score/risk_level de 6 lignes reelles
// de gold.fact_risk_score (scores satures a 100, a 0, et une valeur
// intermediaire non-clampee 24.69) en reinjectant leurs facteurs deja
// stockes -- seule la colonne raw_risk_score stockee en base differe
// (arrondie a 4 decimales par dbt pour l'affichage), sans impact sur
// risk_score/risk_level, qui sont eux calcules a partir de la valeur pleine
// precision des deux cotes.
export default function WhatIfSimulator() {
  const { selectedUserId, isDemoMode } = useDashboard();

  const [exercises, setExercises] = useState([]);
  const [exercisesStatus, setExercisesStatus] = useState("idle");
  const [exerciseId, setExerciseId] = useState(null);
  const [chargeKg, setChargeKg] = useState(50);
  const [reps, setReps] = useState(10);
  const [sets, setSets] = useState(3);
  const [durationMin, setDurationMin] = useState(30);

  const [simulating, setSimulating] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  // Exercices REELLEMENT deja pratiques par cet utilisateur (meme source que
  // "Logger une séance") -- garantit une baseline charge/volume exploitable
  // pour charge_factor/volume_factor, cote backend.
  useEffect(() => {
    if (isDemoMode || !selectedUserId) {
      setExercises([]);
      setExerciseId(null);
      setResult(null);
      return;
    }
    let cancelled = false;
    setExercisesStatus("loading");
    fetchUserExercises(selectedUserId)
      .then((data) => {
        if (cancelled) return;
        const list = data.exercises || [];
        setExercises(list);
        setExerciseId(list[0]?.exercise_id ?? null);
        setExercisesStatus("ready");
      })
      .catch(() => {
        if (!cancelled) setExercisesStatus("error");
      });
    return () => {
      cancelled = true;
    };
  }, [isDemoMode, selectedUserId]);

  // Un resultat affiche pour un autre utilisateur/exercice serait trompeur --
  // efface a chaque changement, jusqu'a la prochaine simulation explicite.
  useEffect(() => {
    setResult(null);
    setError(null);
  }, [selectedUserId, exerciseId]);

  async function handleSimulate(e) {
    e.preventDefault();
    if (!selectedUserId || !exerciseId) return;

    setSimulating(true);
    setError(null);
    try {
      const data = await simulateRisk({
        user_id: selectedUserId,
        exercise_id: exerciseId,
        charge_kg: parseFloat(chargeKg),
        reps: parseInt(reps, 10),
        sets: parseInt(sets, 10),
        duration_minutes: parseFloat(durationMin),
      });
      setResult(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setSimulating(false);
    }
  }

  const factorOrder = ["base_zone", "charge_factor", "volume_factor", "recup_factor", "duree_factor"];

  return (
    <div className="rounded-2xl border border-line bg-panel/60 p-5">
      <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">Simulateur what-if</h2>

      {isDemoMode ? (
        <p className="text-xs text-slate-500">
          Indisponible en mode démo — les scénarios synthétiques n'ont pas d'utilisateur réel associé.
        </p>
      ) : (
        <form onSubmit={handleSimulate} className="flex flex-col gap-3">
          <label className="flex flex-col gap-1 text-xs text-slate-400">
            Exercice
            <select
              className="min-w-0 truncate rounded-lg border border-line bg-panel-alt px-2 py-1.5 text-sm text-slate-200"
              value={exerciseId ?? ""}
              onChange={(e) => setExerciseId(Number(e.target.value))}
              title={
                exercises.find((ex) => ex.exercise_id === exerciseId)
                  ? `${exercises.find((ex) => ex.exercise_id === exerciseId).exercise_name} (${muscleLabel(exercises.find((ex) => ex.exercise_id === exerciseId).muscle_group)})`
                  : ""
              }
              disabled={exercisesStatus !== "ready" || exercises.length === 0}
            >
              {exercisesStatus === "loading" && <option>Chargement…</option>}
              {exercisesStatus === "ready" && exercises.length === 0 && <option>Aucun exercice loggué</option>}
              {exercises.map((ex) => {
                const full = `${ex.exercise_name} (${muscleLabel(ex.muscle_group)})`;
                return (
                  <option key={ex.exercise_id} value={ex.exercise_id} title={full}>
                    {truncateForSelect(full)}
                  </option>
                );
              })}
            </select>
          </label>

          <div className="grid grid-cols-2 gap-3">
            <label className="flex flex-col gap-1 text-xs text-slate-400">
              Charge (kg)
              <input
                type="number"
                min="0"
                step="0.5"
                value={chargeKg}
                onChange={(e) => setChargeKg(e.target.value)}
                className="rounded-lg border border-line bg-panel-alt px-2 py-1.5 text-sm text-slate-200"
              />
            </label>
            <label className="flex flex-col gap-1 text-xs text-slate-400">
              Durée (min)
              <input
                type="number"
                min="0"
                step="1"
                value={durationMin}
                onChange={(e) => setDurationMin(e.target.value)}
                className="rounded-lg border border-line bg-panel-alt px-2 py-1.5 text-sm text-slate-200"
              />
            </label>
            <label className="flex flex-col gap-1 text-xs text-slate-400">
              Répétitions
              <input
                type="number"
                min="1"
                step="1"
                value={reps}
                onChange={(e) => setReps(e.target.value)}
                className="rounded-lg border border-line bg-panel-alt px-2 py-1.5 text-sm text-slate-200"
              />
            </label>
            <label className="flex flex-col gap-1 text-xs text-slate-400">
              Séries
              <input
                type="number"
                min="1"
                step="1"
                value={sets}
                onChange={(e) => setSets(e.target.value)}
                className="rounded-lg border border-line bg-panel-alt px-2 py-1.5 text-sm text-slate-200"
              />
            </label>
          </div>

          <button
            type="submit"
            disabled={simulating || !exerciseId}
            className="mt-1 rounded-lg bg-blue/90 px-3 py-2 text-sm font-semibold text-white transition hover:bg-blue disabled:cursor-not-allowed disabled:opacity-40"
          >
            {simulating ? "Calcul…" : "Simuler"}
          </button>

          {error && <p className="text-xs text-red-400">Échec de la simulation : {error}</p>}
        </form>
      )}

      {result && (
        <div className="mt-4 rounded-xl border border-dashed border-blue-400/50 bg-blue-500/5 p-3">
          <p className="mb-2 text-[11px] font-semibold uppercase tracking-wide text-blue-300">
            ⚗ Simulation — hypothèse, rien n'est enregistré
          </p>
          <p className="mb-2 text-xs text-slate-400">
            {result.exercise_name} — {result.muscle_zone}
          </p>

          <div className="mb-3 flex flex-wrap items-baseline gap-x-4 gap-y-1">
            <div>
              <span
                className="text-2xl font-bold"
                style={{ color: levelColor(result.risk_level_simule), textShadow: `0 0 8px ${levelColor(result.risk_level_simule)}` }}
              >
                {result.risk_score_simule}
              </span>
              <span className="ml-1 text-xs text-slate-500">({result.risk_level_simule})</span>
            </div>
            {result.risk_score_actuel !== null ? (
              <span className="text-xs text-slate-500">
                vs actuel : {result.risk_score_actuel} ({result.risk_level_actuel}) — Δ{" "}
                <span className={result.delta > 0 ? "text-red-400" : result.delta < 0 ? "text-emerald-400" : ""}>
                  {result.delta >= 0 ? "+" : ""}
                  {result.delta}
                </span>
              </span>
            ) : (
              <span className="text-xs text-slate-500">Aucun risk_score déjà calculé pour comparaison.</span>
            )}
          </div>

          <ul className="space-y-1.5 text-[11px] leading-snug text-slate-400">
            {factorOrder.map((key) => {
              const f = result.facteurs[key];
              return (
                <li key={key}>
                  <span className="font-medium text-slate-300">{key}</span> = {f.valeur}
                  <span className="text-slate-500"> — {f.explication}</span>
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </div>
  );
}
