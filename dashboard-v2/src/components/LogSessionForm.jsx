import { useEffect, useRef, useState } from "react";
import { useDashboard } from "../hooks/useDashboard";
import { fetchUserExercises, fetchUserRisk, submitUserSession } from "../api/client";
import { muscleLabel, truncateForSelect } from "../lib/riskHelpers";

// Formulaire "Logger une séance" (Jalon 2, sous-étape 3/5) : POST publie
// sur Kafka, N'ÉCRIT JAMAIS en base directement -- le score est recalculé
// de façon asynchrone par un consumer + un run dbt complet. Polling toutes
// les 3s jusqu'à détecter un changement du score sur la zone concernée, ou
// jusqu'au timeout de 180s (marge mesurée en conditions réelles sur
// l'ancien dashboard : 70s puis 110s, voir CLAUDE.md).
const POLL_INTERVAL_MS = 3000;
const POLL_TIMEOUT_MS = 180000;

export default function LogSessionForm() {
  const { selectedUserId, isDemoMode, muscles, refreshAfterSessionUpdate } = useDashboard();

  const [exercises, setExercises] = useState([]);
  const [exercisesStatus, setExercisesStatus] = useState("idle");
  const [exerciseName, setExerciseName] = useState("");
  const [weight, setWeight] = useState(50);
  const [reps, setReps] = useState(10);
  const [sets, setSets] = useState(3);
  const [durationMin, setDurationMin] = useState(30);

  const [submitting, setSubmitting] = useState(false);
  const [status, setStatus] = useState({ text: "", variant: null });
  const pollTimer = useRef(null);

  useEffect(() => {
    if (isDemoMode || !selectedUserId) {
      setExercises([]);
      setExerciseName("");
      return;
    }
    let cancelled = false;
    setExercisesStatus("loading");
    fetchUserExercises(selectedUserId)
      .then((data) => {
        if (cancelled) return;
        setExercises(data.exercises || []);
        setExerciseName(data.exercises?.[0]?.exercise_name || "");
        setExercisesStatus("ready");
      })
      .catch(() => {
        if (cancelled) return;
        setExercisesStatus("error");
      });
    return () => {
      cancelled = true;
    };
  }, [isDemoMode, selectedUserId]);

  useEffect(() => {
    return () => {
      if (pollTimer.current) clearTimeout(pollTimer.current);
    };
  }, []);

  const selectedExercise = exercises.find((ex) => ex.exercise_name === exerciseName);

  async function handleSubmit(e) {
    e.preventDefault();
    if (isDemoMode || !selectedUserId || !exerciseName) return;

    const muscleGroup = selectedExercise?.muscle_group || null;
    const before = muscles.find((m) => m.muscle_group === muscleGroup);
    const beforeScore = before ? before.risk_score : null;

    setSubmitting(true);
    setStatus({ text: "Envoi de la séance vers Kafka…", variant: "pending" });

    try {
      await submitUserSession(selectedUserId, {
        exercise_name: exerciseName,
        lifted_weight_kg: parseFloat(weight),
        reps: parseInt(reps, 10),
        sets: parseInt(sets, 10),
        duration_seconds: (parseFloat(durationMin) || 0) * 60,
      });
    } catch (err) {
      setStatus({ text: `Échec de l'envoi : ${err.message}`, variant: "error" });
      setSubmitting(false);
      return;
    }

    setStatus({ text: "Séance envoyée — recalcul dbt en cours (≈1 à 2 min)…", variant: "pending" });
    const startedAt = Date.now();

    const poll = async () => {
      const elapsedMs = Date.now() - startedAt;
      if (elapsedMs > POLL_TIMEOUT_MS) {
        setStatus({
          text: `Toujours en attente après ${Math.round(elapsedMs / 1000)}s — le score se mettra à jour automatiquement au prochain rafraîchissement.`,
          variant: "timeout",
        });
        setSubmitting(false);
        return;
      }

      let risk;
      try {
        risk = await fetchUserRisk(selectedUserId);
      } catch {
        pollTimer.current = setTimeout(poll, POLL_INTERVAL_MS);
        return;
      }

      const after = risk.muscles.find((m) => m.muscle_group === muscleGroup);
      const changed = after && (beforeScore === null || after.risk_score !== beforeScore);

      if (changed) {
        const elapsedSeconds = (elapsedMs / 1000).toFixed(1);
        setSubmitting(false);
        refreshAfterSessionUpdate();
        setStatus({
          text: `Score mis à jour en ${elapsedSeconds}s — ${muscleLabel(muscleGroup)} : ${beforeScore ?? "—"} → ${after.risk_score} (${after.risk_level}).`,
          variant: "success",
        });
        return;
      }

      pollTimer.current = setTimeout(poll, POLL_INTERVAL_MS);
    };

    pollTimer.current = setTimeout(poll, POLL_INTERVAL_MS);
  }

  const statusColor =
    status.variant === "success"
      ? "text-emerald-400"
      : status.variant === "error"
        ? "text-red-400"
        : status.variant === "timeout"
          ? "text-amber-400"
          : "text-slate-400";

  return (
    <div className="flex h-full min-h-[420px] flex-col rounded-2xl border border-line bg-panel/60 p-5">
      <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">Logger une séance</h2>

      {isDemoMode ? (
        <p className="flex-1 text-xs text-slate-500">
          Indisponible en mode démo — les scénarios synthétiques n'ont pas d'utilisateur réel associé.
        </p>
      ) : (
        <form onSubmit={handleSubmit} className="flex flex-col gap-3">
          <label className="flex flex-col gap-1 text-xs text-slate-400">
            Exercice
            <select
              className="min-w-0 truncate rounded-lg border border-line bg-panel-alt px-2 py-1.5 text-sm text-slate-200"
              value={exerciseName}
              onChange={(e) => setExerciseName(e.target.value)}
              title={
                selectedExercise
                  ? `${selectedExercise.exercise_name} (${muscleLabel(selectedExercise.muscle_group)})`
                  : ""
              }
              disabled={exercisesStatus !== "ready" || exercises.length === 0}
            >
              {exercisesStatus === "loading" && <option>Chargement…</option>}
              {exercisesStatus === "ready" && exercises.length === 0 && <option>Aucun exercice loggué</option>}
              {exercises.map((ex) => {
                const full = `${ex.exercise_name} (${muscleLabel(ex.muscle_group)})`;
                return (
                  <option key={ex.exercise_id} value={ex.exercise_name} title={full}>
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
                value={weight}
                onChange={(e) => setWeight(e.target.value)}
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
            disabled={submitting || !exerciseName}
            className="mt-1 rounded-lg bg-cyan/90 px-3 py-2 text-sm font-semibold text-slate-900 transition hover:bg-cyan disabled:cursor-not-allowed disabled:opacity-40"
          >
            {submitting ? "Envoi / recalcul en cours…" : "Logger la séance"}
          </button>

          {status.text && <p className={`text-xs ${statusColor}`}>{status.text}</p>}
        </form>
      )}
    </div>
  );
}
