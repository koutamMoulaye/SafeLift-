import { useEffect, useState } from "react";
import { fetchUserRisk } from "../api/client";

// Hook : recupere GET /users/{userId}/risk au montage et a chaque
// changement de userId. Toujours utilise par Silhouette.jsx avec
// user_id=9 code en dur (voir Silhouette.jsx) -- INCHANGE pour cet appel.
// Reutilise aussi par les widgets branches sur le vrai selecteur
// d'utilisateur (voir DashboardContext) : userId peut y valoir null/
// undefined (mode demo, ou selecteur pas encore charge) -- dans ce cas le
// hook n'appelle pas l'API et reste en status "idle", plutot que de
// fetcher /users/null/risk.
// `reloadKey` (optionnel, defaut 0) permet de forcer un refetch sans
// changer userId -- utilise par le formulaire "Logger une seance" pour
// rafraichir apres un recalcul dbt detecte par polling (voir
// DashboardContext.bumpMusclesRefresh).
export function useUserRisk(userId, reloadKey = 0) {
  const [muscles, setMuscles] = useState([]);
  const [status, setStatus] = useState(userId ? "loading" : "idle"); // "idle" | "loading" | "ready" | "error"
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!userId) {
      setMuscles([]);
      setStatus("idle");
      return;
    }

    let cancelled = false;
    setStatus("loading");

    fetchUserRisk(userId)
      .then((data) => {
        if (cancelled) return;
        setMuscles(data.muscles || []);
        setStatus("ready");
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err);
        setStatus("error");
      });

    return () => {
      cancelled = true;
    };
  }, [userId, reloadKey]);

  return { muscles, status, error };
}
