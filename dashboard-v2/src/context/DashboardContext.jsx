import { useEffect, useMemo, useState } from "react";
import { fetchUsers, fetchUserRiskHistory, fetchDemoScenarios } from "../api/client";
import { useUserRisk } from "../hooks/useUserRisk";
import { DashboardContext } from "./dashboardContextObject";

// Etat partage du dashboard : selecteur d'utilisateur, toggle demo,
// donnees de risque (reelles OU scenario demo selon le mode). Justifie ici
// (contrairement au reste du projet, plutot "chaque widget fetch le
// sien") par le nombre de widgets qui ont TOUS besoin de la meme reponse
// GET /users/{id}/risk (TopBar pour le score global, Zones sensibles,
// Tendance) -- centraliser evite 3 fetchs identiques a chaque changement
// d'utilisateur.
//
// Silhouette.jsx lit egalement ce contexte depuis le correctif du
// 2026-07-11 (voir Silhouette.jsx) -- elle suit desormais selectedUserId/
// isDemoMode exactement comme les autres widgets, plus de user_id=9 fige.
//
// Le contexte brut vit dans `dashboardContextObject.js` (pas ici) pour que
// ce fichier n'exporte QUE le composant `DashboardProvider` -- voir la note
// de bug en bas de ce fichier.
export function DashboardProvider({ children }) {
  const [usersWithData, setUsersWithData] = useState([]);
  const [usersWithoutData, setUsersWithoutData] = useState([]);
  const [usersStatus, setUsersStatus] = useState("loading");

  const [selectedUserId, setSelectedUserId] = useState(null);
  const [isDemoMode, setIsDemoMode] = useState(false);

  const [demoScenarios, setDemoScenarios] = useState([]);
  const [demoScenariosStatus, setDemoScenariosStatus] = useState("idle");
  const [selectedScenarioId, setSelectedScenarioId] = useState(null);

  const [history, setHistory] = useState([]);
  const [historyStatus, setHistoryStatus] = useState("idle");

  // Compteurs incrementes pour forcer un refetch sans changer userId --
  // utilise par le formulaire "Logger une seance" une fois qu'un
  // changement de score reel est detecte par son propre polling.
  const [musclesRefreshKey, setMusclesRefreshKey] = useState(0);
  const [historyRefreshKey, setHistoryRefreshKey] = useState(0);

  // Charge la liste des utilisateurs une seule fois -- pre-selectionne
  // toujours un profil AVEC donnees en premier (jamais un ecran vide par
  // defaut), meme regle deja etablie sur l'ancien dashboard.
  useEffect(() => {
    fetchUsers()
      .then((data) => {
        const withData = data.users_with_data || [];
        const withoutData = data.users_without_data || [];
        setUsersWithData(withData);
        setUsersWithoutData(withoutData);
        const first = withData[0] || withoutData[0];
        if (first) setSelectedUserId(first.user_id);
        setUsersStatus("ready");
      })
      .catch(() => setUsersStatus("error"));
  }, []);

  // Scenarios demo charges paresseusement (premiere activation du toggle),
  // comme l'ancien dashboard.
  useEffect(() => {
    if (isDemoMode && demoScenariosStatus === "idle") {
      setDemoScenariosStatus("loading");
      fetchDemoScenarios()
        .then((data) => {
          setDemoScenarios(data);
          if (data.length > 0) setSelectedScenarioId(data[0].scenario_id);
          setDemoScenariosStatus("ready");
        })
        .catch(() => setDemoScenariosStatus("error"));
    }
  }, [isDemoMode, demoScenariosStatus]);

  const { muscles: realMuscles, status: realStatus } = useUserRisk(
    !isDemoMode ? selectedUserId : null,
    musclesRefreshKey
  );

  // Historique reel uniquement -- "Non applicable en mode démo" (memes
  // raisons que l'ancien dashboard : donnees ponctuelles fictives sans
  // serie temporelle).
  useEffect(() => {
    if (isDemoMode || !selectedUserId) {
      setHistory([]);
      setHistoryStatus("idle");
      return;
    }
    let cancelled = false;
    setHistoryStatus("loading");
    fetchUserRiskHistory(selectedUserId)
      .then((data) => {
        if (cancelled) return;
        setHistory(data.history || []);
        setHistoryStatus("ready");
      })
      .catch(() => {
        if (cancelled) return;
        setHistoryStatus("error");
      });
    return () => {
      cancelled = true;
    };
  }, [isDemoMode, selectedUserId, historyRefreshKey]);

  const selectedScenario = useMemo(
    () => demoScenarios.find((s) => s.scenario_id === selectedScenarioId) || null,
    [demoScenarios, selectedScenarioId]
  );

  // En mode demo : un seul scenario = une seule "zone" active a la fois
  // (les autres widgets qui listent des zones n'en verront donc qu'une).
  const muscles = isDemoMode ? (selectedScenario ? [selectedScenario] : []) : realMuscles;
  const musclesStatus = isDemoMode ? (demoScenariosStatus === "loading" ? "loading" : "ready") : realStatus;

  const value = {
    usersWithData,
    usersWithoutData,
    usersStatus,
    selectedUserId,
    setSelectedUserId,
    isDemoMode,
    setIsDemoMode,
    demoScenarios,
    demoScenariosStatus,
    selectedScenarioId,
    setSelectedScenarioId,
    selectedScenario,
    muscles,
    musclesStatus,
    history,
    historyStatus,
    // Appele par le formulaire "Logger une seance" apres detection d'un
    // score change -- rafraichit muscles ET historique d'un seul geste
    // (equivalent du onUserChange() complet de l'ancien dashboard).
    refreshAfterSessionUpdate: () => {
      setMusclesRefreshKey((k) => k + 1);
      setHistoryRefreshKey((k) => k + 1);
    },
  };

  return <DashboardContext.Provider value={value}>{children}</DashboardContext.Provider>;
}

// ⚠️ BUG REEL trouve et corrige (2026-07-11) : le hook `useDashboard` etait
// documente ci-dessus comme "deplace dans src/hooks/useDashboard.js", mais
// ce fichier n'existait PAS et ce module n'exportait plus du tout
// `useDashboard` (seulement `DashboardProvider`) -- la refonte n'avait
// jamais ete terminee. Consequence reelle, reproduite avec Playwright
// (`debug_demo_toggle2.cjs`) : TOUT chargement de page (pas seulement le
// clic sur le toggle demo) echouait avec une erreur JS non rattrapee
// ("does not provide an export named 'useDashboard'"), empechant React de
// monter l'arbre -- page blanche. Le toggle demo semblait etre la cause
// uniquement parce que c'etait la premiere interaction testee apres cet
// etat casse. Corrige en creant reellement `src/hooks/useDashboard.js`,
// qui importe l'objet Context depuis `dashboardContextObject.js` (fichier
// dedie, cree dans le meme correctif -- necessaire car exporter le
// Context brut directement depuis CE fichier declenchait a nouveau
// l'avertissement oxlint `only-export-components` que la refonte
// initiale cherchait justement a eviter).
