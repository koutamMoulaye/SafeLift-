import { useContext } from "react";
import { DashboardContext } from "../context/dashboardContextObject";

// Hook separe du fichier de contexte (DashboardContext.jsx) : Vite Fast
// Refresh gere mal un fichier qui exporte a la fois un composant
// (DashboardProvider) et une fonction hook -- voir la note de bug dans
// DashboardContext.jsx pour le detail de l'incident que ce fichier corrige.
export function useDashboard() {
  const ctx = useContext(DashboardContext);
  if (!ctx) {
    throw new Error("useDashboard doit etre utilise a l'interieur de <DashboardProvider>");
  }
  return ctx;
}
