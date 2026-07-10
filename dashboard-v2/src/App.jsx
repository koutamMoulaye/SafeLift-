import { DashboardProvider } from "./context/DashboardContext";
import TopBar from "./components/TopBar";
import KpiStrip from "./components/KpiStrip";
import Silhouette from "./components/Silhouette";
import SensitiveZones from "./components/SensitiveZones";
import LogSessionForm from "./components/LogSessionForm";
import WhatIfSimulator from "./components/WhatIfSimulator";
import BottomTrends from "./components/BottomTrends";
import OccupancyPanel from "./components/OccupancyPanel";
import NutritionPanel from "./components/NutritionPanel";
import MlPredictionPanel from "./components/MlPredictionPanel";

// SafeLift dashboard-v2 -- layout complet. Grille 3 colonnes (colonne
// gauche = zones sensibles + simulateur what-if empiles / silhouette
// centrale / logger une seance) + grille de widgets en bas (tendance,
// affluence, nutrition, tendance predictive ML). Silhouette centrale
// branchee sur DashboardContext (correctif 2026-07-11) : suit desormais
// le selecteur d'utilisateur et le toggle mode demo comme tous les autres
// widgets.
export default function App() {
  return (
    <DashboardProvider>
      <div className="min-h-screen bg-deep text-slate-200">
        <TopBar />

        <main className="mx-auto flex max-w-7xl flex-col gap-6 px-8 py-8">
          <KpiStrip />

          <div className="grid grid-cols-1 gap-6 lg:grid-cols-[320px_1fr_320px]">
            <div className="flex flex-col gap-6">
              <SensitiveZones />
              <WhatIfSimulator />
            </div>

            <div className="flex flex-col items-center justify-center rounded-2xl border border-line bg-panel/60 p-8">
              <Silhouette />
            </div>

            <LogSessionForm />

            <div className="grid grid-cols-1 gap-6 md:grid-cols-2 lg:col-span-3">
              <BottomTrends />
              <OccupancyPanel />
              <NutritionPanel />
              <MlPredictionPanel />
            </div>
          </div>
        </main>
      </div>
    </DashboardProvider>
  );
}
