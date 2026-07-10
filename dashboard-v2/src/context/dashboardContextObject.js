import { createContext } from "react";

// Objet Context brut, isole dans son propre fichier (aucun composant ni
// hook ici) : DashboardContext.jsx (le Provider) ET hooks/useDashboard.js
// (le hook) en ont tous les deux besoin. Le co-localiser avec l'un des deux
// declencherait a nouveau l'avertissement oxlint `only-export-components`
// (Fast Refresh Vite ne gere proprement que des fichiers n'exportant QUE
// des composants) -- voir la note de bug dans DashboardContext.jsx.
export const DashboardContext = createContext(null);
