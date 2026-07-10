import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// SafeLift dashboard-v2 : projet React independant du dashboard existant
// (dashboard/, servi par FastAPI sur DASHBOARD_PORT_EXPOSED=18000). Ce
// serveur de dev tourne sur le port 5173 (defaut Vite) -- volontairement
// distinct pour ne jamais entrer en conflit avec l'ancien dashboard, qui
// doit rester intact et fonctionnel pendant toute la migration (voir
// CLAUDE.md, section dashboard-v2).
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    strictPort: true,
  },
})
