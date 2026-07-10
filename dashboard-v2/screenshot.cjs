// SafeLift dashboard-v2 -- outil de verification visuelle ponctuelle.
//
// L'extension navigateur Claude-in-Chrome est indisponible sur cette
// machine (constat repete sur des dizaines de sessions du projet, voir
// CLAUDE.md) : ce script contourne le probleme en pilotant directement le
// Chrome DEJA INSTALLE sur la machine (executablePath explicite, PAS de
// telechargement d'un Chromium separe par Playwright -- `playwright-core`
// est volontairement utilise plutot que `playwright` complet, pour rester
// leger et rapide vu le delai serre du 2026-07-13).
//
// Usage : `npm run dev` doit deja tourner (port 5173), puis
// `node screenshot.cjs` -- ecrit silhouette-wireframe.png (ignore par git,
// artefact de verification ponctuelle, pas du code source).
const { chromium } = require("playwright-core");

const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
const TARGET_URL = "http://localhost:5173/";
const OUTPUT_PATH = "silhouette-wireframe.png";

(async () => {
  const browser = await chromium.launch({
    executablePath: CHROME_PATH,
    headless: true,
  });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  await page.goto(TARGET_URL, { waitUntil: "networkidle" });
  // Laisse l'animation Framer Motion ("respiration") demarrer avant la
  // capture, pour un rendu representatif de l'etat regime permanent.
  await page.waitForTimeout(1500);
  await page.screenshot({ path: OUTPUT_PATH });
  console.log(`Capture enregistree -> ${OUTPUT_PATH}`);
  await browser.close();
})().catch((err) => {
  console.error("ECHEC DE LA CAPTURE :", err.message);
  process.exit(1);
});
