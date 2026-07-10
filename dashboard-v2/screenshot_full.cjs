// Capture d'ensemble du dashboard complet (pas juste la silhouette) --
// meme technique que screenshot.cjs (Chrome local via playwright-core,
// executablePath explicite), fullPage:true pour voir tous les widgets
// (zones sensibles, logger seance, tendance, affluence, nutrition, ML).
// Capture aussi une 2e image en mode demo (toggle clique) pour verifier
// que le toggle fonctionne reellement.
const { chromium } = require("playwright-core");

const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
const TARGET_URL = "http://localhost:5173/";

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  // "networkidle" ne se resout jamais ici : la connexion SSE de
  // l'affluence (EventSource) reste active en continu, le navigateur ne
  // considere donc jamais le reseau comme "idle". "load" + attente fixe
  // suffit (laisse le temps aux fetchs paralleles risk/history/exercises/
  // nutrition/prediction et a la 1re donnee SSE d'arriver).
  await page.goto(TARGET_URL, { waitUntil: "load" });
  await page.waitForTimeout(5000);
  await page.screenshot({ path: "dashboard-full-real.png", fullPage: true });
  console.log("Capture reelle enregistree -> dashboard-full-real.png");

  // Bascule le toggle demo (checkbox "Mode démo")
  const demoCheckbox = page.locator('input[type="checkbox"]');
  await demoCheckbox.click();
  await page.waitForTimeout(2500);
  await page.screenshot({ path: "dashboard-full-demo.png", fullPage: true });
  console.log("Capture demo enregistree -> dashboard-full-demo.png");

  await browser.close();
})().catch((err) => {
  console.error("ECHEC DE LA CAPTURE :", err.message);
  process.exit(1);
});
