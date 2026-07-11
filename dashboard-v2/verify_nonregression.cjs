const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));

  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(3500);

  const widgets = {
    logSessionForm: await page.locator("text=Logger une séance").count(),
    whatIf: await page.locator("text=Simulateur what-if").count(),
    occupancy: await page.locator("text=Affluence").count(),
    nutrition: await page.locator("text=/BMR|TDEE/i").count(),
    mlTrend: await page.locator("text=/Tendance prédictive|EXPÉRIMENTAL/i").count(),
    sensitiveZones: await page.locator("text=Zones sensibles").count(),
    zoneDetail: await page.locator("text=Détail de la zone sélectionnée").count(),
  };
  console.log("Widgets presents:", JSON.stringify(widgets));

  // Selecteur d'utilisateur toujours fonctionnel (switch + silhouette suit)
  await page.locator("header select").first().selectOption("21");
  await page.waitForTimeout(2200);
  const status = await page.locator("p", { hasText: /Utilisateur \d+/ }).first().innerText();
  console.log("Silhouette apres switch vers 21:", status);

  console.log("Erreurs JS:", errors.length ? errors : "aucune");
  await browser.close();
})().catch((e) => { console.error("ECHEC:", e.message); process.exit(1); });
