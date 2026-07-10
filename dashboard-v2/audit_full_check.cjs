const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on("pageerror", (err) => errors.push("PAGEERROR: " + err.message));
  page.on("response", (res) => { if (res.status() >= 400 && !res.url().includes("favicon")) errors.push(`HTTP ${res.status()} ${res.url()}`); });

  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(3500);

  const checks = {
    "TopBar (score global)": "text=SCORE GLOBAL",
    "Selecteur utilisateur": "text=avec données",
    "Zones sensibles": "text=ZONES SENSIBLES",
    "Silhouette": "svg",
    "Logger une seance": "text=LOGGER UNE SÉANCE",
    "Simulateur what-if": "text=SIMULATEUR WHAT-IF",
    "KPI Derniere seance": "text=DERNIÈRE SÉANCE",
    "KPI Zones en alerte": "text=ZONES EN ALERTE",
    "Tendance": "text=TENDANCE",
    "Affluence en direct": "text=AFFLUENCE EN DIRECT",
    "Nutrition": "text=NUTRITION",
    "Tendance predictive ML": "text=TENDANCE PRÉDICTIVE",
  };

  console.log("=== MODE REEL ===");
  for (const [name, selector] of Object.entries(checks)) {
    const count = await page.locator(selector).count();
    console.log(`${count > 0 ? "OK " : "MANQUANT "} ${name} (${count} match)`);
  }

  await page.screenshot({ path: "audit-mode-reel-full.png", fullPage: true });

  // Bascule mode demo
  await page.locator('input[type="checkbox"]').click();
  await page.waitForTimeout(2000);

  console.log("\n=== MODE DEMO ===");
  for (const [name, selector] of Object.entries(checks)) {
    const count = await page.locator(selector).count();
    console.log(`${count > 0 ? "OK " : "MANQUANT "} ${name} (${count} match)`);
  }

  await page.screenshot({ path: "audit-mode-demo-full.png", fullPage: true });

  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  console.log("\nDebordement horizontal :", overflow);
  console.log("Erreurs/echecs HTTP capturees :", errors.length ? errors : "aucune");

  await browser.close();
})().catch((err) => { console.error("ECHEC:", err.message); process.exit(1); });
