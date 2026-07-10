// Verifie que l'ancien dashboard (vanilla JS, port 18000) liste aussi
// automatiquement les 5 profils demo, sans modification de code.
const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  await page.goto("http://localhost:18000/", { waitUntil: "load" });
  await page.waitForTimeout(3000);

  const options = await page.locator("select").first().locator("option").allInnerTexts();
  console.log("Options (ancien dashboard):", options.filter((o) => o.includes("Utilisateur") || o.includes("18") || o.includes("19") || o.includes("21")).slice(0, 10));

  // Selectionne user_id=46
  const select = page.locator("select").first();
  const allValues = await select.locator("option").evaluateAll((opts) => opts.map((o) => o.value));
  console.log("Toutes les valeurs (extrait):", allValues.slice(0, 10));
  await select.selectOption("46");
  await page.waitForTimeout(2000);
  await page.screenshot({ path: "verif-old-dashboard-user46.png", fullPage: true });
  console.log("Capture prise -> verif-old-dashboard-user46.png");

  await browser.close();
})().catch((e) => { console.error("ECHEC:", e.message); process.exit(1); });
