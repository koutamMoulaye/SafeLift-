// Jalon 3, sous-etape 6/6 : test de parite final sur les 5 profils reels,
// sur les 2 dashboards (ancien vanilla JS port 18000, dashboard-v2 React
// port 5173). Verifie que chaque widget affiche des donnees coherentes
// (pas juste user_id=9, le plus teste jusqu'ici).
const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
const PROFILES = [9, 21, 34, 46, 83];

async function checkDashboardV2(browser) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  const results = [];
  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(3500);

  for (const uid of PROFILES) {
    const userSelect = page.locator("header select").first();
    await userSelect.selectOption(String(uid));
    await page.waitForTimeout(2200);

    const silhouette = await page.locator("p", { hasText: /Utilisateur \d+/ }).first().innerText().catch(() => "ERREUR");
    const zonesSensibles = await page.locator("text=Zones sensibles").locator("xpath=ancestor::div[contains(@class,'rounded-2xl')]").innerText().catch(() => "ERREUR");
    const whatIf = await page.locator("text=Simulateur what-if").count();
    const nutrition = await page.locator("text=/BMR|TDEE|Nutrition/i").count();
    const mlTrend = await page.locator("text=/Tendance prédictive|EXPÉRIMENTAL/i").count();

    results.push({ uid, silhouette, zonesSensiblesOk: zonesSensibles.length > 20, whatIf, nutrition, mlTrend });
  }
  await page.close();
  return results;
}

async function checkOldDashboard(browser) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  const results = [];
  await page.goto("http://localhost:18000/", { waitUntil: "load" });
  await page.waitForTimeout(2500);

  for (const uid of PROFILES) {
    const select = page.locator("select").first();
    await select.selectOption(String(uid));
    await page.waitForTimeout(2000);

    const scoreText = await page.locator("text=Score global").locator("..").innerText().catch(() => "ERREUR");
    const mlPanel = await page.locator("text=Tendance prédictive").count();
    const nutritionTab = await page.locator("text=Nutrition").count();
    const whatIfPanel = await page.locator("text=Simulateur what-if").count();

    results.push({ uid, scoreText: scoreText.replace(/\n/g, " ").slice(0, 60), mlPanel, nutritionTab, whatIfPanel });
  }
  await page.close();
  return results;
}

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });

  console.log("=== dashboard-v2 (5173) ===");
  const v2 = await checkDashboardV2(browser);
  v2.forEach((r) => console.log(JSON.stringify(r)));

  console.log("\n=== ancien dashboard (18000) ===");
  const old = await checkOldDashboard(browser);
  old.forEach((r) => console.log(JSON.stringify(r)));

  await browser.close();
})().catch((e) => { console.error("ECHEC:", e.message); process.exit(1); });
