const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));

  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(3500);
  await page.locator("header select").first().selectOption("83");
  await page.waitForTimeout(2200);

  const svg = page.locator("main svg").first();
  const box = await svg.boundingBox();
  await page.screenshot({ path: "silhouette-modere-fill.png", clip: box });

  // Clic toujours fonctionnel apres le changement de style ?
  await page.locator('ellipse[cx="164"][cy="90"]').click({ force: true });
  await page.waitForTimeout(500);
  const panelText = await page.locator("text=Détail de la zone sélectionnée").locator("xpath=ancestor::div[contains(@class,'rounded-2xl')]").innerText();
  console.log("Panneau apres clic (epaule, user 83):", panelText.replace(/\n/g, " | ").slice(0, 150));

  console.log("Erreurs JS:", errors.length ? errors : "aucune");
  await browser.close();
})().catch((e) => { console.error("ECHEC:", e.message); process.exit(1); });
