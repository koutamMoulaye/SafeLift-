const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));

  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(3500);

  // Cas "calves" (jamais de donnee reelle) sur user_id=9
  await page.locator('path[d^="M90,352"]').click({ force: true });
  await page.waitForTimeout(500);
  const calvesText = await page
    .locator("text=Détail de la zone sélectionnée")
    .locator("xpath=ancestor::div[contains(@class,'rounded-2xl')]")
    .innerText();
  console.log("--- clic sur calves (jamais de donnee) ---");
  console.log(calvesText.replace(/\n/g, " | "));

  // Mode demo : clique sur "chest" et verifie que ca fonctionne aussi
  // (scenario synthetique)
  await page.locator('input[type="checkbox"]').click();
  await page.waitForTimeout(2000);
  await page.locator('ellipse[cx="103"][cy="110"]').click({ force: true }).catch(() => {});
  await page.waitForTimeout(500);
  const demoText = await page
    .locator("text=Détail de la zone sélectionnée")
    .locator("xpath=ancestor::div[contains(@class,'rounded-2xl')]")
    .innerText();
  console.log("\n--- mode demo, clic sur chest ---");
  console.log(demoText.replace(/\n/g, " | ").slice(0, 260));

  // Retour mode reel : le clic doit avoir ete reinitialise (placeholder)
  await page.locator('input[type="checkbox"]').click();
  await page.waitForTimeout(2000);
  const backText = await page
    .locator("text=Détail de la zone sélectionnée")
    .locator("xpath=ancestor::div[contains(@class,'rounded-2xl')]")
    .innerText();
  console.log("\n--- retour mode reel (selection doit etre reinitialisee) ---");
  console.log(backText.replace(/\n/g, " | "));

  console.log("\nErreurs JS:", errors.length ? errors : "aucune");
  await browser.close();
})().catch((e) => { console.error("ECHEC:", e.message); process.exit(1); });
