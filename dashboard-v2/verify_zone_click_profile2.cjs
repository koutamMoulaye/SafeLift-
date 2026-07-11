const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(3500);
  // Profil different de la 1re verification (83) -- ici user_id=46
  await page.locator("header select").first().selectOption("46");
  await page.waitForTimeout(2200);

  const clicks = [
    { name: "chest", selector: 'ellipse[cx="103"][cy="110"]' },
    { name: "arms (bras gauche)", selector: 'path[d^="M62,86"]' },
    { name: "lower_back", selector: 'rect[x="100"][y="190"]' },
  ];

  for (const c of clicks) {
    await page.locator(c.selector).click({ force: true });
    await page.waitForTimeout(500);
    const panelText = await page
      .locator("text=Détail de la zone sélectionnée")
      .locator("xpath=ancestor::div[contains(@class,'rounded-2xl')]")
      .innerText();
    console.log(`--- user_id=46, clic sur ${c.name} ---`);
    console.log(panelText.replace(/\n/g, " | ").slice(0, 260));
  }

  const panelEl = page.locator("text=Détail de la zone sélectionnée").locator("xpath=ancestor::div[contains(@class,'rounded-2xl')]");
  await panelEl.screenshot({ path: "zone-detail-panel-user46-lowerback.png" });

  console.log("\nErreurs console:", (await page.evaluate(() => window.__errors || "aucune capturee (voir pageerror listener)")));
  await browser.close();
})().catch((e) => { console.error("ECHEC:", e.message); process.exit(1); });
