const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(3500);
  await page.locator('input[type="checkbox"]').click();
  await page.waitForTimeout(2000);
  await page.locator('ellipse[cx="103"][cy="110"]').click({ force: true }).catch(() => {});
  await page.waitForTimeout(500);
  const panelEl = page.locator("text=Détail de la zone sélectionnée").locator("xpath=ancestor::div[contains(@class,'rounded-2xl')]");
  await panelEl.screenshot({ path: "zone-detail-panel-demo-mode.png" });
  console.log("done");
  await browser.close();
})();
