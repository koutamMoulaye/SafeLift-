const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(3500);
  const select = page.locator("header select").first();
  await select.selectOption("83");
  await page.waitForTimeout(2500);
  await page.screenshot({ path: "parity-evidence-v2-user83.png" });

  const page2 = await browser.newPage({ viewport: { width: 1440, height: 1400 } });
  await page2.goto("http://localhost:18000/", { waitUntil: "load" });
  await page2.waitForTimeout(2500);
  await page2.locator("select").first().selectOption("34");
  await page2.waitForTimeout(2000);
  await page2.screenshot({ path: "parity-evidence-old-user34.png", fullPage: true });

  await browser.close();
  console.log("Captures prises.");
})().catch((e) => { console.error(e.message); process.exit(1); });
