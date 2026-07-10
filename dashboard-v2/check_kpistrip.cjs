const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on("pageerror", (err) => errors.push(err.message));
  page.on("console", (msg) => {
    if (msg.type() === "error" && !msg.text().includes('"key" prop') && !msg.text().includes("favicon")) errors.push(msg.text());
  });

  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(3000);
  const kpiReal = await page.locator("text=Dernière séance").locator("xpath=ancestor::div[contains(@class,'grid')][1]").innerText();
  console.log("=== KPI STRIP (mode reel) ===\n", kpiReal, "\n");

  await page.locator('input[type="checkbox"]').click();
  await page.waitForTimeout(1500);
  const kpiDemo = await page.locator("text=Dernière séance").locator("xpath=ancestor::div[contains(@class,'grid')][1]").innerText();
  console.log("=== KPI STRIP (mode demo) ===\n", kpiDemo, "\n");

  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  console.log("Debordement horizontal :", overflow);
  console.log("Erreurs :", errors);

  await browser.close();
})().catch((err) => { console.error("ECHEC:", err.message); process.exit(1); });
