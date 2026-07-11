const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1400 } });
  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(3500);

  // Legende presente ?
  const legendItems = await page.locator("li", { hasText: /Faible \(0-33\)|Modéré \(34-66\)|Élevé \(67-100\)|Pas de donnée/ }).allInnerTexts();
  console.log("Legende trouvee:", legendItems);

  // Clic reel sur une zone (pectoraux)
  await page.locator('ellipse[cx="103"][cy="110"]').click({ force: true });
  await page.waitForTimeout(600);
  const panelText = await page.locator("text=Détail de la zone sélectionnée").locator("xpath=ancestor::div[contains(@class,'rounded-2xl')]").innerText();
  console.log("Panneau apres clic:", panelText.replace(/\n/g, " | ").slice(0, 150));

  await page.screenshot({ path: "diagnose-after-fix.png", fullPage: true });
  console.log("Capture prise -> diagnose-after-fix.png");

  await browser.close();
})().catch((e) => { console.error("ECHEC:", e.message); process.exit(1); });
