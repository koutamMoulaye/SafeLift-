// Test complementaire : un 3e utilisateur distinct (42), pour confirmer
// que le correctif fonctionne au-dela du seul couple 9/1 deja teste dans
// verify_silhouette_userswitch.cjs.
const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

async function silhouetteStatusText(page) {
  return page.locator("p", { hasText: /Utilisateur \d+|Scénario démo/ }).first().innerText();
}

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(4000);

  console.log("1) initial:", await silhouetteStatusText(page));

  await page.getByRole("button", { name: /profils sans séance réelle/ }).click();
  const searchInput = page.locator('input[placeholder*="Numéro d\'utilisateur"]');
  await searchInput.fill("42");
  await searchInput.press("Enter");
  await page.waitForTimeout(2500);
  console.log("2) apres bascule vers user 42:", await silhouetteStatusText(page));
  await page.screenshot({ path: "verif-silhouette-5-user42.png" });

  const userSelect = page.locator("header select").first();
  await userSelect.selectOption("9");
  await page.waitForTimeout(2500);
  console.log("3) retour a user 9:", await silhouetteStatusText(page));

  await browser.close();
})().catch((e) => { console.error("ECHEC:", e.message); process.exit(1); });
