// Verification de l'extension multi-profils (2026-07-11) cote dashboard-v2 :
// le select principal doit desormais lister les 5 profils demo (9/21/34/46/83)
// SANS AUCUNE modification de code (deja branche dynamiquement sur /users).
// Teste la silhouette + le simulateur what-if sur 3 des nouveaux profils.
const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

async function silhouetteStatusText(page) {
  return page.locator("p", { hasText: /Utilisateur \d+|Scénario démo/ }).first().innerText();
}

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));

  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(4000);

  // Verifie que le groupe "avec donnees" du select liste bien les 5 profils
  const options = await page.locator("header select").first().locator("option").allInnerTexts();
  console.log("Options du select 'avec donnees':", options.filter((o) => o.includes("Utilisateur")));

  for (const uid of [21, 34, 46]) {
    const userSelect = page.locator("header select").first();
    await userSelect.selectOption(String(uid));
    await page.waitForTimeout(2200);
    const status = await silhouetteStatusText(page);
    console.log(`user_id=${uid} -> silhouette: ${status}`);
    await page.screenshot({ path: `verif-multiprofile-${uid}.png` });

    // Simulateur what-if : verifie qu'un exercice reel est propose (pas "Aucun exercice loggue")
    const whatIfOptions = await page.locator("text=Simulateur what-if").locator("xpath=ancestor::div[contains(@class,'rounded-2xl')]").locator("select").first().locator("option").allInnerTexts().catch(() => []);
    console.log(`  what-if options (extrait):`, whatIfOptions.slice(0, 2));
  }

  console.log("Erreurs JS:", errors.length ? errors : "aucune");
  await browser.close();
})().catch((e) => { console.error("ECHEC:", e.message); process.exit(1); });
