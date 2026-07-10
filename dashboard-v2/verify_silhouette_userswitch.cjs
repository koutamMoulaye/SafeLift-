// Verification ciblee du correctif "silhouette figee sur user_id=9" --
// meme technique Chrome local (playwright-core) que screenshot_full.cjs.
// Change 2 fois d'utilisateur via le <select> principal du TopBar et
// confirme, a chaque fois, que le texte de statut sous la silhouette
// (rendu par Silhouette.jsx) suit reellement le nouvel utilisateur, PAS
// juste les autres widgets.
const { chromium } = require("playwright-core");

const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
const TARGET_URL = "http://localhost:5173/";

async function silhouetteStatusText(page) {
  // Le <p> de statut est le seul texte contenant "Utilisateur" OU
  // "Scénario démo" a l'interieur du conteneur de la silhouette.
  return page.locator("p", { hasText: /Utilisateur \d+|Scénario démo|Chargement des données/ }).first().innerText();
}

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const consoleErrors = [];
  page.on("pageerror", (err) => consoleErrors.push(`pageerror: ${err.message}`));
  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(`console.error: ${msg.text()}`);
  });

  await page.goto(TARGET_URL, { waitUntil: "load" });
  await page.waitForTimeout(4000);

  const userSelect = page.locator("header select").first();

  // --- Etape 1 : etat initial (doit pre-selectionner user_id=9, seul
  // profil avec donnees reelles) ---
  const initialSelectValue = await userSelect.inputValue();
  await page.waitForTimeout(500);
  const initialStatus = await silhouetteStatusText(page);
  await page.screenshot({ path: "verif-silhouette-1-user9.png" });
  console.log("1) select value =", initialSelectValue, "| silhouette dit:", initialStatus);

  // --- Etape 2 : mode demo -- la silhouette doit suivre le scenario
  // synthetique (1 seule "zone" active), sans planter, puis revenir
  // correctement en mode reel (user_id=9) une fois le toggle desactive.
  // Fait AVANT toute utilisation du champ de recherche (etape 3) pour ne
  // pas polluer ce test avec l'etat local searchValue/searchOpen du
  // champ de recherche (comportement distinct, sans rapport avec ce
  // correctif). ---
  await page.locator('input[type="checkbox"]').click();
  await page.waitForTimeout(2000);
  const demoStatus = await silhouetteStatusText(page);
  console.log("2) mode demo, silhouette dit:", demoStatus);
  await page.locator('input[type="checkbox"]').click();
  await page.waitForTimeout(2500);
  const backToRealStatus = await silhouetteStatusText(page);
  console.log("2) retour mode reel, silhouette dit:", backToRealStatus);

  // --- Etape 3 : bascule vers user_id=1 (profil SANS donnee reelle) via
  // la recherche par ID (le select natif ne liste plus les profils sans
  // donnee individuellement depuis la sous-etape 3/N) ---
  await page.getByRole("button", { name: /profils sans séance réelle/ }).click();
  const searchInput = page.locator('input[placeholder*="Numéro d\'utilisateur"]');
  await searchInput.fill("1");
  await searchInput.press("Enter");
  await page.waitForTimeout(2500);
  const afterSwitchValue = await userSelect.inputValue();
  const afterSwitchStatus = await silhouetteStatusText(page);
  await page.screenshot({ path: "verif-silhouette-2-user1.png" });
  console.log("3) select value =", afterSwitchValue, "| silhouette dit:", afterSwitchStatus);

  // --- Etape 4 : retour a user_id=9 via le <select> normal ---
  await userSelect.selectOption("9");
  await page.waitForTimeout(2500);
  const backValue = await userSelect.inputValue();
  const backStatus = await silhouetteStatusText(page);
  await page.screenshot({ path: "verif-silhouette-3-back-to-9.png" });
  console.log("4) select value =", backValue, "| silhouette dit:", backStatus);

  // --- Etape 5 : non-regression rapide -- les autres widgets/sections
  // cles sont toujours presents (pas de page blanche, pas d'exception) ---
  const stillThere = {
    logSessionForm: await page.locator("text=Logger une séance").count(),
    whatIf: await page.locator("text=Simulateur").count(),
    occupancy: await page.locator("text=Affluence").count(),
  };
  console.log("5) widgets toujours presents:", JSON.stringify(stillThere));

  // Avertissement React "key prop spread" PRE-EXISTANT (deja documente
  // dans CLAUDE.md sous-etape 2/N, localise dans renderZoneShape/
  // commonProps -- fichier non touche par ce correctif) + 404 favicon.ico
  // (pre-existant, sans rapport) : ignores ici, ne sont pas une
  // regression introduite par ce correctif.
  const knownPreExisting = (msg) =>
    msg.includes("key\" prop is being spread") || msg.includes("404 (Not Found)");
  const newErrors = consoleErrors.filter((e) => !knownPreExisting(e));
  console.log("5) erreurs JS NOUVELLES (hors pre-existantes documentees):", newErrors.length ? newErrors : "aucune");

  await browser.close();

  const ok =
    initialStatus.includes("9") &&
    afterSwitchStatus.includes("1") &&
    !afterSwitchStatus.includes("19") &&
    backStatus.includes("9") &&
    demoStatus.includes("démo") &&
    backToRealStatus.includes("9") &&
    newErrors.length === 0;
  console.log(ok ? "\nRESULTAT: OK -- la silhouette suit bien le changement d'utilisateur et le mode demo." : "\nRESULTAT: ECHEC");
  process.exit(ok ? 0 : 1);
})().catch((err) => {
  console.error("ECHEC DU SCRIPT:", err.message);
  process.exit(1);
});
