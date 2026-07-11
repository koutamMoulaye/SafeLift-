// Verification du portage de l'interaction clic-sur-zone + des correctifs
// d'alignement (Silhouette.jsx, ZoneDetailPanel.jsx, 2026-07-11).
const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(3500);
  await page.locator("header select").first().selectOption("83");
  await page.waitForTimeout(2200);

  const svg = page.locator("main svg").first();
  const box = await svg.boundingBox();

  // Zoom tete+epaules (haut du SVG, ~28% de la hauteur) -- verifie
  // l'absence de debordement du glow trapeze sur la tete.
  await page.screenshot({
    path: "zoom-head-shoulder-after.png",
    clip: { x: box.x, y: box.y, width: box.width, height: box.height * 0.28 },
  });

  // Silhouette complete -- verifie les marqueurs coude/hanche recentres.
  await page.screenshot({ path: "silhouette-full-after.png", clip: box });

  // --- Interaction clic sur 3 zones differentes ---
  // Selecteurs CSS directs sur les attributs SVG (cx/cy) -- Playwright
  // recalcule lui-meme la position de clic a CHAQUE `.click()`, plus
  // robuste que des coordonnees ecran precalculees a la main (qui se
  // sont averees perimees des que le panneau de detail change de taille
  // suite a un 1er clic reussi, decalant la mise en page).
  const zonesToClick = [
    { name: "shoulder (ellipse epaule droite, cx=164 cy=90)", selector: 'ellipse[cx="164"][cy="90"]' },
    { name: "chest (ellipse pectoral gauche, cx=103 cy=110)", selector: 'ellipse[cx="103"][cy="110"]' },
    { name: "knee (cercle genou gauche, cx=98 cy=330)", selector: 'circle[cx="98"][cy="330"][r="19"]' },
  ];

  for (const z of zonesToClick) {
    // { force: true } : la silhouette a une animation Framer Motion
    // continue (scale/opacity, "respiration") -- l'attente de stabilite
    // par defaut de Playwright ne se resout jamais sur un element anime
    // en boucle infinie (ce n'est pas une instabilite reelle du point de
    // vue clic, le decalage est de l'ordre de 1.5% en scale).
    await page.locator(z.selector).click({ force: true });
    await page.waitForTimeout(500);
    const panelText = await page.locator("text=Détail de la zone sélectionnée").locator("xpath=ancestor::div[contains(@class,'rounded-2xl')]").innerText();
    console.log(`--- clic sur ${z.name} ---`);
    console.log(panelText.replace(/\n/g, " | ").slice(0, 250));
  }

  // Re-clique sur "chest" pour un panneau bien rempli, puis capture le
  // panneau precisement (pas un clip approximatif).
  await page.locator('ellipse[cx="103"][cy="110"]').click({ force: true });
  await page.waitForTimeout(500);
  const panelEl = page.locator("text=Détail de la zone sélectionnée").locator("xpath=ancestor::div[contains(@class,'rounded-2xl')]");
  await panelEl.screenshot({ path: "zone-detail-panel-open.png" });

  await browser.close();
  console.log("\nTerminé.");
})().catch((e) => { console.error("ECHEC:", e.message); process.exit(1); });
