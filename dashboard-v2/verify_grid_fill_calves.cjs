const { chromium } = require("playwright-core");
const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME_PATH, headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  await page.goto("http://localhost:5173/", { waitUntil: "load" });
  await page.waitForTimeout(3500);

  const svg = page.locator("main svg").first();
  const box = await svg.boundingBox();

  await page.screenshot({ path: "silhouette-full-v3.png", clip: box });
  await page.screenshot({
    path: "zoom-calves-after.png",
    clip: { x: box.x, y: box.y + box.height * 0.62, width: box.width, height: box.height * 0.38 },
  });

  console.log("done");
  await browser.close();
})();
