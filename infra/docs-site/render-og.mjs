// Renders og-card.html to public/og-v2.png (1200x630) with headless Chrome.
// Run from this directory: npx -y -p puppeteer@24 node render-og.mjs
// Puppeteer is not a dependency of the workspace; npx installs it to a temporary directory and puts
// that directory's node_modules/.bin on PATH, which is where this script resolves it from.
import { createRequire } from "node:module";
import { delimiter, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const bin = (process.env.PATH ?? "")
  .split(delimiter)
  .find((p) => p.includes("_npx") && p.endsWith(join("node_modules", ".bin")));
if (bin === undefined)
  throw new Error("Run through npx: npx -y -p puppeteer@24 node render-og.mjs");
const puppeteer = createRequire(join(dirname(bin), "noop.js"))("puppeteer");

const browser = await puppeteer.launch({ headless: true });
const page = await browser.newPage();
await page.setViewport({ width: 1200, height: 630, deviceScaleFactor: 1 });
await page.goto(new URL("og-card.html", import.meta.url).href, { waitUntil: "networkidle0" });
await page.evaluate(() => document.fonts.ready);
await page.screenshot({ path: fileURLToPath(new URL("public/og-v2.png", import.meta.url)) });
await browser.close();
