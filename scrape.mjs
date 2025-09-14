import { chromium } from '@playwright/test';
import fs from 'fs';

const HEADLESS = true;
const RUN_TS = new Date().toISOString(); // timestamp pour forcer un diff/commit

/**
 * Deux parcours :
 *  - funding (Calls for proposals) — liens /topic-details/
 *  - tenders (Calls for tenders)   — liens /tender-details/
 */
const TASKS = [
  {
    name: 'funding',
    baseUrl: 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals',
    params: {
      order: 'DESC',
      sortBy: 'startDate',
      pageSize: 50,
      isExactMatch: true,
      status: '31094501,31094502' // Forthcoming + Open
    },
    linkSelector: 'a[href*="/topic-details/"]',
    expectedMin: 700,   // cible indicative
    maxPages: 1000
  },
  {
    name: 'tenders',
    // ✅ bonne page :
    baseUrl: 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-tenders',
    params: {
      order: 'DESC',
      sortBy: 'startDate',
      pageSize: 50,
      isExactMatch: true,
      status: '31094501,31094502' // Forthcoming + Open
    },
    // ✅ vrai pattern de page détail d’un appel d’offres
    linkSelector: 'a[href*="/tender-details/"]',
    expectedMin: 600,   // cible indicative
    maxPages: 1000
  }
];

// Nettoie une URL (retire query & fragment)
function cleanUrl(href, base) {
  try {
    const u = new URL(href, base);
    return `${u.origin}${u.pathname}`;
  } catch { return href; }
}

// Construit l’URL paginée
function buildPageUrl(base, params, pageNumber) {
  const u = new URL(base);
  Object.entries(params).forEach(([k, v]) => u.searchParams.set(k, String(v)));
  u.searchParams.set('pageNumber', String(pageNumber));
  return u.toString();
}

// Identifie des conteneurs scrollables (utile pour listes virtualisées)
async function findScrollContainers(page) {
  return await page.evaluate(() => {
    const els = Array.from(document.querySelectorAll('main, [role="main"], [data-ft-results], [data-results-container], [class*="scroll"], body, html'));
    const scrollables = els.filter(el => {
      const sh = el.scrollHeight, ch = el.clientHeight;
      const style = window.getComputedStyle(el);
      const overY = style.overflowY;
      return (sh && ch && sh > ch + 50) || overY === 'auto' || overY === 'scroll';
    });
    if (!scrollables.length) return [document.scrollingElement || document.body];
    return scrollables.map(el => {
      let sel = el.tagName.toLowerCase();
      if (el.id) sel += '#' + el.id;
      if (el.className) sel += '.' + String(el.className).trim().split(/\s+/).join('.');
      return sel;
    });
  });
}

// Scroll itératif + collecte au fil de l’eau (virtualisation)
async function collectVirtualized(page, linkSelector, baseForClean, targetAtLeast = 50) {
  const seen = new Set();
  const items = [];
  const maxLoops = 400;
  const pause = 750;

  async function collectOnce() {
    const anchors = await page.locator(linkSelector).all();
    for (const a of anchors) {
      const raw = await a.getAttribute('href');
      if (!raw) continue;
      const href = cleanUrl(raw, baseForClean);
      if (seen.has(href)) continue;
      let title = (await a.innerText())?.trim() || (await a.getAttribute('title')) || href;
      title = title.replace(/\s+/g, ' ').trim();
      seen.add(href);
      items.push({ title, url: href });
    }
  }

  await collectOnce();
  const containers = await findScrollContainers(page);

  let loops = 0, lastCount = 0, stagnation = 0;
  while (seen.size < targetAtLeast && loops < maxLoops) {
    for (const sel of containers) {
      try {
        const loc = page.locator(sel);
        await loc.evaluate(el => { el.scrollTop = el.scrollHeight; });
      } catch { /* ignore */ }
    }
    await page.mouse.wheel(0, 99999);

    await page.waitForTimeout(pause);
    await collectOnce();

    if (seen.size === lastCount) {
      stagnation++;
      for (const sel of containers) {
        try {
          const loc = page.locator(sel);
          await loc.evaluate(el => { el.scrollTop = Math.max(0, el.scrollTop - 600); });
          await page.waitForTimeout(250);
          await loc.evaluate(el => { el.scrollTop = el.scrollTop + 1600; });
        } catch { /* ignore */ }
      }
    } else {
      stagnation = 0;
      lastCount = seen.size;
    }
    if (stagnation >= 18) break; // on a clairement atteint la fin
    loops++;
  }
  return items;
}

// Génère un UL HTML (avec timestamp)
function toHtmlList(items, title) {
  const lis = items.map(it => `  <li><a href="${it.url}" target="_blank" rel="noopener noreferrer">${it.title}</a></li>`).join('\n');
  return `<!-- generated ${RUN_TS} -->\n<h2>${title}</h2>\n<ul>\n${lis}\n</ul>\n`;
}

// Écrit sorties pour un prefix donné (html/csv/json)
function writeOutputs(prefix, items, humanTitle) {
  fs.writeFileSync(`${prefix}-list.html`, toHtmlList(items, humanTitle), 'utf8');

  const header = ['title', 'url'];
  const csvRows = [header.join(',')].concat(
    items.map(r => `"${String(r.title).replace(/"/g,'""')}","${String(r.url).replace(/"/g,'""')}"`)
  );
  fs.writeFileSync(`${prefix}-list.csv`, csvRows.join('\n'), 'utf8');

  fs.writeFileSync(`${prefix}-list.json`, JSON.stringify({ generatedAt: RUN_TS, items }, null, 2), 'utf8');
}

(async () => {
  const browser = await chromium.launch({ headless: HEADLESS });
  const ctx = await browser.newContext();
  const page = await ctx.newPage();

  const index = [];

  for (const task of TASKS) {
    console.log(`\n=== ${task.name.toUpperCase()} ===`);
    const all = [];
    const seen = new Set();

    let noNewInARow = 0; // stop après 2 pages d’affilée sans nouveautés trouvées (vrai “nouveau”)
    for (let p = 1; p <= task.maxPages; p++) {
      const url = buildPageUrl(task.baseUrl, task.params, p);
      console.log(`→ page ${p}: ${url}`);
      await page.goto(url, { waitUntil: 'networkidle' });
      await page.waitForTimeout(700); // laisse la liste se poser

      // attendre qu’au moins un lien arrive (si résultats)
      try {
        await page.waitForSelector(task.linkSelector, { timeout: 30000 });
      } catch {
        console.log('  (aucun résultat détecté) — fin');
        break;
      }

      // collecte robuste (50 / page)
      const pageItems = await collectVirtualized(page, task.linkSelector, url, 50);

      // si la page ne contient vraiment rien (ou quasi), on sort
      if (!pageItems.length) {
        console.log('  page vide — fin');
        break;
      }

      // déduplication globale
      let added = 0;
      for (const it of pageItems) {
        if (seen.has(it.url)) continue;
        seen.add(it.url);
        all.push(it);
        added++;
      }
      console.log(`  ${pageItems.length} trouvés, ${added} nouveaux (total: ${all.length})`);

      // heuristique d’arrêt : 2 pages consécutives sans nouveau lien
      if (added === 0) {
        noNewInARow++;
        if (noNewInARow >= 2) {
          console.log('  deux pages sans nouveaux liens → fin');
          break;
        }
      } else {
        noNewInARow = 0;
      }
    }

    const niceTitle = task.name === 'funding'
      ? `Funding — Calls for proposals (Open + Forthcoming) — ${all.length} items`
      : `Procurement — Calls for tenders (Open + Forthcoming) — ${all.length} items`;

    writeOutputs(task.name, all, niceTitle);
    index.push({ name: task.name, count: all.length });
  }

  // Petit index récap (avec timestamp)
  const idxHtml = [
    `<!-- generated ${RUN_TS} -->`,
    '<h1>EU Funding & Tenders — Extractions</h1>',
    '<ul>',
    ...index.map(x => `  <li>${x.name}: ${x.count} items — <a href="${x.name}-list.html">${x.name}-list.html</a> / <a href="${x.name}-list.csv">${x.name}-list.csv</a></li>`),
    '</ul>'
  ].join('\n');
  fs.writeFileSync('index.html', idxHtml, 'utf8');

  console.log('\n✓ Done. Fichiers générés :', fs.readdirSync('.').filter(f => /index|funding|tenders/.test(f)).join(', '));
  await browser.close();
})();
