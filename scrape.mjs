import { chromium } from '@playwright/test';
import fs from 'fs';

const HEADLESS = true;

// Les deux parcours à faire
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
    expectedMin: 700,         // ta cible: ~723 (on s'arrête si plus rien de nouveau)
    maxPages: 200             // plafond très large
  },
  {
    name: 'tenders',
    baseUrl: 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/tenders',
    params: {
      order: 'DESC',
      sortBy: 'startDate',
      pageSize: 50,
      isExactMatch: true,
      status: '31094501,31094502' // Forthcoming + Open (tenders)
    },
    linkSelector: 'a[href*="/tenders/"]', // liens de détail des appels d'offres
    expectedMin: 600,         // ta cible: ~645
    maxPages: 200
  }
];

// Nettoie une URL (retire query/fragment)
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

// Trouve des conteneurs scrollables (pour listes virtualisées)
async function findScrollContainers(page) {
  return await page.evaluate(() => {
    const els = Array.from(document.querySelectorAll('main, [role="main"], [data-ft-results], [data-results-container], [class*="scroll"], body, html'));
    const scrollables = els.filter(el => {
      const sh = el.scrollHeight, ch = el.clientHeight;
      const style = window.getComputedStyle(el);
      const overY = style.overflowY;
      return (sh && ch && sh > ch + 50) || overY === 'auto' || overY === 'scroll';
    });
    // fallback au document
    if (!scrollables.length) return [document.scrollingElement || document.body];
    return scrollables.map(el => {
      // renvoyer un identifiant “chemin” pour pouvoir le retrouver côté Playwright
      let sel = el.tagName.toLowerCase();
      if (el.id) sel += '#' + el.id;
      if (el.className) sel += '.' + String(el.className).trim().split(/\s+/).join('.');
      return sel;
    });
  });
}

// Scroll itératif + collecte au fil de l’eau (pour ne pas perdre d’items)
async function collectVirtualized(page, linkSelector, baseForClean, targetAtLeast = 50) {
  const seen = new Set();
  const items = [];
  const maxLoops = 300;
  const pause = 700;

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

  // premiers éléments
  await collectOnce();

  // détermine les conteneurs à scroller (plus fiable que window)
  const containers = await findScrollContainers(page);

  let loops = 0, lastCount = 0, stagnation = 0;
  while (seen.size < targetAtLeast && loops < maxLoops) {
    // scroll tous les conteneurs connus
    for (const sel of containers) {
      try {
        const loc = page.locator(sel);
        await loc.evaluate(el => { el.scrollTop = el.scrollHeight; });
      } catch {/* ignore */}
    }
    // roue de secours: scroll global
    await page.mouse.wheel(0, 99999);

    await page.waitForTimeout(pause);
    await collectOnce();

    if (seen.size === lastCount) {
      stagnation++;
      // nudge
      for (const sel of containers) {
        try {
          const loc = page.locator(sel);
          await loc.evaluate(el => { el.scrollTop = Math.max(0, el.scrollTop - 400); });
          await page.waitForTimeout(200);
          await loc.evaluate(el => { el.scrollTop = el.scrollTop + 1400; });
        } catch {/* ignore */}
      }
    } else {
      stagnation = 0;
      lastCount = seen.size;
    }
    if (stagnation >= 12) break;
    loops++;
  }
  return items;
}

// Génère un UL HTML
function toHtmlList(items) {
  const lis = items.map(it => `  <li><a href="${it.url}" target="_blank" rel="noopener noreferrer">${it.title}</a></li>`).join('\n');
  return `<ul>\n${lis}\n</ul>\n`;
}

// Écrit fichiers
function writeOutputs(prefix, items) {
  // HTML
  fs.writeFileSync(`${prefix}-list.html`, toHtmlList(items), 'utf8');
  // CSV
  const header = ['title', 'url'];
  const csvRows = [header.join(',')].concat(items.map(r => `"${String(r.title).replace(/"/g,'""')}","${String(r.url).replace(/"/g,'""')}"`));
  fs.writeFileSync(`${prefix}-list.csv`, csvRows.join('\n'), 'utf8');
  // JSON
  fs.writeFileSync(`${prefix}-list.json`, JSON.stringify(items, null, 2), 'utf8');
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

    for (let p = 1; p <= task.maxPages; p++) {
      const url = buildPageUrl(task.baseUrl, task.params, p);
      console.log(`→ page ${p}: ${url}`);
      await page.goto(url, { waitUntil: 'networkidle' });

      // attendre que la première carte/ligne arrive (si elle existe)
      try {
        await page.waitForSelector(task.linkSelector, { timeout: 30000 });
      } catch {
        console.log('  (aucun résultat détecté)');
        break;
      }

      // collecte solide des 50 (liste virtualisée)
      const pageItems = await collectVirtualized(page, task.linkSelector, url, 50);

      // stop si page vide
      if (!pageItems.length) {
        console.log('  page vide, on arrête la pagination.');
        break;
      }

      // Ajoute en dédupliquant globalement
      let added = 0;
      for (const it of pageItems) {
        if (seen.has(it.url)) continue;
        seen.add(it.url);
        all.push(it);
        added++;
      }
      console.log(`  ${pageItems.length} trouvés, ${added} nouveaux (total cumulé: ${all.length})`);

      // heuristique d’arrêt : si la page suivante n’apporte plus rien
      if (added === 0 && p > 1) break;
    }

    // Écrit les fichiers de cette famille
    writeOutputs(task.name, all);
    index.push({ name: task.name, count: all.length });
  }

  // Petit index pour savoir combien on a
  const idxHtml = [
    '<h1>EU Funding & Tenders — Extractions</h1>',
    '<ul>',
    ...index.map(x => `  <li>${x.name}: ${x.count} items — <a href="${x.name}-list.html">${x.name}-list.html</a> / <a href="${x.name}-list.csv">${x.name}-list.csv</a></li>`),
    '</ul>'
  ].join('\n');
  fs.writeFileSync('index.html', idxHtml, 'utf8');

  console.log('\n✓ Done. Fichiers générés :', fs.readdirSync('.').filter(f => /index|funding|tenders/.test(f)).join(', '));
  await browser.close();
})();
