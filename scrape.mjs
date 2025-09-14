import { chromium } from '@playwright/test';
import fs from 'fs';

const CONFIG = {
  URL: 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals?order=DESC&pageNumber=1&pageSize=50&sortBy=startDate&isExactMatch=true&status=31094501,31094502',
  TARGET_MIN: 50,
  MAX_LOOPS: 220,
  PAUSE_MS: 700,
  STAGNATION_TICKS: 14,
  HEADLESS: true,
  FETCH_DETAILS: true,     // ← passe à false si tu veux juste la liste
  LANG: 'en',
  TIMEOUT_FETCH_MS: 20000
};

function cleanUrl(href, base) {
  try { const u = new URL(href, base); return `${u.origin}${u.pathname}`; }
  catch { return href; }
}

async function fetchTopicDetails(page, slugLower, lang) {
  const url = `https://ec.europa.eu/info/funding-tenders/opportunities/data/topicDetails/${encodeURIComponent(slugLower)}.json?lang=${encodeURIComponent(lang)}`;
  const resp = await page.request.get(url, { timeout: CONFIG.TIMEOUT_FETCH_MS });
  if (!resp.ok()) throw new Error(`HTTP ${resp.status()} for ${url}`);
  return await resp.json();
}
function toIsoDate(v) {
  if (v == null || v === '') return '';
  if (typeof v === 'number') {
    const d = new Date(v); return isNaN(d) ? '' : d.toISOString().slice(0,10);
  }
  const s = String(v);
  const mEU = s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (mEU) { const d = new Date(+mEU[3], +mEU[2]-1, +mEU[1]); return isNaN(d)?'':d.toISOString().slice(0,10); }
  const d = new Date(s); return isNaN(d)?'':d.toISOString().slice(0,10);
}
function extractSlugFromHref(href) {
  const m = String(href).toLowerCase().match(/\/topic-details\/([^/?#]+)/);
  return m ? m[1] : '';
}

(async () => {
  const browser = await chromium.launch({ headless: CONFIG.HEADLESS });
  const ctx = await browser.newContext();
  const page = await ctx.newPage();

  console.log('→ open', CONFIG.URL);
  await page.goto(CONFIG.URL, { waitUntil: 'networkidle' });

  const sel = 'a[href*="/topic-details/"]';
  const seen = new Set(); const items = [];
  let loops = 0, lastCount = 0, stagnation = 0;

  async function collectNow() {
    const anchors = await page.locator(sel).all();
    for (const a of anchors) {
      const raw = await a.getAttribute('href'); if (!raw) continue;
      const href = cleanUrl(raw, page.url());
      if (seen.has(href)) continue;
      let title = (await a.innerText())?.trim() || (await a.getAttribute('title')) || href;
      title = title.replace(/\s+/g, ' ').trim();
      seen.add(href); items.push({ title, url: href });
    }
  }
  await collectNow();

  while (seen.size < CONFIG.TARGET_MIN && loops < CONFIG.MAX_LOOPS) {
    await page.mouse.wheel(0, 99999);
    await page.waitForTimeout(CONFIG.PAUSE_MS);
    await collectNow();
    if (seen.size === lastCount) {
      stagnation++; await page.mouse.wheel(0, -500); await page.waitForTimeout(200); await page.mouse.wheel(0, 1400);
    } else { stagnation = 0; lastCount = seen.size; }
    if (stagnation >= CONFIG.STAGNATION_TICKS) break;
    loops++;
  }
  console.log(`links: ${items.length}`);

  let detailed = [];
  if (CONFIG.FETCH_DETAILS) {
    for (const it of items) {
      const slug = extractSlugFromHref(it.url);
      if (!slug) { detailed.push({ ...it }); continue; }
      try {
        const json = await fetchTopicDetails(page, slug, CONFIG.LANG);
        const TD = json?.TopicDetails || {};
        const actions = Array.isArray(TD.actions) ? TD.actions : [];
        const a0 = actions[0] || {};
        const deadlines = Array.isArray(a0.deadlineDates) ? a0.deadlineDates : [];
        const firstDeadline = deadlines.length ? toIsoDate(deadlines[0]) : '';
        detailed.push({
          identifier: TD.identifier || slug.toUpperCase(),
          title: TD.title || it.title,
          status: TD.topicStatus || TD.status || '',
          plannedOpeningDate: toIsoDate(a0.plannedOpeningDate),
          firstDeadline,
          allDeadlines: deadlines.map(toIsoDate).filter(Boolean).join(' | '),
          destination: TD.destination || TD.destinationCode || '',
          workProgrammePart: TD.workProgramPart || TD.workProgrammePart || TD.workProgramme || '',
          typeOfAction: Array.isArray(TD.typeOfAction) ? TD.typeOfAction.join(' | ')
                        : Array.isArray(TD.typeOfActions) ? TD.typeOfActions.join(' | ')
                        : (TD.typeOfAction || ''),
          typeOfMGAs: Array.isArray(TD.typeOfMGAs) ? TD.typeOfMGAs.join(' | ')
                       : Array.isArray(TD.typeofMGAs) ? TD.typeofMGAs.join(' | ')
                       : '',
          submissionSystem: TD.submissionSystem || '',
          url: it.url
        });
      } catch (e) {
        console.warn('detail ERR', slug, e.message);
        detailed.push({ title: it.title, url: it.url });
      }
    }
  }

  const list = items.map(it => `  <li><a href="${it.url}" target="_blank" rel="noopener noreferrer">${it.title}</a></li>`).join('\n');
  const html = `<ul>\n${list}\n</ul>\n`;
  fs.writeFileSync('list.html', html, 'utf8');

  if (CONFIG.FETCH_DETAILS) {
    const header = [
      'identifier','title','status','plannedOpeningDate','firstDeadline','allDeadlines',
      'destination','workProgrammePart','typeOfAction','typeOfMGAs','submissionSystem','url'
    ];
    const rows = [header.join(',')].concat(detailed.map(r => {
      const vals = header.map(h => (r[h] ?? '').toString().replace(/"/g,'""'));
      return `"${vals.join('","')}"`;
    }));
    fs.writeFileSync('list.csv', rows.join('\n'), 'utf8');
    fs.writeFileSync('list.json', JSON.stringify(detailed, null, 2), 'utf8');
  } else {
    fs.writeFileSync('list.json', JSON.stringify(items, null, 2), 'utf8');
  }

  console.log('done');
  await browser.close();
})();
