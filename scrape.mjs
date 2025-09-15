import { chromium } from '@playwright/test';
import fs from 'fs';

const HEADLESS = true;
const RUN_TS = new Date().toISOString();

// Limites (sécurité) — tu peux les ajuster via variables d’env
const MAX_ENRICH_FUNDING = Number(process.env.MAX_ENRICH_FUNDING || 2000);
const MAX_ENRICH_TENDERS = Number(process.env.MAX_ENRICH_TENDERS || 1500);

// --------- TÂCHES DE COLLECTE (listes brutes) ---------
const TASKS = [
  {
    name: 'funding',
    baseUrl: 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals',
    params: { order:'DESC', sortBy:'startDate', pageSize:50, isExactMatch:true, status:'31094501,31094502' },
    linkSelector: 'a[href*="/topic-details/"]',
    callSelectors: ['a[href*="/call-details/"]','a[href*="/calls/"]'],
    maxPages: 2000
  },
  {
    name: 'tenders',
    baseUrl: 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-tenders',
    params: { order:'DESC', sortBy:'startDate', pageSize:50, isExactMatch:true, status:'31094501,31094502' },
    linkSelector: 'a[href*="/tender-details/"]',
    maxPages: 2000
  }
];

// ------------- UTILITAIRES GÉNÉRAUX -------------
function cleanUrl(href, base) {
  try { const u = new URL(href, base); return `${u.origin}${u.pathname}`; } catch { return href; }
}
function buildPageUrl(base, params, pageNumber) {
  const u = new URL(base);
  Object.entries(params).forEach(([k,v]) => u.searchParams.set(k, String(v)));
  u.searchParams.set('pageNumber', String(pageNumber));
  return u.toString();
}
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
async function collectVirtualized(page, linkSelector, baseForClean, targetAtLeast = 50) {
  const seen = new Set(); const items = [];
  const maxLoops = 400, pause = 750;

  async function collectOnce() {
    const anchors = await page.locator(linkSelector).all();
    for (const a of anchors) {
      const raw = await a.getAttribute('href'); if (!raw) continue;
      const href = cleanUrl(raw, baseForClean);
      if (seen.has(href)) continue;
      let title = (await a.innerText())?.trim() || (await a.getAttribute('title')) || href;
      title = title.replace(/\s+/g,' ').trim();
      seen.add(href); items.push({ title, url: href });
    }
  }
  await collectOnce();
  const containers = await findScrollContainers(page);

  let loops=0, last=0, stagn=0;
  while (seen.size < targetAtLeast && loops < maxLoops) {
    for (const sel of containers) { try { await page.locator(sel).evaluate(el => { el.scrollTop = el.scrollHeight; }); } catch{} }
    await page.mouse.wheel(0, 99999);
    await page.waitForTimeout(pause);
    await collectOnce();

    if (seen.size === last) {
      stagn++;
      for (const sel of containers) {
        try {
          const loc = page.locator(sel);
          await loc.evaluate(el => { el.scrollTop = Math.max(0, el.scrollTop - 600); });
          await page.waitForTimeout(250);
          await loc.evaluate(el => { el.scrollTop = el.scrollTop + 1600; });
        } catch {}
      }
    } else { stagn=0; last=seen.size; }
    if (stagn >= 18) break;
    loops++;
  }
  return items;
}
async function collectLinks(page, selector, baseForClean) {
  const list = []; const seen = new Set();
  const anchors = await page.locator(selector).all();
  for (const a of anchors) {
    const raw = await a.getAttribute('href'); if (!raw) continue;
    const href = cleanUrl(raw, baseForClean);
    if (seen.has(href)) continue;
    seen.add(href);
    let title = (await a.innerText())?.trim() || (await a.getAttribute('title')) || href;
    title = title.replace(/\s+/g,' ').trim();
    list.push({ title, url: href });
  }
  return list;
}
async function scrapeTopicsInsideCall(page, callUrl) {
  await page.goto(callUrl, { waitUntil: 'networkidle' });
  await page.waitForTimeout(700);
  try { for (let i=0;i<12;i++){ await page.mouse.wheel(0,5000); await page.waitForTimeout(300);} } catch {}
  return await collectLinks(page, 'a[href*="/topic-details/"]', callUrl);
}
function writeListOutputs(prefix, items, humanTitle) {
  const lis = items.map(it => `  <li><a href="${it.url}" target="_blank" rel="noopener noreferrer">${it.title}</a></li>`).join('\n');
  fs.writeFileSync(`${prefix}-list.html`, `<!-- generated ${RUN_TS} -->\n<h2>${humanTitle}</h2>\n<ul>\n${lis}\n</ul>\n`, 'utf8');
  const header=['title','url'];
  const csv=[header.join(',')].concat(items.map(r=>`"${String(r.title).replace(/"/g,'""')}","${String(r.url).replace(/"/g,'""')}"`));
  fs.writeFileSync(`${prefix}-list.csv`, csv.join('\n'), 'utf8');
  fs.writeFileSync(`${prefix}-list.json`, JSON.stringify({ generatedAt: RUN_TS, items }, null, 2), 'utf8');
}

// ------------- ENRICHISSEMENT FUNDING -------------
function extractSlugFromTopicUrl(href) {
  const m = String(href).toLowerCase().match(/\/topic-details\/([^/?#]+)/);
  return m ? m[1] : '';
}
function toIsoDate(v) {
  if (v == null || v === '') return '';
  if (typeof v === 'number') { const d=new Date(v); return isNaN(d)?'':d.toISOString().slice(0,10); }
  const s=String(v);
  const mEU=s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (mEU){ const d=new Date(+mEU[3], +mEU[2]-1, +mEU[1]); return isNaN(d)?'':d.toISOString().slice(0,10);}
  const d=new Date(s); return isNaN(d)?'':d.toISOString().slice(0,10);
}
function pick(...vals){ for(const v of vals){ if(v!=null && String(v).trim()!=='') return String(v);} return ''; }
function clampLen(txt,max=600){ if(!txt) return ''; const t=String(txt).replace(/\s+/g,' ').trim(); return t.length>max ? t.slice(0,max-1)+'…' : t; }

async function fetchTopicDetails(page, slugLower, lang='en') {
  const url = `https://ec.europa.eu/info/funding-tenders/opportunities/data/topicDetails/${encodeURIComponent(slugLower)}.json?lang=${encodeURIComponent(lang)}`;
  const resp = await page.request.get(url, { timeout: 20000 });
  if (!resp.ok()) throw new Error(`HTTP ${resp.status()} for ${url}`);
  return await resp.json();
}
function computeNextDeadline(actions) {
  const a0 = Array.isArray(actions) ? actions[0] : null;
  const ds = a0 && Array.isArray(a0.deadlineDates) ? a0.deadlineDates.map(toIsoDate).filter(Boolean) : [];
  if (!ds.length) return { nextDeadline:'', allDeadlines:'' };
  const today = new Date().toISOString().slice(0,10);
  const next = ds.find(d => d >= today) || ds.sort()[ds.length-1];
  return { nextDeadline: next, allDeadlines: ds.join(' | ') };
}
function extractFundingMoneyish(TD){
  // Best-effort: certains topics exposent totalBudget, d’autres des textes; on reste conservateur
  const totalBudget = (TD.totalBudget && Number(TD.totalBudget)) || '';
  let projectBudgetMin='', projectBudgetMax='', fundingRate='';
  // on peut tenter une extraction simple de chiffres depuis TD.budgetOverview / summary
  const src = [TD.budgetOverview, TD.summary, TD.content].map(x=>x||'').join(' ');
  const mRate = src.match(/(\d{1,3})\s?%/);
  if (mRate) fundingRate = mRate[1];
  // plages € (ex: 1-3 million, 500k-2M)
  const mRange = src.match(/(?:€|EUR)?\s?([\d.,]+)\s*(?:k|K|000)?\s*[-–]\s*(?:€|EUR)?\s?([\d.,]+)\s*(?:m|M|million|Millions|Mio|000000)?/i);
  // ceci est volontairement basique; on peut raffiner plus tard
  if (mRange){
    projectBudgetMin = mRange[1];
    projectBudgetMax = mRange[2];
  }
  return { totalBudget, projectBudgetMin, projectBudgetMax, fundingRate };
}
function eligibleCountriesSummary(TD){
  const v = TD.eligibleCountries || TD.associatedCountries || '';
  if (Array.isArray(v)) return v.join(' | ');
  return String(v||'');
}
function consortiumSummary(TD){
  const txt = TD.consortiumRequirements || TD.eligibility || TD.scope || '';
  const m = String(txt).match(/(?:at least|min(?:imum)?)\s*\d+.+?countries?/i);
  return m ? clampLen(m[0], 120) : '';
}
function summaryShortFrom(TD){
  const cand = pick(TD.summary, TD.expectedOutcome, TD.scope, TD.title);
  return clampLen(cand, 600);
}
async function enrichFunding(page, topicUrls) {
  const header = [
    'identifier','title','programme','destination','workProgrammePart','typeOfAction',
    'topicStatus','url','callIdentifier','callUrl',
    'plannedOpeningDate','nextDeadline','allDeadlines','daysToDeadline',
    'totalBudget','projectBudgetMin','projectBudgetMax','fundingRate',
    'eligibleCountries','consortiumSummary','trl',
    'summaryShort','priorityScore','urgencyScore','keywordScore',
    'generatedAt','rawRef'
  ];
  const today = new Date().toISOString().slice(0,10);

  const rows = [header.join(',')];
  let count = 0;

  for (const u of topicUrls.slice(0, MAX_ENRICH_FUNDING)) {
    const slug = extractSlugFromTopicUrl(u);
    if (!slug){ continue; }
    try {
      const data = await fetchTopicDetails(page, slug, 'en');
      const TD = data?.TopicDetails || {};
      const actions = Array.isArray(TD.actions) ? TD.actions : [];
      const a0 = actions[0] || {};
      const { nextDeadline, allDeadlines } = computeNextDeadline(actions);
      const planned = toIsoDate(a0.plannedOpeningDate);
      const daysToDeadline = nextDeadline ? Math.ceil((new Date(nextDeadline) - new Date(today)) / (1000*3600*24)) : '';

      const money = extractFundingMoneyish(TD);
      const programme = pick(TD.frameworkProgramme, TD.programme);
      const typeOfAction = Array.isArray(TD.typeOfAction) ? TD.typeOfAction.join(' | ')
                         : Array.isArray(TD.typeOfActions) ? TD.typeOfActions.join(' | ')
                         : pick(TD.typeOfAction);
      const typeOfMGAs = Array.isArray(TD.typeOfMGAs) ? TD.typeOfMGAs.join(' | ')
                        : Array.isArray(TD.typeofMGAs) ? TD.typeofMGAs.join(' | ')
                        : '';
      const urgencyScore = nextDeadline ? (daysToDeadline <= 14 ? 3 : daysToDeadline <= 30 ? 2 : daysToDeadline <= 45 ? 1 : 0) : 0;
      const keywordScore = 0; // tu feras tes tris dans Sheets; on laisse 0 par défaut
      const priorityScore = urgencyScore + keywordScore;

      const rec = {
        identifier: pick(TD.identifier, TD.topicId, slug.toUpperCase()),
        title: pick(TD.title),
        programme,
        destination: pick(TD.destination, TD.destinationCode, TD.destinationTitle),
        workProgrammePart: pick(TD.workProgramPart, TD.workProgrammePart, TD.workProgramme),
        typeOfAction,
        topicStatus: pick(TD.topicStatus, TD.status),
        url: u,
        callIdentifier: pick(TD.callIdentifier, TD.callId),
        callUrl: pick(TD.callDocumentsLink, TD.callLink, ''),
        plannedOpeningDate: planned,
        nextDeadline,
        allDeadlines,
        daysToDeadline: daysToDeadline === '' ? '' : String(daysToDeadline),
        totalBudget: money.totalBudget,
        projectBudgetMin: money.projectBudgetMin,
        projectBudgetMax: money.projectBudgetMax,
        fundingRate: money.fundingRate || '',
        eligibleCountries: eligibleCountriesSummary(TD),
        consortiumSummary: consortiumSummary(TD),
        trl: pick(TD.technologyReadinessLevel, TD.TRL),
        summaryShort: summaryShortFrom(TD),
        priorityScore: String(priorityScore),
        urgencyScore: String(urgencyScore),
        keywordScore: String(keywordScore),
        generatedAt: RUN_TS,
        rawRef: slug
      };

      const vals = header.map(h => (rec[h] ?? '').toString().replace(/"/g,'""'));
      rows.push(`"${vals.join('","')}"`);
      count++;
      if (count % 100 === 0) console.log(`[Funding] enrichi: ${count}`);
    } catch (e) {
      console.warn('[Funding] ERR', slug, e.message);
    }
  }

  fs.writeFileSync('funding_enriched.csv', rows.join('\n'), 'utf8');
  console.log(`[Funding] Enrichissement terminé: ${count} lignes`);
}

// ------------- ENRICHISSEMENT TENDERS -------------
function getTextOrEmpty(el){ return (el?.trim?.() ? el.trim() : ''); }
function parseMoney(text){
  if (!text) return { amount:'', currency:'' };
  const t = text.replace(/\s+/g,' ');
  const m = t.match(/([€$£]|EUR|USD|GBP)?\s*([\d.,]+)\s*(million|m|M)?/);
  let amount='', currency='';
  if (m){
    currency = (m[1]||'').replace(/[^A-Z€$£]/gi,'') || '';
    const raw = m[2].replace(/\./g,'').replace(/,/g,'.');
    let val = Number(raw);
    if (m[3]) val = val * 1_000_000;
    amount = isNaN(val) ? '' : String(Math.round(val));
  }
  if (/EUR|€/.test(t) && !currency) currency='EUR';
  if (/USD|\$/.test(t) && !currency) currency='USD';
  if (/GBP|£/.test(t) && !currency) currency='GBP';
  return { amount, currency };
}
function pickFirstNonEmpty(...arr){ for(const v of arr){ if(v && String(v).trim()!=='') return String(v).trim(); } return ''; }

async function enrichTenderPage(page, url) {
  await page.goto(url, { waitUntil: 'networkidle' });
  await page.waitForTimeout(500);

  // Stratégie : lire blocs "Key information" + entête
  const data = await page.evaluate(() => {
    function text(sel){ const el=document.querySelector(sel); return el ? el.textContent.trim() : ''; }
    function findByLabel(labels){
      // cherche un dt/dd ou une ligne "Label: Valeur"
      const candidates = Array.from(document.querySelectorAll('dt, .label, .field-label, th'));
      for (const c of candidates) {
        const lbl = c.textContent.trim().toLowerCase();
        for (const l of labels) {
          if (lbl.includes(l)) {
            // valeur voisine
            let val = '';
            const dd = c.nextElementSibling;
            if (dd) val = dd.textContent.trim();
            else val = c.parentElement?.querySelector('dd, .value, td')?.textContent?.trim() || '';
            if (val) return val;
          }
        }
      }
      // fallback : scan des p/li
      const blocks = Array.from(document.querySelectorAll('p, li'));
      for (const b of blocks) {
        const t = b.textContent.trim();
        for (const l of labels) {
          const i = t.toLowerCase().indexOf(l);
          if (i >= 0) {
            const after = t.slice(i + l.length).replace(/^[\s:–-]+/, '');
            if (after) return after;
          }
        }
      }
      return '';
    }

    const title = text('h1, h2, [data-testid="title"], .page-title');
    const reference = findByLabel(['reference','notice number','reference number','ref.']);
    const contractingAuthority = findByLabel(['contracting authority','buyer','purchaser','awarding entity']);
    const buyerCountry = findByLabel(['country']);
    const buyerCity = findByLabel(['city','town']);
    const procedureType = findByLabel(['procedure type','procedure']);
    const contractType = findByLabel(['contract type','type of contract']);
    const cpvTop = findByLabel(['cpv','cpv code']);
    const lotsCountTxt = findByLabel(['lot(s)','lots']);
    const publicationDate = findByLabel(['publication date','date of publication']);
    const deadlineDate = findByLabel(['deadline','time limit','submission deadline']);
    const estimatedValueTxt = findByLabel(['estimated value','contract value','value']);
    const currencyTxt = findByLabel(['currency']);

    const docs = Array.from(document.querySelectorAll('a')).filter(a => /document|annex|spec/i.test(a.textContent||''));
    const documentsCount = docs.length;

    const esub = Array.from(document.querySelectorAll('a')).map(a => ({href:a.href, t:(a.textContent||'').trim()}))
      .find(x => /submit|e-?submission|tender|apply/i.test(x.t) || /submission/.test(x.href));

    return {
      title, reference, contractingAuthority, buyerCountry, buyerCity,
      procedureType, contractType, cpvTop, lotsCountTxt, publicationDate,
      deadlineDate, estimatedValueTxt, currencyTxt,
      documentsCount, esubUrl: esub?.href || ''
    };
  });

  const lotsCount = (data.lotsCountTxt && Number((data.lotsCountTxt.match(/\d+/)||[''])[0])) || '';
  const { amount: estimatedValue, currency } = parseMoney(data.estimatedValueTxt || data.currencyTxt);
  const pubISO = toIsoDate(data.publicationDate);
  const deadISO = toIsoDate(data.deadlineDate);
  const today = new Date().toISOString().slice(0,10);
  const daysToDeadline = deadISO ? Math.ceil((new Date(deadISO) - new Date(today)) / (1000*3600*24)) : '';

  const urgencyScore = deadISO ? (daysToDeadline <= 14 ? 3 : daysToDeadline <= 30 ? 2 : daysToDeadline <= 45 ? 1 : 0) : 0;
  const valueScore = estimatedValue ? (Number(estimatedValue) >= 1_000_000 ? 2 : Number(estimatedValue) >= 250_000 ? 1 : 0) : 0;
  const priorityScore = urgencyScore + valueScore;

  return {
    reference: getTextOrEmpty(data.reference),
    title: getTextOrEmpty(data.title),
    contractingAuthority: getTextOrEmpty(data.contractingAuthority),
    buyerCountry: getTextOrEmpty(data.buyerCountry),
    buyerCity: getTextOrEmpty(data.buyerCity),
    procedureType: getTextOrEmpty(data.procedureType),
    contractType: getTextOrEmpty(data.contractType),
    cpvTop: getTextOrEmpty(data.cpvTop),
    lotsCount: lotsCount === '' ? '' : String(lotsCount),
    publicationDate: pubISO,
    deadlineDate: deadISO,
    daysToDeadline: daysToDeadline === '' ? '' : String(daysToDeadline),
    estimatedValue: estimatedValue,
    currency: currency,
    documentsCount: String(data.documentsCount || 0),
    noticeUrl: url,
    esubmissionLink: data.esubUrl || '',
    urgencyScore: String(urgencyScore),
    valueScore: String(valueScore),
    priorityScore: String(priorityScore),
    generatedAt: RUN_TS
  };
}

async function enrichTenders(page, tenderUrls) {
  const header = [
    'reference','title','contractingAuthority','buyerCountry','buyerCity',
    'procedureType','contractType','cpvTop','lotsCount',
    'publicationDate','deadlineDate','daysToDeadline',
    'estimatedValue','currency','documentsCount',
    'noticeUrl','esubmissionLink',
    'priorityScore','urgencyScore','valueScore',
    'generatedAt'
  ];
  const rows = [header.join(',')];
  let count = 0;

  for (const u of tenderUrls.slice(0, MAX_ENRICH_TENDERS)) {
    try {
      const rec = await enrichTenderPage(page, u);
      const vals = header.map(h => (rec[h] ?? '').toString().replace(/"/g,'""'));
      rows.push(`"${vals.join('","')}"`);
      count++;
      if (count % 50 === 0) console.log(`[Tenders] enrichi: ${count}`);
    } catch (e) {
      console.warn('[Tenders] ERR', u, e.message);
    }
  }

  fs.writeFileSync('tenders_enriched.csv', rows.join('\n'), 'utf8');
  console.log(`[Tenders] Enrichissement terminé: ${count} lignes`);
}

// ------------- MAIN -------------
(async () => {
  const browser = await chromium.launch({ headless: HEADLESS });
  const ctx = await browser.newContext();
  const page = await ctx.newPage();

  // 1) Collecte Funding + Tenders (listes brutes)
  const collected = {}; // { funding: [...], tenders: [...] }
  for (const task of TASKS) {
    console.log(`\n=== ${task.name.toUpperCase()} ===`);
    const all = []; const seen = new Set();

    for (let p = 1; p <= task.maxPages; p++) {
      const url = buildPageUrl(task.baseUrl, task.params, p);
      console.log(`→ page ${p}: ${url}`);
      await page.goto(url, { waitUntil: 'networkidle' });
      await page.waitForTimeout(700);

      try { await page.waitForSelector(task.linkSelector, { timeout: 30000 }); }
      catch { console.log('  (aucun résultat détecté) — fin'); break; }

      const pageTopics = await collectVirtualized(page, task.linkSelector, url, 50);

      let pageTopicsFromCalls = [];
      if (task.callSelectors && task.callSelectors.length) {
        let callLinks = [];
        for (const sel of task.callSelectors) {
          const found = await collectLinks(page, sel, url);
          callLinks.push(...found);
        }
        const seenCalls = new Set();
        callLinks = callLinks.filter(c => (seenCalls.has(c.url) ? false : seenCalls.add(c.url)));
        for (const c of callLinks) {
          try {
            const inner = await scrapeTopicsInsideCall(page, c.url);
            pageTopicsFromCalls.push(...inner);
          } catch (e) {
            console.warn('  call-details ERR', c.url, e.message);
          }
        }
      }

      const pageItems = [...pageTopics, ...pageTopicsFromCalls];
      if (!pageItems.length) { console.log('  page vide — fin'); break; }

      let added = 0;
      for (const it of pageItems) {
        if (seen.has(it.url)) continue;
        seen.add(it.url);
        all.push(it);
        added++;
      }
      console.log(`  ${pageItems.length} trouvés (dont via calls: ${pageTopicsFromCalls.length}), ${added} nouveaux (total: ${all.length})`);
      if (added === 0 && pageItems.length < 5) { // heuristique "vraiment" à la fin
        console.log('  page quasi vide et aucun nouveau — fin pagination');
        break;
      }
    }

    const niceTitle = task.name === 'funding'
      ? `Funding — Calls for proposals (Open + Forthcoming) — ${all.length} items`
      : `Procurement — Calls for tenders (Open + Forthcoming) — ${all.length} items`;

    writeListOutputs(task.name, all, niceTitle);
    collected[task.name] = all.map(x => x.url);
  }

  // 2) ENRICHIR (aucun tri — tu feras les tris dans Google Sheets)
  // Funding
  const fundingUrls = Array.from(new Set(collected.funding || []));
  if (fundingUrls.length) {
    console.log(`\n[Funding] enrichissement de ${Math.min(fundingUrls.length, MAX_ENRICH_FUNDING)} topics…`);
    await enrichFunding(page, fundingUrls);
  } else {
    console.log('[Funding] rien à enrichir.');
    fs.writeFileSync('funding_enriched.csv', 'identifier,title,programme,destination,workProgrammePart,typeOfAction,topicStatus,url,callIdentifier,callUrl,plannedOpeningDate,nextDeadline,allDeadlines,daysToDeadline,totalBudget,projectBudgetMin,projectBudgetMax,fundingRate,eligibleCountries,consortiumSummary,trl,summaryShort,priorityScore,urgencyScore,keywordScore,generatedAt,rawRef\n', 'utf8');
  }

  // Tenders
  const tendersUrls = Array.from(new Set(collected.tenders || []));
  if (tendersUrls.length) {
    console.log(`\n[Tenders] enrichissement de ${Math.min(tendersUrls.length, MAX_ENRICH_TENDERS)} avis…`);
    await enrichTenders(page, tendersUrls);
  } else {
    console.log('[Tenders] rien à enrichir.');
    fs.writeFileSync('tenders_enriched.csv', 'reference,title,contractingAuthority,buyerCountry,buyerCity,procedureType,contractType,cpvTop,lotsCount,publicationDate,deadlineDate,daysToDeadline,estimatedValue,currency,documentsCount,noticeUrl,esubmissionLink,priorityScore,urgencyScore,valueScore,generatedAt\n', 'utf8');
  }

  // 3) Index HTML rapide (info)
  const idxHtml = [
    `<!-- generated ${RUN_TS} -->`,
    '<h1>EU Funding & Tenders — Extractions</h1>',
    '<ul>',
    `  <li>funding-list: <a href="funding-list.html">HTML</a> / <a href="funding-list.csv">CSV</a></li>`,
    `  <li>tenders-list: <a href="tenders-list.html">HTML</a> / <a href="tenders-list.csv">CSV</a></li>`,
    `  <li>funding_enriched: <a href="funding_enriched.csv">CSV</a></li>`,
    `  <li>tenders_enriched: <a href="tenders_enriched.csv">CSV</a></li>`,
    '</ul>'
  ].join('\n');
  fs.writeFileSync('index.html', idxHtml, 'utf8');

  await browser.close();
  console.log('\n✓ Terminé (listes + enriched CSV générés).');
})();
