import { chromium } from 'playwright';
import fs from 'fs';

const RUN_TS = new Date().toISOString();
const HEADLESS = true;

// Mode “rapide” activable par workflow (ne change rien si non défini)
const QUICK_MODE = String(process.env.QUICK_MODE || '0') === '1';
const MAX_ENRICH_FUNDING = Number(process.env.MAX_ENRICH_FUNDING || (QUICK_MODE ? 30 : 2000));
const MAX_ENRICH_TENDERS = Number(process.env.MAX_ENRICH_TENDERS || (QUICK_MODE ? 30 : 1500));

/**
 * Deux parcours :
 *  - funding (Calls for proposals) — liens /topic-details/ (+ crawl des /call-details/)
 *  - tenders (Calls for tenders)   — liens /tender-details/
 */
const TASKS = [
  {
    name: 'funding',
    baseUrl: 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals',
    params: { order:'DESC', sortBy:'startDate', pageSize:50, isExactMatch:true, status:'31094501,31094502' },
    linkSelector: 'a[href*="/topic-details/"]',
    callSelectors: ['a[href*="/call-details/"]','a[href*="/calls/"]'],
    maxPages: QUICK_MODE ? 2 : 2000
  },
  {
    name: 'tenders',
    baseUrl: 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-tenders',
    params: { order:'DESC', sortBy:'startDate', pageSize:50, isExactMatch:true, status:'31094501,31094502' },
    linkSelector: 'a[href*="/tender-details/"]',
    maxPages: QUICK_MODE ? 2 : 2000
  }
];

/* ====================== Utils ====================== */
function cleanUrl(href, base) { try { const u=new URL(href, base); return `${u.origin}${u.pathname}`; } catch { return href; } }
function buildPageUrl(base, params, pageNumber) { const u=new URL(base); Object.entries(params).forEach(([k,v])=>u.searchParams.set(k,String(v))); u.searchParams.set('pageNumber',String(pageNumber)); return u.toString(); }
function normLabel(s){ return String(s||'').toLowerCase().replace(/\s+/g,' ').trim(); }
function clampLen(txt,max=600){ if(!txt) return ''; const t=String(txt).replace(/\s+/g,' ').trim(); return t.length>max ? t.slice(0,max-1)+'…' : t; }
function pick(...vals){ for(const v of vals){ if(v!=null && String(v).trim()!=='') return String(v);} return ''; }
function toIsoDate(v) {
  if (v == null || v === '') return '';
  if (typeof v === 'number') { const d=new Date(v); return isNaN(d)?'':d.toISOString().slice(0,10); }
  const s=String(v).trim();
  const mEU=s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (mEU){ const d=new Date(+mEU[3], +mEU[2]-1, +mEU[1]); return isNaN(d)?'':d.toISOString().slice(0,10); }
  const d=new Date(s); return isNaN(d)?'':d.toISOString().slice(0,10);
}
async function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }
async function safeGoto(page, url, label='page', attempts=3){
  for(let i=1;i<=attempts;i++){
    try{
      await page.goto(url, { waitUntil:'domcontentloaded', timeout:45000 });
      await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(()=>{});
      return;
    }catch(e){
      if(i===attempts) throw new Error(`safeGoto(${label}) fail: ${e.message}`);
      await sleep(800*i);
    }
  }
}
async function safeJsonGet(request, url, attempts=3){
  for(let i=1;i<=attempts;i++){
    const resp = await request.get(url, { timeout: 20000 });
    if (resp.ok()) return await resp.json();
    const status = resp.status();
    if (status === 429) await sleep(1500*i); else await sleep(600*i);
    if (i===attempts) throw new Error(`HTTP ${status} ${url}`);
  }
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
  await safeGoto(page, callUrl, 'call-details');
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

/* ====================== Extraction HTML générique ====================== */
async function extractKeyValuePairs(page){
  return await page.evaluate(() => {
    function grabKv(root){
      const kv = {};
      // dl/dt/dd
      root.querySelectorAll('dl').forEach(dl=>{
        const dts = dl.querySelectorAll('dt');
        dts.forEach(dt=>{
          const dd = dt.nextElementSibling && dt.nextElementSibling.tagName.toLowerCase()==='dd'
            ? dt.nextElementSibling : null;
          const k = dt.textContent?.trim();
          const v = dd?.textContent?.trim();
          if (k && v) kv[k] = v;
        });
      });
      // tables 2 colonnes
      root.querySelectorAll('table').forEach(t=>{
        Array.from(t.querySelectorAll('tr')).forEach(tr=>{
          const tds = tr.querySelectorAll('th,td');
          if (tds.length === 2){
            const k = tds[0].textContent?.trim();
            const v = tds[1].textContent?.trim();
            if (k && v) kv[k] = v;
          }
        });
      });
      return kv;
    }
    const kv = grabKv(document);
    const blocks = Array.from(document.querySelectorAll('[data-testid*="key"],[class*="key-info"],section,article'));
    blocks.forEach(b=>{ Object.assign(kv, grabKv(b)); });
    return kv;
  });
}
async function extractJsonLd(page){
  try{
    const arr = await page.$$eval('script[type="application/ld+json"]', nodes =>
      nodes.map(n=>{ try{ return JSON.parse(n.textContent||'{}'); }catch(e){ return null; } }).filter(Boolean)
    );
    return arr;
  }catch{ return []; }
}

/* ====================== Funding — enrichissement ====================== */
function extractSlugFromTopicUrl(href) { const m=String(href).toLowerCase().match(/\/topic-details\/([^/?#]+)/); return m? m[1] : ''; }
async function fetchTopicDetails(page, slugLower, lang='en') {
  const url = `https://ec.europa.eu/info/funding-tenders/opportunities/data/topicDetails/${encodeURIComponent(slugLower)}.json?lang=${encodeURIComponent(lang)}`;
  return await safeJsonGet(page.request, url);
}
async function scrapeFundingTopicHtml(page, url){
  await safeGoto(page, url, 'topic-details HTML');
  await page.waitForTimeout(600);
  const title = await page.title().catch(()=>'');

  const kv = await extractKeyValuePairs(page);
  const jsonLdArr = await extractJsonLd(page);
  const textBits = await page.evaluate(() => {
    function hasText(el, t){ return el && RegExp(t,'i').test(el.textContent||''); }
    function grabByHeading(keyword){
      const heads = Array.from(document.querySelectorAll('h1,h2,h3,h4'));
      for (const h of heads){
        if (hasText(h, keyword)){
          let n = h.nextElementSibling; let acc='';
          for (let i=0;i<6 && n;i++, n=n.nextElementSibling){
            const txt = n.textContent?.trim();
            if (txt && txt.length > 40) { acc = txt; break; }
          }
          return acc;
        }
      }
      return '';
    }
    const outcome = grabByHeading('Expected Outcome|Outcomes') || '';
    const scope   = grabByHeading('Scope') || '';
    const summary = grabByHeading('Summary|Overview') || '';
    return { outcome, scope, summary };
  }).catch(()=>({outcome:'',scope:'',summary:''}));
  return { title, kv, jsonLdArr, textBits: textBits || {} };
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
  const rows = [header.join(',')];
  const todayISO = new Date().toISOString().slice(0,10);

  let count = 0;
  for (const u of topicUrls.slice(0, MAX_ENRICH_FUNDING)) {
    const slug = extractSlugFromTopicUrl(u);
    if (!slug) continue;

    let TD = {};
    let html = {kv:{}, textBits:{}, jsonLdArr:[], title:''};
    try { const data = await fetchTopicDetails(page, slug, 'en'); TD = data?.TopicDetails || {}; }
    catch (e) { console.warn('[Funding] JSON err', slug, e.message); }
    try { html = await scrapeFundingTopicHtml(page, u); }
    catch (e) { console.warn('[Funding] HTML err', slug, e.message); }

    const programme = pick(TD.frameworkProgramme, TD.programme, html.kv['Programme'], html.kv['Program']);
    const destination = pick(TD.destination, TD.destinationCode, TD.destinationTitle, html.kv['Destination']);
    const workProgrammePart = pick(TD.workProgramPart, TD.workProgrammePart, TD.workProgramme, html.kv['Work programme part'], html.kv['Work Programme']);
    const typeOfAction = Array.isArray(TD.typeOfAction) ? TD.typeOfAction.join(' | ')
                      : Array.isArray(TD.typeOfActions) ? TD.typeOfActions.join(' | ')
                      : pick(TD.typeOfAction, html.kv['Type of action']);
    const topicStatus = pick(TD.topicStatus, TD.status, html.kv['Status']);
    const callIdentifier = pick(TD.callIdentifier, TD.callId, html.kv['Call identifier']);
    const callUrl = pick(TD.callDocumentsLink, TD.callLink, html.kv['Call link'], '');
    const plannedOpeningDate = toIsoDate(pick(TD.actions?.[0]?.plannedOpeningDate, html.kv['Opening date'], html.kv['Planned opening date']));

    const allDs = (Array.isArray(TD.actions?.[0]?.deadlineDates) ? TD.actions[0].deadlineDates.map(toIsoDate) : []).filter(Boolean);
    const dlFromHtml = Object.entries(html.kv).filter(([k]) => /deadline/i.test(k)).map(([,v]) => toIsoDate(v)).filter(Boolean);
    const allDeadlinesArr = Array.from(new Set([...allDs, ...dlFromHtml])).sort();
    const nextDeadline = allDeadlinesArr.find(d => d >= todayISO) || (allDeadlinesArr.length ? allDeadlinesArr[allDeadlinesArr.length-1] : '');
    const daysToDeadline = nextDeadline ? Math.ceil((new Date(nextDeadline) - new Date(todayISO)) / 86400000) : '';

    function extractMoney(txt){
      const m = String(txt||'').replace(/\s+/g,' ').match(/([\d.,]+)\s*(million|m|M)?/);
      return m ? (m[2] ? Number(m[1].replace(/\./g,'').replace(/,/g,'.'))*1e6 : Number(m[1].replace(/\./g,'').replace(/,/g,'.'))) : '';
    }
    const totalBudget = TD.totalBudget || extractMoney(TD.budgetOverview||'');
    const fundingRate = (TD.fundingRate || '').toString().replace(/[^\d%]/g,'');
    const eligibleCountries = Array.isArray(TD.eligibleCountries) ? TD.eligibleCountries.join(' | ')
                               : pick(TD.eligibleCountries, TD.associatedCountries, html.kv['Eligible countries'], html.kv['Eligibility']);
    const trl = pick(TD.technologyReadinessLevel, TD.TRL, html.kv['TRL']);
    const summaryShort = clampLen(pick(TD.summary, TD.expectedOutcome, TD.scope, html.textBits.summary, html.textBits.outcome, html.textBits.scope, html.title), 600);

    const urgencyScore = nextDeadline ? (daysToDeadline <= 14 ? 3 : daysToDeadline <= 30 ? 2 : daysToDeadline <= 45 ? 1 : 0) : 0;
    const keywordScore = 0;
    const priorityScore = urgencyScore + keywordScore;

    const rec = {
      identifier: pick(TD.identifier, TD.topicId, slug.toUpperCase()),
      title: pick(TD.title, html.title),
      programme, destination, workProgrammePart, typeOfAction,
      topicStatus, url: u, callIdentifier, callUrl,
      plannedOpeningDate, nextDeadline, allDeadlines: allDeadlinesArr.join(' | '),
      daysToDeadline: daysToDeadline === '' ? '' : String(daysToDeadline),
      totalBudget, projectBudgetMin:'', projectBudgetMax:'', fundingRate,
      eligibleCountries, consortiumSummary: clampLen(pick(TD.consortiumRequirements, TD.eligibility, html.kv['Consortium']), 180),
      trl, summaryShort,
      priorityScore:String(priorityScore), urgencyScore:String(urgencyScore), keywordScore:String(keywordScore),
      generatedAt: RUN_TS, rawRef: slug
    };

    const header = rows[0].split(',');
    const vals = header.map(h => (rec[h] ?? '').toString().replace(/"/g,'""'));
    rows.push(`"${vals.join('","')}"`);

    // RAW JSONL (audit)
    fs.appendFileSync('funding_raw.jsonl', JSON.stringify({ url:u, slug, topicDetails:TD||{}, htmlKV:html.kv||{}, jsonLd:html.jsonLdArr||[] }) + '\n');

    count++;
    if (count % 100 === 0) console.log(`[Funding] enrichi: ${count}`);
  }

  fs.writeFileSync('funding_enriched.csv', rows.join('\n'), 'utf8');
  console.log(`[Funding] Enrichissement terminé: ${count} lignes`);
}

/* ====================== Tenders — enrichissement ====================== */
function getTextOrEmpty(el){ return (el?.trim?.() ? el.trim() : ''); }
async function enrichTenderPage(page, url) {
  await safeGoto(page, url, 'tender-details');
  await page.waitForTimeout(700);

  const jsonLdArr = await extractJsonLd(page);
  let jl = {};
  if (jsonLdArr && jsonLdArr.length){
    jl = jsonLdArr.sort((a,b)=>JSON.stringify(b).length - JSON.stringify(a).length)[0] || {};
  }
  const kv = await extractKeyValuePairs(page);

  const get = (labels) => {
    const keys = Object.keys(kv);
    for (const lab of labels){
      const match = keys.find(k => normLabel(k).includes(normLabel(lab)));
      if (match) return kv[match];
    }
    return '';
  };

  const pageTitle = await page.title().catch(()=>'');

  const title = (jl.name || jl.title || pageTitle) || get(['title','notice title']);
  const reference = get(['reference','reference number','notice number','ref.']);
  const contractingAuthority = jl?.publisher?.name || get(['contracting authority','buyer','purchaser','awarding entity']);
  const buyerCountry = jl?.address?.addressCountry || get(['country']);
  const buyerCity = jl?.address?.addressLocality || get(['city','town']);
  const procedureType = get(['procedure type','procedure']);
  const contractType = get(['contract type','type of contract']);
  const cpvTop = (Array.isArray(jl?.additionalProperty) ? jl.additionalProperty.find(x=>/cpv/i.test(x?.name||''))?.value : '') || get(['cpv','cpv code']);
  const lotsCountTxt = get(['lot(s)','lots','number of lots']);
  const publicationDate = jl?.datePublished || get(['publication date','date of publication']);
  const deadlineDate = jl?.validThrough || get(['deadline','time limit','submission deadline','deadline for receipt of tenders']);

  const docsCount = (await page.$$eval('a', as => as.filter(a=>/document|annex|spec|tender doc/i.test(a.textContent||'')).length)).toString();
  const esub = await page.$$eval('a', as => {
    const m = as.map(a=>({href:a.href, t:(a.textContent||'').trim()}))
      .find(x => /submit|e-?submission|tender|apply/i.test(x.t) || /submission/.test(x.href));
    return m?.href || '';
  });

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
  const moneyTxt = get(['estimated value','contract value','value']);
  const currencyTxt = get(['currency']);
  const { amount: estimatedValue, currency } = parseMoney(moneyTxt || currencyTxt);

  const pubISO = toIsoDate(publicationDate);
  const deadISO = toIsoDate(deadlineDate);
  const today = new Date().toISOString().slice(0,10);
  const daysToDeadline = deadISO ? Math.ceil((new Date(deadISO) - new Date(today)) / 86400000) : '';

  const urgencyScore = deadISO ? (daysToDeadline <= 14 ? 3 : daysToDeadline <= 30 ? 2 : daysToDeadline <= 45 ? 1 : 0) : 0;
  const valueScore = estimatedValue ? (Number(estimatedValue) >= 1_000_000 ? 2 : Number(estimatedValue) >= 250_000 ? 1 : 0) : 0;
  const priorityScore = urgencyScore + valueScore;

  return {
    reference: getTextOrEmpty(reference) || jl?.identifier || '',
    title: getTextOrEmpty(title),
    contractingAuthority: getTextOrEmpty(contractingAuthority),
    buyerCountry: getTextOrEmpty(buyerCountry),
    buyerCity: getTextOrEmpty(buyerCity),
    procedureType: getTextOrEmpty(procedureType),
    contractType: getTextOrEmpty(contractType),
    cpvTop: getTextOrEmpty(cpvTop),
    lotsCount: (lotsCountTxt && (lotsCountTxt.match(/\d+/)||[''])[0]) || '',
    publicationDate: pubISO,
    deadlineDate: deadISO,
    daysToDeadline: daysToDeadline === '' ? '' : String(daysToDeadline),
    estimatedValue,
    currency,
    documentsCount: docsCount,
    noticeUrl: url,
    esubmissionLink: esub || '',
    urgencyScore: String(urgencyScore),
    valueScore: String(valueScore),
    priorityScore: String(priorityScore),
    generatedAt: RUN_TS,
    _raw: { kv, jsonLd: jl || {} }
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
  const rawOut = fs.createWriteStream('tenders_raw.jsonl', {flags:'w'});
  let count = 0;

  for (const u of tenderUrls.slice(0, MAX_ENRICH_TENDERS)) {
    try {
      const rec = await enrichTenderPage(page, u);
      const vals = header.map(h => (rec[h] ?? '').toString().replace(/"/g,'""'));
      rows.push(`"${vals.join('","')}"`);
      rawOut.write(JSON.stringify({ url:u, kv:rec._raw?.kv || {}, jsonLd:rec._raw?.jsonLd || {} })+'\n');
      count++;
      if (count % 50 === 0) console.log(`[Tenders] enrichi: ${count}`);
    } catch (e) {
      console.warn('[Tenders] ERR', u, e.message);
    }
  }

  fs.writeFileSync('tenders_enriched.csv', rows.join('\n'), 'utf8');
  rawOut.end();
  console.log(`[Tenders] Enrichissement terminé: ${count} lignes`);
}

/* ====================== Main ====================== */
(async () => {
  const browser = await chromium.launch({ headless: HEADLESS });
  const ctx = await browser.newContext();
  // labels stables en anglais
  await ctx.grantPermissions([]);
  await ctx.addInitScript(() => {
    Object.defineProperty(navigator, 'language', { get: ()=>'en-US' });
    Object.defineProperty(navigator, 'languages', { get: ()=>['en-US','en'] });
  });
  const page = await ctx.newPage();

  const collected = {};
  for (const task of TASKS) {
    console.log(`\n=== ${task.name.toUpperCase()} ===`);
    const all = []; const seen = new Set();

    for (let p=1; p<=task.maxPages; p++) {
      const url = buildPageUrl(task.baseUrl, task.params, p);
      console.log(`→ page ${p}: ${url}`);
      await safeGoto(page, url, `list ${task.name} p${p}`);
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
      if (added === 0 && pageItems.length < 5) { console.log('  page quasi vide et aucun nouveau — fin pagination'); break; }
    }

    const niceTitle = task.name === 'funding'
      ? `Funding — Calls for proposals (Open + Forthcoming) — ${all.length} items`
      : `Procurement — Calls for tenders (Open + Forthcoming) — ${all.length} items`;

    writeListOutputs(task.name, all, niceTitle);
    collected[task.name] = all.map(x => x.url);
  }

  const fundingUrls = Array.from(new Set(collected.funding || []));
  if (fundingUrls.length) {
    console.log(`\n[Funding] enrichissement de ${Math.min(fundingUrls.length, MAX_ENRICH_FUNDING)} topics…`);
    await enrichFunding(page, fundingUrls);
  } else {
    console.log('[Funding] rien à enrichir.');
    fs.writeFileSync('funding_enriched.csv', 'identifier,title,programme,destination,workProgrammePart,typeOfAction,topicStatus,url,callIdentifier,callUrl,plannedOpeningDate,nextDeadline,allDeadlines,daysToDeadline,totalBudget,projectBudgetMin,projectBudgetMax,fundingRate,eligibleCountries,consortiumSummary,trl,summaryShort,priorityScore,urgencyScore,keywordScore,generatedAt,rawRef\n', 'utf8');
  }

  const tendersUrls = Array.from(new Set(collected.tenders || []));
  if (tendersUrls.length) {
    console.log(`\n[Tenders] enrichissement de ${Math.min(tendersUrls.length, MAX_ENRICH_TENDERS)} avis…`);
    await enrichTenders(page, tendersUrls);
  } else {
    console.log('[Tenders] rien à enrichir.');
    fs.writeFileSync('tenders_enriched.csv', 'reference,title,contractingAuthority,buyerCountry,buyerCity,procedureType,contractType,cpvTop,lotsCount,publicationDate,deadlineDate,daysToDeadline,estimatedValue,currency,documentsCount,noticeUrl,esubmissionLink,priorityScore,urgencyScore,valueScore,generatedAt\n', 'utf8');
  }

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
