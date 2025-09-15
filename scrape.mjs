import { chromium } from '@playwright/test';
import fs from 'fs';

const HEADLESS = true;
const RUN_TS = new Date().toISOString();

/* =========================
   FAST mode (perf toggles)
   ========================= */
const IS_FAST = process.env.FAST === '1';
const FAST_CFG = {
  fundingMaxPages: 5,        // pagination limitée en FAST
  tendersMaxPages: 5,
  crawlCalls: false,         // pas de crawl call-details en FAST
  fundingParallel: 10,       // paralléliser les fetch JSON Funding
  tendersParallel: 3         // limiter le DOM parsing Tenders
};

/* =========================
   Sources / Sélecteurs
   ========================= */
const TASKS = [
  {
    name: 'funding',
    baseUrl: 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals',
    params: { order:'DESC', sortBy:'startDate', pageSize:50, isExactMatch:true, status:'31094501,31094502' },
    linkSelector: 'a[href*="/topic-details/"]',
    callSelectors: ['a[href*="/call-details/"]','a[href*="/calls/"]'],
    maxPages: IS_FAST ? FAST_CFG.fundingMaxPages : 2000
  },
  {
    name: 'tenders',
    baseUrl: 'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-tenders',
    params: { order:'DESC', sortBy:'startDate', pageSize:50, isExactMatch:true, status:'31094501,31094502' },
    linkSelector: 'a[href*="/tender-details/"]',
    maxPages: IS_FAST ? FAST_CFG.tendersMaxPages : 2000
  }
];

/* =========================
   Enrichissement / scoring
   ========================= */
const ENRICH = {
  lang: 'en',
  timeoutFetchMs: 20000,
  keywords: [
    'artificial intelligence','AI','health','energy','biodiversity','cybersecurity',
    'digital','climate','transport','manufactur','battery','semiconductor',
    'quantum','hydrogen','satellite','education'
  ],
  scoring: {
    urgency: { d14:3, d30:2, d45:1, else:0 },
    valueBands: [ {min:1_000_000,score:3}, {min:250_000,score:2}, {min:50_000,score:1} ]
  }
};

/* =========================
   Utilitaires généraux
   ========================= */
function ensureDir(p){ try{ fs.mkdirSync(p,{recursive:true}); }catch{} }
function cleanUrl(href, base){ try{ const u=new URL(href,base); return `${u.origin}${u.pathname}`; }catch{ return href; } }
function buildPageUrl(base, params, pageNumber){ const u=new URL(base); Object.entries(params).forEach(([k,v])=>u.searchParams.set(k,String(v))); u.searchParams.set('pageNumber',String(pageNumber)); return u.toString(); }
function htmlStrip(x){ return String(x||'').replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim(); }
function clip(str,max=600){ const s=String(str||''); return s.length<=max?s:s.slice(0,max-1)+'…'; }
function toIsoDate(v){
  if (v==null || v==='') return '';
  if (typeof v==='number'){ const d=new Date(v); return isNaN(d)?'':d.toISOString().slice(0,10); }
  const s=String(v);
  const mEU=s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/); // dd/mm/yyyy
  if (mEU){ const d=new Date(+mEU[3], +mEU[2]-1, +mEU[1]); return isNaN(d)?'':d.toISOString().slice(0,10); }
  const d=new Date(s); return isNaN(d)?'':d.toISOString().slice(0,10);
}
function daysBetween(iso){
  if (!iso) return '';
  const d=new Date(iso); if (isNaN(d)) return '';
  const now=new Date(); return Math.ceil((d-now)/(1000*60*60*24));
}
function kwScore(text){
  if (!text) return 0;
  const s=text.toLowerCase(); let score=0;
  for (const kw of ENRICH.keywords) if (s.includes(kw.toLowerCase())) score++;
  return Math.min(score,3);
}

/* =========================
   État incremental (docs/state.json)
   ========================= */
function loadState(){
  try{
    const p='docs/state.json';
    if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p,'utf8'));
  }catch{}
  return { seen:{} };
}
function saveState(st){ try{ ensureDir('docs'); fs.writeFileSync('docs/state.json', JSON.stringify(st,null,2),'utf8'); }catch{} }
const STATE = loadState();
const markSeen = (url) => {
  if (!STATE.seen[url]) STATE.seen[url] = { firstSeen: RUN_TS, lastSeen: RUN_TS };
  else STATE.seen[url].lastSeen = RUN_TS;
};
const isNew = (url) => !STATE.seen[url];

/* =========================
   Collecte liste (virtualized)
   ========================= */
async function findScrollContainers(page){
  return await page.evaluate(() => {
    const els = Array.from(document.querySelectorAll('main,[role="main"],[data-ft-results],[data-results-container],[class*="scroll"],body,html'));
    const out = els.filter(el => {
      const sh=el.scrollHeight, ch=el.clientHeight;
      const st=window.getComputedStyle(el).overflowY;
      return (sh && ch && sh>ch+50) || st==='auto' || st==='scroll';
    });
    if (!out.length) return [document.scrollingElement||document.body];
    return out.map(el => {
      let sel=el.tagName.toLowerCase();
      if (el.id) sel+='#'+el.id;
      if (el.className) sel+='.'+String(el.className).trim().split(/\s+/).join('.');
      return sel;
    });
  });
}
async function collectVirtualized(page, linkSelector, baseForClean, targetAtLeast=50){
  const seen=new Set(); const items=[];
  const maxLoops = IS_FAST ? 120 : 400;
  const pause    = IS_FAST ? 500 : 750;

  async function collectOnce(){
    const anchors = await page.locator(linkSelector).all();
    for (const a of anchors){
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

  let loops=0, last=0, stagnation=0;
  while (seen.size < targetAtLeast && loops < maxLoops){
    for (const sel of containers){
      try { const loc=page.locator(sel); await loc.evaluate(el => { el.scrollTop = el.scrollHeight; }); } catch {}
    }
    await page.mouse.wheel(0, 99999);
    await page.waitForTimeout(pause);
    await collectOnce();

    if (seen.size===last){
      stagnation++;
      for (const sel of containers){
        try{
          const loc=page.locator(sel);
          await loc.evaluate(el => { el.scrollTop = Math.max(0, el.scrollTop - 600); });
          await page.waitForTimeout(200);
          await loc.evaluate(el => { el.scrollTop = el.scrollTop + 1600; });
        }catch{}
      }
    } else { stagnation=0; last=seen.size; }
    if (stagnation >= (IS_FAST?10:18)) break;
    loops++;
  }
  return items;
}
async function collectLinks(page, selector, baseForClean){
  const list=[], seen=new Set();
  const anchors = await page.locator(selector).all();
  for (const a of anchors){
    const raw=await a.getAttribute('href'); if (!raw) continue;
    const href=cleanUrl(raw, baseForClean);
    if (seen.has(href)) continue;
    seen.add(href);
    let title=(await a.innerText())?.trim() || (await a.getAttribute('title')) || href;
    title=title.replace(/\s+/g,' ').trim();
    list.push({ title, url: href });
  }
  return list;
}
async function scrapeTopicsInsideCall(page, callUrl){
  await page.goto(callUrl, { waitUntil:'networkidle' });
  await page.waitForTimeout(600);
  if (!IS_FAST){ // en FULL on stimule le lazy-load
    try{ for (let i=0;i<12;i++){ await page.mouse.wheel(0,5000); await page.waitForTimeout(250); } }catch{}
  }
  return await collectLinks(page, 'a[href*="/topic-details/"]', callUrl);
}

/* =========================
   Funding: topicDetails JSON
   ========================= */
function slugFromTopicUrl(href){ const m=String(href).toLowerCase().match(/\/topic-details\/([^/?#]+)/); return m?m[1]:''; }
async function fetchTopicDetails(page, slugLower, lang='en'){
  const url = `https://ec.europa.eu/info/funding-tenders/opportunities/data/topicDetails/${encodeURIComponent(slugLower)}.json?lang=${encodeURIComponent(lang)}`;
  const resp = await page.request.get(url, { timeout: ENRICH.timeoutFetchMs });
  if (!resp.ok()) throw new Error(`HTTP ${resp.status()} for ${url}`);
  return await resp.json();
}
function mapFundingRecord(json, fallbackTitle, topicUrl){
  const TD = json?.TopicDetails || {};
  const identifier = TD.identifier || (slugFromTopicUrl(topicUrl)?.toUpperCase()) || '';
  const title = TD.title || fallbackTitle || identifier;
  const programme = TD.frameworkProgramme || TD.programme || '';
  const destination = TD.destination || TD.destinationCode || TD.destinationTitle || '';
  const workProgrammePart = TD.workProgramPart || TD.workProgrammePart || TD.workProgramme || '';
  const typeOfAction = Array.isArray(TD.typeOfAction) ? TD.typeOfAction.join(' | ')
                    : Array.isArray(TD.typeOfActions) ? TD.typeOfActions.join(' | ')
                    : (TD.typeOfAction || '');
  const topicStatus = TD.topicStatus || TD.status || '';
  const callIdentifier = TD.callIdentifier || TD.callId || '';
  const callUrl = TD.callIdentifier ? `https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/call-details/${encodeURIComponent(String(TD.callIdentifier).toLowerCase())}` : '';

  const actions = Array.isArray(TD.actions) ? TD.actions : [];
  const a0 = actions[0] || {};
  const plannedOpeningDate = toIsoDate(a0.plannedOpeningDate);
  const deadlines = Array.isArray(a0.deadlineDates) ? a0.deadlineDates.map(toIsoDate).filter(Boolean) : [];
  const allDeadlines = deadlines.join(' | ');
  const now = new Date();
  const next = deadlines.map(d=>({d,t:+new Date(d)})).filter(x=>x.t && x.t>=+now).sort((a,b)=>a.t-b.t)[0];
  const nextDeadline = next ? next.d : (deadlines[0] || '');

  const totalBudget = TD.totalBudget || TD.budgetTotal || '';
  const projectBudgetRange = TD.projectBudgetRange || '';
  let projectBudgetMin='', projectBudgetMax='';
  if (projectBudgetRange){
    const nums = String(projectBudgetRange).match(/(\d[\d\s.,]*)/g);
    if (nums && nums.length){
      const parsed = nums.map(n => Number(String(n).replace(/[^\d.,]/g,'').replace(/\./g,'').replace(',', '.'))).filter(v=>!isNaN(v));
      if (parsed.length === 1) projectBudgetMax = String(parsed[0]);
      if (parsed.length >= 2){ projectBudgetMin=String(Math.min(...parsed)); projectBudgetMax=String(Math.max(...parsed)); }
    }
  }
  const fundingRate = TD.fundingRate || '';

  const eligibleCountries = Array.isArray(TD.eligibleCountries) ? TD.eligibleCountries.join(' | ') : (TD.eligibleCountries || '');
  const consortiumSummary = TD.consortiumRequirements || TD.consortium || '';
  const trl = TD.techReadinessLevel || TD.technologyReadinessLevel || TD.TRL || '';

  const summarySource = TD.summary || TD.scope || TD.expectedOutcome || TD.expectedImpact || '';
  const summaryShort = clip(htmlStrip(summarySource), 600);

  const daysToDeadline = nextDeadline ? daysBetween(nextDeadline) : '';
  let urgencyScore = 0;
  if (daysToDeadline !== '' && !isNaN(daysToDeadline)){
    if (daysToDeadline <= 14) urgencyScore = ENRICH.scoring.urgency.d14;
    else if (daysToDeadline <= 30) urgencyScore = ENRICH.scoring.urgency.d30;
    else if (daysToDeadline <= 45) urgencyScore = ENRICH.scoring.urgency.d45;
  }
  const keywordScore = kwScore(`${title} ${summaryShort} ${destination} ${workProgrammePart} ${typeOfAction}`);
  const priorityScore = urgencyScore + keywordScore;

  return {
    identifier, title, programme, destination, workProgrammePart, typeOfAction, topicStatus,
    url: topicUrl, callIdentifier, callUrl,
    plannedOpeningDate, nextDeadline, allDeadlines, daysToDeadline,
    totalBudget, projectBudgetMin, projectBudgetMax, fundingRate,
    eligibleCountries, consortiumSummary, trl,
    summaryShort,
    urgencyScore, keywordScore, priorityScore,
    generatedAt: RUN_TS,
    rawRef: slugFromTopicUrl(topicUrl)
  };
}

/* =========================
   Tenders: DOM parsing
   ========================= */
function parseMoneyAndCurrency(str){
  if (!str) return { value:'', currency:'' };
  const s=String(str);
  const curr = (s.match(/\b(EUR|USD|GBP|€|£|\$)\b/i) || [,''])[1] || (s.includes('€')?'EUR':'');
  const numMatch = s.replace(/\s/g,'').match(/(\d[\d.,]*)/);
  let value='';
  if (numMatch){ const n=numMatch[1].replace(/\./g,'').replace(',', '.'); const v=Number(n); if (!isNaN(v)) value=String(v); }
  return { value, currency: curr.toUpperCase().replace('€','EUR').replace('£','GBP').replace('$','USD') };
}
async function enrichTenderFromPage(page, noticeUrl){
  await page.goto(noticeUrl, { waitUntil:'networkidle' });
  await page.waitForTimeout(500);

  const data = await page.evaluate(() => ({
    titleBlock: (document.querySelector('h1, h2, .op-detail-title, .title')?.innerText)||'',
    wholeText: document.body.innerText || ''
  }));
  const whole = htmlStrip(data.wholeText);
  const title = htmlStrip(data.titleBlock)||'';

  const refMatch = whole.match(/(Reference|Notice ID|Reference number|Ref\.?):\s*([A-Z0-9\-_/]+)/i);
  const reference = refMatch ? refMatch[2] : '';

  const authMatch = whole.match(/(Contracting authority|Buyer|Authority):\s*([^\n]+)/i);
  const contractingAuthority = authMatch ? authMatch[2].trim() : '';

  const countryMatch = whole.match(/(Country|Buyer country):\s*([A-Za-z\s]+)/i);
  const buyerCountry = countryMatch ? countryMatch[2].trim() : '';

  const cityMatch = whole.match(/(City|Town):\s*([^\n]+)/i);
  const buyerCity = cityMatch ? cityMatch[2].trim() : '';

  const procedureMatch = whole.match(/(Procedure type|Type of procedure):\s*([^\n]+)/i);
  const procedureType = procedureMatch ? procedureMatch[2].trim() : '';

  const contractMatch = whole.match(/(Type of contract|Contract type):\s*([^\n]+)/i);
  const contractType = contractMatch ? contractMatch[2].trim() : '';

  const cpvMatch = whole.match(/CPV[^0-9]*([0-9]{8})/i);
  const cpvTop = cpvMatch ? cpvMatch[1] : '';

  const lotsMatch = whole.match(/(Number of lots|Lots):\s*([0-9]+)/i);
  const lotsCount = lotsMatch ? lotsMatch[2] : '';

  const pubMatch = whole.match(/(Publication date|Published on):\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{2,4}|[0-9]{4}-[0-9]{2}-[0-9]{2})/i);
  const publicationDate = pubMatch ? toIsoDate(pubMatch[2]) : '';

  const deadMatch = whole.match(/(Deadline|Time limit for receipt of tenders|Submission deadline):\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{2,4}|[0-9]{4}-[0-9]{2}-[0-9]{2})/i);
  const deadlineDate = deadMatch ? toIsoDate(deadMatch[2]) : '';

  const estimatedMatch = whole.match(/(Estimated (total )?value|Contract value)[\s:]*([^\n]+)/i);
  const { value: estimatedValue, currency } = parseMoneyAndCurrency(estimatedMatch ? estimatedMatch[3] : '');

  // eSubmission / documents (best-effort)
  let documentsCount=''; let esubmissionLink='';
  try{
    const links = await page.$$eval('a', as => as.map(a => ({ href:a.href, text:(a.innerText||'').trim() })));
    const docs = links.filter(l => /document|annex|download|tender documents/i.test(l.text));
    documentsCount = String(docs.length || '');
    const eSub = links.find(l => /submit|e-?submission|apply/i.test(l.text));
    esubmissionLink = eSub ? eSub.href : '';
  }catch{}

  const daysToDeadline = deadlineDate ? daysBetween(deadlineDate) : '';
  let urgencyScore=0;
  if (daysToDeadline !== '' && !isNaN(daysToDeadline)){
    if (daysToDeadline <= 14) urgencyScore = ENRICH.scoring.urgency.d14;
    else if (daysToDeadline <= 30) urgencyScore = ENRICH.scoring.urgency.d30;
    else if (daysToDeadline <= 45) urgencyScore = ENRICH.scoring.urgency.d45;
  }
  const val = Number(estimatedValue||''); let valueScore=0;
  if (!isNaN(val)){ for (const b of ENRICH.scoring.valueBands) if (val>=b.min) valueScore=Math.max(valueScore,b.score); }
  const priorityScore = urgencyScore + valueScore;

  return {
    reference, title, contractingAuthority, buyerCountry, buyerCity,
    procedureType, contractType, cpvTop, lotsCount,
    publicationDate, deadlineDate, daysToDeadline,
    estimatedValue, currency, documentsCount,
    noticeUrl, esubmissionLink,
    urgencyScore, valueScore, priorityScore,
    generatedAt: RUN_TS
  };
}

/* =========================
   Concurrence contrôlée
   ========================= */
async function pLimit(n, tasks){
  const out = Array(tasks.length);
  let i = 0;
  const running = new Set();
  async function runNext(){
    if (i >= tasks.length) return;
    const idx = i++;
    const p = tasks[idx]().then(v => { out[idx]=v; running.delete(p); }).catch(e => { out[idx]=null; running.delete(p); });
    running.add(p);
    if (running.size >= n) await Promise.race(running);
    return runNext();
  }
  const starters = Math.min(n, tasks.length);
  await Promise.all(Array.from({length: starters}, runNext));
  await Promise.all(running);
  return out.filter(x=>x!=null);
}

/* =========================
   Sorties fichiers
   ========================= */
function toHtmlList(items, title){
  const lis = items.map(it => `  <li><a href="${it.url}" target="_blank" rel="noopener noreferrer">${it.title}</a></li>`).join('\n');
  return `<!-- generated ${RUN_TS} -->\n<h2>${title}</h2>\n<ul>\n${lis}\n</ul>\n`;
}
function writeListOutputs(prefix, items, humanTitle){
  fs.writeFileSync(`${prefix}-list.html`, toHtmlList(items, humanTitle), 'utf8');
  const header=['title','url'];
  const csvRows=[header.join(',')].concat(items.map(r=>`"${String(r.title).replace(/"/g,'""')}","${String(r.url).replace(/"/g,'""')}"`));
  fs.writeFileSync(`${prefix}-list.csv`, csvRows.join('\n'), 'utf8');
  fs.writeFileSync(`${prefix}-list.json`, JSON.stringify({ generatedAt: RUN_TS, items }, null, 2), 'utf8');
}
function writeCsv(path, rows, header){
  const esc=v=>String(v??'').replace(/"/g,'""');
  const out=[header.join(',')].concat(rows.map(r=>`"${header.map(h=>esc(r[h])).join('","')}"`));
  fs.writeFileSync(path, out.join('\n'), 'utf8');
}

/* =========================
   MAIN
   ========================= */
(async ()=>{
  const browser = await chromium.launch({ headless: HEADLESS });
  const ctx = await browser.newContext();
  const page = await ctx.newPage();

  const index = [];
  const fundingTopics = [];
  const tenderNotices = [];

  // 1) SCRAPE listes + pagination (+ crawl calls si FULL)
  for (const task of TASKS){
    console.log(`\n=== ${task.name.toUpperCase()} (FAST=${IS_FAST}) ===`);
    const all=[]; const seen=new Set();

    for (let p=1; p<=task.maxPages; p++){
      const url = buildPageUrl(task.baseUrl, task.params, p);
      console.log(`→ page ${p}: ${url}`);
      await page.goto(url, { waitUntil:'networkidle' });
      await page.waitForTimeout(IS_FAST?500:700);

      try { await page.waitForSelector(task.linkSelector, { timeout: IS_FAST?12000:30000 }); }
      catch { console.log('  (aucun résultat détecté) — fin'); break; }

      const pageDirect = await collectVirtualized(page, task.linkSelector, url, 50);

      let pageViaCalls = [];
      if (!IS_FAST && task.callSelectors && task.callSelectors.length){
        let callLinks=[];
        for (const sel of task.callSelectors){
          const found = await collectLinks(page, sel, url);
          callLinks.push(...found);
        }
        const seenCalls=new Set();
        callLinks = callLinks.filter(c => (seenCalls.has(c.url)?false:seenCalls.add(c.url)));
        for (const c of callLinks){
          try { const inner = await scrapeTopicsInsideCall(page, c.url); pageViaCalls.push(...inner); }
          catch(e){ console.warn('  call-details ERR', c.url, e.message); }
        }
      }

      const pageItems = [...pageDirect, ...pageViaCalls];
      if (!pageItems.length){ console.log('  page vide — fin'); break; }

      let added=0;
      for (const it of pageItems){
        if (seen.has(it.url)) continue;
        seen.add(it.url);
        all.push(it);
        added++;
      }
      console.log(`  ${pageItems.length} trouvés (via calls: ${pageViaCalls.length}), ${added} nouveaux (total: ${all.length})`);
    }

    const niceTitle = task.name==='funding'
      ? `Funding — Calls for proposals (Open + Forthcoming) — ${all.length} items`
      : `Procurement — Calls for tenders (Open + Forthcoming) — ${all.length} items`;

    writeListOutputs(task.name, all, niceTitle);
    index.push({ name: task.name, count: all.length });

    if (task.name==='funding') fundingTopics.push(...all);
    else tenderNotices.push(...all);
  }

  // marquer tous ceux vus (pour l'incrémental)
  for (const it of [...fundingTopics, ...tenderNotices]) markSeen(it.url);

  // 2) ENRICH — Funding (parallélisé ; en FAST → nouveaux seulement)
  console.log('\n=== ENRICH FUNDING ===');
  const candidatesFunding = IS_FAST
    ? fundingTopics.filter(t => isNew(t.url))
    : fundingTopics;

  const fundingTasks = candidatesFunding.map(t => async ()=>{
    const slug = slugFromTopicUrl(t.url); if (!slug) return null;
    try{
      const json = await fetchTopicDetails(page, slug, ENRICH.lang);
      return mapFundingRecord(json, t.title, t.url);
    }catch(e){
      // fallback minimal
      return {
        identifier:(slug||'').toUpperCase(), title:t.title, programme:'', destination:'', workProgrammePart:'',
        typeOfAction:'', topicStatus:'', url:t.url, callIdentifier:'', callUrl:'',
        plannedOpeningDate:'', nextDeadline:'', allDeadlines:'', daysToDeadline:'',
        totalBudget:'', projectBudgetMin:'', projectBudgetMax:'', fundingRate:'',
        eligibleCountries:'', consortiumSummary:'', trl:'',
        summaryShort:'',
        urgencyScore:'', keywordScore:'', priorityScore:'',
        generatedAt: RUN_TS, rawRef: slug
      };
    }
  });
  const fundingEnrichedNew = await pLimit(FAST_CFG.fundingParallel, fundingTasks);

  // Fusion avec ancien enriched (pour conserver l'historique en FAST)
  let fundingEnriched = fundingEnrichedNew;
  if (IS_FAST && fs.existsSync('funding_enriched.csv')){
    // charger ancien CSV et garder les enregistrements non mis à jour dans ce run
    const prev = fs.readFileSync('funding_enriched.csv','utf8');
    const prevRows = csvParse(prev); // petit parse ci-dessous
    const updated = new Set(fundingEnrichedNew.map(x => x.url));
    fundingEnriched = prevRows.filter(x => !updated.has(x.url)).concat(fundingEnrichedNew);
  }

  // 3) ENRICH — Tenders (FAST → nouveaux seulement)
  console.log('\n=== ENRICH TENDERS ===');
  const candidatesTenders = IS_FAST ? tenderNotices.filter(n => isNew(n.url)) : tenderNotices;
  const tendersTasks = candidatesTenders.map(n => async ()=>{
    try{
      const rec = await enrichTenderFromPage(page, n.url);
      if (!rec.title) rec.title = n.title;
      return rec;
    }catch(e){
      return {
        reference:'', title:n.title, contractingAuthority:'', buyerCountry:'', buyerCity:'',
        procedureType:'', contractType:'', cpvTop:'', lotsCount:'',
        publicationDate:'', deadlineDate:'', daysToDeadline:'',
        estimatedValue:'', currency:'', documentsCount:'',
        noticeUrl:n.url, esubmissionLink:'',
        urgencyScore:'', valueScore:'', priorityScore:'',
        generatedAt: RUN_TS
      };
    }
  });
  const tendersEnrichedNew = await pLimit(FAST_CFG.tendersParallel, tendersTasks);

  let tendersEnriched = tendersEnrichedNew;
  if (IS_FAST && fs.existsSync('tenders_enriched.csv')){
    const prev = fs.readFileSync('tenders_enriched.csv','utf8');
    const prevRows = csvParse(prev);
    const updated = new Set(tendersEnrichedNew.map(x => x.noticeUrl));
    tendersEnriched = prevRows.filter(x => !updated.has(x.noticeUrl)).concat(tendersEnrichedNew);
  }

  // 4) Sorties (listes + enriched + index)
  const idxHtml = [
    `<!-- generated ${RUN_TS} (FAST=${IS_FAST}) -->`,
    '<h1>EU Funding & Tenders — Extractions</h1>',
    '<ul>',
    ...index.map(x => `  <li>${x.name}: ${x.count} items — <a href="${x.name}-list.html">${x.name}-list.html</a> / <a href="${x.name}-list.csv">${x.name}-list.csv</a></li>`),
    '</ul>',
    `<p>Mode: <b>${IS_FAST?'FAST (quotidien)':'FULL (hebdo)'}</b></p>`,
    '<h2>Enriched exports</h2>',
    '<ul>',
    '  <li><a href="funding_enriched.csv">funding_enriched.csv</a> • <a href="funding_enriched.json">funding_enriched.json</a></li>',
    '  <li><a href="tenders_enriched.csv">tenders_enriched.csv</a> • <a href="tenders_enriched.json">tenders_enriched.json</a></li>',
    '</ul>'
  ].join('\n');
  fs.writeFileSync('index.html', idxHtml, 'utf8');

  const fundingHeader = [
    'identifier','title','programme','destination','workProgrammePart','typeOfAction','topicStatus',
    'url','callIdentifier','callUrl',
    'plannedOpeningDate','nextDeadline','allDeadlines','daysToDeadline',
    'totalBudget','projectBudgetMin','projectBudgetMax','fundingRate',
    'eligibleCountries','consortiumSummary','trl',
    'summaryShort',
    'urgencyScore','keywordScore','priorityScore',
    'generatedAt','rawRef'
  ];
  writeCsv('funding_enriched.csv', fundingEnriched, fundingHeader);
  fs.writeFileSync('funding_enriched.json', JSON.stringify({ generatedAt: RUN_TS, items: fundingEnriched }, null, 2),'utf8');

  const tendersHeader = [
    'reference','title','contractingAuthority','buyerCountry','buyerCity',
    'procedureType','contractType','cpvTop','lotsCount',
    'publicationDate','deadlineDate','daysToDeadline',
    'estimatedValue','currency','documentsCount',
    'noticeUrl','esubmissionLink',
    'urgencyScore','valueScore','priorityScore',
    'generatedAt'
  ];
  writeCsv('tenders_enriched.csv', tendersEnriched, tendersHeader);
  fs.writeFileSync('tenders_enriched.json', JSON.stringify({ generatedAt: RUN_TS, items: tendersEnriched }, null, 2),'utf8');

  // sauvegarde état
  saveState(STATE);

  console.log('\n✓ Done (FAST='+IS_FAST+').');
  await browser.close();
})();

/* ===== CSV parse minimal (pour merger en FAST) ===== */
function csvParse(text){
  const rows=[]; let i=0, cur=[], cell='', inQ=false;
  while(i<text.length){
    const c=text[i];
    if (inQ){
      if (c === '"'){ if (text[i+1] === '"'){ cell+='"'; i+=2; continue; } inQ=false; i++; continue; }
      cell+=c; i++; continue;
    } else {
      if (c === '"'){ inQ=true; i++; continue; }
      if (c === ','){ cur.push(cell); cell=''; i++; continue; }
      if (c === '\n' || c === '\r'){ if (c==='\r' && text[i+1]==='\n') i++; cur.push(cell); rows.push(cur); cur=[]; cell=''; i++; continue; }
      cell+=c; i++; continue;
    }
  }
  if (cell.length || cur.length){ cur.push(cell); rows.push(cur); }
  const header = rows.shift(); if (!header) return [];
  return rows.filter(r=>r.length===header.length).map(r => Object.fromEntries(header.map((h,j)=>[h, r[j]])));
}
