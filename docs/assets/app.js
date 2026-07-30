// Detect embed mode from query param ?embed=1
(function() {
  if (new URLSearchParams(window.location.search).get("embed") === "1") {
    document.body.classList.add("embed-mode");
  }
})();

window.addEventListener("error", (e) => {
  try {
    const el = document.getElementById("metaBox");
    if (el) el.innerText = "Errore JS: " + (e && e.message ? e.message : "sconosciuto");
  } catch {}
  console.error(e.error || e);
});

window.addEventListener("unhandledrejection", (e) => {
  const r = e && e.reason ? e.reason : "";
  const msg = r && r.message ? r.message : String(r);
  if (msg && msg.includes("verticalFillMode")) return;
  try {
    const el = document.getElementById("metaBox");
    if (el) el.innerText = "Promise rejection: " + msg;
  } catch {}
  console.error(r);
});

/* ────────────────── fetch helpers ────────────────── */

// Le colonne categoriche dei CSV sono numeri, e il significato di quei numeri
// sta nel codebook del manifest. Manifest e CSV vanno quindi letti sempre in
// coppia: un manifest vecchio in cache con dati nuovi decodifica "arrivo" come
// "partenza", il filtro stazione non trova piu' nulla e la dashboard mostra
// zero corse su qualunque stazione. E' esattamente quello che e' successo
// pubblicando una nuova classe dell'istogramma.
//
// Il manifest si rilegge sempre dalla rete, i dati portano in coda la data di
// build come versione: cosi' la cache lavora ma non puo' mai accoppiare due
// versioni diverse.
let _versioneDati = "";

function conVersione(path) {
  if (!_versioneDati) return path;
  return path + (path.includes("?") ? "&" : "?") + "v=" + encodeURIComponent(_versioneDati);
}

async function fetchText(path) {
  const r = await fetch(conVersione(path), { cache: "default" });
  if (!r.ok) throw new Error("Failed fetch " + path + " (" + r.status + ")");
  return await r.text();
}

async function fetchJson(path) {
  const manifest = /manifest\.json(\?|$)/.test(path);
  const r = await fetch(manifest ? path : conVersione(path),
                        manifest ? { cache: "no-store" } : { cache: "default" });
  if (!r.ok) throw new Error("Failed fetch " + path + " (" + r.status + ")");
  return await r.json();
}

async function fetchTextOrNull(path) {
  try { return await fetchText(path); } catch { return null; }
}

async function fetchJsonOrNull(path) {
  try { return await fetchJson(path); } catch { return null; }
}

function ensureTrailingSlash(p) {
  const s = String(p || "");
  return s.endsWith("/") ? s : s + "/";
}

const DATA_ROOT_CANDIDATES = ["data/", "./data/", "docs/data/", "site/data/"];

function isLfsPointer(t) {
  if (typeof t !== "string") return false;
  const trimmed = t.trimStart();
  if (!trimmed.startsWith("version https://git-lfs.github.com")) return false;
  return !trimmed.split("\n")[0].includes(",");
}

async function fetchTextAny(paths) {
  for (const p of paths) {
    const t = await fetchTextOrNull(p);
    if (t && String(t).trim().length && !isLfsPointer(t)) return t;
  }
  return null;
}

async function fetchJsonAny(paths) {
  for (const p of paths) {
    const j = await fetchJsonOrNull(p);
    if (j && typeof j === "object") return j;
  }
  return null;
}

function uniq(arr) { return Array.from(new Set(arr)); }

function isMobile() { return window.innerWidth <= 600; }

/* ────────────────── debounce helper ────────────────── */

let _renderTimer = null;
function debouncedRenderAll(delay) {
  if (_renderTimer) clearTimeout(_renderTimer);
  const ms = delay !== undefined ? delay : (isMobile() ? 120 : 50);
  _renderTimer = setTimeout(() => { _renderTimer = null; renderAll(); }, ms);
}

/**
 * Debounced full filter pipeline: waits for rapid filter changes to settle,
 * then loads required data and renders once.
 * Prevents multiple concurrent ensureDataForCurrentFilters() calls.
 */
let _pipelineTimer = null;
let _pipelineRunning = false;
let _pipelineQueued = false;
function scheduleFilterPipeline() {
  if (_pipelineTimer) clearTimeout(_pipelineTimer);
  // Cancel any pending render-only debounce too
  if (_renderTimer) { clearTimeout(_renderTimer); _renderTimer = null; }
  const ms = isMobile() ? 300 : 80;
  _pipelineTimer = setTimeout(async () => {
    _pipelineTimer = null;
    if (_pipelineRunning) { _pipelineQueued = true; return; }
    _pipelineRunning = true;
    try {
      await ensureDataForCurrentFilters();
      renderAll();
    } catch (e) {
      console.error("Filter pipeline error:", e);
    } finally {
      _pipelineRunning = false;
      // If a filter change arrived while we were running, process it now
      if (_pipelineQueued) { _pipelineQueued = false; scheduleFilterPipeline(); }
    }
  }, ms);
}

/* ────────────────── filter‑result cache ────────────────── */

const _filterCache = { key: "", kpi: null, series: null, hist: null, stationsRows: null, mapRows: null };

function filterFingerprint() {
  const f = state.filters;
  return JSON.stringify([
    f.year, f.cat, f.dep, f.arr, f.month_from, f.month_to,
    f.day_types, f.time_slots,
    (state.data.kpiMonthCat || []).length,
    (state.data.kpiDetailCat || []).length,
    (state.data.odMonthCat || []).length,
    (state.data.odDetailCat || []).length,
    (state.data.stationsMonthNode || []).length,
    (state.data.stationsDetailNode || []).length
  ]);
}

function invalidateFilterCache() { _filterCache.key = ""; }

function getCachedOrFilter(slot, filterFn) {
  const fp = filterFingerprint();
  if (_filterCache.key !== fp) {
    _filterCache.key = fp;
    _filterCache.kpi = null;
    _filterCache.series = null;
    _filterCache.hist = null;
    _filterCache.stationsRows = null;
    _filterCache.mapRows = null;
  }
  if (!_filterCache[slot]) _filterCache[slot] = filterFn();
  return _filterCache[slot];
}

function mobileChartMargins(desktop) {
  if (!isMobile()) return desktop;
  return { l: Math.min(desktop.l || 50, 35), r: Math.min(desktop.r || 20, 10), t: Math.min(desktop.t || 10, 5), b: Math.min(desktop.b || 50, 40) };
}

/** On mobile, purge old Plotly chart before re-rendering to free memory. */
function safePlotlyReact(el, data, layout, config) {
  try {
    if (isMobile() && el && el.data) Plotly.purge(el);
    Plotly.react(el, data, layout, config);
  } catch (e) {
    console.error("Plotly render error for #" + (el && el.id), e);
  }
}

/** On mobile, use lines-only (no markers = fewer SVG elements). */
function mobileTraceMode() {
  return isMobile() ? "lines" : "lines+markers";
}

function mobileFont() {
  return isMobile() ? { color: "#334155", size: 9 } : { color: "#334155" };
}

function candidateFilePaths(root, rel) {
  const r = ensureTrailingSlash(root);
  const clean = String(rel || "").replace(/^\/+/, "");
  const out = [r + clean];
  if (!clean.startsWith("gold/")) out.push(r + "gold/" + clean);
  return uniq(out);
}

async function pickDataBase() {
  const probes = [
    "manifest.json",
    "gold/manifest.json",
    "kpi_mese_categoria.csv",
    "gold/kpi_mese_categoria.csv",
    "kpi_dettaglio_categoria.csv",
    "gold/kpi_dettaglio_categoria.csv",
    "kpi_mese.csv",
    "gold/kpi_mese.csv"
  ];

  for (const base0 of DATA_ROOT_CANDIDATES) {
    const base = ensureTrailingSlash(base0);
    for (const p of probes) {
      const t = await fetchTextOrNull(base + p);
      if (t && String(t).trim().length > 20 && !isLfsPointer(t)) return base;
    }
  }
  return "data/";
}

/* ────────────────── CSV parser ────────────────── */

function detectDelimiter(line) {
  const s = String(line || "");
  let comma = 0, semi = 0, tab = 0;
  let inQ = false;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '"') { if (inQ && s[i + 1] === '"') i++; else inQ = !inQ; continue; }
    if (!inQ) { if (ch === ",") comma++; else if (ch === ";") semi++; else if (ch === "\t") tab++; }
  }
  if (semi > comma && semi >= tab) return ";";
  if (tab > comma && tab > semi) return "\t";
  return ",";
}

function splitCSVLine(line, delim) {
  const d = delim || ",";
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') { if (inQ && line[i + 1] === '"') { cur += '"'; i++; } else { inQ = !inQ; } continue; }
    if (ch === d && !inQ) { out.push(cur); cur = ""; continue; }
    cur += ch;
  }
  out.push(cur);
  return out;
}

function parseCSV(text) {
  const t = String(text || "").trim();
  if (!t) return [];
  const lines = t.split(/\r?\n/).filter((x) => String(x || "").length);
  if (lines.length <= 1) return [];
  const delim = detectDelimiter(lines[0]);
  const header = splitCSVLine(lines[0], delim).map((x) => String(x || "").trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const line = String(lines[i] || "");
    if (!line.trim()) continue;
    const cols = splitCSVLine(line, delim);
    const obj = {};
    for (let j = 0; j < header.length; j++) obj[header[j]] = cols[j] ?? "";
    rows.push(obj);
  }
  return rows;
}

/**
 * Yield back to the event loop without the nested-setTimeout clamp.
 *
 * Browsers force setTimeout(fn, 0) to a 4 ms minimum once the callbacks nest
 * more than five deep, which is exactly what a chunked parser does. On the
 * largest table that was ~300 yields of pure idle waiting. A MessageChannel
 * message is delivered on the next macrotask with no such floor.
 */
var _yieldChannel = (typeof MessageChannel === "function") ? new MessageChannel() : null;
var _yieldQueue = [];
if (_yieldChannel) {
  _yieldChannel.port1.onmessage = function() {
    var fn = _yieldQueue.shift();
    if (fn) fn();
  };
}
function yieldToBrowser(fn) {
  if (!_yieldChannel) { setTimeout(fn, 0); return; }
  _yieldQueue.push(fn);
  _yieldChannel.port2.postMessage(0);
}

/**
 * Async CSV parser that yields on a time budget rather than a fixed row count,
 * so the main thread stays responsive without over-yielding.
 *
 * A fixed chunk size cannot be right for every table: 5000 rows of the KPI
 * table parse in well under a frame, while 5000 rows of the wide station
 * histogram take much longer. Measuring elapsed time instead keeps each slice
 * near one frame whatever the row width.
 */
/**
 * Build the row decoder for the columns the build publishes as integer codes
 * (tipo_giorno, fascia_oraria, ruolo, bucket_ritardo_arrivo). Returns null
 * when the deployed data is not encoded, so older deployments still parse.
 *
 * Decoding also makes every row share the same string instance from the
 * codebook instead of holding its own copy of "infrasettimanale".
 */
function buildRowDecoder() {
  var cb = state.manifest && state.manifest.codebook;
  if (!cb) return null;
  var cols = Object.keys(cb).filter(function(c) { return Array.isArray(cb[c]) && cb[c].length; });
  if (!cols.length) return null;

  return function(obj) {
    for (var i = 0; i < cols.length; i++) {
      var col = cols[i];
      var raw = obj[col];
      if (raw === undefined || raw === "") continue;
      var decoded = cb[col][+raw];
      if (decoded !== undefined) obj[col] = decoded;
    }
    return obj;
  };
}

function parseCSVAsync(text, chunkSize, filterFn) {
  return new Promise(function(resolve) {
    var t = String(text || "").trim();
    if (!t) { resolve([]); return; }
    var lines = t.split(/\r?\n/);
    t = null;  // Free the original text – we only need the lines array now
    // Remove empty lines in-place to avoid creating another full array
    var write = 0;
    for (var r = 0; r < lines.length; r++) {
      if (lines[r].length) lines[write++] = lines[r];
    }
    lines.length = write;
    if (lines.length <= 1) { resolve([]); return; }
    var delim = detectDelimiter(lines[0]);
    var header = splitCSVLine(lines[0], delim).map(function(x) { return String(x || "").trim(); });
    var nCols = header.length;
    var rows = [];
    // Rows checked between clock reads: reading the clock per row would cost
    // more than the work it is timing.
    var BATCH = Math.max(256, Math.min(chunkSize || 2048, 8192));
    var SLICE_MS = 12;
    var idx = 1;
    var decode = buildRowDecoder();

    function chunk() {
      var started = (typeof performance === "object" && performance.now) ? performance.now() : Date.now();
      while (idx < lines.length) {
        var end = Math.min(idx + BATCH, lines.length);
        for (; idx < end; idx++) {
          var line = lines[idx];
          lines[idx] = null;  // Release processed line for GC
          if (!line || !line.trim()) continue;
          var cols = splitCSVLine(line, delim);
          var obj = {};
          for (var j = 0; j < nCols; j++) obj[header[j]] = cols[j] ?? "";
          if (decode) decode(obj);
          if (!filterFn || filterFn(obj)) rows.push(obj);
        }
        var now = (typeof performance === "object" && performance.now) ? performance.now() : Date.now();
        if (now - started >= SLICE_MS) break;
      }

      if (idx < lines.length) {
        yieldToBrowser(chunk);
      } else {
        lines = null;  // Release the lines array
        resolve(rows);
      }
    }
    chunk();
  });
}

/* ────────────────── format helpers ────────────────── */

function toNum(x) { const v = Number(x); return Number.isFinite(v) ? v : 0; }

function parseNumberAny(x) {
  if (x === null || typeof x === "undefined") return NaN;
  if (typeof x === "number") return x;
  let s = String(x).trim();
  if (!s) return NaN;
  s = s.replace(/\s+/g, "");
  if (s.includes(",") && !s.includes(".")) s = s.replace(",", ".");
  const v = Number(s);
  return Number.isFinite(v) ? v : NaN;
}

function fmtInt(x) { return Math.round(Number(x) || 0).toLocaleString("it-IT"); }

function fmtFloat(x) {
  const v = Number(x);
  if (!Number.isFinite(v)) return "";
  return v.toLocaleString("it-IT", { maximumFractionDigits: 2 });
}

function normalizeText(s) {
  const raw = String(s || "").toLowerCase().trim();
  const base = typeof raw.normalize === "function" ? raw.normalize("NFD") : raw;
  return base.replace(/[\u0300-\u036f]/g, "");
}

/** Normalize station name for grouping codes that refer to the same station.
 *  Handles Trenord/FerrovieNord vs RFI naming differences. */
/**
 * Deve restare allineata a normalize_station_name() in scripts/utils.py: se le
 * due divergono, il browser raggruppa nelle tendine stazioni che la pipeline ha
 * tenuto separate (o viceversa).
 *
 * La sorgente alterna forma lunga e abbreviata per la stessa stazione
 * ("BOLOGNA C.LE" / "BOLOGNA CENTRALE", "VENEZIA S.LUCIA" / "VENEZIA SANTA
 * LUCIA"). Da "S." non si puo' dedurre se valga SAN o SANTA, quindi tutte le
 * forme collassano su un unico token.
 */
var _STATION_ABBREV = [
  // Deve restare allineato a _ABBREVIATIONS in scripts/utils.py.
  // Sigle di citta' e di porta: la sorgente le usa solo su alcune stazioni, che
  // percio' risultavano contate due volte ("MI P GENOVA" e "MILANO PORTA
  // GENOVA" erano due voci per la stessa stazione).
  [/^MI\b/, "MILANO"],
  [/^BS\b/, "BRESCIA"],
  [/\bM\.?\s?SE\b/, "MILANESE"],
  [/\bP\s+(GENOVA|GARIBALDI|GAR|VITTORIA|ROMANA|SUSA|VOLTA)\b/, "PORTA $1"],
  [/\bGAR\b/, "GARIBALDI"],
  [/\bPIAZ\b/, "PIAZZALE"],
  [/\bSOTT\b/, "SOTTERRANEA"],
  [/\bC\.?\s?L\.?E\.?\b/g, "CENTRALE"],
  [/\bCENT\.?\b/g, "CENTRALE"],
  [/\bS\.?M\.?N\.?\b/g, "S MARIA NOVELLA"],
  [/\bP\.?\s?TA\b/g, "PORTA"],
  [/\bP\.?\s?Z(?:Z)?A\b/g, "PIAZZA"],
  [/\bP\.?\s?NUOVA\b/g, "PORTA NUOVA"],
  [/\bAER\.?\b/g, "AEROPORTO"],
  [/\bM\.?\s?MO\b/g, "MARITTIMO"],
  [/\bMAR\.?\s?MO\b/g, "MARITTIMO"],
  [/\bSCR\.?\b/g, "SCRIVIA"],
  [/\bMOV\.?\b/g, "MOVIMENTO"],
  // "Reggio di Calabria Centrale" e "Reggio Calabria Centrale" sono la stessa
  // stazione: erano due voci per 218.826 corse.
  [/\bDI\b/g, " "],
  [/\bSS\.?\b/g, "S"],
  [/\bSANTI\b/g, "S"],
  [/\bSANTA\b/g, "S"],
  [/\bSANTO\b/g, "S"],
  [/\bSANT'/g, "S "],
  [/\bSAN\b/g, "S"],
  [/\bS\./g, "S "],
  [/\bF\.?S\.?\b/g, ""]
];

function normalizeStationName(s) {
  var t = String(s || "").toUpperCase().trim();
  // Deve restare identico a normalize_station_name in scripts/utils.py:
  // l'accento finale arriva come apice inverso, barra rovesciata o niente, e
  // trattarli in modo diverso spezza la stessa stazione in piu' voci.
  t = t.replace(/[`\\]/g, "'");
  t = t.replace(/-/g, " ").replace(/'/g, "' ").replace(/\s+/g, " ");
  // Expand "M N" abbreviation (Trenord: Milano Nord)
  t = t.replace(/^M N\b/, "MILANO NORD");
  // Strip " FNM" suffix (FerrovieNord Milano)
  t = t.replace(/\s+FNM$/, "");
  // Strip " POLITECNICO" suffix
  t = t.replace(/\s+POLITECNICO$/, "");
  // Strip "NORD" after known city prefixes (Trenord convention)
  t = t.replace(/\b(MILANO|COMO|VARESE)\s+NORD\b/, "$1");
  for (var i = 0; i < _STATION_ABBREV.length; i++) {
    t = t.replace(_STATION_ABBREV[i][0], _STATION_ABBREV[i][1]);
  }
  t = t.replace(/'/g, "").replace(/\./g, " ");
  return t.replace(/\s+/g, " ").trim();
}

function yearFromMonth(mese) { return String(mese || "").slice(0, 4); }

function firstEl(ids) {
  for (const id of ids) { const el = document.getElementById(id); if (el) return el; }
  return null;
}

function setTextByIds(ids, value) { const el = firstEl(ids); if (el) el.innerText = value; }

function setMeta(text) { const el = document.getElementById("metaBox"); if (el) el.innerText = text; }

/**
 * Scrive nella scheda dell'istogramma la quota di misure scartate.
 *
 * Il numero stava scritto a mano nell'HTML ("il 3,5% delle corse effettuate")
 * e col crescere dei dati non tornava piu'. Ora lo calcola la build a ogni
 * pubblicazione e qui si legge dal manifest, cosi' non puo' invecchiare di
 * nuovo.
 */
function scriviQuotaScartate() {
  const el = document.getElementById("pctMisureScartate");
  if (!el) return;
  const q = state.manifest && state.manifest.pct_misure_scartate;
  if (typeof q !== "number" || !isFinite(q)) {
    // Senza il dato si scrive una formula priva di cifre invece di lasciare un
    // trattino: una percentuale mancante e' meno grave di una sbagliata.
    el.textContent = "una quota minima";
    return;
  }
  el.textContent = q.toLocaleString("it-IT", { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + "%";
}

/* ────────────────── manifest defaults ────────────────── */

function safeManifestDefaults() {
  return {
    built_at_utc: "",
    gold_files: [
      "kpi_mese.csv",
      "kpi_mese_categoria.csv",
      "kpi_dettaglio.csv",
      "kpi_dettaglio_categoria.csv",
      "hist_mese_categoria.csv",
      "hist_dettaglio_categoria.csv",
      "stazioni_mese_categoria_nodo.csv",
      "stazioni_dettaglio_categoria_nodo.csv",
      "od_mese_categoria.csv",
      "od_dettaglio_categoria.csv",
      "hist_stazioni_mese_categoria_ruolo.csv",
      "hist_stazioni_dettaglio_categoria_ruolo.csv"
    ],
    // Ripiego usato solo se il manifest non arriva. Tenuto allineato ai bucket
    // di config/pipeline.yml, con in coda la classe delle corse non effettuate.
    delay_bucket_labels: [
      "-5","(-5,-1]","(-1,0]","(0,1]","(1,5]","(5,10]","(10,15]","(15,30]",
      "(30,60]","(60,120]","> 120","parzialmente cancellate","non effettuate"
    ]
  };
}

/* ────────────────── global state ────────────────── */

const DAY_TYPES   = ["infrasettimanale", "weekend"];
const TIME_SLOTS  = ["mattina", "tarda_mattina", "pomeriggio", "sera", "notte"];

const state = {
  dataBase: "data/",
  manifest: safeManifestDefaults(),
  data: {
    // Le coppie di fermate della stazione di partenza selezionata, quando
    // esistono: vedi ensureTratte.
    tratta: [],
    kpiMonth: [],
    kpiMonthCat: [],
    kpiDetail: [],
    kpiDetailCat: [],
    histMonthCat: [],
    histDetailCat: [],
    stationsMonthNode: [],
    stationsDetailNode: [],
    odMonthCat: [],
    odDetailCat: [],
    histStationsMonthRuolo: [],
    histStationsDetailRuolo: []
  },
  stationsRef: new Map(),
  capoluoghiSet: new Set(),
  _depItems: [],
  _arrItems: [],
  _depAliases: null,  // Set of all codes for selected dep station
  _arrAliases: null,  // Set of all codes for selected arr station
  map: null,
  markers: [],
  // La mappa della lentezza e' separata da quella delle stazioni: fondo
  // diverso, dati diversi, e non risponde ai filtri.
  mapRete: null,
  reteLayer: null,
  filters: {
    year: "all",
    cat: "all",
    dep: "all",
    arr: "all",
    month_from: "",
    month_to: "",
    day_types:  [true, true],                    // infrasettimanale, weekend
    time_slots: [true, true, true, true, true]   // mattina, tarda_mattina, pomeriggio, sera, notte
  }
};

/* ────────────────── station helpers ────────────────── */

function stationName(code, fallback) {
  const c = String(code || "").trim();
  const ref = state.stationsRef.get(c);
  const n = ref && ref.name ? String(ref.name).trim() : "";
  if (n) return n;
  const fb = String(fallback || "").trim();
  return fb || c;
}

function stationCity(code, fallbackStationName) {
  const c = String(code || "").trim();
  const ref = state.stationsRef.get(c);
  const city = ref && ref.city ? String(ref.city).trim() : "";
  if (city) return city;
  return stationName(c, fallbackStationName);
}

function stationCoords(code) {
  const c = String(code || "").trim();
  const ref = state.stationsRef.get(c);
  if (!ref) return null;
  const lat = ref.lat, lon = ref.lon;
  if (typeof lat !== "number" || typeof lon !== "number") return null;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return { lat, lon };
}

/**
 * Deduplicate stations: group codes sharing the same name,
 * keep one representative code per name (prefer codes with coordinates).
 */
function buildStationItems(codes) {
  const byName = new Map();
  for (const code of (codes || [])) {
    const name = stationName(code, code);
    const key = normalizeText(normalizeStationName(name));
    if (!byName.has(key)) {
      byName.set(key, { code, name, codes: [code] });
    } else {
      const entry = byName.get(key);
      entry.codes.push(code);
      // prefer a code that has coordinates, and prefer S-codes (official RFI)
      const curCoords = stationCoords(entry.code);
      const newCoords = stationCoords(code);
      const curIsS = String(entry.code).startsWith("S");
      const newIsS = String(code).startsWith("S");
      if ((!curCoords && newCoords) || (!curIsS && newIsS)) {
        entry.code = code;
        entry.name = name;
      }
    }
  }
  const items = Array.from(byName.values()).map((e) => ({
    code: e.code, name: e.name, codes: e.codes,
    needle: normalizeText(e.name + " " + e.codes.join(" "))
  }));
  items.sort((a, b) => a.name.localeCompare(b.name, "it", { sensitivity: "base" }));
  return items;
}

/**
 * Register code→name pairs stations_dim.csv does not know about, letting each
 * new code inherit coordinates from an existing entry with the same normalized
 * name. Station codes change over time (N_ACF3D2764DA3 → S01700 for Milano
 * Centrale) and the newer code is often missing from stations_dim.csv.
 */
function mergeStationNames(codeNamePairs) {
  if (!codeNamePairs || !codeNamePairs.length) return 0;

  const nameToCoords = new Map();
  for (const [, ref] of state.stationsRef) {
    if (!ref.name) continue;
    const key = normalizeText(normalizeStationName(ref.name));
    if (!key) continue;
    if (Number.isFinite(ref.lat) && Number.isFinite(ref.lon) && !nameToCoords.has(key)) {
      nameToCoords.set(key, { lat: ref.lat, lon: ref.lon, city: ref.city || "" });
    }
  }

  let added = 0;
  for (const [code, name] of codeNamePairs) {
    if (!code || !name || state.stationsRef.has(code)) continue;
    const coords = nameToCoords.get(normalizeText(normalizeStationName(name)));
    state.stationsRef.set(code, {
      code,
      name,
      lat: coords ? coords.lat : NaN,
      lon: coords ? coords.lon : NaN,
      city: coords ? coords.city : ""
    });
    added++;
  }
  return added;
}

/**
 * Fallback for data deployments that still carry per-row station names.
 *
 * The fact tables no longer ship nome_partenza / nome_arrivo / nome_stazione:
 * those columns repeated the same few thousand strings across millions of rows
 * and accounted for most of the bytes the browser had to download and parse.
 * Names now arrive once, via station_names.csv. This stays so the dashboard
 * keeps working against an older deployment of the data.
 */
function enrichStationsRefFromFacts() {
  const pairs = [];
  const push = (code, name) => {
    const c = String(code || "").trim();
    const n = String(name || "").trim();
    if (c && n) pairs.push([c, n]);
  };

  for (const key of ["stationsMonthNode", "stationsDetailNode", "histStationsMonthRuolo"]) {
    for (const r of (state.data[key] || [])) push(r.cod_stazione, r.nome_stazione);
  }
  for (const key of ["odMonthCat", "odDetailCat"]) {
    for (const r of (state.data[key] || [])) {
      push(r.cod_partenza, r.nome_partenza);
      push(r.cod_arrivo, r.nome_arrivo);
    }
  }

  const added = mergeStationNames(pairs);
  if (added > 0) console.log("enrichStationsRefFromFacts: added " + added + " codes from fact tables");
}

/* Map: station name (normalized) -> array of all codes sharing that name */
function buildNameToCodesMap() {
  const map = new Map();
  for (const [code, ref] of state.stationsRef) {
    const key = normalizeText(normalizeStationName(ref.name || code));
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(code);
  }
  return map;
}

function fillStationSelect(selectEl, items, query) {
  if (!selectEl) return;
  const q = normalizeText(query);
  const cur = selectEl.value;
  selectEl.innerHTML = "";
  selectEl.appendChild(new Option("Tutte", "all"));
  for (const it of items) {
    if (q && !it.needle.includes(q)) continue;
    selectEl.appendChild(new Option(it.name, it.code));
  }
  const stillThere = Array.from(selectEl.options).some((o) => o.value === cur);
  selectEl.value = stillThere ? cur : "all";
}

function ensureSearchInput(selectEl, inputId, placeholder, items) {
  if (!selectEl || !selectEl.parentNode) return;
  let input = document.getElementById(inputId);
  if (!input) {
    input = document.createElement("input");
    input.id = inputId;
    input.type = "search";
    input.autocomplete = "off";
    input.placeholder = placeholder;
    input.style.width = "100%";
    input.style.margin = "0 0 6px 0";
    selectEl.parentNode.insertBefore(input, selectEl);
  }
  input.oninput = () => fillStationSelect(selectEl, items, input.value);
}

/* ────────────────── new filter logic ────────────────── */

function hasDetailFilter() {
  return groupIsRestrictive(state.filters.day_types) || groupIsRestrictive(state.filters.time_slots);
}

function hasStationFilter() {
  return state.filters.dep !== "all" || state.filters.arr !== "all";
}

function hasMonthRange() {
  return !!(state.filters.month_from || state.filters.month_to);
}

function passCat(r) {
  if (state.filters.cat === "all") return true;
  return String(r.categoria || "").trim() === state.filters.cat;
}

function passDep(r) {
  if (state.filters.dep === "all") return true;
  const code = String(r.cod_partenza || "").trim();
  if (code === state.filters.dep) return true;
  const aliases = state._depAliases;
  return aliases ? aliases.has(code) : false;
}

function passArr(r) {
  if (state.filters.arr === "all") return true;
  const code = String(r.cod_arrivo || "").trim();
  if (code === state.filters.arr) return true;
  const aliases = state._arrAliases;
  return aliases ? aliases.has(code) : false;
}

function passYear(r, field) {
  if (state.filters.year === "all") return true;
  return String(r[field] || "").slice(0, 4) === state.filters.year;
}

function passMonthRange(r, field) {
  if (!hasMonthRange()) return true;
  // Extract month number (MM) from YYYY-MM field
  const raw = String(r[field] || "").slice(5, 7);
  if (!raw) return false;
  const mm = parseInt(raw, 10);
  if (!mm) return false;

  const from = parseInt(state.filters.month_from || "0", 10);
  const to   = parseInt(state.filters.month_to || "0", 10);
  const a = from || to;
  const b = to || from;
  // L'intervallo si legge da "a" a "b" nell'ordine dei mesi, e quando "b"
  // precede "a" scavalca il capodanno. Prima si prendevano il minimo e il
  // massimo: chiedendo ottobre-febbraio si otteneva febbraio-ottobre, cioe'
  // esattamente i nove mesi complementari ai cinque voluti, con l'etichetta
  // del filtro che continuava a dire "10 - 02".
  if (a <= b) return mm >= a && mm <= b;
  return mm >= a || mm <= b;
}

/**
 * A group with nothing selected excludes every row, which renders the whole
 * dashboard as zeros with no explanation. togglePillGroup() now prevents
 * reaching that state through the UI; this is the backstop for any other route
 * into it (restored state, a future control), where "nothing included" is read
 * as "no filter on this dimension".
 */
function groupIsRestrictive(flags) {
  return flags.some((x) => !x) && flags.some((x) => x);
}

function passDetailDimensions(r) {
  // tipo_giorno
  const dt = state.filters.day_types;
  if (groupIsRestrictive(dt)) {
    const tg = String(r.tipo_giorno || "").trim();
    const idx = DAY_TYPES.indexOf(tg);
    if (idx === -1 || !dt[idx]) return false;
  }
  // fascia_oraria
  const ts = state.filters.time_slots;
  if (groupIsRestrictive(ts)) {
    const fa = String(r.fascia_oraria || "").trim();
    const idx = TIME_SLOTS.indexOf(fa);
    if (idx === -1 || !ts[idx]) return false;
  }
  return true;
}

/* ────────────────── toggle controls init ────────────────── */

/**
 * Handle a click on a pill of an "included values" group.
 *
 * Two traps in the previous behaviour, both of which sent the whole dashboard
 * to zero without saying why:
 *
 *  1. Every pill starts active, because active means "included" and the default
 *     is "include everything". Clicking "Sera" to *choose* the evening
 *     therefore excluded it, and the figures went up instead of down. The first
 *     click on a fully-selected group now isolates the value clicked, which is
 *     what the gesture is asking for.
 *
 *  2. Nothing stopped the user from switching every pill off. An empty group
 *     matches no row, so every KPI, chart and map marker read zero \u2014 the state
 *     the "Milano -> Verona, REG, infrasettimanale, sera" report landed in.
 *     The last active pill of a group can no longer be switched off.
 */
function togglePillGroup(flags, index) {
  const active = flags.filter(Boolean).length;

  if (active === flags.length) {
    // Gruppo intero attivo: il click vale "solo questo".
    for (let i = 0; i < flags.length; i++) flags[i] = (i === index);
    return true;
  }

  if (flags[index] && active === 1) {
    // Ultimo valore incluso: spegnerlo non filtrerebbe, azzererebbe.
    return false;
  }

  flags[index] = !flags[index];
  return true;
}

function buildToggleGroup(wrap, labels, titles, flags) {
  labels.forEach((label, i) => {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "toggle-pill" + (flags[i] ? "" : " off");
    b.innerText = label;
    b.title = titles[i];
    b.onclick = () => {
      if (!togglePillGroup(flags, i)) {
        // Feedback minimo: senza questo il click sembra semplicemente ignorato.
        b.classList.add("pill-locked");
        setTimeout(() => b.classList.remove("pill-locked"), 400);
        return;
      }
      syncToggleUI();
      scheduleFilterPipeline();
    };
    wrap.appendChild(b);
  });
}

function initToggleControls() {
  const dayTypeWrap = document.getElementById("dayTypeWrap");
  const timeSlotWrap = document.getElementById("timeSlotWrap");
  if (!dayTypeWrap || !timeSlotWrap) return;

  // Already built?
  if (dayTypeWrap.children.length) return;

  buildToggleGroup(
    dayTypeWrap,
    ["Infrasettimanale", "Fine settimana"],
    ["Luned\u00ec\u2013Venerd\u00ec", "Sabato\u2013Domenica"],
    state.filters.day_types
  );

  buildToggleGroup(
    timeSlotWrap,
    ["Mattina", "Tarda mattina", "Pomeriggio", "Sera", "Notte"],
    ["6:00\u201308:59", "9:00\u201313:59", "14:00\u201317:59", "18:00\u201321:59", "22:00\u201305:59"],
    state.filters.time_slots
  );
}

function syncToggleUI() {
  const dayTypeWrap = document.getElementById("dayTypeWrap");
  const timeSlotWrap = document.getElementById("timeSlotWrap");
  if (dayTypeWrap) {
    Array.from(dayTypeWrap.children).forEach((b, i) => {
      b.classList.toggle("off", !state.filters.day_types[i]);
    });
  }
  if (timeSlotWrap) {
    Array.from(timeSlotWrap.children).forEach((b, i) => {
      b.classList.toggle("off", !state.filters.time_slots[i]);
    });
  }
}

/* ────────────────── map init ────────────────── */

function initMap() {
  const mapEl = firstEl(["map", "mapStations", "stationsMap"]);
  if (!mapEl) return;
  if (typeof L !== "object" || typeof L.map !== "function") return;
  if (state.map) return;

  state.map = L.map(mapEl.id, { center: [42.5, 12.5], zoom: 6, zoomSnap: 0.5 });
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors", maxZoom: 18
  }).addTo(state.map);

  setTimeout(() => { try { state.map.invalidateSize(); } catch {} }, 150);
}

function clearMarkers() {
  if (!state.map) return;
  for (const m of state.markers) { try { state.map.removeLayer(m); } catch {} }
  state.markers = [];
}

/**
 * La mappa e' aperta all'arrivo sulla pagina, ma non viene costruita durante il
 * caricamento: le tessere OpenStreetMap e il file per stazione (2,6 MB) sono la
 * parte piu' costosa della pagina, e messi nel percorso critico rimandavano il
 * primo disegno di tutto il resto. Qui partono appena il browser ha finito,
 * cosi' la mappa c'e' senza che il caricamento ne paghi il prezzo.
 *
 * Prima la scheda era chiusa per lo stesso motivo, ma una mappa chiusa in una
 * dashboard geografica sembra una mappa che non c'e'.
 */
function avviaMappaDifferita() {
  const mapEl = document.getElementById("map");
  const kmEl = document.getElementById("chartKm");
  const vuoleMappa = mapEl && !isCardCollapsed(mapEl);
  const vuoleKm = kmEl && !isCardCollapsed(kmEl);
  if (!vuoleMappa && !vuoleKm) return;

  const parti = function() {
    if (vuoleMappa) {
      initMap();
      ensureStationsData().then(function() {
        renderMap();
        // Leaflet calcola le dimensioni al momento della creazione: se il
        // contenitore era ancora in fase di disegno le tessere restano tagliate.
        setTimeout(function() { try { state.map.invalidateSize(); } catch {} }, 200);
      });
    }
    // La classifica per chilometro e' un file di un centinaio di KB: sta nella
    // stessa finestra di attesa della mappa senza pesare.
    if (vuoleKm) ensureKmData().then(renderKmRanking);
    avviaMappaRete();
  };

  if (typeof requestIdleCallback === "function") requestIdleCallback(parti, { timeout: 1500 });
  else setTimeout(parti, 250);
}

/* ────────────────── filters init ────────────────── */

/* ────────────────── category label map ────────────────── */

const CATEGORY_LABELS = {
  "DIR":  "DIR \u2013 Diretto",
  "EC":   "EC \u2013 EuroCity",
  "ECFR": "ECFR \u2013 EuroCity FrecciaRossa",
  "EN":   "EN \u2013 EuroNight",
  "EXP":  "EXP \u2013 Espresso",
  "FA":   "FA \u2013 Freccia Argento",
  "FB":   "FB \u2013 Freccia Bianca",
  "FR":   "FR \u2013 Freccia Rossa",
  "IC":   "IC \u2013 InterCity",
  "ICN":  "ICN \u2013 InterCity Notte",
  "IR":   "IR \u2013 InterRegionale",
  "MET":  "MET \u2013 Metropolitano",
  "NCL":  "NCL \u2013 Notte cuccette",
  "REG":  "REG \u2013 Regionale"
};

function categoryDisplayName(cat) {
  const c = String(cat || "").trim();
  return CATEGORY_LABELS[c] || c;
}

/* ────────────────── month names ────────────────── */

const MONTH_NAMES = [
  "Gennaio","Febbraio","Marzo","Aprile","Maggio","Giugno",
  "Luglio","Agosto","Settembre","Ottobre","Novembre","Dicembre"
];

/** Convert "YYYY-MM" to "MM/AA" for compact x-axis labels */
function fmtMonthShort(ym) {
  const parts = String(ym || "").split("-");
  if (parts.length < 2) return ym;
  return parts[1] + "/" + parts[0].slice(2);
}

function updateDepAliases() {
  if (state.filters.dep === "all") { state._depAliases = null; return; }
  const item = state._depItems.find((it) => it.code === state.filters.dep);
  state._depAliases = item ? new Set(item.codes) : new Set([state.filters.dep]);
}

function updateArrAliases() {
  if (state.filters.arr === "all") { state._arrAliases = null; return; }
  const item = state._arrItems.find((it) => it.code === state.filters.arr);
  state._arrAliases = item ? new Set(item.codes) : new Set([state.filters.arr]);
}

/**
 * Build OD pair index: maps each dep item code → Set of reachable arr item codes
 * and each arr item code → Set of dep item codes that reach it.
 * Uses the canonical (grouped) codes from buildStationItems.
 */
function buildOdPairIndex() {
  const odRows = state.data.odMonthCat || [];
  if (!odRows.length) { state._odDepToArr = null; state._odArrToDep = null; return; }

  // Map raw code → canonical item code (the representative from buildStationItems)
  const rawToDepItem = new Map();
  for (const it of state._depItems) {
    for (const c of it.codes) rawToDepItem.set(c, it.code);
  }
  const rawToArrItem = new Map();
  for (const it of state._arrItems) {
    for (const c of it.codes) rawToArrItem.set(c, it.code);
  }

  const depToArr = new Map();
  const arrToDep = new Map();
  for (const r of odRows) {
    const dc = rawToDepItem.get(String(r.cod_partenza || "").trim());
    const ac = rawToArrItem.get(String(r.cod_arrivo || "").trim());
    if (!dc || !ac) continue;
    if (!depToArr.has(dc)) depToArr.set(dc, new Set());
    depToArr.get(dc).add(ac);
    if (!arrToDep.has(ac)) arrToDep.set(ac, new Set());
    arrToDep.get(ac).add(dc);
  }
  state._odDepToArr = depToArr;
  state._odArrToDep = arrToDep;
}

/** Refresh the arrival dropdown to show only stations reachable from current dep. */
function refreshArrDropdown() {
  const arrSel = firstEl(["arrSel", "stazioneArrivoSel", "arrStationSel"]);
  if (!arrSel) return;
  const searchInput = document.getElementById("arrSearch");
  const query = searchInput ? searchInput.value : "";

  let items = state._arrItems;
  if (state.filters.dep !== "all" && state._odDepToArr) {
    const reachable = state._odDepToArr.get(state.filters.dep);
    if (reachable) {
      items = items.filter((it) => reachable.has(it.code));
    } else {
      items = [];
    }
  }
  fillStationSelect(arrSel, items, query);
  if (searchInput) {
    searchInput.oninput = () => fillStationSelect(arrSel, items, searchInput.value);
  }
  // If current selection was filtered out, reset it
  if (arrSel.value !== state.filters.arr) {
    state.filters.arr = arrSel.value || "all";
    updateArrAliases();
  }
}

/** Refresh the departure dropdown to show only stations that reach current arr. */
function refreshDepDropdown() {
  const depSel = firstEl(["depSel", "stazionePartenzaSel", "depStationSel"]);
  if (!depSel) return;
  const searchInput = document.getElementById("depSearch");
  const query = searchInput ? searchInput.value : "";

  let items = state._depItems;
  if (state.filters.arr !== "all" && state._odArrToDep) {
    const sources = state._odArrToDep.get(state.filters.arr);
    if (sources) {
      items = items.filter((it) => sources.has(it.code));
    } else {
      items = [];
    }
  }
  fillStationSelect(depSel, items, query);
  if (searchInput) {
    searchInput.oninput = () => fillStationSelect(depSel, items, searchInput.value);
  }
  if (depSel.value !== state.filters.dep) {
    state.filters.dep = depSel.value || "all";
    updateDepAliases();
  }
}

/** Rebuild station dropdown items and OD index after OD data becomes available. */
function rebuildStationDropdowns() {
  const deps = uniq([
    ...(state.data.odMonthCat || []).map((r) => r.cod_partenza),
    ...(state.data.odDetailCat || []).map((r) => r.cod_partenza)
  ].filter(Boolean));
  const arrs = uniq([
    ...(state.data.odMonthCat || []).map((r) => r.cod_arrivo),
    ...(state.data.odDetailCat || []).map((r) => r.cod_arrivo)
  ].filter(Boolean));

  state._depItems = buildStationItems(deps);
  state._arrItems = buildStationItems(arrs);
  buildOdPairIndex();

  // Re-fill both dropdowns with the real OD-based items
  const depSel = firstEl(["depSel", "stazionePartenzaSel", "depStationSel"]);
  const arrSel = firstEl(["arrSel", "stazioneArrivoSel", "arrStationSel"]);
  if (depSel) {
    const q = (document.getElementById("depSearch") || {}).value || "";
    fillStationSelect(depSel, state._depItems, q);
    depSel.value = state.filters.dep || "all";
    if (document.getElementById("depSearch")) {
      document.getElementById("depSearch").oninput = () => fillStationSelect(depSel, state._depItems, document.getElementById("depSearch").value);
    }
  }
  if (arrSel) {
    const q = (document.getElementById("arrSearch") || {}).value || "";
    fillStationSelect(arrSel, state._arrItems, q);
    arrSel.value = state.filters.arr || "all";
    if (document.getElementById("arrSearch")) {
      document.getElementById("arrSearch").oninput = () => fillStationSelect(arrSel, state._arrItems, document.getElementById("arrSearch").value);
    }
  }
  // Apply cascading filter if a station is already selected
  if (state.filters.dep !== "all") refreshArrDropdown();
  if (state.filters.arr !== "all") refreshDepDropdown();
}

function initFilters() {
  const yearSel = firstEl(["yearSel", "annoSel", "year"]);
  const catSel = firstEl(["catSel", "categoriaSel", "category"]);
  const depSel = firstEl(["depSel", "stazionePartenzaSel", "depStationSel"]);
  const arrSel = firstEl(["arrSel", "stazioneArrivoSel", "arrStationSel"]);
  const mapMetricSel = firstEl(["mapMetricSel", "mapSel", "mappaSel"]);
  const resetBtn = firstEl(["resetBtn", "btnReset", "reset"]);
  const monthFrom = document.getElementById("monthFrom");
  const monthTo   = document.getElementById("monthTo");

  const years = uniq(state.data.kpiMonth.map((r) => yearFromMonth(r.mese)).filter(Boolean)).sort();
  const cats = uniq(state.data.kpiMonthCat.map((r) => String(r.categoria || "").trim()).filter((c) => c && c !== "NaN"))
    .sort((a, b) => String(a).localeCompare(String(b), "it", { sensitivity: "base" }));

  if (yearSel) {
    yearSel.innerHTML = "";
    yearSel.appendChild(new Option("Tutti", "all"));
    years.forEach((y) => yearSel.appendChild(new Option(y, y)));
    yearSel.value = state.filters.year || "all";
    yearSel.onchange = () => { state.filters.year = yearSel.value || "all"; invalidateLazyOnYearChange(); scheduleFilterPipeline(); };
  }

  if (catSel) {
    catSel.innerHTML = "";
    catSel.appendChild(new Option("Tutte", "all"));
    cats.forEach((c) => catSel.appendChild(new Option(categoryDisplayName(c), c)));
    catSel.value = state.filters.cat || "all";
    catSel.onchange = () => { state.filters.cat = catSel.value || "all"; scheduleFilterPipeline(); };
  }

  let deps, arrs;
  if (!(state.data.odMonthCat && state.data.odMonthCat.length)) {
    // OD data not loaded yet; use stationsRef for dropdown lists
    const allCodes = Array.from(state.stationsRef.keys());
    deps = allCodes;
    arrs = allCodes;
  } else {
    deps = uniq([
      ...(state.data.odMonthCat || []).map((r) => r.cod_partenza),
      ...(state.data.odDetailCat || []).map((r) => r.cod_partenza)
    ].filter(Boolean));
    arrs = uniq([
      ...(state.data.odMonthCat || []).map((r) => r.cod_arrivo),
      ...(state.data.odDetailCat || []).map((r) => r.cod_arrivo)
    ].filter(Boolean));
  }

  const depItems = buildStationItems(deps);
  const arrItems = buildStationItems(arrs);
  state._depItems = depItems;
  state._arrItems = arrItems;

  buildOdPairIndex();

  if (depSel) {
    fillStationSelect(depSel, depItems, "");
    ensureSearchInput(depSel, "depSearch", "Cerca stazione di partenza", depItems);
    depSel.value = state.filters.dep || "all";
    depSel.onchange = () => {
      state.filters.dep = depSel.value || "all"; updateDepAliases();
      refreshArrDropdown();
      caricaTratteDiPartenza();
      scheduleFilterPipeline();
    };
  }

  if (arrSel) {
    fillStationSelect(arrSel, arrItems, "");
    ensureSearchInput(arrSel, "arrSearch", "Cerca stazione di arrivo", arrItems);
    arrSel.value = state.filters.arr || "all";
    arrSel.onchange = () => {
      state.filters.arr = arrSel.value || "all"; updateArrAliases();
      refreshDepDropdown();
      // Idempotente: se il file della partenza e' gia' in cache non fa nulla,
      // se stava ancora arrivando garantisce il ridisegno quando arriva. Senza,
      // scegliendo la destinazione mentre il file era in volo restavano a
      // schermo i numeri della vista per capolinea.
      caricaTratteDiPartenza();
      scheduleFilterPipeline();
    };
  }

  if (mapMetricSel) {
    if (!mapMetricSel.value) mapMetricSel.value = "pct_ritardo";
    mapMetricSel.onchange = () => { renderSeries(); renderMap(); };
  }

  // Month-only selects (1-12) instead of YYYY-MM inputs
  if (monthFrom) {
    monthFrom.innerHTML = "";
    monthFrom.appendChild(new Option("--", ""));
    for (let i = 0; i < 12; i++) monthFrom.appendChild(new Option(MONTH_NAMES[i], String(i + 1).padStart(2, "0")));
    monthFrom.value = state.filters.month_from || "";
    monthFrom.onchange = () => { state.filters.month_from = monthFrom.value || ""; scheduleFilterPipeline(); };
  }
  if (monthTo) {
    monthTo.innerHTML = "";
    monthTo.appendChild(new Option("--", ""));
    for (let i = 0; i < 12; i++) monthTo.appendChild(new Option(MONTH_NAMES[i], String(i + 1).padStart(2, "0")));
    monthTo.value = state.filters.month_to || "";
    monthTo.onchange = () => { state.filters.month_to = monthTo.value || ""; scheduleFilterPipeline(); };
  }

  if (resetBtn) {
    resetBtn.onclick = () => {
      // On mobile, reset to the most recent year (not "all") to keep data light
      var defaultYear = "all";
      if (isMobile() && years.length) defaultYear = years[years.length - 1];
      state.filters.year = defaultYear;
      state.filters.cat = "all";
      state.filters.dep = "all";
      state.filters.arr = "all";
      state.filters.month_from = "";
      state.filters.month_to = "";
      // Riempiti sul posto, non sostituiti. I bottoni di "Tipo giornata" e
      // "Fascia oraria" tengono nella closure il riferimento all'array che
      // avevano al momento della costruzione: riassegnandolo, le pill
      // continuavano a modificare un array orfano che nessuno leggeva piu'.
      // Effetto: dopo un Reset i filtri avanzati non rispondevano piu', il
      // click non cambiava nemmeno il colore della pill, e l'unico modo per
      // riaverli era ricaricare la pagina.
      state.filters.day_types.fill(true);
      state.filters.time_slots.fill(true);
      state._depAliases = null;
      state._arrAliases = null;

      if (yearSel) yearSel.value = defaultYear;
      if (catSel) catSel.value = "all";
      if (depSel) { fillStationSelect(depSel, depItems, ""); depSel.value = "all"; }
      if (arrSel) { fillStationSelect(arrSel, arrItems, ""); arrSel.value = "all"; }
      var depSearchInput = document.getElementById("depSearch");
      var arrSearchInput = document.getElementById("arrSearch");
      if (depSearchInput) { depSearchInput.value = ""; depSearchInput.oninput = () => fillStationSelect(depSel, depItems, depSearchInput.value); }
      if (arrSearchInput) { arrSearchInput.value = ""; arrSearchInput.oninput = () => fillStationSelect(arrSel, arrItems, arrSearchInput.value); }
      if (monthFrom) monthFrom.value = "";
      if (monthTo) monthTo.value = "";

      syncToggleUI();
      invalidateFilterCache();
      scheduleFilterPipeline();
    };
  }
}

/* ────────────────── hist toggle (count / %) ────────────────── */

function ensureHistToggleStyles() {
  if (document.getElementById("histToggleStyles")) return;
  const style = document.createElement("style");
  style.id = "histToggleStyles";
  style.textContent = `
    .histToggleWrap { display:flex; align-items:center; gap:10px; margin:0 0 8px 0; }
    .histModeText { font-size:13px; color:#334155; opacity:0.65; user-select:none; }
    .histModeText.active { opacity:1; font-weight:600; }
    .histSwitch { position:relative; display:inline-block; width:44px; height:24px; }
    .histSwitch input { opacity:0; width:0; height:0; }
    .histSlider { position:absolute; cursor:pointer; inset:0; background:rgba(15,23,42,0.15); transition:0.18s; border-radius:24px; }
    .histSlider:before { position:absolute; content:""; height:18px; width:18px; left:3px; top:3px; background:#0073E6; transition:0.18s; border-radius:50%; }
    .histSwitch input:checked + .histSlider { background:rgba(0,115,230,0.20); }
    .histSwitch input:checked + .histSlider:before { transform: translateX(20px); }
  `;
  document.head.appendChild(style);
}

function updateHistToggleUI() {
  const t = document.getElementById("histModeToggle");
  const left = document.getElementById("histModeTextCount");
  const right = document.getElementById("histModeTextPct");
  if (!t || !left || !right) return;
  if (t.checked) { left.classList.remove("active"); right.classList.add("active"); }
  else { left.classList.add("active"); right.classList.remove("active"); }
}

function ensureHistToggle() {
  const chart = firstEl(["chartHist", "histChart", "chartDistribution"]);
  if (!chart) return;
  ensureHistToggleStyles();

  let t = document.getElementById("histModeToggle");
  if (t) { updateHistToggleUI(); t.onchange = () => { updateHistToggleUI(); renderHist(); }; return; }

  const wrap = document.createElement("div");
  wrap.className = "histToggleWrap";

  const left = document.createElement("span");
  left.id = "histModeTextCount";
  left.className = "histModeText active";
  left.innerText = "Conteggi";

  const right = document.createElement("span");
  right.id = "histModeTextPct";
  right.className = "histModeText";
  right.innerText = "%";

  const sw = document.createElement("label");
  sw.className = "histSwitch";

  t = document.createElement("input");
  t.id = "histModeToggle";
  t.type = "checkbox";
  t.checked = false;

  const slider = document.createElement("span");
  slider.className = "histSlider";

  sw.appendChild(t);
  sw.appendChild(slider);
  wrap.appendChild(left);
  wrap.appendChild(sw);
  wrap.appendChild(right);

  const parent = chart.parentNode;
  if (parent) parent.insertBefore(wrap, chart);
  t.onchange = () => { updateHistToggleUI(); renderHist(); };
}

/* ────────────────── metric helpers ────────────────── */

function useDetailAggregation() {
  return hasDetailFilter() && state.data.kpiDetailCat && state.data.kpiDetailCat.length > 0;
}

function getMetricMode() {
  const sel = firstEl(["mapMetricSel", "mapSel", "mappaSel"]);
  const v = sel ? String(sel.value || "") : "";
  if (v === "in_ritardo" || v === "conteggio_ritardo") return "count_late";
  if (v === "corse_osservate") return "count_total";
  if (v === "minuti_ritardo_tot") return "minutes";
  if (v === "soppresse" || v === "soppressi") return "suppressed";
  if (v === "cancellate" || v === "cancellati" || v === "cancellate_tot") return "cancelled";
  return "pct";
}

function metricLabel() {
  const mode = getMetricMode();
  if (mode === "count_late") return "In ritardo";
  if (mode === "count_total") return "Corse";
  if (mode === "minutes") return "Minuti";
  if (mode === "suppressed") return "Soppressi";
  if (mode === "cancelled") return "Cancellati";
  return "% in ritardo";
}

/**
 * Runs whose arrival delay was actually measured.
 *
 * "% in ritardo" divides the late count by this, not by corse_osservate: a
 * suppressed train is observed but has no arrival to be late for, so leaving
 * it in the denominator quietly reports a route as more punctual the more
 * often it gets cancelled. Falls back to corse_osservate against older data
 * that predates the column.
 */
function measuredRuns(r) {
  const m = toNum(r.corse_con_misura);
  if (m > 0) return m;
  return (r.corse_con_misura === undefined || r.corse_con_misura === "")
    ? toNum(r.corse_osservate)
    : 0;
}

function computeValue(corse, ritardo, minuti, sopp, canc, misurate) {
  const mode = getMetricMode();
  if (mode === "count_late") return ritardo;
  if (mode === "count_total") return corse;
  if (mode === "minutes") return minuti;
  if (mode === "suppressed") return sopp || 0;
  if (mode === "cancelled") return canc || 0;
  const den = (misurate === undefined || misurate === null) ? corse : misurate;
  return den > 0 ? (ritardo / den) * 100 : 0;
}

function isCardCollapsed(el) {
  if (!el) return false;
  const card = el.closest && el.closest(".card");
  return card ? card.classList.contains("card--collapsed") : false;
}

/* ────────────────── common filter pipeline ────────────────── */

function applyCommonFilters(rows, keyField) {
  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (hasMonthRange()) rows = rows.filter((r) => passMonthRange(r, keyField));
  return rows;
}

function applyDetailDimFilter(rows) {
  if (hasDetailFilter()) rows = rows.filter(passDetailDimensions);
  return rows;
}

/* ────────────────── tratte per fermata ────────────────── */

/**
 * Le corse che percorrono davvero una tratta, non solo quelle che ci nascono.
 *
 * La sorgente pubblica di ogni treno i soli capolinea, quindi la tabella
 * origine-destinazione conosce un treno Milano-Verona solo come Milano-Verona.
 * Chi cercava "Milano Centrale - Treviglio" trovava le nove corse al mese che
 * finiscono a Treviglio, mentre i treni che partono da Milano Centrale e vi
 * fermano sono 599: le altre proseguono per Verona e per Brescia. La vista
 * descriveva un servizio che non esiste.
 *
 * Il ramo delle fermate permette di contare le coppie vere. La tabella intera
 * sono quattro milioni e mezzo di righe, quindi e' divisa per stazione di
 * partenza: si scarica il solo file della stazione scelta, un megabyte e mezzo
 * nel caso peggiore.
 */
const _tratteCache = new Map();
let _tratteIndice = null;

function slugStazione(nome) {
  return String(nome || "")
    .normalize("NFKD").replace(/[̀-ͯ]/g, "")
    .replace(/[^A-Za-z0-9]+/g, "-").replace(/^-+|-+$/g, "").toLowerCase() || "senza-nome";
}

async function ensureTratteIndice() {
  if (_tratteIndice) return _tratteIndice;
  const base = ensureTrailingSlash(state.dataBase || "data/");
  const t = await fetchTextAny([base + "tratte_indice.json", "data/tratte_indice.json"]);
  try { _tratteIndice = t ? JSON.parse(t) : {}; } catch (e) { _tratteIndice = {}; }
  return _tratteIndice;
}

async function ensureTratte(nomePartenza) {
  const chiave = String(nomePartenza || "").trim();
  if (!chiave) return [];
  if (_tratteCache.has(chiave)) return _tratteCache.get(chiave);
  const indice = await ensureTratteIndice();
  const s = (indice && indice[chiave]) || slugStazione(chiave);
  const base = ensureTrailingSlash(state.dataBase || "data/");
  const t = await fetchTextAny([base + "tratte/" + s + ".csv", "data/tratte/" + s + ".csv"]);
  const righe = t ? parseCSV(t) : [];
  // Si rinominano qui, una volta sola, nei nomi che il resto della dashboard
  // gia' conosce: cosi' KPI, istogramma e serie non devono sapere da quale
  // delle due tabelle arrivano le righe. `fermate_soppresse` diventa il
  // disservizio della tratta, che su una coppia di fermate e' la fermata
  // saltata, non la corsa mai partita.
  for (const r of righe) {
    // Le due colonne che la build non pubblica perche' ridondanti si ricavano
    // qui, una volta sola: oltre_5 coincide con in_ritardo (soglia a quattro
    // minuti, valori interi) e in_orario e' il resto delle misurate. Il ritardo
    // medio non si ricava per riga: nessuno lo legge, perche' la media va
    // ripesata sulle corse ogni volta che si sommano piu' mesi e renderKPI la
    // ottiene dividendo i minuti totali per le misurate dell'aggregato.
    const mis = toNum(r.con_misura), rit = toNum(r.in_ritardo), ant = toNum(r.in_anticipo);
    r.oltre_5 = rit;
    r.in_orario = Math.max(0, mis - rit - ant);
    r.corse_osservate = r.corse;
    r.corse_con_misura = r.con_misura;
    r.in_ritardo_effettuate = r.in_ritardo;
    r.soppresse = r.fermate_soppresse;
    r.cancellate_tot = r.fermate_soppresse;
    r.non_effettuate = r.fermate_soppresse;
    r.parzialmente_cancellate = 0;
  }
  _tratteCache.set(chiave, righe);
  return righe;
}

/**
 * Scarica le tratte della stazione di partenza scelta e ridisegna.
 *
 * Parte al cambio della partenza e non a quello dell'arrivo, perche' il file e'
 * uno per stazione di partenza: quando l'utente sceglie la destinazione i dati
 * sono gia' li'.
 */
function caricaTratteDiPartenza() {
  const dep = state.filters.dep;
  if (dep === "all") { state.data.tratta = []; return; }
  const nome = stationName(dep, "");
  if (!nome) { state.data.tratta = []; return; }
  ensureTratte(nome).then((righe) => {
    // Al ritorno la partenza puo' essere gia' cambiata: scrivere qui i dati di
    // una stazione che non e' piu' selezionata mostrerebbe numeri di un'altra
    // tratta, che e' l'errore che questa vista deve smettere di fare.
    if (state.filters.dep !== dep) return;
    state.data.tratta = righe;
    invalidateFilterCache();
    renderAll();
  }).catch(() => { state.data.tratta = []; });
}

/** Vero quando la vista per tratta puo' usare le fermate invece dei capolinea. */
function trattaPerFermate() {
  return state.filters.dep !== "all" && state.filters.arr !== "all" &&
         Array.isArray(state.data.tratta) && state.data.tratta.length > 0 &&
         righeTratta().length > 0;
}

/** Le righe della tratta scelta, filtrate per arrivo, categoria, anno e mesi. */
function righeTratta() {
  const arrivo = stationName(state.filters.arr, "");
  let righe = (state.data.tratta || []).filter((r) => String(r.a || "") === arrivo);
  // La categoria e' una scomposizione esatta del totale della coppia: filtrarci
  // sopra da' lo stesso conto che darebbe la vista per capolinea, ma sulle
  // corse vere. Senza questa riga il menu restava selezionato su "Regionale" e
  // il numero non si muoveva di una corsa.
  if (state.filters.cat !== "all") {
    righe = righe.filter((r) => String(r.categoria || "") === String(state.filters.cat));
  }
  if (state.filters.year !== "all") righe = righe.filter((r) => passYear(r, "mese"));
  if (hasMonthRange()) righe = righe.filter((r) => passMonthRange(r, "mese"));
  return righe;
}

/* I filtri che la tabella per fermata non sa onorare.
 *
 * Le tratte sono aggregate per mese, coppia di stazioni e categoria: il giorno
 * della settimana e l'ora di partenza non ci sono, e ricostruirli vorrebbe dire
 * pubblicare la tabella per giorno, che sono duecento milioni di righe.
 *
 * Il problema non era mancare quei tagli, era mancarli in silenzio: il menu
 * restava acceso su "feriali, sera" e il totale continuava a mostrare tutte le
 * 43.408 corse come se il filtro fosse applicato. Un filtro che non filtra e
 * non lo dice e' peggio di un filtro assente, perche' chi legge crede di aver
 * ristretto la popolazione e non l'ha fatto. */
function filtriNonApplicabiliAllaTratta() {
  if (!trattaPerFermate()) return [];
  const f = [];
  const g = state.filters.day_types || [];
  const o = state.filters.time_slots || [];
  if (g.some((x) => !x)) f.push("tipo di giorno");
  if (o.some((x) => !x)) f.push("fascia oraria");
  return f;
}

/* ────────────────── KPI ────────────────── */

function _computeKpiRows() {
  // Con entrambe le stazioni scelte e le fermate disponibili, la tratta e'
  // quella vera: tutte le corse che la percorrono, non le sole che vi
  // cominciano e finiscono.
  if (trattaPerFermate()) return righeTratta();
  const stationFiltered = hasStationFilter();
  let useDetail = useDetailAggregation();
  let base;

  if (stationFiltered) {
    const haveOdDet = state.data.odDetailCat && state.data.odDetailCat.length > 0;
    if (useDetail && haveOdDet) {
      base = state.data.odDetailCat;
    } else {
      base = state.data.odMonthCat;
      if (!haveOdDet) useDetail = false;
    }
  } else {
    base = useDetail ? state.data.kpiDetailCat : state.data.kpiMonthCat;
  }

  let rows = base || [];
  rows = applyCommonFilters(rows, "mese");
  if (useDetail) rows = applyDetailDimFilter(rows);

  if (stationFiltered) {
    if (state.filters.dep !== "all") rows = rows.filter(passDep);
    if (state.filters.arr !== "all") rows = rows.filter(passArr);
  }
  return rows;
}

function renderKPI() {
  const rows = getCachedOrFilter("kpi", _computeKpiRows);

  const total = rows.reduce((a, r) => a + toNum(r.corse_osservate), 0);
  const late  = rows.reduce((a, r) => a + toNum(r.in_ritardo), 0);
  const mins  = rows.reduce((a, r) => a + toNum(r.minuti_ritardo_tot), 0);
  const canc  = rows.reduce((a, r) => {
    const v = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    return a + toNum(v);
  }, 0);
  const sopp = rows.reduce((a, r) => a + toNum(r.soppresse), 0);

  setTextByIds(["cardTotal","kpiTotal","kpiCorse","totalRuns","corseOsservate","corse_osservate"], fmtInt(total));
  setTextByIds(["cardLate","kpiLate","kpiRitardo","lateRuns","inRitardo","in_ritardo"], fmtInt(late));
  setTextByIds(["cardMin","kpiMinutes","kpiMinuti","delayMinutes","kpiLateMin","kpiDelayMinutes","kpiMinTotRitardo","minutiTotali","minuti_totali_ritardo","minutiRitardoTotali","minutesTotal"], fmtInt(mins));
  setTextByIds(["cardCanc","kpiCancelled","kpiCancellati","cancellati","cancellate"], fmtInt(canc));
  setTextByIds(["cardSopp","kpiSuppressed","kpiSoppressi","soppressi","soppresse"], fmtInt(sopp));
}

/* ────────────────── series helpers ────────────────── */

function aggregateByMonth(rows) {
  const by = new Map();
  for (const r of rows) {
    const m = String(r.mese || "").slice(0, 7);
    if (!m) continue;
    if (!by.has(m)) by.set(m, { key: m, corse: 0, mis: 0, rit: 0, ritEff: 0, min: 0, sopp: 0, canc: 0 });
    const o = by.get(m);
    o.corse += toNum(r.corse_osservate);
    o.mis   += measuredRuns(r);
    o.rit   += toNum(r.in_ritardo);
    // I ritardi delle sole corse fatte per intero, che non si sovrappongono
    // alle cancellate. Le pubblicazioni precedenti non hanno la colonna: li'
    // si ripiega su in_ritardo, che e' il comportamento di prima.
    o.ritEff += toNum(r.in_ritardo_effettuate !== undefined && r.in_ritardo_effettuate !== ""
      ? r.in_ritardo_effettuate : r.in_ritardo);
    o.min   += toNum(r.minuti_ritardo_tot);
    o.sopp  += toNum(r.soppresse);
    const cv = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    o.canc += toNum(cv);
  }
  return conMesiMancanti(Array.from(by.values()).sort((a, b) => String(a.key).localeCompare(String(b.key))));
}

/* Inserisce i mesi assenti fra il primo e l'ultimo, marcati come vuoti.
 *
 * Senza questo, l'asse categorico di Plotly mette in sequenza solo i mesi che
 * esistono e la linea li congiunge: con lo storico caricato l'asse passava da
 * 12/23 direttamente a 06/24, disegnando un andamento continuo sopra cinque
 * mesi che non ci sono. Un buco invisibile e' peggio di un buco: chi legge non
 * ha modo di sapere che manca qualcosa. Il valore nullo interrompe la linea. */
function conMesiMancanti(out) {
  if (out.length < 2) return out;
  const parse = (k) => { const p = String(k).split("-"); return [Number(p[0]), Number(p[1])]; };
  const pieno = [];
  let [y, m] = parse(out[0].key);
  const [yf, mf] = parse(out[out.length - 1].key);
  const indice = new Map(out.map((o) => [o.key, o]));
  // Limite di sicurezza: se le chiavi non fossero interpretabili, meglio
  // restituire la serie originale che iterare senza fine.
  for (let i = 0; i < 1200 && (y < yf || (y === yf && m <= mf)); i++) {
    const key = String(y) + "-" + String(m).padStart(2, "0");
    pieno.push(indice.get(key) || { key, vuoto: true, corse: 0, mis: 0, rit: 0, min: 0, sopp: 0, canc: 0 });
    m += 1;
    if (m > 12) { m = 1; y += 1; }
  }
  return pieno.length >= out.length ? pieno : out;
}

function _computeSeriesRows() {
  // Stessa sostituzione dei KPI: con la tratta vera la serie mensile e il
  // Delay Index si calcolano sulle corse che la percorrono davvero.
  if (trattaPerFermate()) return righeTratta();
  const stationFiltered = hasStationFilter();
  let useDetail = useDetailAggregation();
  let rows;

  if (stationFiltered) {
    const haveOdDet = state.data.odDetailCat && state.data.odDetailCat.length > 0;
    if (useDetail && haveOdDet) {
      rows = state.data.odDetailCat;
    } else {
      rows = state.data.odMonthCat;
      if (!haveOdDet) useDetail = false;
    }
  } else {
    rows = useDetail
      ? (state.data.kpiDetailCat || [])
      : (state.data.kpiMonthCat && state.data.kpiMonthCat.length ? state.data.kpiMonthCat : state.data.kpiMonth);
  }
  rows = rows || [];

  rows = applyCommonFilters(rows, "mese");
  if (useDetail) rows = applyDetailDimFilter(rows);

  if (stationFiltered) {
    if (state.filters.dep !== "all") rows = rows.filter(passDep);
    if (state.filters.arr !== "all") rows = rows.filter(passArr);
  }

  return rows;
}

function getFilteredSeriesRows() {
  return getCachedOrFilter("series", _computeSeriesRows);
}

function seriesMonthly() {
  const rows = getFilteredSeriesRows();
  const out = aggregateByMonth(rows);
  const scarsi = mesiPocheGiornate();
  return {
    x: out.map((o) => fmtMonthShort(o.key)),
    // Stessa riserva del Delay Index, ma solo quando la metrica e' una
    // percentuale: su un conteggio "due corse" e' l'informazione, su una
    // percentuale e' rumore che oscilla fra zero e cento.
    y: out.map((o) => {
      if (o.vuoto) return null;
      if (getMetricMode() === "pct" && o.mis < SOGLIA_CORSE_SIGNIFICATIVE) return null;
      // Un mese descritto da due o tre giornate non ha una percentuale mensile,
      // per quante corse contengano quelle giornate. Il conteggio invece resta
      // vero, quindi si nasconde solo la percentuale.
      if (getMetricMode() === "pct" && scarsi.has(o.key)) return null;
      return computeValue(o.corse, o.rit, o.min, o.sopp, o.canc, o.mis);
    })
  };
}

function seriesDelayIndex() {
  const rows = getFilteredSeriesRows();
  const out = aggregateByMonth(rows);
  const scarsi = mesiPocheGiornate();
  return {
    x: out.map((o) => fmtMonthShort(o.key)),
    // I due addendi devono descrivere insiemi disgiunti, altrimenti l'indice
    // conta due volte la stessa corsa. Due sovrapposizioni sono state tolte:
    // cancellate_tot contiene gia' le soppressioni (sommare o.sopp a parte
    // contava due volte 198.053 corse, +1,06 punti), e contiene anche le
    // parzialmente cancellate, che pero' hanno una misura di ritardo valida e
    // finivano quindi pure in o.rit (+0,73 punti). o.ritEff conta i ritardi
    // delle sole corse fatte per intero.
    y: out.map((o) => {
      if (o.vuoto) return null;
      // Un mese con pochissime corse non ha una percentuale: ne ha una che vale
      // 0 o 100 a seconda di come e' andata la singola corsa. Su Milano
      // Centrale - Treviglio, 288 corse in ottantadue mesi, la linea diventava
      // una scala di punti isolati fra zero e cento che si legge come un
      // servizio impazzito, mentre e' solo una tratta con tre corse al mese.
      // Sotto la soglia il punto non si disegna, come per i mesi assenti.
      if (o.corse < SOGLIA_CORSE_SIGNIFICATIVE) return null;
      if (scarsi.has(o.key)) return null;
      return o.corse > 0 ? ((o.ritEff + o.canc) / o.corse) * 100 : 0;
    })
  };
}

/* ────────────────── render series ────────────────── */

function renderSeries() {
  if (typeof Plotly !== "object") return;

  const diEl = document.getElementById("chartDelayIndex");
  const mEl = firstEl(["chartMonthly","chartMonth","chartMese","chartSeriesMonthly"]);

  if (diEl && !isCardCollapsed(diEl)) {
    const di = seriesDelayIndex();
    safePlotlyReact(diEl,
      [{ x: di.x, y: di.y, type: "scatter", mode: mobileTraceMode(), name: "Delay Index (%)", line: { color: "#e11d48" } }],
      { margin:mobileChartMargins({l:55,r:20,t:10,b:50}), yaxis:{title:isMobile()?"":"Delay Index (%)",rangemode:"tozero"}, xaxis:{type:"category"}, paper_bgcolor:"rgba(0,0,0,0)", plot_bgcolor:"rgba(0,0,0,0)", font:mobileFont() },
      { displayModeBar: false, responsive: true }
    );
  }

  if (mEl && !isCardCollapsed(mEl)) {
    const m = seriesMonthly();
    const yTitle = metricLabel();
    safePlotlyReact(mEl,
      [{ x: m.x, y: m.y, type: "scatter", mode: mobileTraceMode(), name: yTitle }],
      { margin:mobileChartMargins({l:50,r:20,t:10,b:50}), yaxis:{title:isMobile()?"":yTitle,rangemode:"tozero"}, xaxis:{type:"category"}, paper_bgcolor:"rgba(0,0,0,0)", plot_bgcolor:"rgba(0,0,0,0)", font:mobileFont() },
      { displayModeBar: false, responsive: true }
    );
  }

  scriviNotaMeseInCorso();
}

/**
 * Avverte quando l'ultimo punto delle serie e' un mese non ancora finito.
 *
 * Il mese in corso ha solo i giorni gia' passati: sulle percentuali non cambia
 * nulla, ma su un conteggio l'ultimo punto scende sempre, e si legge come un
 * crollo del traffico invece che come un mese a meta'. Al momento di scrivere
 * queste righe luglio 2026 aveva venticinque giorni su trentuno.
 *
 * Non serve un dato nuovo: il mese si dice in corso quando coincide con quello
 * in cui i dati sono stati pubblicati.
 */
function scriviNotaMeseInCorso() {
  const el = document.getElementById("notaMeseParziale");
  if (!el) return;
  const build = state.manifest && state.manifest.built_at_utc
    ? String(state.manifest.built_at_utc).slice(0, 7) : "";
  let ultimo = "";
  for (const r of getFilteredSeriesRows()) {
    const m = String(r.mese || "").slice(0, 7);
    if (m > ultimo) ultimo = m;
  }
  const avvisi = [];
  if (build && ultimo && ultimo === build) {
    avvisi.push("L'ultimo punto è il mese in corso, che non è finito: i conteggi sono parziali, le percentuali no.");
  }

  // I mesi troppo piccoli per una percentuale non vengono disegnati, e un buco
  // muto e' peggio di un punto sbagliato: qui si dice quanti sono. Con un
  // filtro stretto possono essere quasi tutti, e allora il grafico quasi vuoto
  // e' l'informazione, non un guasto.
  const mesi = aggregateByMonth(getFilteredSeriesRows()).filter((o) => !o.vuoto);
  const nascosti = mesi.filter((o) => o.corse < SOGLIA_CORSE_SIGNIFICATIVE).length;
  if (nascosti > 0) {
    avvisi.push(nascosti === mesi.length
      ? `Nessun mese ha almeno ${SOGLIA_CORSE_SIGNIFICATIVE} corse: su numeri così piccoli una percentuale vale 0 o 100 a seconda della singola corsa, quindi non viene disegnata.`
      : `${nascosti} mesi su ${mesi.length} hanno meno di ${SOGLIA_CORSE_SIGNIFICATIVE} corse e non sono disegnati: la percentuale lì non direbbe nulla.`);
  }

  // I mesi in cui la categoria scelta esiste su una manciata di giornate. Non
  // e' un caso di pochi dati: e' un caso di dati non rappresentativi, e va detto
  // con la sua causa, altrimenti sembra un guasto.
  const scarsi = mesiPocheGiornate();
  if (scarsi.size) {
    const dentro = mesi.filter((o) => scarsi.has(o.key));
    if (dentro.length) {
      const esempio = state.coperturaCat.get(String(state.filters.cat)).get(dentro[0].key);
      avvisi.push(`Questa categoria compare solo in alcune giornate del mese ` +
        `(${esempio.giorni} su ${esempio.giorniMese} in ${fmtMonthShort(dentro[0].key)}): ` +
        `i conteggi sono veri, le percentuali non descrivono il mese e non vengono disegnate. ` +
        `Dipende dalla sorgente, che nel formato usato fino a giugno 2026 non pubblica le Frecce.`);
    }
  }

  // Un filtro acceso che non filtra va detto qui, non lasciato indovinare.
  const inerti = filtriNonApplicabiliAllaTratta();
  if (inerti.length) {
    avvisi.push(`Su questa tratta i dati sono per mese, stazione e categoria: ` +
      `il filtro per ${inerti.join(" e ")} non è applicato ai numeri qui sopra.`);
  }
  el.textContent = avvisi.join(" ");
}

/* ────────────────── render histogram ────────────────── */

function normalizeBucketLabel(s) { return String(s || "").replace(/\s+/g, "").trim(); }

/**
 * La curva di quante corse restano oltre una certa soglia di ritardo.
 *
 * L'istogramma dice quante corse stanno in ogni classe, che e' la domanda
 * sbagliata per chi vuole sapere se il treno arriva tardi: le barre dopo i
 * cinque minuti sono basse una per una, ma sommate no. La cumulata risponde
 * alla domanda giusta, "quante superano i dieci minuti", leggendo un punto solo.
 *
 * Parte dai cinque minuti perche' sotto quella soglia lo scostamento e' il
 * margine dell'orario, non un ritardo che qualcuno percepisce, e si ferma prima
 * delle due classi in coda, che non sono ritardi ma corse mai arrivate.
 */
function cumulataOltre5(byBucket, totale) {
  if (!(totale > 0)) return null;
  const SOGLIE = [
    { min: 5,   etichetta: "(5,10]" },
    { min: 10,  etichetta: "(10,15]" },
    { min: 15,  etichetta: "(15,30]" },
    { min: 30,  etichetta: "(30,60]" },
    { min: 60,  etichetta: "(60,120]" },
    { min: 120, etichetta: "> 120" }
  ];
  const conta = (lab) => {
    const o = byBucket.get(normalizeBucketLabel(lab));
    return o ? o.count : 0;
  };
  // Ogni punto e' la somma delle classi da li' in poi.
  const x = [], y = [], testo = [];
  for (let i = 0; i < SOGLIE.length; i++) {
    let somma = 0;
    for (let j = i; j < SOGLIE.length; j++) somma += conta(SOGLIE[j].etichetta);
    const pct = (somma / totale) * 100;
    x.push(SOGLIE[i].etichetta);
    y.push(pct);
    testo.push(`oltre ${SOGLIE[i].min} min: ${fmtFloat(pct)}% (${fmtInt(somma)} corse)`);
  }
  if (!y.some((v) => v > 0)) return null;

  // Una corsa mai arrivata e' peggio di una in ritardo, e sulla curva dei soli
  // minuti non compare. La seconda linea, tratteggiata, somma a ogni soglia le
  // cancellate, totali e parziali: lo scarto fra le due e' costante, perche'
  // non dipende dai minuti, ed e' proprio quello il punto, perche' misura il
  // disservizio che un asse di minuti non puo' mostrare.
  const cancellate = conta("parzialmente cancellate") + conta("non effettuate");
  const yConCanc = y.map((v) => v + (cancellate / totale) * 100);

  return {
    traccia: {
      x, y, type: "scatter", mode: "lines+markers", name: "oltre la soglia",
      yaxis: "y2", line: { color: "#b45309", width: 2 }, marker: { size: 6 },
      hovertext: testo, hovertemplate: "%{hovertext}<extra></extra>"
    },
    tracciaConCancellate: cancellate > 0 ? {
      x, y: yConCanc, type: "scatter", mode: "lines", name: "o cancellata",
      yaxis: "y2", line: { color: "#b45309", width: 2, dash: "dot" },
      hovertext: SOGLIE.map((s, i) => `oltre ${s.min} min o cancellata: ${fmtFloat(yConCanc[i])}%`),
      hovertemplate: "%{hovertext}<extra></extra>"
    } : null,
    layout: {
      yaxis2: {
        title: isMobile() ? "" : "% oltre la soglia", overlaying: "y", side: "right",
        rangemode: "tozero", showgrid: false, ticksuffix: "%",
        tickfont: { color: "#b45309", size: isMobile() ? 8 : undefined },
        titlefont: { color: "#b45309" }
      },
      showlegend: false
    }
  };
}

// Le due classi in coda all'asse non sono intervalli di minuti: sono corse che
// a destinazione non ci sono arrivate, del tutto o in parte. Con lo stesso
// colore delle altre si leggono come l'ultimo scaglione di ritardo, che e'
// proprio l'equivoco da evitare.
const CLASSI_FUORI_DISTRIBUZIONE = new Set(
  ["parzialmente cancellate", "non effettuate"].map(normalizeBucketLabel)
);

function coloreClasse(etichetta) {
  return CLASSI_FUORI_DISTRIBUZIONE.has(normalizeBucketLabel(etichetta))
    ? "rgba(185,28,28,0.70)" : "rgba(0,115,230,0.70)";
}

/** Return true if a histogram bucket label represents delay > 5 minutes.
 *  Matches: (5,10], (10,15], (15,30], (30,60], (60,120], > 120 */
function isBucketOver5(label) {
  var s = String(label || "").trim();
  if (s.startsWith(">")) return true;
  // Extract lower bound from "(lower,upper]"
  var m = s.match(/\((\d+),/);
  return m ? parseInt(m[1], 10) >= 5 : false;
}

/** GDP per capita per minute: 37000 / 365 / 24 / 60 */
var COST_PER_MINUTE = 37000 / (365 * 24 * 60);

function renderHist() {
  if (typeof Plotly !== "object") return;
  const chart = firstEl(["chartHist","histChart","chartDistribution"]);
  if (!chart || isCardCollapsed(chart)) return;

  ensureHistToggle();
  const toggle = document.getElementById("histModeToggle");
  const showPct = !!(toggle && toggle.checked);

  const stationFiltered = hasStationFilter();
  let useDetail = useDetailAggregation();
  let base;

  if (stationFiltered) {
    const haveStDet = state.data.histStationsDetailRuolo && state.data.histStationsDetailRuolo.length > 0;
    if (useDetail && haveStDet) {
      base = state.data.histStationsDetailRuolo;
    } else {
      base = state.data.histStationsMonthRuolo;
      if (!haveStDet) useDetail = false;
    }
  } else {
    const haveDetHist = state.data.histDetailCat && state.data.histDetailCat.length > 0;
    if (useDetail && haveDetHist) {
      base = state.data.histDetailCat;
    } else {
      base = state.data.histMonthCat;
      if (!haveDetHist) useDetail = false;
    }
  }

  let rows = base || [];
  rows = applyCommonFilters(rows, "mese");
  if (useDetail) rows = applyDetailDimFilter(rows);

  if (stationFiltered) {
    const dep = state.filters.dep;
    const arr = state.filters.arr;
    if (arr !== "all") {
      const aliases = state._arrAliases;
      rows = rows.filter((r) => {
        const code = String(r.cod_stazione || "").trim();
        if (String(r.ruolo || "").trim() !== "arrivo") return false;
        if (code === arr) return true;
        return aliases ? aliases.has(code) : false;
      });
    } else if (dep !== "all") {
      const aliases = state._depAliases;
      rows = rows.filter((r) => {
        const code = String(r.cod_stazione || "").trim();
        if (String(r.ruolo || "").trim() !== "partenza") return false;
        if (code === dep) return true;
        return aliases ? aliases.has(code) : false;
      });
    }
  }

  // Con partenza E arrivo selezionati, la tabella per stazione non sa
  // rispondere: e' indicizzata per stazione e ruolo, non per coppia
  // origine-destinazione, e filtrando solo sull'arrivo mostrerebbe tutti gli
  // arrivi in quella stazione da qualunque origine (12.102 corse dove i KPI ne
  // contano 2.117).
  //
  // La distribuzione della singola tratta si ricava invece dalle tabelle O/D
  // che il browser ha gia' scaricato: in_anticipo, in_orario e le soglie
  // cumulative oltre_5/10/15/30/60 si differenziano in classi disgiunte. La
  // granularita' e' piu' grossa dei bucket per stazione, ma la popolazione e'
  // quella giusta.
  const noteEl = (function() {
    let n = document.getElementById("histScopeNote");
    if (!n) {
      const badges = document.getElementById("badgesHist");
      if (!badges || !badges.parentNode) return null;
      n = document.createElement("div");
      n.id = "histScopeNote";
      n.className = "card-desc card-desc--warn";
      badges.parentNode.insertBefore(n, badges.nextSibling);
    }
    return n;
  }());

  const odMode = state.filters.dep !== "all" && state.filters.arr !== "all";
  if (odMode) {
    const odRows = getCachedOrFilter("kpi", _computeKpiRows);
    const sum = (k) => odRows.reduce((a, r) => a + toNum(r[k]), 0);
    const o5 = sum("oltre_5"), o10 = sum("oltre_10"), o15 = sum("oltre_15"),
          o30 = sum("oltre_30"), o60 = sum("oltre_60");
    // Le corse cancellate e soppresse chiudono l'asse. Sono nei totali in
    // testa alla pagina ma prima non comparivano qui, e una tratta molto
    // cancellata risultava indistinguibile da una puntuale: le sue corse
    // semplicemente non c'erano nel grafico.
    const classi = [
      ["in anticipo", sum("in_anticipo")],
      ["0–4", sum("in_orario")],
      ["5–9", o5 - o10],
      ["10–14", o10 - o15],
      ["15–29", o15 - o30],
      ["30–59", o30 - o60],
      ["≥ 60", o60],
      // Anche qui le due code sono separate: la tratta con molte limitazioni e
      // quella con molte soppressioni non sono lo stesso disservizio, e prima
      // le prime non comparivano affatto in questa vista.
      ["parzialmente cancellate", sum("parzialmente_cancellate")],
      ["non effettuate", sum("non_effettuate")]
    ];
    const misurate = classi.slice(0, -2).reduce((a, c) => a + Math.max(0, c[1]), 0);
    const tot = classi.reduce((a, c) => a + Math.max(0, c[1]), 0);

    if (noteEl) {
      const dn = etichettaStazioneSelezionata("depSel", state.filters.dep);
      const an = etichettaStazioneSelezionata("arrSel", state.filters.arr);
      // Le corse osservate sulla tratta non sono la somma delle barre: quelle
      // partite senza una misura utilizzabile non hanno una classe qui, e su
      // Milano Centrale - Roma Termini sono 1.459 su 14.595, il dieci per
      // cento. Nel grafico generale il divario si legge nella descrizione,
      // qui non si leggeva da nessuna parte.
      const osservate = sum("corse_osservate");
      const fuori = Math.max(0, osservate - tot);
      noteEl.textContent = "Distribuzione della tratta " + dn + " → " + an
        + " (" + fmtInt(misurate) + " corse arrivate a destinazione, " + fmtInt(tot - misurate)
        + " cancellate del tutto o in parte"
        + (fuori > 0 ? ", più " + fmtInt(fuori) + " senza una misura utilizzabile, che non hanno una barra" : "")
        + ")."
        + (misurate === 0 && tot > 0
            ? " Su questa tratta la sorgente non rileva mai l'arrivo alla stazione di "
              + "destinazione: le corse ci sono, la misura del ritardo no, e ogni percentuale "
              + "qui sarebbe zero per assenza di dato, non perché i treni siano puntuali. "
              + "Succede su 7.975 coppie, l'8,4% delle corse."
            : "")
        + (trattaPerFermate()
            ? " Sono tutte le corse che percorrono la tratta, comprese quelle che proseguono "
              + "oltre: fra Milano Centrale e Treviglio sono 599 al mese, non le 9 che a "
              + "Treviglio terminano. Il filtro per categoria vale anche qui; il tipo di "
              + "giorno e la fascia oraria no, perché il dettaglio è per mese. La copertura "
              + "parte da gennaio 2020, da quando la sorgente pubblica le fermate."
            : " Attenzione: qui contano solo le corse che partono da " + dn + " e finiscono a "
              + an + ", non quelle che ci passano e proseguono, perché per questa tratta il "
              + "dettaglio delle fermate non è disponibile.");
      noteEl.style.display = "";
    }

    // Qui la cumulata non va ricavata dalle barre: le colonne oltre_N del gold
    // sono gia' cumulate per costruzione, quindi si leggono direttamente.
    const cancTratta = Math.max(0, sum("parzialmente_cancellate")) + Math.max(0, sum("non_effettuate"));
    const cumOd = tot > 0 && o5 > 0 ? {
      x: ["5–9", "10–14", "15–29", "30–59", "≥ 60"],
      y: [o5, o10, o15, o30, o60].map((v) => (Math.max(0, v) / tot) * 100),
      soglie: [5, 10, 15, 30, 60],
      conteggi: [o5, o10, o15, o30, o60],
      // Stessa seconda curva dell'istogramma per stazione: il ritardo da solo
      // non racconta le corse che a destinazione non sono arrivate.
      cancellate: cancTratta
    } : null;

    safePlotlyReact(chart,
      [{ x: classi.map((c) => c[0]),
         y: classi.map((c) => showPct ? (tot > 0 ? (Math.max(0, c[1]) / tot) * 100 : 0) : Math.max(0, c[1])),
         type: "bar", name: showPct ? "%" : "Conteggio",
         marker: { color: classi.map((c) => coloreClasse(c[0])) } },
       ...(cumOd ? [{
         x: cumOd.x, y: cumOd.y, type: "scatter", mode: "lines+markers",
         name: "oltre la soglia", yaxis: "y2",
         line: { color: "#b45309", width: 2 }, marker: { size: 6 },
         hovertext: cumOd.soglie.map((s, i) => `oltre ${s} min: ${fmtFloat(cumOd.y[i])}% (${fmtInt(cumOd.conteggi[i])} corse)`),
         hovertemplate: "%{hovertext}<extra></extra>"
       }] : []),
       ...(cumOd && cumOd.cancellate > 0 ? [{
         x: cumOd.x, y: cumOd.y.map((v) => v + (cumOd.cancellate / tot) * 100),
         type: "scatter", mode: "lines", name: "o cancellata", yaxis: "y2",
         line: { color: "#b45309", width: 2, dash: "dot" },
         hovertext: cumOd.soglie.map((s, i) => `oltre ${s} min o cancellata: ${fmtFloat(cumOd.y[i] + (cumOd.cancellate / tot) * 100)}%`),
         hovertemplate: "%{hovertext}<extra></extra>"
       }] : [])],
      // Le due classi in coda hanno etichette lunghe il triplo delle altre, e
      // orizzontali si scavallavano fra loro e sopra "≥ 60": inclinate e con
      // automargin ognuna ha il suo spazio. Stessa scelta dell'istogramma per
      // stazione, che aveva lo stesso difetto.
      Object.assign({ margin: mobileChartMargins({l:50,r:cumOd?(isMobile()?28:70):20,t:10,b:110}),
        yaxis: {title: isMobile() ? "" : (showPct ? "%" : "Conteggio"), rangemode: "tozero"},
        xaxis: {title: isMobile() ? "" : "minuti di ritardo all'arrivo", tickangle: isMobile() ? -45 : -35,
                automargin: true, tickfont: {size: isMobile() ? 8 : undefined}},
        paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", font: mobileFont() },
        cumOd ? { yaxis2: { title: isMobile() ? "" : "% oltre la soglia", overlaying: "y", side: "right",
                            rangemode: "tozero", showgrid: false, ticksuffix: "%",
                            tickfont: { color: "#b45309", size: isMobile() ? 8 : undefined },
                            titlefont: { color: "#b45309" } }, showlegend: false } : {}),
      { displayModeBar: false, responsive: true }
    );

    // L'indicatore di costo e' definito sui minuti dei soli ritardi oltre 5
    // minuti. La tabella origine-destinazione espone i minuti di ritardo
    // complessivi, che sono un'altra grandezza: mostrarli nella stessa casella
    // darebbe un valore piu' alto con la stessa etichetta. Meglio nasconderlo.
    const costElOd = document.getElementById("costIndicator");
    if (costElOd) costElOd.style.display = "none";
    return;
  }

  const byBucket = new Map();
  let total = 0;
  let delayMinsOver5 = 0;
  for (const r of rows) {
    const raw = String(r.bucket_ritardo_arrivo || r.bucket || "").trim();
    if (!raw) continue;
    const key = normalizeBucketLabel(raw);
    // "missing" collects the runs with no usable arrival measurement, above
    // all the suppressed ones. It has no bar of its own, so counting it in
    // the total would make the percentage bars quietly sum to less than 100.
    if (key === "missing") continue;
    const c = toNum(r.count);
    total += c;
    if (!byBucket.has(key)) byBucket.set(key, { label: raw, count: 0 });
    byBucket.get(key).count += c;
    // Accumulate delay minutes for buckets > 5 min
    if (isBucketOver5(raw)) delayMinsOver5 += toNum(r.minuti_ritardo);
  }

  // Dichiara sopra al grafico su quale popolazione e' calcolata la
  // distribuzione: con entrambe le stazioni selezionate non e' la coppia
  // origine-destinazione, e senza dirlo il confronto con i KPI non torna.
  // Il caso "partenza e arrivo entrambi selezionati" e' gestito sopra, dalla
  // tabella origine-destinazione. Qui c'e' una sola stazione selezionata (o
  // nessuna), e i bucket fini della tabella per stazione sono quelli corretti.
  if (noteEl) {
    noteEl.textContent = "";
    noteEl.style.display = "none";
  }

  const order = Array.isArray(state.manifest.delay_bucket_labels) && state.manifest.delay_bucket_labels.length
    ? state.manifest.delay_bucket_labels : safeManifestDefaults().delay_bucket_labels;

  const x = [], y = [];
  for (const lab of order) {
    const key = normalizeBucketLabel(lab);
    const obj = byBucket.get(key);
    const c = obj ? obj.count : 0;
    x.push(lab);
    y.push(showPct ? (total > 0 ? (c / total) * 100 : 0) : c);
  }

  const cum = cumulataOltre5(byBucket, total);

  safePlotlyReact(chart,
    [{ x, y, type: "bar", name: showPct ? "%" : "Conteggio",
       marker: { color: x.map((lab) => coloreClasse(lab)) } },
     ...(cum ? [cum.traccia] : []),
     ...(cum && cum.tracciaConCancellate ? [cum.tracciaConCancellate] : [])],
    // Margine basso piu' alto e automargin: "parzialmente cancellate" e' lunga
    // il triplo di "(30,60]" e inclinata usciva dal riquadro, tagliata a meta'.
    // Con la cumulata serve spazio a destra: l'asse secondario porta i suoi tick
    // e il suo titolo, e con il margine tarato su un asse solo il titolo finiva
    // scritto sopra i numeri.
    Object.assign({ margin:mobileChartMargins({l:50,r:cum?(isMobile()?28:70):20,t:10,b:110}), yaxis:{title:isMobile()?"":showPct?"%":"Conteggio",rangemode:"tozero"}, xaxis:{tickangle:isMobile()?-45:-35,automargin:true,tickfont:{size:isMobile()?7:undefined}}, paper_bgcolor:"rgba(0,0,0,0)", plot_bgcolor:"rgba(0,0,0,0)", font:mobileFont() },
      cum ? cum.layout : {}),
    { displayModeBar: false, responsive: true }
  );

  // Cost indicator: GDP per-capita cost of delays > 5 min
  var costEl = document.getElementById("costIndicator");
  var costVal = document.getElementById("costValue");
  if (costEl && costVal) {
    if (delayMinsOver5 > 0) {
      costVal.textContent = fmtEuro(delayMinsOver5 * COST_PER_MINUTE);
      costEl.style.display = "";
    } else {
      costEl.style.display = "none";
    }
  }
}

/**
 * Un importo in euro, scritto come lo scrive un italiano.
 *
 * Prima c'era un solo salto di scala, e senza filtri l'indicatore mostrava
 * "€ 2481.6k": due milioni e mezzo scritti come "2481,6 mila", per giunta col
 * punto decimale mentre tutto il resto della dashboard passa da toLocaleString.
 */
function fmtEuro(v) {
  var n = Number(v) || 0;
  var it = function(x, d) { return x.toLocaleString("it-IT", { minimumFractionDigits: d, maximumFractionDigits: d }); };
  if (n >= 1e9) return "€ " + it(n / 1e9, 1) + " mld";
  if (n >= 1e6) return "€ " + it(n / 1e6, 1) + " mln";
  if (n >= 1e3) return "€ " + it(Math.round(n), 0);
  return "€ " + it(n, 2);
}

/* ────────────────── map helpers ────────────────── */

// Sotto questo numero di osservazioni una percentuale non dice niente. La usano
// sia la mappa sia la Top 10, che leggono le stesse righe: tenerla in un solo
// posto evita che le due viste raccontino due cose diverse sugli stessi dati.
const SOGLIA_CORSE_SIGNIFICATIVE = 30;

function capoluogoKey(cityName) {
  // La stessa normalizzazione usata ovunque per i nomi stazione, non il solo
  // normalizeText: senza di essa "REGGIO DI CALABRIA CENTRALE" non trovava il
  // capoluogo "reggio calabria" (218.826 corse fuori da mappa e Top 10, e la
  // stazione che appariva o spariva a seconda dell'anno, perche' lo shard 2026
  // la scrive senza "DI"), e i nomi che portano l'accento come apice inverso,
  // FORLI` e L`AQUILA, non lo trovavano mai.
  const name = normalizeText(normalizeStationName(cityName));
  if (!name) return "";
  if (!state.capoluoghiSet || state.capoluoghiSet.size === 0) return name;
  if (state.capoluoghiSet.has(name)) return name;
  for (const cap of state.capoluoghiSet) {
    if (name.startsWith(cap + " ") || name.startsWith(cap + "-") || name.startsWith(cap + "'")) return cap;
  }
  return "";
}

/**
 * Il testo dell'opzione selezionata in una tendina di stazioni, o il nome
 * della stazione quando quell'opzione non c'e' piu'.
 *
 * Le due tendine vengono ripopolate con le sole stazioni presenti nella
 * tabella origine-destinazione, che sono meno di quelle dell'anagrafica. Se
 * la stazione scelta non e' fra queste, il DOM porta selectedIndex a -1 e
 * options[-1] e' undefined: leggerne .text sollevava un TypeError dentro
 * renderFilterBadges, che e' la prima riga di renderAll, e faceva saltare in
 * silenzio l'intero ridisegno (KPI, serie, istogramma, mappa, Top 10).
 */
function etichettaStazioneSelezionata(idTendina, codice) {
  var sel = document.getElementById(idTendina);
  if (sel && sel.selectedIndex >= 0 && sel.options[sel.selectedIndex]) {
    return sel.options[sel.selectedIndex].text;
  }
  return stationName(codice, codice);
}

/**
 * Il nome di citta' come va scritto in etichetta.
 *
 * Prima, quando il nome della stazione coincideva con quello della citta', si
 * restituiva il grezzo della sorgente, che e' tutto maiuscolo: nello stesso
 * grafico convivevano "PAVIA" e "Como". Ora la forma e' sempre la stessa.
 */
function prettyCityName(cityKey) {
  return String(cityKey || "").toLowerCase().replace(/\b([a-zàèéìòù])/g, (m) => m.toUpperCase());
}

function getStationsMetric() {
  const sel = document.getElementById("stationsMetricSel");
  return sel ? (sel.value || "pct_ritardo") : "pct_ritardo";
}

function stationsMetricLabel() {
  const m = getStationsMetric();
  const labels = { pct_ritardo:"% in ritardo", in_ritardo:"In ritardo", minuti_ritardo_tot:"Minuti ritardo", cancellate_tot:"Cancellati", soppresse:"Soppressi", corse_osservate:"Corse osservate" };
  return labels[m] || m;
}

/* ────────────────── stations filtered rows (cached) ────────────────── */

function _computeStationsRows() {
  const useDetail = useDetailAggregation() && state.data.stationsDetailNode && state.data.stationsDetailNode.length > 0;
  const base = useDetail ? state.data.stationsDetailNode : state.data.stationsMonthNode;
  let rows = base || [];
  rows = applyCommonFilters(rows, "mese");
  if (useDetail) rows = applyDetailDimFilter(rows);
  return rows;
}

/* ────────────────── stations top 10 (capoluoghi only) ────────────────── */

function renderStationsTop10() {
  if (typeof Plotly !== "object") return;
  const chart = document.getElementById("chartStationsTop10");
  if (!chart || isCardCollapsed(chart)) return;

  const rows = getCachedOrFilter("stationsRows", _computeStationsRows);

  // Aggregate by capoluogo (provincial capital) instead of individual station
  const agg = new Map();
  for (const r of rows) {
    const code = String(r.cod_stazione || "").trim();
    if (!code) continue;
    const city = stationCity(code, r.nome_stazione || code);
    if (!city) continue;
    const cityKey = capoluogoKey(city);
    if (!cityKey) continue;  // skip non-capoluogo stations

    if (!agg.has(cityKey)) {
      agg.set(cityKey, { nome: prettyCityName(cityKey), corse_osservate:0, corse_con_misura:0, in_ritardo:0, minuti_ritardo_tot:0, cancellate_tot:0, soppresse:0 });
    }
    const a = agg.get(cityKey);
    a.corse_osservate += toNum(r.corse_osservate);
    a.corse_con_misura += measuredRuns(r);
    a.in_ritardo += toNum(r.in_ritardo);
    a.minuti_ritardo_tot += toNum(r.minuti_ritardo_tot);
    const canc = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    a.cancellate_tot += toNum(canc);
    a.soppresse += toNum(r.soppresse);
  }

  let out = Array.from(agg.values());
  out.forEach((o) => { o.pct_ritardo = o.corse_con_misura > 0 ? (o.in_ritardo / o.corse_con_misura) * 100 : 0; });

  const metric = getStationsMetric();
  // La stessa soglia di significativita' della mappa, che lavora sugli stessi
  // dati: senza, con un filtro stretto la classifica si riempiva di capoluoghi
  // al 100% di ritardo su una corsa sola, e Bologna con 786 corse finiva sesta.
  // Vale solo per le percentuali: sui conteggi assoluti un numero piccolo e'
  // gia' l'informazione, non rumore.
  //
  // Basta che ne resti una. La condizione era "almeno tre", e sotto quel numero
  // riammetteva l'intera classifica, cioe' faceva rientrare le percentuali su
  // una corsa proprio nel caso, il filtro stretto, in cui sono piu' probabili.
  // Una barra sola e' un risultato magro ma vero; dieci barre di rumore no.
  if (metric === "pct_ritardo") {
    const significativi = out.filter((o) => o.corse_con_misura >= SOGLIA_CORSE_SIGNIFICATIVE);
    if (significativi.length) out = significativi;
  }
  out.sort((a, b) => toNum(b[metric]) - toNum(a[metric]));
  const top10 = out.slice(0, 10).reverse();

  const yLabels = top10.map((o) => o.nome);
  const xValues = top10.map((o) => toNum(o[metric]));
  const label = stationsMetricLabel();

  // Su quante corse poggia la barra. Senza questo, la classifica mette sulla
  // stessa riga Pordenone, che sullo storico intero ha 340 corse misurate, e
  // Milano, che ne ha 3,3 milioni: passano entrambe la soglia, ma non sono
  // numeri della stessa solidita' e chi legge non ha modo di accorgersene. La
  // mappa lo dice gia' nel popup, qui mancava.
  const percentuale = metric === "pct_ritardo";
  // Con la metrica "corse osservate" la base sarebbe il valore stesso: la
  // seconda riga direbbe due volte lo stesso numero.
  const conBase = metric !== "corse_osservate";
  const dettaglio = top10.map((o) => [
    percentuale ? fmtFloat(toNum(o[metric])) + "%" : fmtInt(toNum(o[metric])),
    percentuale
      ? fmtInt(o.corse_con_misura) + " corse misurate"
      : fmtInt(o.corse_osservate) + " corse osservate"
  ]);

  safePlotlyReact(chart,
    [{ x:xValues, y:yLabels, type:"bar", orientation:"h", name:label, marker:{color:"rgba(0,115,230,0.70)"},
       customdata: dettaglio,
       hovertemplate: "<b>%{y}</b><br>" + label + ": %{customdata[0]}"
         + (conBase ? "<br>su %{customdata[1]}" : "") + "<extra></extra>" }],
    { margin:isMobile()?{l:10,r:10,t:10,b:40}:{l:180,r:30,t:10,b:50}, xaxis:{title:isMobile()?"":label,rangemode:"tozero"}, yaxis:{automargin:true}, paper_bgcolor:"rgba(0,0,0,0)", plot_bgcolor:"rgba(0,0,0,0)", font:mobileFont() },
    { displayModeBar: false, responsive: true }
  );
}

/* ────────────────── map render ────────────────── */

function mapMetricValue(row) {
  return computeValue(toNum(row.corse_osservate), toNum(row.in_ritardo), toNum(row.minuti_ritardo_tot), toNum(row.soppresse), toNum(row.cancellate_tot), measuredRuns(row));
}

// Le soglie disponibili sono quelle che il gold pubblica come colonne
// cumulative: non si possono scegliere valori arbitrari senza ricalcolare
// l'intero storico, e queste cinque coprono la domanda utile.
const SOGLIE_MINUTI = [5, 10, 15, 30, 60];

function statoSoglia() {
  const on = document.getElementById("mapSogliaOn");
  const min = document.getElementById("mapSogliaMin");
  const pct = document.getElementById("mapSogliaPct");
  const attiva = !!(on && on.checked);
  return {
    attiva,
    minuti: min ? Number(min.value) || 15 : 15,
    pct: pct ? Number(pct.value) || 0 : 0,
  };
}

function collegaControlliSoglia() {
  const on = document.getElementById("mapSogliaOn");
  if (!on || on.dataset.collegato) return;
  on.dataset.collegato = "1";
  const controlli = document.getElementById("mapSogliaControlli");
  const min = document.getElementById("mapSogliaMin");
  const pct = document.getElementById("mapSogliaPct");
  const out = document.getElementById("mapSogliaPctVal");
  const metrica = document.getElementById("mapMetricSel");

  // Chrome ripristina i valori dei controlli quando si torna su una pagina gia'
  // visitata, e lo fa anche con autocomplete="off": il cursore della soglia
  // ripartiva da dove l'utente lo aveva lasciato l'ultima volta invece che dal
  // 10% scritto nel markup, e l'interruttore poteva risultare acceso senza che
  // nessuno lo avesse toccato in questa visita. Lo stato iniziale deve venire
  // dalla pagina, non dalla memoria del browser.
  on.checked = on.hasAttribute("checked");
  if (pct) pct.value = pct.getAttribute("value") || "10";
  if (min) {
    const predefinita = min.querySelector("option[selected]");
    if (predefinita) min.value = predefinita.value;
  }

  const sincronizzaUI = () => {
    if (controlli) controlli.hidden = !on.checked;
    if (out && pct) out.textContent = pct.value + "%";
    // Il menu della metrica NON va disabilitato: sta nella barra dei filtri in
    // cima, e' condiviso con le altre viste, e vederlo bloccato da un
    // interruttore dentro la card della mappa e' incomprensibile. La modalita'
    // soglia si limita a sostituire la metrica della mappa, e lo dichiara in
    // legenda e nei popup.
    if (metrica) metrica.disabled = false;
  };
  const aggiorna = () => { sincronizzaUI(); renderMap(); };
  on.onchange = aggiorna;
  if (min) min.onchange = aggiorna;
  if (pct) pct.oninput = aggiorna;
  // Solo lo stato iniziale dell'interfaccia: chiamare renderMap qui
  // rientrerebbe nella funzione che ci ha appena invocati.
  sincronizzaUI();
}

// La mappa ha una metrica propria quando la soglia e' attiva, quindi non puo'
// usare metricLabel(), che serve anche all'asse della serie mensile.
function etichettaMetricaMappa() {
  const s = statoSoglia();
  return s.attiva ? ("% oltre " + s.minuti + " min") : metricLabel();
}

function renderMap() {
  if (!state.map) return;
  const mapEl = document.getElementById("map");
  if (isCardCollapsed(mapEl)) return;

  collegaControlliSoglia();
  clearMarkers();

  const rows = getCachedOrFilter("stationsRows", _computeStationsRows);

  const agg = new Map();
  for (const r of rows) {
    const code = String(r.cod_stazione || "").trim();
    if (!code) continue;
    const city = stationCity(code, r.nome_stazione || code);
    if (!city) continue;
    const cityKey = capoluogoKey(city);
    if (!cityKey) continue;
    const coords = stationCoords(code);
    if (!coords) continue;

    if (!agg.has(cityKey)) {
      agg.set(cityKey, { cityKey, nome:prettyCityName(cityKey), corse_osservate:0, corse_con_misura:0, in_ritardo:0, minuti_ritardo_tot:0, soppresse:0, cancellate_tot:0, oltre_5:0, oltre_10:0, oltre_15:0, oltre_30:0, oltre_60:0, lat_weighted_sum:0, lon_weighted_sum:0, weight_sum:0 });
    }
    const a = agg.get(cityKey);
    const corse = toNum(r.corse_osservate);
    const weight = Math.max(1, corse);
    a.corse_osservate += corse;
    a.corse_con_misura += measuredRuns(r);
    a.in_ritardo += toNum(r.in_ritardo);
    for (const k of SOGLIE_MINUTI) a["oltre_" + k] += toNum(r["oltre_" + k]);
    a.minuti_ritardo_tot += toNum(r.minuti_ritardo_tot);
    a.soppresse += toNum(r.soppresse);
    const canc = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    a.cancellate_tot += toNum(canc);
    a.lat_weighted_sum += toNum(coords.lat) * weight;
    a.lon_weighted_sum += toNum(coords.lon) * weight;
    a.weight_sum += weight;
  }

  // Modalita' soglia: la metrica diventa la quota di corse in ritardo oltre i
  // minuti scelti, e le stazioni sotto la percentuale minima escono dalla
  // mappa. Serve a rispondere a una domanda che la vista normale non permette:
  // dove i ritardi non sono solo frequenti ma gravi. Il denominatore sono le
  // corse con una misura utilizzabile, non tutte le osservate: gli anticipi
  // oltre i cinque minuti restano nel dato ma fuori dal calcolo, altrimenti
  // gonfierebbero la base e sfaserebbero la percentuale.
  const soglia = statoSoglia();
  const pts = Array.from(agg.values()).map((o) => {
    const w = o.weight_sum > 0 ? o.weight_sum : 1;
    const v = soglia.attiva
      ? (o.corse_con_misura > 0 ? (o["oltre_" + soglia.minuti] / o.corse_con_misura) * 100 : 0)
      : mapMetricValue(o);
    return { ...o, coords:{ lat: o.lat_weighted_sum/w, lon: o.lon_weighted_sum/w }, v };
  }).filter((o) => Number.isFinite(o.coords.lat) && Number.isFinite(o.coords.lon))
    .filter((o) => !soglia.attiva || o.v >= soglia.pct);

  pts.sort((a, b) => toNum(b.v) - toNum(a.v));
  const top = pts.slice(0, 250);

  // Due canali visivi indipendenti invece di uno.
  //
  // Prima il raggio codificava la metrica e il colore era il blu di default di
  // Leaflet: una citta' con poche corse molto in ritardo e una con molte corse
  // poco in ritardo si distinguevano male, e il volume di traffico non era
  // leggibile. Ora il colore porta la metrica selezionata e la dimensione porta
  // il numero di corse osservate, che e' il peso di quella metrica.
  //
  // Restano marker su tile: un coropleto per aree costerebbe milioni di byte di
  // geometrie (la mappa comunale del sito ne scarica 4,6 MB e disegna quasi
  // 8.000 path) e comunque obbligherebbe ad aggregare per provincia, perdendo
  // il dettaglio per stazione che e' il senso di questa vista.
  const values = top.map((p) => Math.max(0, Number(p.v) || 0));
  const volumes = top.map((p) => Math.max(0, toNum(p.corse_osservate)));
  const maxVolume = volumes.length ? Math.max(...volumes) : 0;

  // Stazioni troppo poco osservate perche' una percentuale voglia dire qualcosa.
  //
  // Segnalato da un lettore: Pordenone risultava in ritardo nel 45% dei casi,
  // su tre corse. Non e' un errore di estrazione, e' che il dato copre solo
  // origine e destinazione di ogni corsa, e Pordenone e' una stazione di
  // transito: i treni ci fermano ma non ci nascono ne' ci finiscono. Tre corse
  // restano tre corse, e il 45% non e' una statistica.
  //
  // Non si nascondono: sparire dalla mappa e' peggio che comparire con una
  // riserva. Si disegnano in grigio, con il conteggio nel popup, e restano
  // fuori dalla scala di colore, altrimenti un valore estremo calcolato su tre
  // osservazioni allarga la rampa e sbiadisce tutte le altre.
  // La soglia va sul denominatore della metrica disegnata, non sulle corse
  // osservate: quando il colore porta una percentuale, il denominatore sono le
  // corse con una misura utilizzabile, e una citta' con tante corse osservate
  // ma poche misurate ha una percentuale altrettanto fragile pur passando il
  // controllo. Con una metrica di conteggio la riserva non ha senso: dieci
  // soppressioni sono dieci soppressioni, non un campione. Stessa scelta della
  // Top 10, che applica la soglia solo alla percentuale.
  const SOGLIA_CORSE = SOGLIA_CORSE_SIGNIFICATIVE;
  const inPercentuale = soglia.attiva || getMetricMode() === "pct";
  const affidabile = (p) => !inPercentuale || measuredRuns(p) >= SOGLIA_CORSE;

  // Scala di colore su percentili: i valori sono molto asimmetrici e una scala
  // lineare sul massimo appiattirebbe quasi tutto sul primo colore.
  const sorted = top.filter(affidabile).map((p) => Math.max(0, Number(p.v) || 0))
    .sort((a, b) => a - b);
  const quantile = (q) => {
    if (!sorted.length) return 0;
    const i = Math.min(sorted.length - 1, Math.max(0, Math.round(q * (sorted.length - 1))));
    return sorted[i];
  };
  const lo = quantile(0.05), hi = quantile(0.95);

  // Rampa sequenziale, leggibile anche con deficit di visione dei colori.
  const RAMP = ["#2c7bb6", "#abd9e9", "#ffffbf", "#fdae61", "#d7191c"];
  const colorFor = (v) => {
    if (!(hi > lo)) return RAMP[0];
    const t = Math.min(1, Math.max(0, (v - lo) / (hi - lo)));
    return RAMP[Math.min(RAMP.length - 1, Math.floor(t * RAMP.length))];
  };

  const minRadius = 4, maxRadius = 20;
  const bounds = [];
  for (const p of top) {
    const val = Math.max(0, Number(p.v) || 0);
    const vol = Math.max(0, toNum(p.corse_osservate));
    const ratio = maxVolume > 0 ? Math.sqrt(vol / maxVolume) : 0;
    const radius = minRadius + ratio * (maxRadius - minRadius);
    const poche = !affidabile(p);
    // Quante corse non sono state fatte, distinguendo chi non e' partito da chi
    // e' partito e si e' fermato per strada. Il popup dava solo le corse
    // osservate, e una citta' con molte soppressioni sembrava uguale a una
    // senza: il dato peggiore era l'unico che non si poteva leggere qui.
    const sopp = Math.max(0, toNum(p.soppresse));
    const cancTot = Math.max(0, toNum(p.cancellate_tot));
    const parziali = Math.max(0, cancTot - sopp);
    const quota = (n) => vol > 0 ? " (" + fmtFloat(100 * n / vol) + "%)" : "";
    const label = "<b>" + p.nome + "</b><br>" + etichettaMetricaMappa() + ": " + fmtFloat(val)
      + "<br>Corse osservate: " + fmtInt(vol)
      + "<br>Non effettuate: " + fmtInt(sopp) + quota(sopp)
      + (parziali > 0 ? "<br>Cancellate in parte: " + fmtInt(parziali) + quota(parziali) : "")
      // Il numero che regge la percentuale e' questo, non quello sopra: si
      // mostra dove fa la differenza, cioe' quando la riserva scatta.
      + (poche ? "<br>Corse con misura: " + fmtInt(measuredRuns(p))
                 + "<br><i>Sotto le " + SOGLIA_CORSE + " corse misurate: la percentuale non è "
                 + "significativa. Il dato copre solo origine e destinazione, quindi le stazioni "
                 + "di transito compaiono di rado.</i>" : "");

    const m = L.circleMarker([p.coords.lat, p.coords.lon], {
      radius: radius,
      color: "#333", weight: 0.7, opacity: poche ? 0.5 : 0.85,
      fillColor: poche ? "#bdbdbd" : colorFor(val), fillOpacity: poche ? 0.45 : 0.85
    }).addTo(state.map);
    try { m.bindPopup(label); } catch {}
    state.markers.push(m);
    bounds.push([p.coords.lat, p.coords.lon]);
  }

  renderMapLegend(lo, hi, RAMP);

  if (bounds.length > 3) { try { state.map.fitBounds(bounds, { padding:[20,20] }); } catch {} }
  setTimeout(() => { try { state.map.invalidateSize(); } catch {} }, 100);
}

/** Senza legenda una scala di colore non e' leggibile: dice "piu' scuro e'
 *  peggio" ma non quanto. Il colore porta la metrica, la dimensione il volume. */
function renderMapLegend(lo, hi, ramp) {
  const mapEl = document.getElementById("map");
  if (!mapEl || !mapEl.parentNode) return;

  let box = document.getElementById("mapLegend");
  if (!box) {
    box = document.createElement("div");
    box.id = "mapLegend";
    box.className = "map-legend";
    mapEl.parentNode.insertBefore(box, mapEl.nextSibling);
  }

  const swatches = ramp.map(function(c) {
    return '<span class="map-legend__swatch" style="background:' + c + '"></span>';
  }).join("");

  box.innerHTML =
    '<div class="map-legend__row"><span class="map-legend__label">' + etichettaMetricaMappa() + '</span>' +
    swatches +
    '<span class="map-legend__scale">' + fmtFloat(lo) + ' → ' + fmtFloat(hi) + '</span></div>' +
    '<div class="map-legend__row map-legend__note">La dimensione del cerchio indica il numero di corse osservate.</div>';
}

/* ────────────────── mobile memory management ────────────────── */

/**
 * On mobile, release heavy datasets that are no longer required by the
 * current filter combination.  This prevents holding 150+ MB of parsed
 * objects in memory simultaneously — the main cause of OOM crashes.
 */
function releaseUnusedDatasets() {
  if (!isMobile()) return;
  const station = hasStationFilter();
  const detail  = hasDetailFilter();

  // Detail datasets — only needed when a detail filter (day type / time slot) is active
  if (!detail) {
    if (state.data.kpiDetailCat.length)            { state.data.kpiDetailCat = [];            delete _lazyLoaded["kpi_dettaglio_categoria.csv"]; }
    if (state.data.histDetailCat.length)            { state.data.histDetailCat = [];            delete _lazyLoaded["hist_dettaglio_categoria.csv"]; }
    if (state.data.stationsDetailNode.length)       { state.data.stationsDetailNode = [];       delete _lazyLoaded["stazioni_dettaglio_categoria_nodo.csv"]; }
    if (state.data.histStationsDetailRuolo.length)  { state.data.histStationsDetailRuolo = [];  delete _lazyLoaded["hist_stazioni_dettaglio_categoria_ruolo.csv"]; }
    if (state.data.odDetailCat.length)              { state.data.odDetailCat = [];              delete _lazyLoaded["od_dettaglio_categoria.csv"]; }
  }

  // Station (OD) datasets — only needed when dep/arr is set
  if (!station) {
    if (state.data.odMonthCat.length)               { state.data.odMonthCat = [];               delete _lazyLoaded["od_mese_categoria.csv"]; }
    if (state.data.odDetailCat.length)              { state.data.odDetailCat = [];              delete _lazyLoaded["od_dettaglio_categoria.csv"]; }
    if (state.data.histStationsMonthRuolo.length)   { state.data.histStationsMonthRuolo = [];   delete _lazyLoaded["hist_stazioni_mese_categoria_ruolo.csv"]; }
    if (state.data.histStationsDetailRuolo.length)  { state.data.histStationsDetailRuolo = [];  delete _lazyLoaded["hist_stazioni_dettaglio_categoria_ruolo.csv"]; }
  }

  invalidateFilterCache();
}

/* ────────────────── mobile lazy loading ────────────────── */

const _lazyLoaded = {};
const _lazyLoading = {};  // Track in-progress loads to prevent duplicate concurrent fetches

/**
 * On mobile, build a row filter that drops rows outside the selected year
 * during parsing. This avoids allocating 200k+ objects that will immediately
 * be discarded by the filter pipeline.
 */
function mobileYearFilter() {
  if (!isMobile()) return null;
  var y = state.filters.year;
  if (!y || y === "all") return null;
  return function(row) {
    var m = row.mese || "";
    return m.slice(0, 4) === y;
  };
}

var _lazyLoadedYear = null;  // year selection the lazy cache was filled for

/** When the year changes, flush lazy-loaded data so it gets re-fetched for
 *  the new selection. This used to run on mobile only, but with year-sharded
 *  files desktop needs it too: otherwise a shard fetched for 2025 would stay
 *  in state after the user switched to 2026. */
function invalidateLazyOnYearChange() {
  var keys = Object.keys(_lazyLoaded);
  for (var i = 0; i < keys.length; i++) delete _lazyLoaded[keys[i]];
  var slotMap = {
    "od_mese_categoria.csv": "odMonthCat",
    "od_dettaglio_categoria.csv": "odDetailCat",
    "hist_stazioni_mese_categoria_ruolo.csv": "histStationsMonthRuolo",
    "hist_stazioni_dettaglio_categoria_ruolo.csv": "histStationsDetailRuolo",
    "kpi_dettaglio_categoria.csv": "kpiDetailCat",
    "hist_dettaglio_categoria.csv": "histDetailCat",
    "stazioni_dettaglio_categoria_nodo.csv": "stationsDetailNode",
    "stazioni_mese_categoria_nodo.csv": "stationsMonthNode"
  };
  for (var file in slotMap) {
    if (state.data[slotMap[file]]) state.data[slotMap[file]] = [];
  }
  _lazyLoadedYear = null;
  invalidateFilterCache();
}

/**
 * Files the build publishes one-per-year alongside the full-history file.
 * The detail and station tables are the heavy ones: od_dettaglio_categoria
 * alone was 45 MB of CSV that the browser had to parse in full the moment a
 * user touched a station filter, for a view that then showed a single year.
 */
function isYearSharded(fileName) {
  var sharded = (state.manifest && state.manifest.year_sharded) || [];
  var base = String(fileName).replace(/\.csv$/, "");
  return sharded.indexOf(base) !== -1;
}

/** Resolve a logical file name to the concrete paths to try, preferring the
 *  shard for the selected year and falling back to the full-history file. */
function shardCandidates(fileName) {
  var year = state.filters.year;
  var paths = [];
  if (year && year !== "all" && isYearSharded(fileName)) {
    var shard = String(fileName).replace(/\.csv$/, "." + year + ".csv");
    paths = paths.concat(candidateFilePaths(state.dataBase, shard));
  }
  return paths.concat(candidateFilePaths(state.dataBase, fileName));
}

async function lazyLoadCSV(fileName, stateKey) {
  if (_lazyLoadedYear !== null && _lazyLoadedYear !== state.filters.year) {
    invalidateLazyOnYearChange();
  }
  if (_lazyLoaded[fileName]) return;
  if (state.data[stateKey] && state.data[stateKey].length > 0) { _lazyLoaded[fileName] = true; return; }
  // If this file is already being loaded, wait for the existing load instead of starting a new one
  if (_lazyLoading[fileName]) return _lazyLoading[fileName];
  // L'anno per cui questo scaricamento e' partito. Al ritorno puo' non essere
  // piu' quello selezionato: cambiare anno mentre una fetch e' in volo non la
  // annulla, e la risposta vecchia scriveva comunque i suoi dati marcandoli
  // come "caricati per l'anno nuovo". Il file non veniva piu' richiesto e
  // restava a schermo un anno con i dati di un altro, o zero corse con i badge
  // del filtro che dicevano il contrario, finche' non si cambiava anno di nuovo.
  var annoRichiesto = state.filters.year;
  _lazyLoading[fileName] = (async function() {
    try {
      var t = await fetchTextAny(shardCandidates(fileName));
      if (state.filters.year !== annoRichiesto) return;  // risposta ormai vecchia
      var rows = t ? await parseCSVAsync(t, 5000, mobileYearFilter()) : [];
      t = null;  // Release text reference for GC
      if (state.filters.year !== annoRichiesto) return;
      state.data[stateKey] = rows;
      _lazyLoaded[fileName] = true;
      _lazyLoadedYear = annoRichiesto;
      invalidateFilterCache();
      enrichStationsRefFromFacts();
    } finally {
      delete _lazyLoading[fileName];
    }
  })();
  return _lazyLoading[fileName];
}

async function ensureStationsData() {
  await lazyLoadCSV("stazioni_mese_categoria_nodo.csv", "stationsMonthNode");
}

async function ensureOdData() {
  const wasEmpty = !state.data.odMonthCat || !state.data.odMonthCat.length;
  await lazyLoadCSV("od_mese_categoria.csv", "odMonthCat");
  if (wasEmpty && state.data.odMonthCat && state.data.odMonthCat.length) {
    rebuildStationDropdowns();
  }
}

async function ensureOdDetailData() {
  await lazyLoadCSV("od_dettaglio_categoria.csv", "odDetailCat");
}

async function ensureHistStationsData() {
  await lazyLoadCSV("hist_stazioni_mese_categoria_ruolo.csv", "histStationsMonthRuolo");
}

async function ensureHistStationsDetailData() {
  await lazyLoadCSV("hist_stazioni_dettaglio_categoria_ruolo.csv", "histStationsDetailRuolo");
}

async function ensureDetailData() {
  if (isMobile()) {
    await lazyLoadCSV("kpi_dettaglio_categoria.csv", "kpiDetailCat");
    await lazyLoadCSV("hist_dettaglio_categoria.csv", "histDetailCat");
  } else {
    await Promise.all([
      lazyLoadCSV("kpi_dettaglio_categoria.csv", "kpiDetailCat"),
      lazyLoadCSV("hist_dettaglio_categoria.csv", "histDetailCat")
    ]);
  }
}

async function ensureStationsDetailNodeData() {
  await lazyLoadCSV("stazioni_dettaglio_categoria_nodo.csv", "stationsDetailNode");
}

/* Load all station-specific detail datasets needed when combining station + detail filters */
async function ensureStationDetailData() {
  if (isMobile()) {
    await ensureOdDetailData();
    await ensureHistStationsDetailData();
  } else {
    await Promise.all([
      ensureOdDetailData(),
      ensureHistStationsDetailData()
    ]);
  }
}

/**
 * Ensure all datasets needed for the current filter combination are loaded.
 * Call this before renderAll() whenever filters change.
 * On mobile, loads are sequential to limit peak memory usage and
 * unused datasets are released first to free memory before new loads.
 */
async function ensureDataForCurrentFilters() {
  const station = hasStationFilter();
  const detail = hasDetailFilter();

  // On mobile, release datasets no longer required BEFORE loading new ones
  releaseUnusedDatasets();

  if (isMobile()) {
    // Sequential loading on mobile to limit peak memory
    if (station) {
      await ensureOdData();
      await ensureHistStationsData();
    }
    if (detail) {
      await ensureDetailData();
      await ensureStationsDetailNodeData();
    }
    if (station && detail) {
      await ensureStationDetailData();
    }
  } else {
    const loads = [];
    if (station) {
      loads.push(ensureOdData());
      loads.push(ensureHistStationsData());
    }
    if (detail) {
      loads.push(ensureDetailData());
      loads.push(ensureStationsDetailNodeData());
    }
    if (station && detail) {
      loads.push(ensureStationDetailData());
    }
    if (loads.length) await Promise.all(loads);
  }
}

/* ────────────────── collapsible extra filters ────────────────── */

function initFiltersToggle() {
  const toggle = document.getElementById("filtersToggle");
  const wrap = document.getElementById("filtersExtraWrap");
  if (!toggle || !wrap) return;

  // On mobile, start collapsed (CSS sets max-height:0)
  if (isMobile()) {
    toggle.classList.add("collapsed");
    wrap.classList.add("collapsed");
  }

  toggle.addEventListener("click", function() {
    const isCollapsed = wrap.classList.toggle("collapsed");
    toggle.classList.toggle("collapsed", isCollapsed);
  });
}

/* ────────────────── collapsible cards ────────────────── */

function initCollapsibleCards() {
  document.querySelectorAll(".card.collapsible").forEach(function(card) {
    const toggle = card.querySelector(".card-toggle");
    if (!toggle) return;
    toggle.addEventListener("click", function() {
      const collapsed = card.classList.toggle("card--collapsed");
      toggle.textContent = collapsed ? "\u25B6" : "\u25BC";
      if (!collapsed) {
        const chartEl = card.querySelector(".chart, .map");
        if (!chartEl) return;
        const id = chartEl.id;
        if (id === "chartDelayIndex" || id === "chartMonthly") {
          ensureDataForCurrentFilters().then(renderSeries);
        }
        else if (id === "chartHist") {
          ensureDataForCurrentFilters().then(renderHist);
        }
        else if (id === "map") {
          initMap();
          ensureStationsData().then(function() { renderMap(); setTimeout(function(){ try{state.map.invalidateSize();}catch{} },200); });
        }
        else if (id === "chartStationsTop10") {
          ensureStationsData().then(renderStationsTop10);
        }
        else if (id === "chartKm") {
          // La tabella per chilometro e' un file a parte, indipendente dai
          // filtri: si scarica alla prima apertura e non si ricalcola piu'.
          ensureKmData().then(renderKmRanking);
        }
        else if (id === "mapRete") {
          avviaMappaRete();
        }
      }
    });
  });
}

function initStationsMetricSel() {
  const sel = document.getElementById("stationsMetricSel");
  if (!sel) return;
  sel.onchange = function() { renderStationsTop10(); };
}

/* ────────────────── tratte per chilometro ────────────────── */

/**
 * La classifica per chilometro. Le tre metriche rispondono a domande diverse e
 * vanno lette insieme:
 *
 *   min_per_100km      minuti che l'orario concede per chilometro. E' la
 *                      lentezza della rete, e non dipende da come e' andata
 *                      quel giorno.
 *   ritardo_per_100km  minuti che il servizio aggiunge a quell'orario. E' la
 *                      lentezza della gestione.
 *   km_h_programmati   la prima letta al contrario, in unita' leggibili.
 *
 * Sulle 1.203 tratte pubblicate la correlazione fra le prime due e' 0,28: se
 * bastasse il ritardo per capire dove la rete non funziona, sarebbe vicina a 1.
 */
const METRICHE_KM = {
  min_per_100km:     { titolo: "Minuti d'orario per 100 km",     decrescente: true,  decimali: 0 },
  ritardo_per_100km: { titolo: "Minuti di ritardo per 100 km",   decrescente: true,  decimali: 1 },
  // Qui il caso peggiore e' il valore piu' basso, quindi l'ordine si inverte:
  // in cima deve restare la tratta messa peggio, come nelle altre due.
  km_h_programmati:  { titolo: "Velocità commerciale (km/h)",    decrescente: false, decimali: 0 }
};

// Venticinque invece di quindici: con quindici il grafico mostrava solo il
// pendolarismo piu' lento, e chi cercava una tratta lunga non trovava mai un
// termine di paragone. Sopra la trentina l'asse verticale diventa illeggibile.
const QUANTE_TRATTE_KM = 25;

// Le fasce servono a confrontare tratte confrontabili. La classifica su tutte
// le lunghezze premia per costruzione le piu' corte, dove manovre e fermate
// pesano su pochi chilometri: le quattro tratte che attraversano lo Stretto
// stanno intorno al novecentesimo posto su millecentosettanta, ma dentro la
// fascia oltre i cinquecento chilometri sono prima, seconda, terza e quinta su
// cinquantasei. Sono cioe' le piu' lente d'Italia fra le lunghe, e senza le
// fasce non c'era modo di vederlo.
const FASCE_KM = {
  "0-60": [0, 60],
  "60-200": [60, 200],
  "200-500": [200, 500],
  "500-99999": [500, 99999]
};

function fasciaKmSelezionata() {
  const sel = document.getElementById("kmLunghezzaSel");
  const v = sel ? String(sel.value || "all") : "all";
  return FASCE_KM[v] ? { chiave: v, min: FASCE_KM[v][0], max: FASCE_KM[v][1] } : null;
}

function getKmMetric() {
  const sel = document.getElementById("kmMetricSel");
  const v = sel ? sel.value : "min_per_100km";
  return METRICHE_KM[v] ? v : "min_per_100km";
}

async function ensureKmData() {
  if (state.data.km && state.data.km.length) return state.data.km;
  const file = (state.manifest && state.manifest.km_file) || "indicatori_km.csv";
  const t = await fetchTextAny([
    ...candidateFilePaths(ensureTrailingSlash(state.dataBase || "data/"), file),
    ...candidateFilePaths("data/", file)
  ]);
  state.data.km = t ? parseCSV(t) : [];
  return state.data.km;
}

function titoloTratta(r) {
  const a = String(r.partenza || "").trim();
  const b = String(r.arrivo || "").trim();
  return titleCase(a) + " – " + titleCase(b);
}

function titleCase(s) {
  return String(s).toLowerCase().replace(/(^|[\s'’-])([a-zà-ù])/g, (m, p, c) => p + c.toUpperCase());
}

function renderKmRanking() {
  const chart = document.getElementById("chartKm");
  if (!chart || isCardCollapsed(chart)) return;
  if (typeof Plotly !== "object") return;

  let righe = (state.data.km || []).filter((r) => Number.isFinite(toNum(r.km)) && toNum(r.km) > 0);
  const fascia = fasciaKmSelezionata();
  if (fascia) righe = righe.filter((r) => toNum(r.km) >= fascia.min && toNum(r.km) < fascia.max);
  const nota = document.getElementById("kmNote");
  if (!righe.length) {
    safePlotlyReact(chart, [], {}, { displayModeBar: false, responsive: true });
    if (nota) nota.textContent = "Dati per chilometro non disponibili in questa build.";
    return;
  }

  const metrica = getKmMetric();
  const cfg = METRICHE_KM[metrica];
  const ordinate = righe.slice().sort((a, b) =>
    cfg.decrescente ? toNum(b[metrica]) - toNum(a[metrica])
                    : toNum(a[metrica]) - toNum(b[metrica]));
  // Plotly disegna le barre orizzontali dal basso: si inverte per avere il
  // caso peggiore in cima.
  const top = ordinate.slice(0, QUANTE_TRATTE_KM).reverse();

  const etichette = top.map(titoloTratta);
  const valori = top.map((r) => toNum(r[metrica]));
  const rif = riferimentoStretto(righe, metrica, cfg, Math.max.apply(null, valori));
  // Su quanti mesi poggia la media, e fra quali estremi. Una tratta esistita
  // sette mesi sparsi fra il 2020 e il 2024 sta nella stessa classifica di una
  // osservata per sei anni di fila, e senza questo non c'e' modo di saperlo:
  // Costa Masnaga - Milano Porta Garibaldi e' quarta con sette mesi di dati.
  const dettaglio = top.map((r) => {
    const mesi = toNum(r.mesi);
    const arco = mesi > 0
      ? `, presente in ${fmtInt(mesi)} mesi` +
        (r.primo_mese && r.ultimo_mese ? ` (${r.primo_mese} → ${r.ultimo_mese})` : "")
      : "";
    return `${toNum(r.km).toFixed(0)} km, ${toNum(r.durata_media_min).toFixed(0)} min d'orario, ` +
      `${toNum(r.ritardo_medio_min).toFixed(1)} min di ritardo, ` +
      `${toNum(r.km_h_programmati).toFixed(0)} km/h, ${fmtInt(toNum(r.corse))} corse${arco}`;
  });

  safePlotlyReact(chart,
    [{
      x: valori, y: etichette, type: "bar", orientation: "h", name: cfg.titolo,
      marker: { color: "rgba(0,115,230,0.70)" },
      // hovertext, non text: con `text` Plotly stampa la riga dentro la barra e
      // il grafico diventa un muro di cifre illeggibile.
      hovertext: dettaglio, textposition: "none",
      hovertemplate: "<b>%{y}</b><br>%{x:.1f}<br>%{hovertext}<extra></extra>"
    }],
    Object.assign({
      // Sopra il grafico ci sono due etichette, il riferimento delle tratte via
      // nave e la media della platea: con dieci pixel di margine finivano fuori
      // dall'area e non si vedevano.
      margin: isMobile() ? { l:10, r:10, t:30, b:40 } : { l:230, r:30, t:30, b:50 },
      xaxis: { title: isMobile() ? "" : cfg.titolo, rangemode: "tozero" },
      yaxis: { automargin: true },
      paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", font: mobileFont()
    }, rif.layout),
    { displayModeBar: false, responsive: true }
  );

  if (nota) {
    const ufficiali = righe.filter((r) => String(r.qualita_km) === "ufficiale").length;
    // Che siano le peggiori, e in che verso si legga il grafico, non lo diceva
    // nessuna riga della pagina: il lettore poteva prenderle per un campione, o
    // peggio per le migliori, e sulla velocita' commerciale l'ordine si
    // capovolge perche' li' il caso peggiore e' il numero piu' basso.
    // Non tutte le tratte vivono per tutto il periodo, e la classifica non lo
    // diceva: 226 su 1.168 stanno sotto i due anni di presenza, 66 sotto
    // l'anno, e una tratta attiva solo durante una deviazione per lavori ha per
    // costruzione tempi peggiori, quindi sale in cima proprio perche' e' un
    // caso eccezionale. Il conto delle poco continue si fa sulle quindici
    // mostrate, che sono quelle che il lettore ha davanti.
    const MESI_CONTINUA = 24;
    const parziali = top.filter((r) => toNum(r.mesi) > 0 && toNum(r.mesi) < MESI_CONTINUA).length;
    nota.textContent =
      `Le ${top.length} messe peggio, dalla peggiore in cima, ` +
      (fascia
        ? `su ${fmtInt(righe.length)} tratte ${fascia.max >= 99999 ? "oltre i " + fascia.min : (fascia.min ? "fra " + fascia.min + " e " + fascia.max : "fino a " + fascia.max)} km; `
        : `su ${fmtInt(righe.length)} tratte con almeno 300 corse e 30 km; `) +
      `${Math.round(100 * ufficiali / righe.length)}% con la distanza ufficiale del RINF. ` +
      "Sotto i 30 km il rapporto per chilometro misura le manovre invece del viaggio." +
      (parziali > 0
        ? ` Attenzione: ${parziali} di queste ${top.length} esistono in meno di due anni di dati e non `
          + "sono confrontabili con le altre; il passaggio del mouse dice su quanti mesi poggia ciascuna."
        : "") +
      (rif.nota ? " " + rif.nota : "");
  }

  renderStretto(righe);
}

/**
 * La linea verticale che segna dove cadono le tratte che passano dallo Stretto.
 *
 * Con una metrica per chilometro quelle tratte non entrano mai in classifica:
 * i 122 minuti della nave, spalmati su 649 km di Roma-Messina, valgono 19
 * minuti per 100 km, e le quattro tratte via mare stanno intorno al
 * novecentesimo posto su millecentosessantotto. Non e' un difetto del calcolo,
 * e' che dividere per la lunghezza premia le tratte lunghe: la correlazione fra
 * chilometri e minuti per 100 km e' -0,53, e le prime quindici hanno una
 * lunghezza mediana di 37 km contro i 79 di tutte le altre.
 *
 * Una barra in fondo al grafico direbbe la stessa cosa allungandolo; una linea
 * verticale dice dove cadono senza toccare la classifica. La banda copre il
 * minimo e il massimo delle quattro, perche' un valore solo fingerebbe una
 * precisione che non c'e'.
 *
 * Quando il riferimento cade oltre la barra piu' lunga non si disegna, e lo
 * dice la nota sotto il grafico. Sulla velocita' commerciale succede: le prime
 * quindici stanno fra 25 e 31 km/h, le tratte via nave a 67-74, e per farle
 * entrare Plotly allungava l'asse fino a 83, schiacciando in un terzo dello
 * spazio proprio le barre che il grafico deve mettere a confronto. Un
 * riferimento accessorio non puo' rovinare la vista principale.
 */
/**
 * La media della platea con cui la classifica sta confrontando.
 *
 * Una barra a 89 minuti per cento chilometri non dice niente da sola: dice
 * molto se accanto c'e' il valore tipico delle tratte con cui la si sta
 * confrontando. Cambiando fascia il riferimento si sposta, ed e' il punto: le
 * lunghe percorrenze stanno intorno ai 54 minuti per cento chilometri, le
 * tratte fino a sessanta chilometri intorno ai 130, quindi una tratta lunga a
 * 89 e' lenta per la sua categoria mentre una corta a 89 sarebbe fra le piu'
 * veloci d'Italia.
 *
 * La media e' pesata sulle corse, non sulle tratte: conta quanto servizio
 * viaggia a quella velocita', non quante righe ci sono in tabella.
 */
function mediaDellaPlatea(righe, metrica) {
  let n = 0, d = 0;
  for (const r of righe) {
    const v = toNum(r[metrica]), peso = Math.max(0, toNum(r.corse));
    if (Number.isFinite(v) && v > 0 && peso > 0) { n += v * peso; d += peso; }
  }
  return d > 0 ? n / d : null;
}

function riferimentoStretto(righe, metrica, cfg, massimoBarre) {
  // Con la precisione della metrica, non con quella di fmtFloat: su una
  // classifica di numeri interi l'etichetta scriveva "80,94-88,99".
  const dec = cfg.decimali;
  const scrivi = (v) => v.toLocaleString("it-IT", { minimumFractionDigits: dec, maximumFractionDigits: dec });

  // La media della platea vale per ogni fascia, anche dove non passa nessuna
  // tratta via nave: si calcola prima di qualunque uscita anticipata, o sulle
  // fasce corte sparirebbe proprio dove serve di piu' per dare la misura.
  const media = mediaDellaPlatea(righe, metrica);
  const conMedia = media !== null && media > 0 && media <= massimoBarre;
  const formeMedia = conMedia ? [{
    type: "line", xref: "x", yref: "paper", x0: media, x1: media, y0: 0, y1: 1,
    line: { color: "#0f766e", width: 2 }
  }] : [];
  const noteMedia = conMedia ? [{
    x: media, xref: "x", y: 1.01, yref: "paper", yanchor: "bottom",
    xanchor: media > massimoBarre * 0.6 ? "right" : "left",
    text: "media: " + scrivi(media), showarrow: false,
    font: { size: isMobile() ? 9 : 11, color: "#0f766e" }
  }] : [];
  const soloMedia = { layout: conMedia ? { shapes: formeMedia, annotations: noteMedia } : {}, nota: "" };

  const VERO = new Set(["true", "1", "si", "sì", "vero"]);
  const valori = righe
    .filter((r) => VERO.has(String(r.attraversa_stretto).toLowerCase()))
    .map((r) => toNum(r[metrica]))
    .filter((v) => Number.isFinite(v) && v > 0)
    .sort((a, b) => a - b);
  if (!valori.length) return soloMedia;

  const lo = valori[0], hi = valori[valori.length - 1];
  const intervallo = scrivi(lo) === scrivi(hi) ? scrivi(lo) : scrivi(lo) + "-" + scrivi(hi);

  if (!(hi <= massimoBarre)) {
    // L'unita' sta fra parentesi nel titolo della metrica, dove c'e': senza,
    // la frase finirebbe con un numero nudo ("stanno a 67-74, fuori dalla").
    const unita = (String(cfg.titolo).match(/\(([^)]+)\)/) || [])[1];
    return {
      layout: soloMedia.layout,
      nota: `Le ${valori.length} tratte che attraversano lo Stretto stanno a ` +
            `${intervallo}${unita ? " " + unita : ""}, fuori dalla scala di questo grafico.`
    };
  }

  const linea = (x) => ({
    type: "line", xref: "x", yref: "paper", x0: x, x1: x, y0: 0, y1: 1,
    line: { color: "#b91c1c", width: 1.5, dash: "dot" }
  });
  const forme = [linea(lo)].concat(formeMedia);
  // Con una sola linea a coprire un intervallo si perderebbe l'estremo opposto:
  // meglio due linee e la fascia in mezzo, che e' dove stanno tutte e quattro.
  if (hi > lo) {
    forme.push(linea(hi));
    forme.push({
      type: "rect", xref: "x", yref: "paper", x0: lo, x1: hi, y0: 0, y1: 1,
      fillcolor: "rgba(185,28,28,0.07)", line: { width: 0 }
    });
  }

  return {
    layout: {
      shapes: forme,
      annotations: [{
        x: hi, xref: "x", y: 1, yref: "paper", yanchor: "bottom", xanchor: isMobile() ? "right" : "left",
        text: "tratte via nave: " + intervallo, showarrow: false,
        font: { size: isMobile() ? 9 : 11, color: "#b91c1c" }
      }].concat(noteMedia)
    },
    nota: ""
  };
}

/**
 * L'esempio che chiude l'avvertenza sulla mappa, calcolato invece che scritto.
 *
 * Il testo diceva "Milano-Piacenza risulta a 54 km/h": la media pesata sulle
 * corse delle tratte Milano-Piacenza e' 53,4, e il 54 era il valore della sola
 * Lambrate, ottocentottantasei corse su novantanovemila. Un numero battuto nel
 * codice si scolla dai dati alla prima ricostruzione, quindi qui si ricava.
 */
function esempioRegionali() {
  const righe = state.data.km || [];
  let n = 0, d = 0;
  for (const r of righe) {
    const a = String(r.partenza || ""), b = String(r.arrivo || "");
    const coppia = (a.startsWith("MILANO") && b === "PIACENZA") ||
                   (b.startsWith("MILANO") && a === "PIACENZA");
    if (!coppia) continue;
    const peso = toNum(r.corse), v = toNum(r.km_h_programmati);
    if (peso > 0 && v > 0) { n += peso * v; d += peso; }
  }
  if (!(d > 0)) return "";
  return " Milano-Piacenza risulta a " + Math.round(n / d) + " km/h per questo motivo.";
}

/**
 * Il costo della traversata dello Stretto, misurato invece che raccontato.
 *
 * Roma-Reggio Calabria e Roma-Messina sono lunghe uguali a cinque chilometri di
 * differenza, e la seconda ci mette due ore in piu'. Quelle due ore sono la
 * nave: manovra, imbarco, traversata, sbarco. E' l'unico punto della rete dove
 * il tempo non ha niente a che vedere con la distanza, e sui rapporti per
 * chilometro non si vede, perche' diluito su seicento chilometri sparisce.
 *
 * Il confronto si calcola dai dati pubblicati e non e' scritto a mano: se
 * l'orario cambia, cambia il numero.
 */
function renderStretto(righe) {
  const el = document.getElementById("kmStretto");
  if (!el) return;
  const trova = (a, b) => righe.find((r) =>
    (r.partenza === a && r.arrivo === b) || (r.partenza === b && r.arrivo === a));
  const viaNave = trova("MESSINA CENTRALE", "ROMA TERMINI");
  const viaTerra = trova("REGGIO CALABRIA CENTRALE", "ROMA TERMINI");
  if (!viaNave || !viaTerra) { el.textContent = ""; return; }

  const dKm = toNum(viaNave.km) - toNum(viaTerra.km);
  const dMin = toNum(viaNave.durata_media_min) - toNum(viaTerra.durata_media_min);
  if (!(dMin > 0) || Math.abs(dKm) > 40) { el.textContent = ""; return; }

  // "2h00" in mezzo a una frase si legge male: quando i minuti tornano tondi
  // si scrive "2 ore", che e' come lo direbbe chi la frase la legge ad alta voce.
  const ore = (m) => {
    const h = Math.floor(m / 60);
    const min = Math.round(m % 60);
    if (min === 0) return h + (h === 1 ? " ora" : " ore");
    return h + "h" + String(min).padStart(2, "0");
  };
  let testo =
    "<strong>La traversata dello Stretto costa " + Math.round(dMin) + " minuti.</strong> " +
    "Roma-Reggio Calabria è " + toNum(viaTerra.km).toFixed(0) + " km in " +
    ore(toNum(viaTerra.durata_media_min)) + ", Roma-Messina " +
    toNum(viaNave.km).toFixed(0) + " km in " + ore(toNum(viaNave.durata_media_min)) + ": " +
    // Anche questo si ricava: se un giorno la differenza scendesse a novanta
    // minuti, il titolo direbbe "90 minuti" e la frase "due ore".
    Math.abs(dKm).toFixed(0) + " km in meno e " + ore(dMin) + " in più, perché il treno sale sulla nave " +
    "(manovra, imbarco, traversata, sbarco). E si vede sulla velocità:";

  // Il confronto va fatto a parita' di lunghezza, altrimenti mente. Rapportate
  // a tutte le 1.192 tratte, quelle dello Stretto sembrano sopra la media: ma
  // quella platea e' fatta in maggioranza di regionali che fermano ovunque, e
  // un intercity di ottocento chilometri ci fa una figura facile. Confrontate
  // con le altre tratte lunghe, che e' il paragone giusto, stanno in fondo.
  const stretto = righe.filter((r) => VERO.has(String(r.attraversa_stretto).toLowerCase()));
  const media = (v) => {
    let n = 0, d = 0;
    for (const r of v) {
      const peso = toNum(r.corse), val = toNum(r.km_h_programmati);
      if (peso > 0 && val > 0) { n += peso * val; d += peso; }
    }
    return d > 0 ? n / d : NaN;
  };

  const SOGLIA_LUNGA_KM = 400;
  const lunghe = righe.filter((r) => toNum(r.km) >= SOGLIA_LUNGA_KM);
  const strettoLunghe = stretto.filter((r) => toNum(r.km) >= SOGLIA_LUNGA_KM);
  const altreLunghe = lunghe.filter((r) => !VERO.has(String(r.attraversa_stretto).toLowerCase()));

  if (strettoLunghe.length >= 2 && altreLunghe.length >= 10) {
    const vStretto = media(strettoLunghe);
    const vAltre = media(altreLunghe);
    const ordinate = lunghe.slice().sort((a, b) => toNum(b.km_h_programmati) - toNum(a.km_h_programmati));
    const posizioni = strettoLunghe
      .map((r) => ordinate.findIndex((x) => x === r) + 1)
      .sort((a, b) => a - b);

    const box = (etichetta, valore, sotto, forte) =>
      '<div class="mini-kpi' + (forte ? " mini-kpi--forte" : "") + '">' +
      '<div class="mini-kpi__label">' + etichetta + "</div>" +
      '<div class="mini-kpi__value">' + valore + "</div>" +
      '<div class="mini-kpi__note">' + sotto + "</div></div>";

    testo += '<div class="mini-kpis">' +
      box("Via Stretto", vStretto.toFixed(0) + " km/h",
          strettoLunghe.length + " tratte, " +
          fmtInt(strettoLunghe.reduce((s, r) => s + toNum(r.corse), 0)) + " corse", true) +
      box("Altre tratte lunghe", vAltre.toFixed(0) + " km/h",
          fmtInt(altreLunghe.length) + " tratte oltre " + SOGLIA_LUNGA_KM + " km", false) +
      box("Posizione in classifica",
          posizioni[0] + "ª–" + posizioni[posizioni.length - 1] + "ª",
          "su " + fmtInt(lunghe.length) + " tratte lunghe, dalla più veloce", false) +
      "</div>" +
      '<div class="mini-kpis__coda">Il confronto va fatto a parità di lunghezza: rapportate ' +
      "a tutte le " + fmtInt(righe.length) + " tratte queste sembrerebbero sopra la media, ma " +
      "quella platea è fatta soprattutto di regionali che fermano ovunque. " +
      "Fra le tratte lunghe, dove il paragone regge, sono fra le più lente d’Italia.</div>";
  }
  el.innerHTML = testo;
}

// I CSV scrivono i booleani come li scrive pandas, e il browser li rilegge come
// testo: meglio elencare le forme accettate che fidarsi di una sola.
const VERO = new Set(["true", "1", "vero", "sì", "si"]);


function initKmMetricSel() {
  const sel = document.getElementById("kmMetricSel");
  if (!sel) return;
  sel.onchange = function() { ensureKmData().then(renderKmRanking); };
  const selL = document.getElementById("kmLunghezzaSel");
  if (selL && !selL.dataset.collegato) {
    selL.dataset.collegato = "1";
    selL.onchange = function() { ensureKmData().then(renderKmRanking); };
  }
}

/* ────────────────── mappa della lentezza della rete ────────────────── */

/**
 * La classifica dice quali tratte sono lente, la mappa dice *dove*. Sono due
 * domande diverse: dalla classifica non si vede che la lentezza del Sud e'
 * continua da Battipaglia in giu', ne' che la dorsale tirrenica e quella
 * adriatica si comportano in modo opposto.
 *
 * Le linee sono i binari veri, non le congiungenti fra i capolinea: una retta
 * Roma-Siracusa taglierebbe il Tirreno, e una mappa che disegna treni sul mare
 * non la guarda nessuno. La geometria arriva da OpenStreetMap ed e' la stessa
 * su cui sono misurate le distanze.
 *
 * Le soglie non sono tonde per caso: 60 km/h e' la mediana della rete, 130 la
 * soglia sotto cui nessuna linea veloce dovrebbe stare. Il verde quindi non
 * vuol dire "buono in assoluto", vuol dire "fra i migliori di questa rete".
 */
const SCALA_RETE = [
  { fino: 40,       colore: "#b2182b", etichetta: "meno di 40" },
  { fino: 60,       colore: "#ef6548", etichetta: "40 - 60" },
  { fino: 80,       colore: "#fdae61", etichetta: "60 - 80" },
  // Il giallo chiaro della scala RdYlGn su fondo bianco e' invisibile: qui
  // serve un ambra che si veda, non il colore canonico.
  { fino: 100,      colore: "#e6b800", etichetta: "80 - 100" },
  { fino: 130,      colore: "#66bd63", etichetta: "100 - 130" },
  { fino: Infinity, colore: "#12703a", etichetta: "oltre 130" }
];

function coloreRete(kmh) {
  for (const s of SCALA_RETE) if (kmh < s.fino) return s.colore;
  return SCALA_RETE[SCALA_RETE.length - 1].colore;
}

async function ensureReteData() {
  if (state.data.rete) return state.data.rete;
  const base = ensureTrailingSlash(state.dataBase || "data/");
  const file = (state.manifest && state.manifest.rete_file) || "velocita_rete.geojson";
  const t = await fetchTextAny([
    ...candidateFilePaths(base, file),
    ...candidateFilePaths("data/", file)
  ]);
  try { state.data.rete = t ? JSON.parse(t) : null; }
  catch { state.data.rete = null; }
  return state.data.rete;
}

function initMappaRete() {
  const el = document.getElementById("mapRete");
  if (!el || state.mapRete) return;
  if (typeof L !== "object" || typeof L.map !== "function") return;
  state.mapRete = L.map("mapRete", { center: [42.0, 12.5], zoom: 5.5, zoomSnap: 0.5 });
  // Fondo in scala di grigi: su una mappa a colori pieni il rosso delle linee
  // si confonde con quello delle strade principali.
  L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors &copy; CARTO", maxZoom: 18
  }).addTo(state.mapRete);
  setTimeout(() => { try { state.mapRete.invalidateSize(); } catch {} }, 150);
}

function renderMappaRete() {
  const el = document.getElementById("mapRete");
  if (!el || isCardCollapsed(el) || !state.mapRete) return;
  const geo = state.data.rete;
  const nota = document.getElementById("reteNota");
  if (!geo || !geo.features || !geo.features.length) {
    if (nota) nota.textContent = "Geometria della rete non disponibile in questa build.";
    return;
  }
  if (state.reteLayer) return;   // si disegna una volta sola, non dipende dai filtri

  state.reteLayer = L.geoJSON(geo, {
    style: (f) => ({
      color: coloreRete(f.properties.kmh),
      weight: f.properties.kmh < 60 ? 3 : 2.2,
      opacity: 0.85
    }),
    onEachFeature: (f, strato) => {
      strato.bindTooltip(
        "<b>" + Math.round(f.properties.kmh) + " km/h</b> di media sui treni " +
        "che passano di qui<br>" + fmtInt(f.properties.corse) + " corse",
        { sticky: true });
    }
  }).addTo(state.mapRete);

  // La vista si adatta alla rete disegnata invece di partire da un centro
  // fisso, altrimenti l'Italia finisce in un angolo circondata da mezza Europa
  // vuota. L'ordine conta: invalidateSize prima, perche' fitBounds calcola lo
  // zoom sulle dimensioni che Leaflet crede di avere, e appena creata la mappa
  // dentro una scheda quelle dimensioni sono ancora quelle sbagliate.
  try {
    state.mapRete.invalidateSize();
    state.mapRete.fitBounds(state.reteLayer.getBounds(), { padding: [12, 12] });
  } catch {}

  renderLegendaRete(geo);
  if (nota) {
    nota.innerHTML =
      "Il colore è la media, pesata sulle corse, della velocità commerciale dei " +
      "treni che percorrono quel tratto, non la velocità della linea. " +
      "<strong>Due avvertenze per leggerla bene:</strong> la sorgente dà i tempi solo " +
      "ai capolinea e non alle fermate intermedie, quindi la velocità di un " +
      "viaggio si spalma uguale su tutto il percorso; e dove passano molti " +
      "regionali il tratto risulta lento anche su infrastruttura veloce, perché " +
      "sono loro a fare il numero delle corse." + esempioRegionali();
  }
}

function renderLegendaRete(geo) {
  const el = document.getElementById("reteLegenda");
  if (!el) return;
  const conta = SCALA_RETE.map(() => 0);
  for (const f of geo.features) {
    for (let i = 0; i < SCALA_RETE.length; i++) {
      if (f.properties.kmh < SCALA_RETE[i].fino) { conta[i]++; break; }
    }
  }
  el.innerHTML =
    '<span class="rete-legenda__titolo">Velocità commerciale (km/h)</span>' +
    SCALA_RETE.map((s, i) =>
      '<span class="rete-legenda__voce">' +
      '<i style="background:' + s.colore + '"></i>' + s.etichetta +
      '<b>' + conta[i] + '</b></span>').join("");
}

function avviaMappaRete() {
  const el = document.getElementById("mapRete");
  if (!el || isCardCollapsed(el)) return;
  initMappaRete();
  // Anche i chilometri, non solo la geometria: l'avvertenza sotto la mappa
  // chiude con un esempio calcolato su quella tabella, e chi apre la mappa
  // senza aver mai aperto la classifica per chilometro se lo vedeva sparire.
  Promise.all([ensureReteData(), ensureKmData()]).then(function() {
    renderMappaRete();
    // Secondo giro dopo che il disegno si e' assestato: se il contenitore ha
    // cambiato dimensione nel frattempo, la vista si riadatta.
    setTimeout(function() {
      try {
        state.mapRete.invalidateSize();
        if (state.reteLayer) {
          state.mapRete.fitBounds(state.reteLayer.getBounds(), { padding: [12, 12] });
        }
      } catch {}
    }, 300);
  });
}

/* ────────────────── filter badges ────────────────── */

function renderFilterBadges() {
  var f = state.filters;
  var badges = [];

  // Year
  if (f.year !== "all") badges.push({ label: "Anno: " + f.year, type: "active" });
  // Category
  if (f.cat !== "all") badges.push({ label: "Cat: " + f.cat, type: "active" });
  // Month range
  if (f.month_from || f.month_to) {
    // Con un solo estremo scelto il filtro tiene quel mese e basta (vedi
    // passMonthRange). L'etichetta scriveva "10 - ...", che si legge come "da
    // ottobre in poi" e prometteva tre mesi dove ne passava uno.
    var rangeLabel = f.month_from && f.month_to && f.month_from !== f.month_to
      ? "Mese: " + f.month_from + " \u2013 " + f.month_to
      : "Mese: " + (f.month_from || f.month_to);
    badges.push({ label: rangeLabel, type: "active" });
  }

  // Station filters (only apply to kpi, series, hist)
  var depLabel = "";
  var arrLabel = "";
  if (f.dep !== "all") {
    depLabel = etichettaStazioneSelezionata("depSel", f.dep);
    badges.push({ label: "Partenza: " + depLabel, type: "active", stationFilter: true, kind: "dep" });
  }
  if (f.arr !== "all") {
    arrLabel = etichettaStazioneSelezionata("arrSel", f.arr);
    badges.push({ label: "Arrivo: " + arrLabel, type: "active", stationFilter: true, kind: "arr" });
  }

  // Day type
  var dayNames = ["Infrasettimanale", "Fine settimana"];
  var dtOff = [];
  f.day_types.forEach(function(on, i) { if (!on) dtOff.push(dayNames[i]); });
  if (dtOff.length > 0 && dtOff.length < dayNames.length) {
    var dtOn = [];
    f.day_types.forEach(function(on, i) { if (on) dtOn.push(dayNames[i]); });
    badges.push({ label: "Giorno: " + dtOn.join(", "), type: "active" });
  }

  // Time slots
  var slotNames = ["Mattina", "Tarda matt.", "Pomeriggio", "Sera", "Notte"];
  var tsOff = [];
  f.time_slots.forEach(function(on, i) { if (!on) tsOff.push(slotNames[i]); });
  if (tsOff.length > 0 && tsOff.length < slotNames.length) {
    var tsOn = [];
    f.time_slots.forEach(function(on, i) { if (on) tsOn.push(slotNames[i]); });
    badges.push({ label: "Fascia: " + tsOn.join(", "), type: "active" });
  }

  // If no active filters, nothing to show
  var targets = [
    { id: "badgesDelayIndex", stationApplies: true },
    { id: "badgesSeries", stationApplies: true },
    { id: "badgesHist", stationApplies: true },
    { id: "badgesMap", stationApplies: false },
    { id: "badgesTop10", stationApplies: false }
  ];

  targets.forEach(function(t) {
    var el = document.getElementById(t.id);
    if (!el) return;
    el.innerHTML = "";
    if (badges.length === 0) return;

    badges.forEach(function(b) {
      if (b.stationFilter && !t.stationApplies) {
        // Show as non-applicable
        var span = document.createElement("span");
        span.className = "filter-badge filter-badge--na";
        span.textContent = b.label;
        span.title = "Questo filtro non si applica a questa vista";
        el.appendChild(span);
      } else {
        var span = document.createElement("span");
        span.className = "filter-badge filter-badge--active";
        span.textContent = b.label;
        el.appendChild(span);
      }
    });
  });
}

function renderAll() {
  renderFilterBadges();
  renderKPI();

  if (isMobile()) {
    // Stagger heavy chart renders across animation frames so the browser
    // can reclaim memory between draws and keep the UI responsive.
    // Each render is wrapped in try-catch so one failure doesn't block the rest.
    requestAnimationFrame(() => {
      try { renderSeries(); } catch (e) { console.error("renderSeries error:", e); }
      requestAnimationFrame(() => {
        try { renderHist(); } catch (e) { console.error("renderHist error:", e); }
        requestAnimationFrame(() => {
          try { renderStationsTop10(); } catch (e) { console.error("renderStationsTop10 error:", e); }
          requestAnimationFrame(() => { try { renderMap(); } catch (e) { console.error("renderMap error:", e); } });
        });
      });
    });
  } else {
    renderSeries();
    renderHist();
    renderStationsTop10();
    renderMap();
  }
}

/* ────────────────── data loading ────────────────── */

async function loadStationsDimAnyBase(primaryBase) {
  const base = ensureTrailingSlash(primaryBase);
  const tries = uniq([
    ...candidateFilePaths(base, "stations_dim.csv"),
    ...candidateFilePaths("data/", "stations_dim.csv"),
    ...candidateFilePaths("./data/", "stations_dim.csv")
  ]);
  for (const p of tries) {
    const t = await fetchTextOrNull(p);
    if (t && String(t).trim().length > 20) return parseCSV(t);
  }
  return [];
}

/**
 * Load the code→name lookup the build publishes once for every station code
 * appearing in any fact table. Roughly 30 KB, replacing name columns that used
 * to be repeated on every one of the millions of fact rows.
 */
async function loadStationNamesAnyBase(primaryBase) {
  const base = ensureTrailingSlash(primaryBase);
  const file = (state.manifest && state.manifest.station_names_file) || "station_names.csv";
  const tries = uniq([
    ...candidateFilePaths(base, file),
    ...candidateFilePaths("data/", file)
  ]);
  const t = await fetchTextAny(tries);
  if (!t) return 0;

  const rows = parseCSV(t);
  const pairs = rows.map((r) => [
    String(r.cod_stazione || "").trim(),
    String(r.nome_stazione || "").trim()
  ]);
  return mergeStationNames(pairs);
}

async function loadCapoluoghiAnyBase(primaryBase) {
  const base = ensureTrailingSlash(primaryBase);
  const tries = uniq([
    ...candidateFilePaths(base, "capoluoghi_provincia.csv"),
    ...candidateFilePaths("data/", "capoluoghi_provincia.csv"),
    ...candidateFilePaths("./data/", "capoluoghi_provincia.csv")
  ]);
  for (const p of tries) {
    const t = await fetchTextOrNull(p);
    if (t && String(t).trim().length > 5) return parseCSV(t);
  }
  return [];
}

/* Su quante giornate del mese esiste ciascuna categoria.
 *
 * Serve a distinguere "poche corse" da "poche giornate". La soglia delle trenta
 * corse copre il primo caso, non il secondo, e sono difetti diversi: con poche
 * corse la percentuale e' instabile, con poche giornate e' non rappresentativa,
 * e la seconda cosa non si aggiusta aspettando piu' dati.
 *
 * Il caso che ha portato a questa tabella: la sorgente pubblica due formati, e
 * quello vecchio non contiene le Frecce. Su 2.481 giorni di storico i giorni in
 * formato nuovo sono sei, e le Frecce esistono solo li'. Chi scegliesse
 * "FR - Freccia Rossa" leggeva "52,5% in ritardo a giugno 2026": le 202 corse
 * superavano la soglia, e venivano tutte dal 28 giugno. */
async function loadCoperturaCategoria(primaryBase) {
  const base = ensureTrailingSlash(primaryBase);
  const t = await fetchTextAny(uniq([
    ...candidateFilePaths(base, "copertura_categoria.csv"),
    ...candidateFilePaths("data/", "copertura_categoria.csv")
  ]));
  const mappa = new Map();
  for (const r of (t ? parseCSV(t) : [])) {
    const cat = String(r.categoria || "");
    const mese = String(r.mese || "").slice(0, 7);
    if (!mese) continue;
    if (!mappa.has(cat)) mappa.set(cat, new Map());
    mappa.get(cat).set(mese, {
      giorni: toNum(r.giorni_con_dati),
      giorniMese: toNum(r.giorni_nel_mese)
    });
  }
  return mappa;
}

/* Quota minima di giornate del mese perche' una percentuale sia rappresentativa.
 * Sotto un terzo il mese e' descritto da una manciata di giorni, che possono
 * essere tutti feriali o tutti festivi: il numero resta preciso e non dice
 * niente sul mese. */
const QUOTA_GIORNI_MINIMA = 1 / 3;

/** I mesi in cui la categoria scelta copre troppe poche giornate. */
function mesiPocheGiornate() {
  const cat = state.filters.cat;
  if (cat === "all" || !state.coperturaCat) return new Set();
  const perMese = state.coperturaCat.get(String(cat));
  if (!perMese) return new Set();
  const fuori = new Set();
  for (const [mese, o] of perMese) {
    if (o.giorniMese > 0 && o.giorni < o.giorniMese * QUOTA_GIORNI_MINIMA) fuori.add(mese);
  }
  return fuori;
}

async function loadAll() {
  setMeta("Caricamento dati...");

  const base = await pickDataBase();
  state.dataBase = base;

  const man = await fetchJsonAny(candidateFilePaths(base, "manifest.json"));
  state.manifest = man || safeManifestDefaults();

  const built = state.manifest && state.manifest.built_at_utc ? state.manifest.built_at_utc : "";
  // Da qui in avanti ogni CSV viene chiesto con questa versione in coda, cosi'
  // non puo' arrivare un file di una build diversa dal manifest appena letto.
  _versioneDati = built;
  setMeta(built ? "Aggiornamento: " + built : "Caricamento...");
  scriviQuotaScartate();

  const files = state.manifest && Array.isArray(state.manifest.gold_files) && state.manifest.gold_files.length
    ? state.manifest.gold_files : safeManifestDefaults().gold_files;

  // Skip heavy files on initial load (both mobile and desktop).
  // They will be lazy-loaded on demand when the user expands a card
  // or activates a filter that needs them.
  const HEAVY_FILES = new Set([
    "od_mese_categoria.csv",               // ~10 MB
    "od_dettaglio_categoria.csv",           // ~42 MB
    "hist_stazioni_mese_categoria_ruolo.csv",  // ~12 MB
    "hist_stazioni_dettaglio_categoria_ruolo.csv", // ~72 MB
    "stazioni_mese_categoria_nodo.csv",     // ~2.6 MB
    "stazioni_dettaglio_categoria_nodo.csv", // ~17 MB
    "stazioni_mese_categoria_ruolo.csv",    // ~4.6 MB
    "stazioni_dettaglio_categoria_ruolo.csv", // ~28 MB
    "kpi_dettaglio.csv",                    // detail
    "kpi_dettaglio_categoria.csv",          // detail
    "hist_dettaglio_categoria.csv"          // detail
  ]);

  const mobile = isMobile();

  const wanted = uniq([
    ...files,
    "kpi_mese.csv",
    "kpi_mese_categoria.csv",
    "kpi_dettaglio.csv",
    "kpi_dettaglio_categoria.csv",
    "hist_mese_categoria.csv",
    "hist_dettaglio_categoria.csv",
    "stazioni_mese_categoria_nodo.csv",
    "stazioni_dettaglio_categoria_nodo.csv",
    "od_mese_categoria.csv",
    "od_dettaglio_categoria.csv",
    "hist_stazioni_mese_categoria_ruolo.csv",
    "hist_stazioni_dettaglio_categoria_ruolo.csv"
  ]);

  // Only fetch lightweight files at startup; heavy ones are lazy-loaded
  const toFetch = wanted.filter((f) => !HEAVY_FILES.has(f));

  const texts = await Promise.all(toFetch.map((f) => fetchTextAny(candidateFilePaths(base, f))));

  const parsed = {};
  for (let i = 0; i < toFetch.length; i++) {
    parsed[toFetch[i]] = texts[i] ? await parseCSVAsync(texts[i]) : [];
  }

  state.data.kpiMonth              = parsed["kpi_mese.csv"] || [];
  state.data.kpiMonthCat           = parsed["kpi_mese_categoria.csv"] || [];
  state.data.kpiDetail             = parsed["kpi_dettaglio.csv"] || [];
  state.data.kpiDetailCat          = parsed["kpi_dettaglio_categoria.csv"] || [];
  state.data.histMonthCat          = parsed["hist_mese_categoria.csv"] || [];
  state.data.histDetailCat         = parsed["hist_dettaglio_categoria.csv"] || [];
  state.data.stationsMonthNode     = parsed["stazioni_mese_categoria_nodo.csv"] || [];
  state.data.stationsDetailNode    = parsed["stazioni_dettaglio_categoria_nodo.csv"] || [];
  state.data.odMonthCat            = parsed["od_mese_categoria.csv"] || [];
  state.data.odDetailCat           = parsed["od_dettaglio_categoria.csv"] || [];
  state.data.histStationsMonthRuolo  = parsed["hist_stazioni_mese_categoria_ruolo.csv"] || [];
  state.data.histStationsDetailRuolo = parsed["hist_stazioni_dettaglio_categoria_ruolo.csv"] || [];

  const stRows = await loadStationsDimAnyBase(base);
  state.stationsRef.clear();

  let stationDimBuilt = "";
  for (const r of stRows) {
    const b = r.built_at_utc || r.built_at || r.data_build || r.data || "";
    if (b && !stationDimBuilt) stationDimBuilt = String(b).trim();
    const code = String(r.cod_stazione || r.codice || r.cod || "").trim();
    if (!code) continue;
    const name = String(r.nome_stazione || r.nome_norm || r.nome || "").trim();
    const city = String(r.citta || r.comune || r.city || r.nome_comune || "").trim();
    const lat = parseNumberAny(r.lat ?? r.latitude ?? r.latitudine ?? r.y);
    const lon = parseNumberAny(r.lon ?? r.lng ?? r.longitude ?? r.longitudine ?? r.x);
    state.stationsRef.set(code, { code, name, lat: Number.isFinite(lat)?lat:NaN, lon: Number.isFinite(lon)?lon:NaN, city });
  }

  // Codes that changed over time (S01700 replacing N_ACF3D2764DA3 for Milano
  // Centrale) are missing from stations_dim.csv. They now arrive from the
  // published lookup instead of being harvested out of the fact tables.
  const namesAdded = await loadStationNamesAnyBase(base);
  if (namesAdded) console.log("station_names.csv: added " + namesAdded + " codici");
  enrichStationsRefFromFacts();

  const capRows = await loadCapoluoghiAnyBase(base);
  // I capoluoghi passano per la stessa normalizzazione dei nomi stazione, cosi'
  // che le due parti del confronto in capoluogoKey siano scritte nello stesso
  // alfabeto: normalizzare un solo lato lasciava fuori i capoluoghi che le
  // regole toccano.
  state.capoluoghiSet = new Set(
    capRows
      .map((r) => normalizeText(normalizeStationName(r.citta || r.capoluogo || r.nome || r.city || "")))
      .filter(Boolean)
  );

  state.coperturaCat = await loadCoperturaCategoria(base);

  // Enable tap-to-show tooltips on mobile
  document.querySelectorAll(".info-tip[data-tooltip]").forEach(function(tip) {
    tip.addEventListener("click", function(e) {
      e.stopPropagation();
      // Toggle a "tapped" class to show tooltip on mobile
      const isActive = tip.classList.contains("tip-active");
      document.querySelectorAll(".info-tip.tip-active").forEach(function(t) { t.classList.remove("tip-active"); });
      if (!isActive) tip.classList.add("tip-active");
    });
  });
  document.addEventListener("click", function() {
    document.querySelectorAll(".info-tip.tip-active").forEach(function(t) { t.classList.remove("tip-active"); });
  });

  // On mobile, default to the most recent year so we only process ~12 months
  // instead of the full 36+. Users can still select "Tutti" if they want.
  if (isMobile() && state.filters.year === "all") {
    const years = uniq(state.data.kpiMonth.map((r) => yearFromMonth(r.mese)).filter(Boolean)).sort();
    if (years.length) state.filters.year = years[years.length - 1];
  }

  initFilters();
  initToggleControls();
  initFiltersToggle();

  ensureHistToggle();
  initCollapsibleCards();
  initStationsMetricSel();
  initKmMetricSel();

  // On mobile, collapse all cards by default to reduce initial rendering cost
  if (isMobile()) {
    document.querySelectorAll(".card.collapsible").forEach(function(card) {
      if (!card.classList.contains("card--collapsed")) {
        card.classList.add("card--collapsed");
        const toggle = card.querySelector(".card-toggle");
        if (toggle) toggle.textContent = "\u25B6";
      }
    });
  }

  renderAll();
  avviaMappaDifferita();

  const haveAny =
    (state.data.kpiMonthCat && state.data.kpiMonthCat.length) ||
    (state.data.kpiDetailCat && state.data.kpiDetailCat.length) ||
    (state.data.histMonthCat && state.data.histMonthCat.length);

  const coordCount = Array.from(state.stationsRef.values()).filter((s) => Number.isFinite(s.lat) && Number.isFinite(s.lon)).length;
  console.log("Data stats: mese cat=" + (state.data.kpiMonthCat ? state.data.kpiMonthCat.length : 0) +
    ", dettaglio cat=" + (state.data.kpiDetailCat ? state.data.kpiDetailCat.length : 0) +
    ", stazioni dim=" + stRows.length + ", coord=" + coordCount);

  if (!haveAny) setMeta("Errore: non trovo dati validi");
  else setMeta(built ? "Aggiornamento: " + built : "");

  // Dismiss loading overlay and reveal content
  document.body.classList.add("loaded");
  var overlay = document.getElementById("loadingOverlay");
  if (overlay) {
    overlay.classList.add("hidden");
    setTimeout(function() { overlay.remove(); }, 500);
  }
}

loadAll().catch((err) => {
  console.error(err);
  setMeta("Errore caricamento dati: " + (err && err.message ? err.message : String(err)));
  document.body.classList.add("loaded");
  var overlay = document.getElementById("loadingOverlay");
  if (overlay) { overlay.classList.add("hidden"); setTimeout(function() { overlay.remove(); }, 500); }
});
