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

  if (msg && msg.includes("verticalFillMode")) {
    console.warn("Promise rejection Tabulator ignorata:", msg);
    return;
  }

  try {
    const el = document.getElementById("metaBox");
    if (el) el.innerText = "Promise rejection: " + msg;
  } catch {}
  console.error(r);
});

async function fetchText(path) {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new Error("Failed fetch " + path + " (" + r.status + ")");
  return await r.text();
}

async function fetchJson(path) {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new Error("Failed fetch " + path + " (" + r.status + ")");
  return await r.json();
}

async function fetchTextOrNull(path) {
  try {
    return await fetchText(path);
  } catch {
    return null;
  }
}

async function fetchJsonOrNull(path) {
  try {
    return await fetchJson(path);
  } catch {
    return null;
  }
}

const DATA_BASE_CANDIDATES = ["data/", "docs/data/", "site/data/"];

async function pickDataBase() {
  const probes = ["kpi_mese.csv", "kpi_mese_categoria.csv", "kpi_giorno_categoria.csv", "manifest.json"];
  for (const base of DATA_BASE_CANDIDATES) {
    for (const p of probes) {
      const t = await fetchTextOrNull(base + p);
      if (t && String(t).trim().length > 20) return base;
    }
  }
  return "data/";
}

function detectDelimiter(line) {
  const s = String(line || "");
  let comma = 0, semi = 0, tab = 0;
  let inQ = false;

  for (let i = 0; i < s.length; i++) {
    const ch = s[i];

    if (ch === '"') {
      if (inQ && s[i + 1] === '"') { i++; }
      else inQ = !inQ;
      continue;
    }

    if (!inQ) {
      if (ch === ",") comma++;
      else if (ch === ";") semi++;
      else if (ch === "\t") tab++;
    }
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

    if (ch === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
      else inQ = !inQ;
      continue;
    }

    if (ch === d && !inQ) { out.push(cur); cur = ""; continue; }
    cur += ch;
  }

  out.push(cur);
  return out;
}

function parseCSV(text) {
  const t = String(text || "").trim();
  if (!t) return [];

  const lines = t.split(/\r?\n/).filter(x => String(x || "").length);
  if (lines.length <= 1) return [];

  const delim = detectDelimiter(lines[0]);
  const header = splitCSVLine(lines[0], delim).map(x => String(x || "").trim());

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

function toNum(x) {
  const v = Number(x);
  return Number.isFinite(v) ? v : 0;
}

function uniq(arr) {
  return Array.from(new Set(arr));
}

function fmtInt(x) {
  return Math.round(x).toLocaleString("it-IT");
}

function fmtFloat(x) {
  const v = Number(x);
  if (!Number.isFinite(v)) return "";
  return v.toLocaleString("it-IT", { maximumFractionDigits: 2 });
}

function yearFromMonth(mese) {
  return String(mese).slice(0, 4);
}

function normalizeText(s) {
  const raw = String(s || "").toLowerCase().trim();
  const base = typeof raw.normalize === "function" ? raw.normalize("NFD") : raw;
  return base.replace(/[\u0300-\u036f]/g, "");
}

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
  const n = stationName(c, fallbackStationName);
  return n;
}

function stationCoords(code) {
  const c = String(code || "").trim();
  const ref = state.stationsRef.get(c);
  if (!ref) return null;
  const lat = Number(ref.lat);
  const lon = Number(ref.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return { lat, lon };
}

function buildStationItems(codes) {
  const items = (codes || []).map(code => {
    const name = stationName(code, code);
    return {
      code,
      name,
      needle: normalizeText(name + " " + code)
    };
  });
  items.sort((a, b) => a.name.localeCompare(b.name, "it", { sensitivity: "base" }));
  return items;
}

function fillStationSelect(selectEl, items, query) {
  if (!selectEl) return;
  const q = normalizeText(query);
  const cur = selectEl.value;

  selectEl.innerHTML = "";
  selectEl.appendChild(new Option("Tutte", "all"));

  for (const it of items) {
    if (q && !it.needle.includes(q)) continue;
    selectEl.appendChild(new Option(it.name + " (" + it.code + ")", it.code));
  }

  const stillThere = Array.from(selectEl.options).some(o => o.value === cur);
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

  input.oninput = () => fillStationSelect(selectEl, items, input.value || "");
}

const state = {
  dataBase: "data/",
  manifest: null,
  kpiDay: [],
  kpiDayCat: [],
  kpiMonth: [],
  kpiMonthCat: [],
  histMonthCat: [],
  histDayCat: [],
  stationsMonthNode: [],
  stationsDayNode: [],
  odMonthCat: [],
  odDayCat: [],
  stationsRef: new Map(),
  capoluoghiSet: new Set(),
  filters: {
    year: "all",
    cat: "all",
    dep: "all",
    arr: "all",
    day_from: "",
    day_to: ""
  },
  map: null,
  markers: [],
  tables: {
    stations: null,
    od: null,
    cities: null
  }
};

function setMeta(text) {
  const el = document.getElementById("metaBox");
  if (el) el.innerText = text;
}

function safeManifestDefaults() {
  return {
    built_at_utc: "",
    gold_files: [
      "hist_mese_categoria.csv",
      "kpi_giorno.csv",
      "kpi_giorno_categoria.csv",
      "kpi_mese.csv",
      "kpi_mese_categoria.csv",
      "od_mese_categoria.csv",
      "stazioni_mese_categoria_nodo.csv"
    ]
  };
}

function passCat(r) {
  if (state.filters.cat === "all") return true;
  return String(r.categoria || "").trim() === state.filters.cat;
}

function passDep(r) {
  if (state.filters.dep === "all") return true;
  return String(r.cod_partenza || "").trim() === state.filters.dep;
}

function passArr(r) {
  if (state.filters.arr === "all") return true;
  return String(r.cod_arrivo || "").trim() === state.filters.arr;
}

function passYear(r, field) {
  if (state.filters.year === "all") return true;
  const k = String(r[field] || "");
  if (field === "mese") return k.slice(0, 4) === state.filters.year;
  if (field === "giorno") return k.slice(0, 4) === state.filters.year;
  return true;
}

function parseISODate(d) {
  const s = String(d || "").trim();
  if (!s) return null;
  const dt = new Date(s + "T00:00:00");
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function hasDayFilter() {
  return !!(state.filters.day_from || state.filters.day_to);
}

function clampDateRange(dayFrom, dayTo) {
  const a = parseISODate(dayFrom);
  const b = parseISODate(dayTo);
  if (!a && !b) return { from: null, to: null };
  if (a && !b) return { from: a, to: a };
  if (!a && b) return { from: b, to: b };
  return a <= b ? { from: a, to: b } : { from: b, to: a };
}

function passDay(r, field) {
  if (!hasDayFilter()) return true;

  const key = String(r[field] || "").trim();
  if (!key) return false;

  const d = parseISODate(key);
  if (!d) return false;

  const rng = clampDateRange(state.filters.day_from, state.filters.day_to);
  if (!rng.from || !rng.to) return true;

  return d >= rng.from && d <= rng.to;
}

function passMonthFromDayRange(r, field) {
  if (!hasDayFilter()) return true;

  const key = String(r[field] || "").trim();
  if (!key) return false;

  const m0 = key + "-01";
  const md = parseISODate(m0);
  if (!md) return false;

  const rng = clampDateRange(state.filters.day_from, state.filters.day_to);
  if (!rng.from || !rng.to) return true;

  const start = new Date(md.getFullYear(), md.getMonth(), 1);
  const end = new Date(md.getFullYear(), md.getMonth() + 1, 0);

  return end >= rng.from && start <= rng.to;
}

function setCard(id, value) {
  const el = document.getElementById(id);
  if (!el) return;
  el.innerText = value;
}

function initDayControls() {
  const dayFrom = document.getElementById("dayFrom");
  const dayTo = document.getElementById("dayTo");
  if (dayFrom) dayFrom.onchange = () => { state.filters.day_from = dayFrom.value || ""; renderAll(); };
  if (dayTo) dayTo.onchange = () => { state.filters.day_to = dayTo.value || ""; renderAll(); };
}

function safeSetData(tab, data) {
  if (!tab) return;
  try { tab.setData(data); } catch {}
}

function initTables() {
  if (typeof Tabulator !== "function") return;

  const stationsEl = document.getElementById("stationsTable");
  const odEl = document.getElementById("odTable");
  const citiesEl = document.getElementById("citiesTable");

  if (stationsEl) {
    state.tables.stations = new Tabulator(stationsEl, {
      data: [],
      layout: "fitColumns",
      pagination: "local",
      paginationSize: 10,
      columns: [
        { title: "Stazione", field: "nome_stazione", sorter: "string" },
        { title: "Codice", field: "cod_stazione", sorter: "string", width: 110 },
        { title: "Corse", field: "corse_osservate", sorter: "number", hozAlign: "right", width: 110 },
        { title: "% ritardo", field: "pct_ritardo", sorter: "number", hozAlign: "right", width: 110, formatter: (c) => fmtFloat(c.getValue()) },
        { title: "Minuti", field: "minuti_ritardo_tot", sorter: "number", hozAlign: "right", width: 130 },
        { title: "Cancellati", field: "cancellate", sorter: "number", hozAlign: "right", width: 110 },
        { title: "Soppressi", field: "soppresse", sorter: "number", hozAlign: "right", width: 110 }
      ]
    });
  }

  if (odEl) {
    state.tables.od = new Tabulator(odEl, {
      data: [],
      layout: "fitColumns",
      pagination: "local",
      paginationSize: 10,
      columns: [
        { title: "Partenza", field: "nome_partenza", sorter: "string" },
        { title: "Arrivo", field: "nome_arrivo", sorter: "string" },
        { title: "Corse", field: "corse_osservate", sorter: "number", hozAlign: "right", width: 110 },
        { title: "% ritardo", field: "pct_ritardo", sorter: "number", hozAlign: "right", width: 110, formatter: (c) => fmtFloat(c.getValue()) },
        { title: "Minuti", field: "minuti_ritardo_tot", sorter: "number", hozAlign: "right", width: 130 }
      ]
    });
  }

  if (citiesEl) {
    state.tables.cities = new Tabulator(citiesEl, {
      data: [],
      layout: "fitColumns",
      pagination: "local",
      paginationSize: 10,
      columns: [
        { title: "Città", field: "city", sorter: "string" },
        { title: "Corse", field: "corse_osservate", sorter: "number", hozAlign: "right", width: 110 },
        { title: "% ritardo", field: "pct_ritardo", sorter: "number", hozAlign: "right", width: 110, formatter: (c) => fmtFloat(c.getValue()) },
        { title: "Minuti", field: "minuti_ritardo_tot", sorter: "number", hozAlign: "right", width: 130 },
        { title: "Cancellati", field: "cancellate", sorter: "number", hozAlign: "right", width: 110 },
        { title: "Soppressi", field: "soppresse", sorter: "number", hozAlign: "right", width: 110 }
      ]
    });
  }
}

function initFilters() {
  const years = uniq(state.kpiMonth.map(r => yearFromMonth(r.mese))).sort();
  const cats = uniq(state.kpiMonthCat.map(r => r.categoria)).sort((a, b) => String(a).localeCompare(String(b), "it", { sensitivity: "base" }));

  const yearSel = document.getElementById("yearSel");
  if (yearSel) {
    yearSel.innerHTML = "";
    yearSel.appendChild(new Option("Tutti", "all"));
    years.forEach(y => yearSel.appendChild(new Option(y, y)));
    yearSel.onchange = () => { state.filters.year = yearSel.value; renderAll(); };
  }

  const catSel = document.getElementById("catSel");
  if (catSel) {
    catSel.innerHTML = "";
    catSel.appendChild(new Option("Tutte", "all"));
    cats.forEach(c => catSel.appendChild(new Option(c, c)));
    catSel.onchange = () => { state.filters.cat = catSel.value; renderAll(); };
  }

  const depSel = document.getElementById("depSel");
  const arrSel = document.getElementById("arrSel");

  const deps = uniq([...(state.odMonthCat || []).map(r => r.cod_partenza), ...(state.odDayCat || []).map(r => r.cod_partenza)].filter(Boolean));
  const arrs = uniq([...(state.odMonthCat || []).map(r => r.cod_arrivo), ...(state.odDayCat || []).map(r => r.cod_arrivo)].filter(Boolean));

  const depItems = buildStationItems(deps);
  const arrItems = buildStationItems(arrs);

  fillStationSelect(depSel, depItems, "");
  fillStationSelect(arrSel, arrItems, "");

  ensureSearchInput(depSel, "depSearch", "Cerca stazione di partenza", depItems);
  ensureSearchInput(arrSel, "arrSearch", "Cerca stazione di arrivo", arrItems);

  if (depSel) depSel.onchange = () => { state.filters.dep = depSel.value; renderAll(); };
  if (arrSel) arrSel.onchange = () => { state.filters.arr = arrSel.value; renderAll(); };

  const resetBtn = document.getElementById("resetBtn");
  if (resetBtn) {
    resetBtn.onclick = () => {
      state.filters.year = "all";
      state.filters.cat = "all";
      state.filters.dep = "all";
      state.filters.arr = "all";
      state.filters.day_from = "";
      state.filters.day_to = "";

      if (yearSel) yearSel.value = "all";
      if (catSel) catSel.value = "all";
      if (depSel) depSel.value = "all";
      if (arrSel) arrSel.value = "all";

      const dayFrom = document.getElementById("dayFrom");
      const dayTo = document.getElementById("dayTo");
      if (dayFrom) dayFrom.value = "";
      if (dayTo) dayTo.value = "";

      renderAll();
    };
  }
}

function initMap() {
  const mapEl = document.getElementById("map");
  if (!mapEl) return;

  if (typeof L !== "object" || typeof L.map !== "function") return;

  state.map = L.map("map", {
    center: [42.5, 12.5],
    zoom: 6,
    zoomSnap: 0.5
  });

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors",
    maxZoom: 18
  }).addTo(state.map);
}

function clearMarkers() {
  if (!state.map) return;
  for (const m of state.markers) {
    try { state.map.removeLayer(m); } catch {}
  }
  state.markers = [];
}

function clampPct(x) {
  const v = Number(x);
  if (!Number.isFinite(v)) return 0;
  return Math.max(0, Math.min(100, v));
}

function hasValidCoords(c) {
  if (!c) return false;
  const lat = Number(c.lat);
  const lon = Number(c.lon);
  return Number.isFinite(lat) && Number.isFinite(lon);
}

function mapMetricValue(row, metric) {
  if (!row) return 0;
  if (metric === "pct_ritardo") {
    const n = toNum(row.corse_osservate);
    const d = toNum(row.in_ritardo);
    return n > 0 ? (d / n) * 100 : 0;
  }
  if (metric === "minuti_ritardo_tot") return toNum(row.minuti_ritardo_tot);
  if (metric === "ritardo_medio") return toNum(row.ritardo_medio);
  if (metric === "p90") return toNum(row.p90);
  if (metric === "p95") return toNum(row.p95);
  return 0;
}

function normalizeBucketLabel(s) {
  return String(s || "").replace(/\s+/g, "").trim();
}

function initHistModeToggle() {
  const sel = document.getElementById("histModeSel");
  if (!sel) return;
  sel.onchange = () => renderAll();
}

function getHistMode() {
  const sel = document.getElementById("histModeSel");
  if (!sel) return "month";
  const v = String(sel.value || "month");
  return v === "day" ? "day" : "month";
}

function getMapMetric() {
  const sel = document.getElementById("mapMetricSel");
  if (!sel) return "pct_ritardo";
  return String(sel.value || "pct_ritardo");
}

function renderKPI() {
  const useDay = hasDayFilter() && state.kpiDayCat.length > 0;

  const base = useDay ? state.kpiDayCat : state.kpiMonthCat;
  const keyField = useDay ? "giorno" : "mese";

  let rows = base;
  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (useDay) rows = rows.filter(r => passDay(r, "giorno"));
  if (!useDay && hasDayFilter()) rows = rows.filter(r => passMonthFromDayRange(r, "mese"));

  const total = rows.reduce((a, r) => a + toNum(r.corse_osservate), 0);
  const inrit = rows.reduce((a, r) => a + toNum(r.in_ritardo), 0);
  const minTot = rows.reduce((a, r) => a + toNum(r.minuti_ritardo_tot), 0);
  const canc = rows.reduce((a, r) => a + toNum(r.cancellate), 0);
  const sopp = rows.reduce((a, r) => a + toNum(r.soppresse), 0);

  setCard("cardTotal", fmtInt(total));
  setCard("cardLate", fmtInt(inrit));
  setCard("cardMin", fmtInt(minTot));
  setCard("cardCanc", fmtInt(canc));
  setCard("cardSopp", fmtInt(sopp));
}

function seriesDataDaily() {
  let rows = state.kpiDayCat.length ? state.kpiDayCat : state.kpiDay;
  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, "giorno"));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (hasDayFilter()) rows = rows.filter(r => passDay(r, "giorno"));

  const byDay = new Map();

  for (const r of rows) {
    const day = String(r.giorno || "").trim();
    if (!day) continue;
    if (!byDay.has(day)) byDay.set(day, { giorno: day, corse: 0, rit: 0 });
    const o = byDay.get(day);
    o.corse += toNum(r.corse_osservate);
    o.rit += toNum(r.in_ritardo);
  }

  const out = Array.from(byDay.values()).sort((a, b) => String(a.giorno).localeCompare(String(b.giorno)));
  const x = out.map(o => o.giorno);
  const y = out.map(o => o.corse > 0 ? (o.rit / o.corse) * 100 : 0);

  return { x, y };
}

function seriesDataMonthly() {
  let rows = state.kpiMonthCat.length ? state.kpiMonthCat : state.kpiMonth;
  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, "mese"));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (hasDayFilter()) rows = rows.filter(r => passMonthFromDayRange(r, "mese"));

  const byM = new Map();

  for (const r of rows) {
    const m = String(r.mese || "").trim();
    if (!m) continue;
    if (!byM.has(m)) byM.set(m, { mese: m, corse: 0, rit: 0 });
    const o = byM.get(m);
    o.corse += toNum(r.corse_osservate);
    o.rit += toNum(r.in_ritardo);
  }

  const out = Array.from(byM.values()).sort((a, b) => String(a.mese).localeCompare(String(b.mese)));
  const x = out.map(o => o.mese);
  const y = out.map(o => o.corse > 0 ? (o.rit / o.corse) * 100 : 0);

  return { x, y };
}

function renderSeries() {
  if (typeof Plotly !== "object") return;

  const d = seriesDataDaily();
  const m = seriesDataMonthly();

  const t1 = document.getElementById("chartDay");
  const t2 = document.getElementById("chartMonth");

  if (t1) {
    Plotly.react(t1, [{
      x: d.x,
      y: d.y,
      type: "scatter",
      mode: "lines+markers",
      name: "% in ritardo"
    }], {
      margin: { l: 50, r: 20, t: 10, b: 50 },
      yaxis: { title: "% in ritardo", rangemode: "tozero" },
      xaxis: { type: "category" },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { color: "#e8eefc" }
    }, { displayModeBar: false, responsive: true });
  }

  if (t2) {
    Plotly.react(t2, [{
      x: m.x,
      y: m.y,
      type: "scatter",
      mode: "lines+markers",
      name: "% in ritardo"
    }], {
      margin: { l: 50, r: 20, t: 10, b: 50 },
      yaxis: { title: "% in ritardo", rangemode: "tozero" },
      xaxis: { type: "category" },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { color: "#e8eefc" }
    }, { displayModeBar: false, responsive: true });
  }
}

function renderHist() {
  if (typeof Plotly !== "object") return;

  const mode = getHistMode();
  const useDay = mode === "day" && state.histDayCat.length > 0;
  const base = useDay ? state.histDayCat : state.histMonthCat;
  const keyField = useDay ? "giorno" : "mese";

  let rows = base;
  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (useDay && hasDayFilter()) rows = rows.filter(r => passDay(r, "giorno"));
  if (!useDay && hasDayFilter()) rows = rows.filter(r => passMonthFromDayRange(r, "mese"));

  const byBucket = new Map();

  for (const r of rows) {
    const b = normalizeBucketLabel(r.bucket_ritardo_arrivo || "");
    if (!b) continue;
    if (!byBucket.has(b)) byBucket.set(b, { bucket: r.bucket_ritardo_arrivo, count: 0 });
    const o = byBucket.get(b);
    o.count += toNum(r.count);
  }

  const bucketsOrder = (state.manifest && Array.isArray(state.manifest.delay_bucket_labels))
    ? state.manifest.delay_bucket_labels
    : [
      "<=-60",
      "(-60,-30]",
      "(-30,-15]",
      "(-15,-10]",
      "(-10,-5]",
      "(-5,-1]",
      "(-1,0]",
      "(0,1]",
      "(1,5]",
      "(5,10]",
      "(10,15]",
      "(15,30]",
      "(30,60]",
      "(60,120]",
      ">120"
    ];

  const orderedKeys = bucketsOrder.map(normalizeBucketLabel);
  const x = [];
  const y = [];

  for (let i = 0; i < bucketsOrder.length; i++) {
    const k = orderedKeys[i];
    const obj = byBucket.get(k);
    x.push(bucketsOrder[i]);
    y.push(obj ? obj.count : 0);
  }

  const el = document.getElementById("chartHist");
  if (!el) return;

  Plotly.react(el, [{
    x,
    y,
    type: "bar",
    name: "Conteggio"
  }], {
    margin: { l: 50, r: 20, t: 10, b: 70 },
    yaxis: { title: "Conteggio", rangemode: "tozero" },
    xaxis: { tickangle: -35 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e8eefc" }
  }, { displayModeBar: false, responsive: true });
}

function renderTables() {
  const useDay = hasDayFilter() && state.stationsDayNode.length > 0;
  const baseStations = useDay ? state.stationsDayNode : state.stationsMonthNode;
  const keyField = useDay ? "giorno" : "mese";

  let rows = baseStations;
  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (useDay && hasDayFilter()) rows = rows.filter(r => passDay(r, "giorno"));
  if (!useDay && hasDayFilter()) rows = rows.filter(r => passMonthFromDayRange(r, "mese"));

  const agg = new Map();

  for (const r of rows) {
    const code = String(r.cod_stazione || "").trim();
    if (!code) continue;
    if (!agg.has(code)) agg.set(code, {
      cod_stazione: code,
      nome_stazione: stationName(code, r.nome_stazione || ""),
      corse_osservate: 0,
      in_ritardo: 0,
      minuti_ritardo_tot: 0,
      cancellate: 0,
      soppresse: 0
    });

    const a = agg.get(code);
    a.corse_osservate += toNum(r.corse_osservate);
    a.in_ritardo += toNum(r.in_ritardo);
    a.minuti_ritardo_tot += toNum(r.minuti_ritardo_tot);
    a.cancellate += toNum(r.cancellate);
    a.soppresse += toNum(r.soppresse);
  }

  let out = Array.from(agg.values());
  out.forEach(o => {
    o.pct_ritardo = o.corse_osservate > 0 ? (o.in_ritardo / o.corse_osservate) * 100 : 0;
  });

  out.sort((a, b) => toNum(b.pct_ritardo) - toNum(a.pct_ritardo));
  out = out.slice(0, 200);

  safeSetData(state.tables.stations, out);
  try { state.tables.stations.setSort("pct_ritardo", "desc"); } catch {}

  const useDayOD = hasDayFilter() && state.odDayCat.length > 0;
  const baseOD = useDayOD ? state.odDayCat : state.odMonthCat;
  const keyFieldOD = useDayOD ? "giorno" : "mese";

  let od = baseOD;
  if (state.filters.year !== "all") od = od.filter(r => passYear(r, keyFieldOD));
  if (state.filters.cat !== "all") od = od.filter(passCat);
  if (useDayOD && hasDayFilter()) od = od.filter(r => passDay(r, "giorno"));
  if (!useDayOD && hasDayFilter()) od = od.filter(r => passMonthFromDayRange(r, "mese"));
  if (state.filters.dep !== "all") od = od.filter(passDep);
  if (state.filters.arr !== "all") od = od.filter(passArr);

  const odOut = od.map(r => {
    const corse = toNum(r.corse_osservate);
    const rit = toNum(r.in_ritardo);
    return {
      cod_partenza: r.cod_partenza,
      cod_arrivo: r.cod_arrivo,
      nome_partenza: stationName(r.cod_partenza, r.nome_partenza),
      nome_arrivo: stationName(r.cod_arrivo, r.nome_arrivo),
      corse_osservate: corse,
      pct_ritardo: corse > 0 ? (rit / corse) * 100 : 0,
      minuti_ritardo_tot: toNum(r.minuti_ritardo_tot)
    };
  });

  odOut.sort((a, b) => toNum(b.pct_ritardo) - toNum(a.pct_ritardo));
  const odTop = odOut.slice(0, 200);

  safeSetData(state.tables.od, odTop);
  try { state.tables.od.setSort("pct_ritardo", "desc"); } catch {}
}

function renderMap() {
  if (!state.map) return;

  clearMarkers();

  const metric = getMapMetric();

  const useDay = hasDayFilter() && state.stationsDayNode.length > 0;
  const baseStations = useDay ? state.stationsDayNode : state.stationsMonthNode;
  const keyField = useDay ? "giorno" : "mese";

  let rows = baseStations;
  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (useDay && hasDayFilter()) rows = rows.filter(r => passDay(r, "giorno"));
  if (!useDay && hasDayFilter()) rows = rows.filter(r => passMonthFromDayRange(r, "mese"));

  const agg = new Map();

  for (const r of rows) {
    const code = String(r.cod_stazione || "").trim();
    if (!code) continue;

    const coords = stationCoords(code);
    if (!hasValidCoords(coords)) continue;

    if (!agg.has(code)) agg.set(code, {
      code,
      coords,
      nome: stationName(code, r.nome_stazione || ""),
      corse_osservate: 0,
      in_ritardo: 0,
      minuti_ritardo_tot: 0,
      ritardo_medio: 0,
      p90: 0,
      p95: 0
    });

    const a = agg.get(code);
    a.corse_osservate += toNum(r.corse_osservate);
    a.in_ritardo += toNum(r.in_ritardo);
    a.minuti_ritardo_tot += toNum(r.minuti_ritardo_tot);
    a.ritardo_medio += toNum(r.ritardo_medio) * toNum(r.corse_osservate);
    a.p90 += toNum(r.p90) * toNum(r.corse_osservate);
    a.p95 += toNum(r.p95) * toNum(r.corse_osservate);
  }

  const pts = Array.from(agg.values()).map(o => {
    const n = toNum(o.corse_osservate);
    const ritm = n > 0 ? o.ritardo_medio / n : 0;
    const p90 = n > 0 ? o.p90 / n : 0;
    const p95 = n > 0 ? o.p95 / n : 0;

    const row = {
      corse_osservate: n,
      in_ritardo: toNum(o.in_ritardo),
      minuti_ritardo_tot: toNum(o.minuti_ritardo_tot),
      ritardo_medio: ritm,
      p90: p90,
      p95: p95
    };

    const v = mapMetricValue(row, metric);
    return { ...o, v };
  });

  pts.sort((a, b) => toNum(b.v) - toNum(a.v));

  const top = pts.slice(0, 200);

  for (const p of top) {
    const val = p.v;
    const label = p.nome + "\n" + fmtFloat(val);
    const m = L.circleMarker([p.coords.lat, p.coords.lon], {
      radius: 6 + Math.sqrt(Math.max(0, val)) * 0.2,
      opacity: 0.9,
      fillOpacity: 0.6
    }).addTo(state.map);

    try { m.bindPopup(label); } catch {}
    state.markers.push(m);
  }
}

function isCapoluogoCity(city) {
  const k = normalizeText(city);
  return state.capoluoghiSet.has(k);
}

function initAgg(city) {
  return {
    city,
    corse_osservate: 0,
    in_ritardo: 0,
    minuti_ritardo_tot: 0,
    cancellate: 0,
    soppresse: 0,
    pct_ritardo: 0
  };
}

function renderCities() {
  const metricSel = document.getElementById("citiesMetricSel");
  const metric = metricSel ? String(metricSel.value || "pct_ritardo") : "pct_ritardo";

  const minSel = document.getElementById("citiesMinSel");
  const minN = minSel ? Number(minSel.value || 50) : 50;

  const modeSel = document.getElementById("citiesModeSel");
  const mode = modeSel ? String(modeSel.value || "from_station_node") : "from_station_node";

  const agg = new Map();

  if (mode === "from_station_node") {
    const useDay = hasDayFilter() && state.stationsDayNode.length > 0;
    const base = useDay ? state.stationsDayNode : state.stationsMonthNode;
    const keyField = useDay ? "giorno" : "mese";

    let rows = base;
    if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, keyField));
    if (state.filters.cat !== "all") rows = rows.filter(passCat);
    if (useDay && hasDayFilter()) rows = rows.filter(r => passDay(r, "giorno"));
    if (!useDay && hasDayFilter()) rows = rows.filter(r => passMonthFromDayRange(r, "mese"));

    for (const r of rows) {
      const n = toNum(r.corse_osservate);
      if (n <= 0) continue;

      const code = String(r.cod_stazione || "").trim();
      if (!code) continue;

      const city = stationCity(code, r.nome_stazione || code);
      if (!city) continue;
      if (!isCapoluogoCity(city)) continue;

      const k = normalizeText(city);
      if (!agg.has(k)) agg.set(k, initAgg(city));

      const a = agg.get(k);
      a.corse_osservate += n;
      a.in_ritardo += toNum(r.in_ritardo);
      a.minuti_ritardo_tot += toNum(r.minuti_ritardo_tot);
      a.cancellate += toNum(r.cancellate);
      a.soppresse += toNum(r.soppresse);
    }
  } else {
    const useDay = hasDayFilter() && state.odDayCat.length > 0;
    const base = useDay ? state.odDayCat : state.odMonthCat;
    const keyField = useDay ? "giorno" : "mese";

    let od = base;
    if (state.filters.year !== "all") od = od.filter(r => passYear(r, keyField));
    if (state.filters.cat !== "all") od = od.filter(passCat);
    if (useDay) od = od.filter(r => passDay(r, "giorno"));
    if (!useDay && hasDayFilter()) od = od.filter(r => passMonthFromDayRange(r, "mese"));
    if (state.filters.dep !== "all") od = od.filter(passDep);
    if (state.filters.arr !== "all") od = od.filter(passArr);

    const groupField = mode === "from_dep_rank_arr_city" ? "cod_arrivo" : "cod_partenza";

    for (const r of od) {
      const n = toNum(r.corse_osservate);
      if (n <= 0) continue;

      const code = String(r[groupField] || "").trim();
      const city = stationCity(code, code);
      if (!city) continue;
      if (!isCapoluogoCity(city)) continue;

      const k = normalizeText(city);
      if (!agg.has(k)) agg.set(k, initAgg(city));

      const a = agg.get(k);
      a.corse_osservate += n;
      a.in_ritardo += toNum(r.in_ritardo);
      a.minuti_ritardo_tot += toNum(r.minuti_ritardo_tot);
      a.cancellate += toNum(r.cancellate);
      a.soppresse += toNum(r.soppresse);
    }
  }

  let out = Array.from(agg.values());
  out.forEach(o => {
    o.pct_ritardo = o.corse_osservate > 0 ? (o.in_ritardo / o.corse_osservate) * 100 : 0;
  });

  out = out.filter(o => o.corse_osservate >= minN);
  out.sort((a, b) => toNum(b[metric]) - toNum(a[metric]));
  out = out.slice(0, 50);

  safeSetData(state.tables.cities, out);
  try {
    state.tables.cities.setSort(metric, "desc");
  } catch {}
}

function renderAll() {
  renderKPI();
  renderSeries();
  renderHist();
  renderTables();
  renderMap();
  renderCities();
}

async function loadAll() {
  setMeta("Caricamento dati...");

  const base = await pickDataBase();
  state.dataBase = base;

  const man = await fetchJsonOrNull(base + "manifest.json");
  state.manifest = man || safeManifestDefaults();

  if (state.manifest && state.manifest.built_at_utc) {
    setMeta("Build: " + state.manifest.built_at_utc + " | base: " + base);
  } else {
    setMeta("Build: manifest non trovato | base: " + base);
  }

  const files = Array.isArray(state.manifest.gold_files) && state.manifest.gold_files.length
    ? state.manifest.gold_files
    : safeManifestDefaults().gold_files;

  const texts = await Promise.all(files.map(f => fetchTextOrNull(base + f)));
  const parsed = {};
  let foundAnyGold = false;

  for (let i = 0; i < files.length; i++) {
    const txt = texts[i];
    if (txt) {
      parsed[files[i]] = parseCSV(txt);
      if ((parsed[files[i]] || []).length) foundAnyGold = true;
    } else {
      parsed[files[i]] = [];
    }
  }

  state.kpiDayCat = parsed["kpi_giorno_categoria.csv"] || [];
  state.kpiMonthCat = parsed["kpi_mese_categoria.csv"] || [];
  state.kpiDay = parsed["kpi_giorno.csv"] || [];
  state.kpiMonth = parsed["kpi_mese.csv"] || [];
  state.histMonthCat = parsed["hist_mese_categoria.csv"] || [];
  state.stationsMonthNode = parsed["stazioni_mese_categoria_nodo.csv"] || [];
  state.odMonthCat = parsed["od_mese_categoria.csv"] || [];

  const extraFiles = [
    "od_giorno_categoria.csv",
    "stazioni_giorno_categoria_nodo.csv",
    "hist_giorno_categoria.csv"
  ];

  const extraTexts = await Promise.all(extraFiles.map(f => fetchTextOrNull(base + f)));
  const extraParsed = {};
  for (let i = 0; i < extraFiles.length; i++) {
    extraParsed[extraFiles[i]] = extraTexts[i] ? parseCSV(extraTexts[i]) : [];
  }

  state.odDayCat = extraParsed["od_giorno_categoria.csv"] || [];
  state.stationsDayNode = extraParsed["stazioni_giorno_categoria_nodo.csv"] || [];
  state.histDayCat = extraParsed["hist_giorno_categoria.csv"] || [];

  const stTxt = await fetchTextOrNull(base + "stations_dim.csv");
  const stRows = stTxt ? parseCSV(stTxt) : [];

  state.stationsRef.clear();
  stRows.forEach(r => {
    const code = String(r.cod_stazione || r.codice || r.cod || "").trim();
    if (!code) return;

    const name = String(r.nome_stazione || r.nome_norm || r.nome || "").trim();
    const lat = Number(r.lat);
    const lon = Number(r.lon);
    const city = String(r.citta || r.comune || r.city || r.nome_comune || "").trim();

    state.stationsRef.set(code, { code, name, lat, lon, city });
  });

  const capTxt = await fetchTextOrNull(base + "capoluoghi_provincia.csv");
  const capRows = capTxt ? parseCSV(capTxt) : [];
  state.capoluoghiSet = new Set(
    capRows.map(r => normalizeText(r.citta || r.capoluogo || r.nome || "")).filter(Boolean)
  );

  initFilters();
  initDayControls();
  initMap();
  initTables();
  initHistModeToggle();

  requestAnimationFrame(() => renderAll());

  if (!foundAnyGold) {
    setMeta("Errore: non trovo i CSV nella cartella dati. Controlla il deploy e che pubblichi la cartella corretta.");
  }
}

loadAll().catch(err => {
  console.error(err);
  setMeta("Errore caricamento dati: " + (err && err.message ? err.message : String(err)));
});
