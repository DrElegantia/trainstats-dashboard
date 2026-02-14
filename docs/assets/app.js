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

function parseCSV(text) {
  const t = String(text || "").trim();
  if (!t) return [];
  const lines = t.split(/\r?\n/);
  if (lines.length <= 1) return [];
  const header = lines[0].split(",");
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = splitCSVLine(lines[i]);
    const obj = {};
    for (let j = 0; j < header.length; j++) obj[header[j]] = cols[j] ?? "";
    rows.push(obj);
  }
  return rows;
}

function splitCSVLine(line) {
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
    if (ch === "," && !inQ) { out.push(cur); cur = ""; continue; }
    cur += ch;
  }
  out.push(cur);
  return out;
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

function stationName(code, fallback) {
  const c = String(code || "").trim();
  const ref = state.stationsRef.get(c);
  const n = ref && ref.name ? String(ref.name).trim() : "";
  if (n) return n;
  const fb = String(fallback || "").trim();
  return fb || c;
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

const state = {
  manifest: null,
  kpiDay: [],
  kpiDayCat: [],
  kpiMonth: [],
  kpiMonthCat: [],
  histMonthCat: [],
  stationsMonthNode: [],
  odMonthCat: [],
  stationsRef: new Map(),
  filters: {
    year: "all",
    cat: "all",
    dep: "all",
    arr: "all"
  },
  map: null,
  markers: [],
  tables: {
    stations: null,
    od: null
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
    ],
    punctuality: { on_time_threshold_minutes: 5 },
    delay_buckets_minutes: { labels: [] },
    min_counts: { leaderboard_min_trains: 20 }
  };
}

async function loadAll() {
  setMeta("Caricamento dati...");

  const man = await fetchJsonOrNull("data/manifest.json");
  state.manifest = man || safeManifestDefaults();

  if (state.manifest && state.manifest.built_at_utc) {
    setMeta("Build: " + state.manifest.built_at_utc);
  } else {
    setMeta("Build: manifest non trovato, carico i CSV disponibili");
  }

  const files = Array.isArray(state.manifest.gold_files) && state.manifest.gold_files.length
    ? state.manifest.gold_files
    : safeManifestDefaults().gold_files;

  const texts = await Promise.all(files.map(f => fetchTextOrNull("data/" + f)));
  const parsed = {};
  let foundAnyGold = false;

  for (let i = 0; i < files.length; i++) {
    const txt = texts[i];
    if (txt) {
      parsed[files[i]] = parseCSV(txt);
      foundAnyGold = true;
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

  const stTxt = await fetchTextOrNull("data/stations_dim.csv");
  const stRows = stTxt ? parseCSV(stTxt) : [];

  state.stationsRef.clear();
  stRows.forEach(r => {
    const code = String(r.cod_stazione || r.codice || r.cod || "").trim();
    if (!code) return;

    const name = String(r.nome_stazione || r.nome_norm || r.nome || "").trim();
    const lat = Number(r.lat);
    const lon = Number(r.lon);

    state.stationsRef.set(code, { code, name, lat, lon });
  });

  initFilters();
  initMap();
  initTables();
  renderAll();

  if (!foundAnyGold) {
    setMeta("Errore: non trovo i CSV in site/data. Controlla il deploy di GitHub Pages e che pubblichi la cartella site completa.");
  }
}

function initFilters() {
  const years = uniq(state.kpiMonth.map(r => yearFromMonth(r.mese))).sort();
  const cats = uniq(state.kpiMonthCat.map(r => r.categoria)).sort();

  const yearSel = document.getElementById("yearSel");
  yearSel.innerHTML = "";
  yearSel.appendChild(new Option("Tutti", "all"));
  years.forEach(y => yearSel.appendChild(new Option(y, y)));

  const catSel = document.getElementById("catSel");
  catSel.innerHTML = "";
  catSel.appendChild(new Option("Tutte", "all"));
  cats.forEach(c => catSel.appendChild(new Option(c, c)));

  const depSel = document.getElementById("depSel");
  const arrSel = document.getElementById("arrSel");

  const deps = uniq(state.odMonthCat.map(r => r.cod_partenza)).sort();
  const arrs = uniq(state.odMonthCat.map(r => r.cod_arrivo)).sort();

  depSel.innerHTML = "";
  depSel.appendChild(new Option("Tutte", "all"));
  deps.forEach(code => {
    const name = stationName(code, code);
    depSel.appendChild(new Option(name + " (" + code + ")", code));
  });

  arrSel.innerHTML = "";
  arrSel.appendChild(new Option("Tutte", "all"));
  arrs.forEach(code => {
    const name = stationName(code, code);
    arrSel.appendChild(new Option(name + " (" + code + ")", code));
  });

  yearSel.onchange = () => { state.filters.year = yearSel.value; renderAll(); };
  catSel.onchange = () => { state.filters.cat = catSel.value; renderAll(); };
  depSel.onchange = () => { state.filters.dep = depSel.value; renderAll(); };
  arrSel.onchange = () => { state.filters.arr = arrSel.value; renderAll(); };

  document.getElementById("resetBtn").onclick = () => {
    state.filters.year = "all";
    state.filters.cat = "all";
    state.filters.dep = "all";
    state.filters.arr = "all";
    yearSel.value = "all";
    catSel.value = "all";
    depSel.value = "all";
    arrSel.value = "all";
    renderAll();
  };

  const thr = state.manifest && state.manifest.punctuality ? state.manifest.punctuality.on_time_threshold_minutes : 5;
  document.getElementById("noteThreshold").innerText =
    "In orario significa ritardo arrivo tra 0 e " + thr + " minuti. Anticipo è ritardo negativo.";
}

function passYear(row, keyField) {
  if (state.filters.year === "all") return true;
  const v = String(row[keyField] || "");
  return v.startsWith(state.filters.year);
}

function passCat(row) {
  if (state.filters.cat === "all") return true;
  return String(row.categoria) === state.filters.cat;
}

function passDep(row) {
  if (state.filters.dep === "all") return true;
  return String(row.cod_partenza) === state.filters.dep;
}

function passArr(row) {
  if (state.filters.arr === "all") return true;
  return String(row.cod_arrivo) === state.filters.arr;
}

function sumRows(rows) {
  const keys = [
    "corse_osservate","effettuate","cancellate","soppresse","parzialmente_cancellate","info_mancante",
    "in_orario","in_ritardo","in_anticipo",
    "oltre_5","oltre_10","oltre_15","oltre_30","oltre_60",
    "minuti_ritardo_tot","minuti_anticipo_tot","minuti_netti_tot"
  ];
  const out = {};
  keys.forEach(k => out[k] = 0);
  rows.forEach(r => keys.forEach(k => out[k] += toNum(r[k])));
  return out;
}

function renderKPI() {
  let rows = state.kpiMonthCat;

  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, "mese"));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (state.filters.dep !== "all" || state.filters.arr !== "all") {
    let od = state.odMonthCat;
    if (state.filters.year !== "all") od = od.filter(r => passYear(r, "mese"));
    if (state.filters.cat !== "all") od = od.filter(passCat);
    if (state.filters.dep !== "all") od = od.filter(passDep);
    if (state.filters.arr !== "all") od = od.filter(passArr);

    const s = sumRows(od);
    document.getElementById("kpiTotal").innerText = fmtInt(s.corse_osservate);
    document.getElementById("kpiLate").innerText = fmtInt(s.in_ritardo);
    document.getElementById("kpiLateMin").innerText = fmtInt(s.minuti_ritardo_tot);
    document.getElementById("kpiCancelled").innerText = fmtInt(s.cancellate);
    document.getElementById("kpiSuppressed").innerText = fmtInt(s.soppresse);
    return;
  }

  const s = sumRows(rows);
  document.getElementById("kpiTotal").innerText = fmtInt(s.corse_osservate);
  document.getElementById("kpiLate").innerText = fmtInt(s.in_ritardo);
  document.getElementById("kpiLateMin").innerText = fmtInt(s.minuti_ritardo_tot);
  document.getElementById("kpiCancelled").innerText = fmtInt(s.cancellate);
  document.getElementById("kpiSuppressed").innerText = fmtInt(s.soppresse);
}

function renderSeries() {
  let daily = state.kpiDayCat;
  if (state.filters.year !== "all") daily = daily.filter(r => passYear(r, "giorno"));
  if (state.filters.cat !== "all") daily = daily.filter(passCat);
  if (state.filters.dep !== "all" || state.filters.arr !== "all") daily = [];
  daily = daily.sort((a,b) => String(a.giorno).localeCompare(String(b.giorno)));

  const x = daily.map(r => r.giorno);
  const yLate = daily.map(r => toNum(r.in_ritardo));
  const yMin = daily.map(r => toNum(r.minuti_ritardo_tot));
  const yCanc = daily.map(r => toNum(r.cancellate));
  const ySupp = daily.map(r => toNum(r.soppresse));

  Plotly.newPlot("chartDaily", [
    { x, y: yLate, name: "In ritardo", type: "scatter", mode: "lines" },
    { x, y: yMin, name: "Minuti ritardo", type: "scatter", mode: "lines", yaxis: "y2" },
    { x, y: yCanc, name: "Cancellati", type: "scatter", mode: "lines" },
    { x, y: ySupp, name: "Soppressi", type: "scatter", mode: "lines" }
  ], {
    margin: { t: 10, l: 40, r: 40, b: 40 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e6e9f2" },
    xaxis: { gridcolor: "rgba(255,255,255,0.08)" },
    yaxis: { title: "Conteggi", gridcolor: "rgba(255,255,255,0.08)" },
    yaxis2: { title: "Minuti", overlaying: "y", side: "right" }
  }, { displayModeBar: false });

  let monthly = state.kpiMonthCat;
  if (state.filters.year !== "all") monthly = monthly.filter(r => passYear(r, "mese"));
  if (state.filters.cat !== "all") monthly = monthly.filter(passCat);

  if (state.filters.dep !== "all" || state.filters.arr !== "all") {
    let od = state.odMonthCat;
    if (state.filters.year !== "all") od = od.filter(r => passYear(r, "mese"));
    if (state.filters.cat !== "all") od = od.filter(passCat);
    if (state.filters.dep !== "all") od = od.filter(passDep);
    if (state.filters.arr !== "all") od = od.filter(passArr);
    monthly = od;
  }

  monthly = monthly.sort((a,b) => String(a.mese).localeCompare(String(b.mese)));
  const xm = monthly.map(r => r.mese);
  const ymLate = monthly.map(r => toNum(r.in_ritardo));
  const ymMin = monthly.map(r => toNum(r.minuti_ritardo_tot));
  const ymCanc = monthly.map(r => toNum(r.cancellate));
  const ymSupp = monthly.map(r => toNum(r.soppresse));

  Plotly.newPlot("chartMonthly", [
    { x: xm, y: ymLate, name: "In ritardo", type: "bar" },
    { x: xm, y: ymMin, name: "Minuti ritardo", type: "scatter", mode: "lines", yaxis: "y2" },
    { x: xm, y: ymCanc, name: "Cancellati", type: "bar" },
    { x: xm, y: ymSupp, name: "Soppressi", type: "bar" }
  ], {
    barmode: "group",
    margin: { t: 10, l: 40, r: 40, b: 40 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e6e9f2" },
    xaxis: { gridcolor: "rgba(255,255,255,0.08)" },
    yaxis: { title: "Conteggi", gridcolor: "rgba(255,255,255,0.08)" },
    yaxis2: { title: "Minuti", overlaying: "y", side: "right" }
  }, { displayModeBar: false });
}

function renderHist() {
  let rows = state.histMonthCat;
  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, "mese"));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (state.filters.dep !== "all" || state.filters.arr !== "all") rows = [];

  const labels = state.manifest && state.manifest.delay_buckets_minutes && Array.isArray(state.manifest.delay_buckets_minutes.labels)
    ? state.manifest.delay_buckets_minutes.labels
    : [];

  if (!labels.length) {
    Plotly.newPlot("chartHist", [{ x: [], y: [], type: "bar" }], {
      margin: { t: 10, l: 40, r: 20, b: 60 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { color: "#e6e9f2" }
    }, { displayModeBar: false });
    return;
  }

  const byBucket = new Map();
  labels.forEach(l => byBucket.set(l, 0));

  rows.forEach(r => {
    const b = r.bucket_ritardo_arrivo;
    const c = toNum(r.count);
    if (byBucket.has(b)) byBucket.set(b, byBucket.get(b) + c);
  });

  const x = labels;
  const y = labels.map(l => byBucket.get(l) || 0);

  Plotly.newPlot("chartHist", [{ x, y, name: "Conteggio", type: "bar" }], {
    margin: { t: 10, l: 40, r: 20, b: 90 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e6e9f2" },
    xaxis: { tickangle: -35, gridcolor: "rgba(255,255,255,0.08)" },
    yaxis: { gridcolor: "rgba(255,255,255,0.08)" }
  }, { displayModeBar: false });
}

function initTables() {
  state.tables.stations = new Tabulator("#tableStations", {
    layout: "fitColumns",
    height: "360px",
    movableColumns: true,
    placeholder: "Nessun dato per i filtri selezionati",
    columns: [
      { title: "Mese", field: "mese", sorter: "string" },
      { title: "Categoria", field: "categoria", sorter: "string" },
      { title: "Stazione", field: "nome_stazione", sorter: "string" },
      { title: "Codice", field: "cod_stazione", sorter: "string" },
      { title: "Treni", field: "corse_osservate", sorter: "number", hozAlign: "right" },
      { title: "In ritardo", field: "in_ritardo", sorter: "number", hozAlign: "right" },
      { title: "Minuti ritardo", field: "minuti_ritardo_tot", sorter: "number", hozAlign: "right" },
      { title: "Cancellati", field: "cancellate", sorter: "number", hozAlign: "right" },
      { title: "Soppressi", field: "soppresse", sorter: "number", hozAlign: "right" },
      { title: "% ritardo", field: "pct_ritardo", sorter: "number", hozAlign: "right", formatter: (c) => fmtFloat(c.getValue()) }
    ]
  });

  state.tables.od = new Tabulator("#tableOD", {
    layout: "fitColumns",
    height: "360px",
    movableColumns: true,
    placeholder: "Nessun dato per i filtri selezionati",
    columns: [
      { title: "Mese", field: "mese", sorter: "string" },
      { title: "Categoria", field: "categoria", sorter: "string" },
      { title: "Partenza", field: "nome_partenza", sorter: "string" },
      { title: "Arrivo", field: "nome_arrivo", sorter: "string" },
      { title: "Treni", field: "corse_osservate", sorter: "number", hozAlign: "right" },
      { title: "In ritardo", field: "in_ritardo", sorter: "number", hozAlign: "right" },
      { title: "Minuti ritardo", field: "minuti_ritardo_tot", sorter: "number", hozAlign: "right" },
      { title: "Cancellati", field: "cancellate", sorter: "number", hozAlign: "right" },
      { title: "Soppressi", field: "soppresse", sorter: "number", hozAlign: "right" },
      { title: "% ritardo", field: "pct_ritardo", sorter: "number", hozAlign: "right", formatter: (c) => fmtFloat(c.getValue()) }
    ]
  });
}

function renderTables() {
  const minN = state.manifest && state.manifest.min_counts ? toNum(state.manifest.min_counts.leaderboard_min_trains) : 20;

  let st = state.stationsMonthNode;
  if (state.filters.year !== "all") st = st.filter(r => passYear(r, "mese"));
  if (state.filters.cat !== "all") st = st.filter(passCat);
  if (state.filters.dep !== "all" || state.filters.arr !== "all") st = [];

  st = st.map(r => {
    const n = toNum(r.corse_osservate);
    const late = toNum(r.in_ritardo);
    const pct = n > 0 ? (late / n) * 100 : 0;
    return { ...r, pct_ritardo: pct };
  }).filter(r => toNum(r.corse_osservate) >= minN);

  state.tables.stations.setData(st);

  let od = state.odMonthCat;
  if (state.filters.year !== "all") od = od.filter(r => passYear(r, "mese"));
  if (state.filters.cat !== "all") od = od.filter(passCat);
  if (state.filters.dep !== "all") od = od.filter(passDep);
  if (state.filters.arr !== "all") od = od.filter(passArr);

  od = od.map(r => {
    const n = toNum(r.corse_osservate);
    const late = toNum(r.in_ritardo);
    const pct = n > 0 ? (late / n) * 100 : 0;
    return { ...r, pct_ritardo: pct };
  }).filter(r => toNum(r.corse_osservate) >= minN);

  state.tables.od.setData(od);
}

function initMap() {
  state.map = L.map("map", { preferCanvas: true }).setView([41.9, 12.5], 6);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 18,
    attribution: "&copy; OpenStreetMap contributors"
  }).addTo(state.map);

  document.getElementById("mapMetricSel").onchange = () => renderMap();
}

function clearMarkers() {
  state.markers.forEach(m => m.remove());
  state.markers = [];
}

function renderMap() {
  clearMarkers();
  const metric = document.getElementById("mapMetricSel").value;
  const minN = state.manifest && state.manifest.min_counts ? toNum(state.manifest.min_counts.leaderboard_min_trains) : 20;

  let st = state.stationsMonthNode;
  if (state.filters.year !== "all") st = st.filter(r => passYear(r, "mese"));
  if (state.filters.cat !== "all") st = st.filter(passCat);
  if (state.filters.dep !== "all" || state.filters.arr !== "all") st = [];

  let missingCoords = 0;

  st.forEach(r => {
    const n = toNum(r.corse_osservate);
    if (n < minN) return;

    const code = String(r.cod_stazione || "").trim();
    const coords = stationCoords(code);
    if (!coords) {
      missingCoords++;
      return;
    }

    const late = toNum(r.in_ritardo);
    const pct = n > 0 ? (late / n) * 100 : 0;

    let v = 0;
    if (metric === "pct_ritardo") v = pct;
    if (metric === "in_ritardo") v = late;
    if (metric === "minuti_ritardo_tot") v = toNum(r.minuti_ritardo_tot);
    if (metric === "cancellate") v = toNum(r.cancellate);
    if (metric === "soppresse") v = toNum(r.soppresse);

    const radius = Math.max(4, Math.min(18, Math.sqrt(v + 1)));
    const marker = L.circleMarker([coords.lat, coords.lon], {
      radius,
      weight: 1,
      opacity: 0.9,
      fillOpacity: 0.35
    }).addTo(state.map);

    marker.bindTooltip(
      `<div style="font-size:12px">
        <div><b>${stationName(code, r.nome_stazione)}</b> (${code})</div>
        <div>Treni: ${fmtInt(n)}</div>
        <div>In ritardo: ${fmtInt(late)} (${fmtFloat(pct)}%)</div>
        <div>Minuti ritardo: ${fmtInt(toNum(r.minuti_ritardo_tot))}</div>
        <div>Cancellati: ${fmtInt(toNum(r.cancellate))}</div>
        <div>Soppressi: ${fmtInt(toNum(r.soppresse))}</div>
      </div>`
    );

    state.markers.push(marker);
  });

  const note = missingCoords > 0
    ? "Alcune stazioni non hanno coordinate e non sono disegnate sulla mappa. Completa data/stations/stations.csv oppure guarda data/stations/stations_unknown.csv."
    : "Coordinate stazioni complete per il set filtrato.";
  document.getElementById("mapNote").innerText = note;
}

function renderAll() {
  renderKPI();
  renderSeries();
  renderHist();
  renderTables();
  renderMap();
}

loadAll().catch(err => {
  console.error(err);
  setMeta("Errore caricamento dati: " + (err && err.message ? err.message : String(err)));
});
