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

function ensureTrailingSlash(p) {
  const s = String(p || "");
  return s.endsWith("/") ? s : s + "/";
}

const DATA_ROOT_CANDIDATES = ["data/", "./data/", "docs/data/", "site/data/"];

async function fetchTextAny(paths) {
  for (const p of paths) {
    const t = await fetchTextOrNull(p);
    if (t && String(t).trim().length) return t;
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

function uniq(arr) {
  return Array.from(new Set(arr));
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
    "kpi_giorno_categoria.csv",
    "gold/kpi_giorno_categoria.csv",
    "kpi_mese.csv",
    "gold/kpi_mese.csv",
    "kpi_giorno.csv",
    "gold/kpi_giorno.csv"
  ];

  for (const base0 of DATA_ROOT_CANDIDATES) {
    const base = ensureTrailingSlash(base0);
    for (const p of probes) {
      const t = await fetchTextOrNull(base + p);
      if (t && String(t).trim().length > 20) return base;
    }
  }
  return "data/";
}

function detectDelimiter(line) {
  const s = String(line || "");
  let comma = 0,
    semi = 0,
    tab = 0;
  let inQ = false;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '"') {
      if (inQ && s[i + 1] === '"') i++;
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
      if (inQ && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQ = !inQ;
      }
      continue;
    }
    if (ch === d && !inQ) {
      out.push(cur);
      cur = "";
      continue;
    }
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

function toNum(x) {
  const v = Number(x);
  return Number.isFinite(v) ? v : 0;
}

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

function fmtInt(x) {
  return Math.round(Number(x) || 0).toLocaleString("it-IT");
}

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

function yearFromMonth(mese) {
  return String(mese || "").slice(0, 4);
}

function firstEl(ids) {
  for (const id of ids) {
    const el = document.getElementById(id);
    if (el) return el;
  }
  return null;
}

function setTextByIds(ids, value) {
  const el = firstEl(ids);
  if (el) el.innerText = value;
}

function setMeta(text) {
  const el = document.getElementById("metaBox");
  if (el) el.innerText = text;
}

function safeManifestDefaults() {
  return {
    built_at_utc: "",
    gold_files: [
      "kpi_mese.csv",
      "kpi_mese_categoria.csv",
      "kpi_giorno.csv",
      "kpi_giorno_categoria.csv",
      "hist_mese_categoria.csv",
      "hist_giorno_categoria.csv",
      "stazioni_mese_categoria_nodo.csv",
      "stazioni_giorno_categoria_nodo.csv",
      "od_mese_categoria.csv",
      "od_giorno_categoria.csv",
      "hist_stazioni_mese_categoria_ruolo.csv",
      "hist_stazioni_giorno_categoria_ruolo.csv"
    ],
    delay_bucket_labels: [
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
    ]
  };
}

const state = {
  dataBase: "data/",
  manifest: safeManifestDefaults(),
  data: {
    kpiMonth: [],
    kpiMonthCat: [],
    kpiDay: [],
    kpiDayCat: [],
    histMonthCat: [],
    histDayCat: [],
    stationsMonthNode: [],
    stationsDayNode: [],
    odMonthCat: [],
    odDayCat: [],
    histStationsMonthRuolo: [],
    histStationsDayRuolo: []
  },
  stationsRef: new Map(),
  capoluoghiSet: new Set(),
  tables: {
    stations: null,
    od: null,
    cities: null
  },
  map: null,
  markers: [],
  filters: {
    year: "all",
    cat: "all",
    dep: "all",
    arr: "all",
    day_from: "",
    day_to: "",
    weekdays: [true, true, true, true, true, true, true],
    time_all: true,
    time_from: "00:00",
    time_to: "23:59"
  }
};

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
  const lat = ref.lat;
  const lon = ref.lon;
  if (typeof lat !== "number" || typeof lon !== "number") return null;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return { lat, lon };
}

function buildStationItems(codes) {
  const items = (codes || []).map((code) => {
    const name = stationName(code, code);
    return { code, name, needle: normalizeText(name + " " + code) };
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
  return k.slice(0, 4) === state.filters.year;
}

function ensureWeekdays() {
  if (!Array.isArray(state.filters.weekdays) || state.filters.weekdays.length !== 7) {
    state.filters.weekdays = [true, true, true, true, true, true, true];
  }
}

function hasWeekdayFilter() {
  ensureWeekdays();
  return state.filters.weekdays.some((x) => !x);
}

function hasStationFilter() {
  return state.filters.dep !== "all" || state.filters.arr !== "all";
}

function hasDayFilter() {
  return !!(state.filters.day_from || state.filters.day_to);
}

function hasTimeFilter() {
  if (state.filters.time_all) return false;
  const a = String(state.filters.time_from || "00:00").trim() || "00:00";
  const b = String(state.filters.time_to || "23:59").trim() || "23:59";
  return !(a === "00:00" && b === "23:59");
}

function dowIndexFromISO(isoDate) {
  const s = String(isoDate || "").slice(0, 10);
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
  if (!m) return null;
  const y = parseInt(m[1], 10);
  const mo = parseInt(m[2], 10);
  const d = parseInt(m[3], 10);
  const dt = new Date(y, mo - 1, d);
  const js = dt.getDay();
  return (js + 6) % 7;
}

function passWeekdays(isoDate) {
  if (!hasWeekdayFilter()) return true;
  const idx = dowIndexFromISO(isoDate);
  if (idx === null) return false;
  ensureWeekdays();
  return !!state.filters.weekdays[idx];
}

function parseTimeToMinutes(s) {
  const t = String(s || "").trim();
  const m = /^(\d{1,2})(?::(\d{2}))?$/.exec(t);
  if (!m) return null;
  const hh = parseInt(m[1], 10);
  const mm = parseInt(m[2] || "0", 10);
  if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
  if (hh < 0 || hh > 23 || mm < 0 || mm > 59) return null;
  return hh * 60 + mm;
}

function timeInRange(mins, fromMins, toMins) {
  if (mins === null || fromMins === null || toMins === null) return true;
  if (fromMins <= toMins) return mins >= fromMins && mins <= toMins;
  return mins >= fromMins || mins <= toMins;
}

function extractTimeFromRow(row) {
  const v =
    row.ora ??
    row.ora_partenza ??
    row.orario ??
    row.hh ??
    row.hour ??
    row.ora_di_partenza ??
    row.time ??
    "";
  if (v === "" || v === null || typeof v === "undefined") return null;
  if (typeof v === "number" && Number.isFinite(v)) {
    const hh = Math.max(0, Math.min(23, Math.floor(v)));
    return hh * 60;
  }
  const mins = parseTimeToMinutes(v);
  return mins;
}

function passTime(row) {
  if (!hasTimeFilter()) return true;
  const fromMins = parseTimeToMinutes(state.filters.time_from || "00:00");
  const toMins = parseTimeToMinutes(state.filters.time_to || "23:59");
  const mins = extractTimeFromRow(row);
  if (mins === null) return true;
  return timeInRange(mins, fromMins, toMins);
}

function passDayKey(row, field) {
  const d = String(row[field] || "").slice(0, 10);
  if (!d) return false;

  const from = String(state.filters.day_from || "").trim();
  const to = String(state.filters.day_to || "").trim();

  if (from || to) {
    const a = from || to;
    const b = to || from;
    const lo = a <= b ? a : b;
    const hi = a <= b ? b : a;
    if (d < lo || d > hi) return false;
  }

  if (!passWeekdays(d)) return false;
  if (!passTime(row)) return false;

  return true;
}

function passMonthFromDayRange(row, field) {
  if (!hasDayFilter()) return true;

  const from = String(state.filters.day_from || "").trim();
  const to = String(state.filters.day_to || "").trim();
  const a = from || to;
  const b = to || from;

  const lo = (a <= b ? a : b).slice(0, 7);
  const hi = (a <= b ? b : a).slice(0, 7);

  const m = String(row[field] || "").slice(0, 7);
  if (!m) return false;

  return m >= lo && m <= hi;
}

function safeSetData(table, data) {
  if (!table || typeof table.setData !== "function") return;
  try {
    const p = table.setData(data);
    if (p && typeof p.catch === "function") p.catch(() => {});
  } catch {}
}

function initTables() {
  if (typeof Tabulator !== "function") return;

  const stationsEl = firstEl(["stationsTable", "tableStations", "tblStations"]);
  const odEl = firstEl(["odTable", "tableOD", "tblOD"]);
  const citiesEl = firstEl(["citiesTable", "tableCities", "tblCities"]);

  if (stationsEl && !state.tables.stations) {
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
        { title: "In ritardo", field: "in_ritardo", sorter: "number", hozAlign: "right", width: 120 },
        { title: "Minuti", field: "minuti_ritardo_tot", sorter: "number", hozAlign: "right", width: 130 },
        { title: "Cancellati", field: "cancellate_tot", sorter: "number", hozAlign: "right", width: 120 },
        { title: "Soppressi", field: "soppresse", sorter: "number", hozAlign: "right", width: 110 }
      ]
    });
  }

  if (odEl && !state.tables.od) {
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
        { title: "In ritardo", field: "in_ritardo", sorter: "number", hozAlign: "right", width: 120 },
        { title: "Minuti", field: "minuti_ritardo_tot", sorter: "number", hozAlign: "right", width: 130 }
      ]
    });
  }

  if (citiesEl && !state.tables.cities) {
    state.tables.cities = new Tabulator(citiesEl, {
      data: [],
      layout: "fitColumns",
      pagination: "local",
      paginationSize: 10,
      columns: [
        { title: "Città", field: "city", sorter: "string" },
        { title: "Corse", field: "corse_osservate", sorter: "number", hozAlign: "right", width: 110 },
        { title: "% ritardo", field: "pct_ritardo", sorter: "number", hozAlign: "right", width: 110, formatter: (c) => fmtFloat(c.getValue()) },
        { title: "In ritardo", field: "in_ritardo", sorter: "number", hozAlign: "right", width: 120 },
        { title: "Minuti", field: "minuti_ritardo_tot", sorter: "number", hozAlign: "right", width: 130 },
        { title: "Cancellati", field: "cancellate_tot", sorter: "number", hozAlign: "right", width: 120 },
        { title: "Soppressi", field: "soppresse", sorter: "number", hozAlign: "right", width: 110 }
      ]
    });
  }
}

function initMap() {
  const mapEl = firstEl(["map", "mapStations", "stationsMap"]);
  if (!mapEl) return;
  if (typeof L !== "object" || typeof L.map !== "function") return;
  if (state.map) return;

  state.map = L.map(mapEl.id, { center: [42.5, 12.5], zoom: 6, zoomSnap: 0.5 });

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors",
    maxZoom: 18
  }).addTo(state.map);

  setTimeout(() => {
    try {
      state.map.invalidateSize();
    } catch {}
  }, 150);
}

function clearMarkers() {
  if (!state.map) return;
  for (const m of state.markers) {
    try {
      state.map.removeLayer(m);
    } catch {}
  }
  state.markers = [];
}

function ensureExtraControls() {
  const anchor = firstEl(["yearSel", "annoSel", "year"]);
  if (!anchor) return null;

  let extra = document.getElementById("filtersExtra");
  if (extra) return extra;

  const host =
    anchor.closest("#filters") ||
    anchor.closest(".filters") ||
    anchor.closest(".controls") ||
    anchor.closest(".filtersRow") ||
    anchor.closest(".filtersGrid") ||
    anchor.parentNode;

  extra = document.createElement("div");
  extra.id = "filtersExtra";
  extra.style.display = "flex";
  extra.style.alignItems = "center";
  extra.style.gap = "10px";
  extra.style.marginTop = "8px";
  extra.style.flexWrap = "wrap";

  if (host && host.parentNode) host.parentNode.insertBefore(extra, host.nextSibling);
  else (document.body || document.documentElement).appendChild(extra);

  return extra;
}

function initDayWeekTimeControls() {
  if (document.getElementById("dayFrom")) return;

  const extra = ensureExtraControls();
  if (!extra) return;

  const dayWrap = document.createElement("div");
  dayWrap.style.display = "flex";
  dayWrap.style.alignItems = "center";
  dayWrap.style.gap = "10px";
  dayWrap.style.flexWrap = "wrap";

  const dayLab = document.createElement("div");
  dayLab.innerText = "";

  const from = document.createElement("input");
  from.type = "date";
  from.id = "dayFrom";
  from.value = state.filters.day_from || "";

  const to = document.createElement("input");
  to.type = "date";
  to.id = "dayTo";
  to.value = state.filters.day_to || "";

  dayWrap.appendChild(dayLab);
  dayWrap.appendChild(from);
  dayWrap.appendChild(to);

  const wdLab = document.createElement("div");
  wdLab.innerText = "";

  const wdWrap = document.createElement("div");
  wdWrap.id = "weekdayWrap";
  wdWrap.style.display = "flex";
  wdWrap.style.alignItems = "center";
  wdWrap.style.gap = "6px";

  ensureWeekdays();
  const wdLabels = ["Lu", "Ma", "Me", "Gi", "Ve", "Sa", "Do"];

  const refreshWdStyles = () => {
    const btns = wdWrap.querySelectorAll("button[data-wd]");
    btns.forEach((b) => {
      const idx = parseInt(String(b.dataset.wd || "0"), 10);
      const on = !!state.filters.weekdays[idx];
      b.style.opacity = on ? "1" : "0.35";
      b.style.borderColor = on ? "rgba(255,255,255,0.85)" : "rgba(255,255,255,0.35)";
    });
  };

  wdLabels.forEach((t, i) => {
    const b = document.createElement("button");
    b.type = "button";
    b.dataset.wd = String(i);
    b.innerText = t;
    b.style.width = "28px";
    b.style.height = "28px";
    b.style.borderRadius = "999px";
    b.style.border = "1px solid rgba(255,255,255,0.85)";
    b.style.background = "transparent";
    b.style.color = "inherit";
    b.style.cursor = "pointer";
    b.style.display = "inline-flex";
    b.style.alignItems = "center";
    b.style.justifyContent = "center";
    b.style.padding = "0";
    b.onclick = () => {
      ensureWeekdays();
      state.filters.weekdays[i] = !state.filters.weekdays[i];
      refreshWdStyles();
      updateFiltersNote();
      renderAll();
    };
    wdWrap.appendChild(b);
  });

  refreshWdStyles();

  const timeLab = document.createElement("div");
  timeLab.innerText = "Orari";

  const timeAllWrap = document.createElement("label");
  timeAllWrap.style.display = "inline-flex";
  timeAllWrap.style.alignItems = "center";
  timeAllWrap.style.gap = "6px";
  timeAllWrap.style.cursor = "pointer";

  const timeAll = document.createElement("input");
  timeAll.type = "checkbox";
  timeAll.id = "timeAll";
  timeAll.checked = !!state.filters.time_all;

  const timeAllTxt = document.createElement("span");
  timeAllTxt.innerText = "Tutta la giornata";

  timeAllWrap.appendChild(timeAll);
  timeAllWrap.appendChild(timeAllTxt);

  const timeFrom = document.createElement("input");
  timeFrom.type = "time";
  timeFrom.id = "timeFrom";
  timeFrom.step = "60";
  timeFrom.value = state.filters.time_from || "00:00";

  const timeTo = document.createElement("input");
  timeTo.type = "time";
  timeTo.id = "timeTo";
  timeTo.step = "60";
  timeTo.value = state.filters.time_to || "23:59";

  const note = document.createElement("div");
  note.id = "filtersNote";
  note.style.fontSize = "12px";
  note.style.opacity = "0.75";
  note.style.marginLeft = "6px";

  const syncTimeDisabled = () => {
    const allDay = timeAll.checked;
    timeFrom.disabled = allDay;
    timeTo.disabled = allDay;
  };

  const apply = () => {
    state.filters.day_from = String(from.value || "").trim();
    state.filters.day_to = String(to.value || "").trim();
    state.filters.time_all = !!timeAll.checked;
    state.filters.time_from = String(timeFrom.value || "00:00").trim() || "00:00";
    state.filters.time_to = String(timeTo.value || "23:59").trim() || "23:59";
    syncTimeDisabled();
    updateFiltersNote();
    renderAll();
  };

  from.onchange = apply;
  to.onchange = apply;
  timeAll.onchange = apply;
  timeFrom.onchange = apply;
  timeTo.onchange = apply;

  syncTimeDisabled();

  extra.appendChild(dayWrap);
  extra.appendChild(wdLab);
  extra.appendChild(wdWrap);
  extra.appendChild(timeLab);
  extra.appendChild(timeAllWrap);
  extra.appendChild(timeFrom);
  extra.appendChild(timeTo);
  extra.appendChild(note);

  updateFiltersNote();
}

function updateFiltersNote() {
  const el = document.getElementById("filtersNote");
  if (!el) return;

  const d = hasDayFilter();
  const w = hasWeekdayFilter();
  const t = hasTimeFilter();

  if (!d && !w && !t) {
    el.innerText = "";
    return;
  }

  const haveDay = state.data.kpiDayCat && state.data.kpiDayCat.length > 0;
  const haveOdDay = state.data.odDayCat && state.data.odDayCat.length > 0;
  const haveStDay = state.data.stationsDayNode && state.data.stationsDayNode.length > 0;

  let msg = "Filtro attivo.";
  if ((d || w) && !haveDay) msg = "Filtro attivo, ma mancano le tabelle giornaliere.";
  if (t && haveDay) msg = "Filtro attivo. Se non esiste la dimensione oraria nei CSV, il filtro orario non cambia i risultati.";
  if ((d || w) && haveDay && (!haveOdDay || !haveStDay)) msg = msg + " Per tabelle tratte e stazioni serve anche OD e stazioni giornaliere.";

  el.innerText = msg;
}

function initFilters() {
  const yearSel = firstEl(["yearSel", "annoSel", "year"]);
  const catSel = firstEl(["catSel", "categoriaSel", "category"]);
  const depSel = firstEl(["depSel", "stazionePartenzaSel", "depStationSel"]);
  const arrSel = firstEl(["arrSel", "stazioneArrivoSel", "arrStationSel"]);
  const mapMetricSel = firstEl(["mapMetricSel", "mapSel", "mappaSel"]);
  const resetBtn = firstEl(["resetBtn", "btnReset", "reset"]);

  const years = uniq(state.data.kpiMonth.map((r) => yearFromMonth(r.mese)).filter(Boolean)).sort();
  const cats = uniq(state.data.kpiMonthCat.map((r) => String(r.categoria || "").trim()).filter(Boolean)).sort((a, b) =>
    String(a).localeCompare(String(b), "it", { sensitivity: "base" })
  );

  if (yearSel) {
    yearSel.innerHTML = "";
    yearSel.appendChild(new Option("Tutti", "all"));
    years.forEach((y) => yearSel.appendChild(new Option(y, y)));
    yearSel.value = state.filters.year || "all";
    yearSel.onchange = () => {
      state.filters.year = yearSel.value || "all";
      renderAll();
    };
  }

  if (catSel) {
    catSel.innerHTML = "";
    catSel.appendChild(new Option("Tutte", "all"));
    cats.forEach((c) => catSel.appendChild(new Option(c, c)));
    catSel.value = state.filters.cat || "all";
    catSel.onchange = () => {
      state.filters.cat = catSel.value || "all";
      renderAll();
    };
  }

  const deps = uniq(
    [
      ...(state.data.odMonthCat || []).map((r) => r.cod_partenza),
      ...(state.data.odDayCat || []).map((r) => r.cod_partenza)
    ].filter(Boolean)
  );
  const arrs = uniq(
    [
      ...(state.data.odMonthCat || []).map((r) => r.cod_arrivo),
      ...(state.data.odDayCat || []).map((r) => r.cod_arrivo)
    ].filter(Boolean)
  );

  const depItems = buildStationItems(deps);
  const arrItems = buildStationItems(arrs);

  if (depSel) {
    fillStationSelect(depSel, depItems, "");
    ensureSearchInput(depSel, "depSearch", "Cerca stazione di partenza", depItems);
    depSel.value = state.filters.dep || "all";
    depSel.onchange = () => {
      state.filters.dep = depSel.value || "all";
      renderAll();
    };
  }

  if (arrSel) {
    fillStationSelect(arrSel, arrItems, "");
    ensureSearchInput(arrSel, "arrSearch", "Cerca stazione di arrivo", arrItems);
    arrSel.value = state.filters.arr || "all";
    arrSel.onchange = () => {
      state.filters.arr = arrSel.value || "all";
      renderAll();
    };
  }

  if (mapMetricSel) {
    if (!mapMetricSel.value) mapMetricSel.value = "pct_ritardo";
    mapMetricSel.onchange = () => {
      renderSeries();
      renderMap();
    };
  }

  if (resetBtn) {
    resetBtn.onclick = () => {
      state.filters.year = "all";
      state.filters.cat = "all";
      state.filters.dep = "all";
      state.filters.arr = "all";
      state.filters.day_from = "";
      state.filters.day_to = "";
      state.filters.weekdays = [true, true, true, true, true, true, true];
      state.filters.time_all = true;
      state.filters.time_from = "00:00";
      state.filters.time_to = "23:59";

      if (yearSel) yearSel.value = "all";
      if (catSel) catSel.value = "all";
      if (depSel) depSel.value = "all";
      if (arrSel) arrSel.value = "all";

      const dayFrom = document.getElementById("dayFrom");
      const dayTo = document.getElementById("dayTo");
      const timeAll = document.getElementById("timeAll");
      const timeFrom = document.getElementById("timeFrom");
      const timeTo = document.getElementById("timeTo");

      if (dayFrom) dayFrom.value = "";
      if (dayTo) dayTo.value = "";
      if (timeAll) timeAll.checked = true;
      if (timeFrom) timeFrom.value = "00:00";
      if (timeTo) timeTo.value = "23:59";

      initDayWeekTimeControls();
      updateFiltersNote();
      renderAll();
    };
  }
}

function ensureHistToggleStyles() {
  if (document.getElementById("histToggleStyles")) return;
  const style = document.createElement("style");
  style.id = "histToggleStyles";
  style.textContent = `
    .histToggleWrap { display:flex; align-items:center; gap:10px; margin:0 0 8px 0; }
    .histModeText { font-size:13px; color:#e6e9f2; opacity:0.65; user-select:none; }
    .histModeText.active { opacity:1; font-weight:600; }
    .histSwitch { position:relative; display:inline-block; width:44px; height:24px; }
    .histSwitch input { opacity:0; width:0; height:0; }
    .histSlider { position:absolute; cursor:pointer; inset:0; background:rgba(255,255,255,0.22); transition:0.18s; border-radius:24px; }
    .histSlider:before { position:absolute; content:""; height:18px; width:18px; left:3px; top:3px; background:#ffffff; transition:0.18s; border-radius:50%; }
    .histSwitch input:checked + .histSlider { background:rgba(255,255,255,0.38); }
    .histSwitch input:checked + .histSlider:before { transform: translateX(20px); }
  `;
  document.head.appendChild(style);
}

function updateHistToggleUI() {
  const t = document.getElementById("histModeToggle");
  const left = document.getElementById("histModeTextCount");
  const right = document.getElementById("histModeTextPct");
  if (!t || !left || !right) return;
  if (t.checked) {
    left.classList.remove("active");
    right.classList.add("active");
  } else {
    left.classList.add("active");
    right.classList.remove("active");
  }
}

function ensureHistToggle() {
  const chart = firstEl(["chartHist", "histChart", "chartDistribution"]);
  if (!chart) return;

  ensureHistToggleStyles();

  let t = document.getElementById("histModeToggle");
  if (t) {
    updateHistToggleUI();
    t.onchange = () => {
      updateHistToggleUI();
      renderHist();
    };
    return;
  }

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

  t.onchange = () => {
    updateHistToggleUI();
    renderHist();
  };
}

function useDailyAggregation() {
  const haveDay = state.data.kpiDayCat && state.data.kpiDayCat.length > 0;
  if (!haveDay) return false;
  return true;
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

function computeValue(corse, ritardo, minuti, sopp, canc) {
  const mode = getMetricMode();
  if (mode === "count_late") return ritardo;
  if (mode === "count_total") return corse;
  if (mode === "minutes") return minuti;
  if (mode === "suppressed") return sopp || 0;
  if (mode === "cancelled") return canc || 0;
  return corse > 0 ? (ritardo / corse) * 100 : 0;
}


function renderKPI() {
  const stationFiltered = hasStationFilter();
  const useDay = useDailyAggregation();
  let base, keyField;

  if (stationFiltered) {
    const haveOdDay = state.data.odDayCat && state.data.odDayCat.length > 0;
    if (useDay && haveOdDay) {
      base = state.data.odDayCat;
      keyField = "giorno";
    } else {
      base = state.data.odMonthCat;
      keyField = "mese";
    }
  } else {
    base = useDay ? state.data.kpiDayCat : state.data.kpiMonthCat;
    keyField = useDay ? "giorno" : "mese";
  }

  let rows = base || [];
  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (stationFiltered) {
    if (state.filters.dep !== "all") rows = rows.filter(passDep);
    if (state.filters.arr !== "all") rows = rows.filter(passArr);
  }

  if (useDay || (stationFiltered && keyField === "giorno")) rows = rows.filter((r) => passDayKey(r, "giorno"));
  else if (hasDayFilter()) rows = rows.filter((r) => passMonthFromDayRange(r, keyField));

  const total = rows.reduce((a, r) => a + toNum(r.corse_osservate), 0);
  const late = rows.reduce((a, r) => a + toNum(r.in_ritardo), 0);
  const mins = rows.reduce((a, r) => a + toNum(r.minuti_ritardo_tot), 0);

  const canc = rows.reduce((a, r) => {
    const v = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    return a + toNum(v);
  }, 0);

  const sopp = rows.reduce((a, r) => a + toNum(r.soppresse), 0);

  setTextByIds(["cardTotal", "kpiTotal", "kpiCorse", "totalRuns", "corseOsservate", "corse_osservate"], fmtInt(total));
  setTextByIds(["cardLate", "kpiLate", "kpiRitardo", "lateRuns", "inRitardo", "in_ritardo"], fmtInt(late));

  setTextByIds(
    [
      "cardMin",
      "kpiMinutes",
      "kpiMinuti",
      "delayMinutes",
      "kpiLateMin",
      "kpiDelayMinutes",
      "kpiMinTotRitardo",
      "minutiTotali",
      "minuti_totali_ritardo",
      "minutiRitardoTotali",
      "minutesTotal"
    ],
    fmtInt(mins)
  );

  setTextByIds(["cardCanc", "kpiCancelled", "kpiCancellati", "cancellati", "cancellate"], fmtInt(canc));
  setTextByIds(["cardSopp", "kpiSuppressed", "kpiSoppressi", "soppressi", "soppresse"], fmtInt(sopp));
}

function seriesDaily() {
  const stationFiltered = hasStationFilter();
  let rows;

  if (stationFiltered) {
    rows = state.data.odDayCat && state.data.odDayCat.length ? state.data.odDayCat : [];
  } else {
    rows = state.data.kpiDayCat && state.data.kpiDayCat.length ? state.data.kpiDayCat : state.data.kpiDay;
  }
  rows = rows || [];

  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, "giorno"));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (stationFiltered) {
    if (state.filters.dep !== "all") rows = rows.filter(passDep);
    if (state.filters.arr !== "all") rows = rows.filter(passArr);
  }

  if (hasDayFilter() || hasWeekdayFilter() || hasTimeFilter()) rows = rows.filter((r) => passDayKey(r, "giorno"));

  const by = new Map();
  for (const r of rows) {
    const day = String(r.giorno || "").slice(0, 10);
    if (!day) continue;
    if (!by.has(day)) by.set(day, { key: day, corse: 0, rit: 0, min: 0, sopp: 0, canc: 0 });
    const o = by.get(day);
    o.corse += toNum(r.corse_osservate);
    o.rit += toNum(r.in_ritardo);
    o.min += toNum(r.minuti_ritardo_tot);
    o.sopp += toNum(r.soppresse);
    const cv = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    o.canc += toNum(cv);
  }

  const out = Array.from(by.values()).sort((a, b) => String(a.key).localeCompare(String(b.key)));
  const x = out.map((o) => o.key);
  const y = out.map((o) => computeValue(o.corse, o.rit, o.min, o.sopp, o.canc));

  return { x, y };
}

function seriesMonthly() {
  const stationFiltered = hasStationFilter();
  let rows;

  if (stationFiltered) {
    rows = state.data.odMonthCat && state.data.odMonthCat.length ? state.data.odMonthCat : [];
  } else {
    rows = state.data.kpiMonthCat && state.data.kpiMonthCat.length ? state.data.kpiMonthCat : state.data.kpiMonth;
  }
  rows = rows || [];

  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, "mese"));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (stationFiltered) {
    if (state.filters.dep !== "all") rows = rows.filter(passDep);
    if (state.filters.arr !== "all") rows = rows.filter(passArr);
  }

  if (hasDayFilter()) rows = rows.filter((r) => passMonthFromDayRange(r, "mese"));

  const by = new Map();
  for (const r of rows) {
    const m = String(r.mese || "").slice(0, 7);
    if (!m) continue;
    if (!by.has(m)) by.set(m, { key: m, corse: 0, rit: 0, min: 0, sopp: 0, canc: 0 });
    const o = by.get(m);
    o.corse += toNum(r.corse_osservate);
    o.rit += toNum(r.in_ritardo);
    o.min += toNum(r.minuti_ritardo_tot);
    o.sopp += toNum(r.soppresse);
    const cv = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    o.canc += toNum(cv);
  }

  const out = Array.from(by.values()).sort((a, b) => String(a.key).localeCompare(String(b.key)));
  const x = out.map((o) => o.key);
  const y = out.map((o) => computeValue(o.corse, o.rit, o.min, o.sopp, o.canc));

  return { x, y };
}

function renderSeries() {
  if (typeof Plotly !== "object") return;

  const dEl = firstEl(["chartDaily", "chartDay", "chartGiorno", "chartSeriesDaily"]);
  const mEl = firstEl(["chartMonthly", "chartMonth", "chartMese", "chartSeriesMonthly"]);

  const d = seriesDaily();
  const m = seriesMonthly();

  const yTitle = metricLabel();

  if (dEl) {
    Plotly.react(
      dEl,
      [{ x: d.x, y: d.y, type: "scatter", mode: "lines+markers", name: yTitle }],
      {
        margin: { l: 50, r: 20, t: 10, b: 50 },
        yaxis: { title: yTitle, rangemode: "tozero" },
        xaxis: { type: "category" },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { color: "#e8eefc" }
      },
      { displayModeBar: false, responsive: true }
    );
  }

  if (mEl) {
    Plotly.react(
      mEl,
      [{ x: m.x, y: m.y, type: "scatter", mode: "lines+markers", name: yTitle }],
      {
        margin: { l: 50, r: 20, t: 10, b: 50 },
        yaxis: { title: yTitle, rangemode: "tozero" },
        xaxis: { type: "category" },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { color: "#e8eefc" }
      },
      { displayModeBar: false, responsive: true }
    );
  }
}

function normalizeBucketLabel(s) {
  return String(s || "").replace(/\s+/g, "").trim();
}

function renderHist() {
  if (typeof Plotly !== "object") return;

  const chart = firstEl(["chartHist", "histChart", "chartDistribution"]);
  if (!chart) return;

  ensureHistToggle();

  const toggle = document.getElementById("histModeToggle");
  const showPct = !!(toggle && toggle.checked);

  const stationFiltered = hasStationFilter();
  let base, keyField, useDay;

  if (stationFiltered) {
    const haveStDay = state.data.histStationsDayRuolo && state.data.histStationsDayRuolo.length > 0;
    useDay = haveStDay && (hasDayFilter() || hasWeekdayFilter() || hasTimeFilter());
    base = useDay ? state.data.histStationsDayRuolo : state.data.histStationsMonthRuolo;
    keyField = useDay ? "giorno" : "mese";
  } else {
    const haveDayHist = state.data.histDayCat && state.data.histDayCat.length > 0;
    useDay = haveDayHist && (hasDayFilter() || hasWeekdayFilter() || hasTimeFilter());
    base = useDay ? state.data.histDayCat : state.data.histMonthCat;
    keyField = useDay ? "giorno" : "mese";
  }

  let rows = base || [];

  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (stationFiltered) {
    const dep = state.filters.dep;
    const arr = state.filters.arr;
    if (arr !== "all") {
      rows = rows.filter((r) => String(r.cod_stazione || "").trim() === arr && String(r.ruolo || "").trim() === "arrivo");
    } else if (dep !== "all") {
      rows = rows.filter((r) => String(r.cod_stazione || "").trim() === dep && String(r.ruolo || "").trim() === "partenza");
    }
  }

  if (useDay) rows = rows.filter((r) => passDayKey(r, "giorno"));
  else if (hasDayFilter()) rows = rows.filter((r) => passMonthFromDayRange(r, "mese"));

  const byBucket = new Map();
  let total = 0;

  for (const r of rows) {
    const raw = String(r.bucket_ritardo_arrivo || r.bucket || "").trim();
    if (!raw) continue;
    const key = normalizeBucketLabel(raw);
    const c = toNum(r.count);
    total += c;
    if (!byBucket.has(key)) byBucket.set(key, { label: raw, count: 0 });
    byBucket.get(key).count += c;
  }

  const order =
    Array.isArray(state.manifest.delay_bucket_labels) && state.manifest.delay_bucket_labels.length
      ? state.manifest.delay_bucket_labels
      : safeManifestDefaults().delay_bucket_labels;

  const x = [];
  const y = [];

  for (const lab of order) {
    const key = normalizeBucketLabel(lab);
    const obj = byBucket.get(key);
    const c = obj ? obj.count : 0;
    x.push(lab);
    y.push(showPct ? (total > 0 ? (c / total) * 100 : 0) : c);
  }

  Plotly.react(
    chart,
    [{ x, y, type: "bar", name: showPct ? "%" : "Conteggio" }],
    {
      margin: { l: 50, r: 20, t: 10, b: 70 },
      yaxis: { title: showPct ? "%" : "Conteggio", rangemode: "tozero" },
      xaxis: { tickangle: -35 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { color: "#e8eefc" }
    },
    { displayModeBar: false, responsive: true }
  );
}

function capoluogoKey(cityName) {
  const name = normalizeText(cityName);
  if (!name) return "";
  if (!state.capoluoghiSet || state.capoluoghiSet.size === 0) return name;
  if (state.capoluoghiSet.has(name)) return name;

  for (const cap of state.capoluoghiSet) {
    if (name.startsWith(cap + " ") || name.startsWith(cap + "-") || name.startsWith(cap + "'")) return cap;
  }

  return "";
}


function prettyCityName(cityKey, fallback) {
  const raw = String(fallback || "").trim();
  if (raw && normalizeText(raw) === cityKey) return raw;

  return String(cityKey || "")
    .toLowerCase()
    .replace(/\b([a-zàèéìòù])/g, (m) => m.toUpperCase());
}

function renderStationsTable() {
  const useDay = useDailyAggregation() && state.data.stationsDayNode && state.data.stationsDayNode.length > 0;
  const base = useDay ? state.data.stationsDayNode : state.data.stationsMonthNode;
  const keyField = useDay ? "giorno" : "mese";

  let rows = base || [];

  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (useDay) rows = rows.filter((r) => passDayKey(r, "giorno"));
  else if (hasDayFilter()) rows = rows.filter((r) => passMonthFromDayRange(r, "mese"));

  const agg = new Map();

  for (const r of rows) {
    const code = String(r.cod_stazione || "").trim();
    if (!code) continue;

    if (!agg.has(code)) {
      agg.set(code, {
        cod_stazione: code,
        nome_stazione: stationName(code, r.nome_stazione || ""),
        corse_osservate: 0,
        in_ritardo: 0,
        minuti_ritardo_tot: 0,
        cancellate_tot: 0,
        soppresse: 0
      });
    }

    const a = agg.get(code);
    a.corse_osservate += toNum(r.corse_osservate);
    a.in_ritardo += toNum(r.in_ritardo);
    a.minuti_ritardo_tot += toNum(r.minuti_ritardo_tot);

    const canc = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    a.cancellate_tot += toNum(canc);

    a.soppresse += toNum(r.soppresse);
  }

  let out = Array.from(agg.values());
  out.forEach((o) => {
    o.pct_ritardo = o.corse_osservate > 0 ? (o.in_ritardo / o.corse_osservate) * 100 : 0;
  });

  out.sort((a, b) => toNum(b.pct_ritardo) - toNum(a.pct_ritardo));
  safeSetData(state.tables.stations, out.slice(0, 200));
  try {
    if (state.tables.stations) state.tables.stations.setSort("pct_ritardo", "desc");
  } catch {}
}

function renderODTable() {
  const useDay = useDailyAggregation() && state.data.odDayCat && state.data.odDayCat.length > 0;
  const base = useDay ? state.data.odDayCat : state.data.odMonthCat;
  const keyField = useDay ? "giorno" : "mese";

  let rows = base || [];

  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (useDay) rows = rows.filter((r) => passDayKey(r, "giorno"));
  else if (hasDayFilter()) rows = rows.filter((r) => passMonthFromDayRange(r, "mese"));

  if (state.filters.dep !== "all") rows = rows.filter(passDep);
  if (state.filters.arr !== "all") rows = rows.filter(passArr);

  const out = rows
    .map((r) => {
      const corse = toNum(r.corse_osservate);
      const rit = toNum(r.in_ritardo);
      const min = toNum(r.minuti_ritardo_tot);
      return {
        cod_partenza: r.cod_partenza,
        cod_arrivo: r.cod_arrivo,
        nome_partenza: stationName(r.cod_partenza, r.nome_partenza),
        nome_arrivo: stationName(r.cod_arrivo, r.nome_arrivo),
        corse_osservate: corse,
        in_ritardo: rit,
        pct_ritardo: corse > 0 ? (rit / corse) * 100 : 0,
        minuti_ritardo_tot: min
      };
    })
    .filter((r) => r.corse_osservate > 0);

  out.sort((a, b) => toNum(b.pct_ritardo) - toNum(a.pct_ritardo));
  safeSetData(state.tables.od, out.slice(0, 200));
  try {
    if (state.tables.od) state.tables.od.setSort("pct_ritardo", "desc");
  } catch {}
}

function renderCitiesTable() {
  const useDay = useDailyAggregation() && state.data.stationsDayNode && state.data.stationsDayNode.length > 0;
  const base = useDay ? state.data.stationsDayNode : state.data.stationsMonthNode;
  const keyField = useDay ? "giorno" : "mese";

  let rows = base || [];

  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (useDay) rows = rows.filter((r) => passDayKey(r, "giorno"));
  else if (hasDayFilter()) rows = rows.filter((r) => passMonthFromDayRange(r, "mese"));

  const agg = new Map();

  for (const r of rows) {
    const code = String(r.cod_stazione || "").trim();
    if (!code) continue;

    const city = stationCity(code, r.nome_stazione || code);
    if (!city) continue;

    const k = capoluogoKey(city);
    if (!k) continue;

    if (!agg.has(k)) {
      agg.set(k, {
        city: prettyCityName(k, city),
        corse_osservate: 0,
        in_ritardo: 0,
        minuti_ritardo_tot: 0,
        cancellate_tot: 0,
        soppresse: 0
      });
    }

    const a = agg.get(k);
    a.corse_osservate += toNum(r.corse_osservate);
    a.in_ritardo += toNum(r.in_ritardo);
    a.minuti_ritardo_tot += toNum(r.minuti_ritardo_tot);

    const canc = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    a.cancellate_tot += toNum(canc);

    a.soppresse += toNum(r.soppresse);
  }

  let out = Array.from(agg.values());
  out.forEach((o) => (o.pct_ritardo = o.corse_osservate > 0 ? (o.in_ritardo / o.corse_osservate) * 100 : 0));

  out.sort((a, b) => toNum(b.pct_ritardo) - toNum(a.pct_ritardo));
  safeSetData(state.tables.cities, out.slice(0, 80));
  try {
    if (state.tables.cities) state.tables.cities.setSort("pct_ritardo", "desc");
  } catch {}
}

function mapMetricValue(row) {
  const corse = toNum(row.corse_osservate);
  const rit = toNum(row.in_ritardo);
  const min = toNum(row.minuti_ritardo_tot);
  const sopp = toNum(row.soppresse);
  const canc = toNum(row.cancellate_tot);
  return computeValue(corse, rit, min, sopp, canc);
}

function renderMap() {
  if (!state.map) return;

  clearMarkers();

  const useDay = useDailyAggregation() && state.data.stationsDayNode && state.data.stationsDayNode.length > 0;
  const base = useDay ? state.data.stationsDayNode : state.data.stationsMonthNode;
  const keyField = useDay ? "giorno" : "mese";

  let rows = base || [];

  if (state.filters.year !== "all") rows = rows.filter((r) => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (useDay) rows = rows.filter((r) => passDayKey(r, "giorno"));
  else if (hasDayFilter()) rows = rows.filter((r) => passMonthFromDayRange(r, "mese"));

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
      agg.set(cityKey, {
        cityKey,
        nome: prettyCityName(cityKey, city),
        corse_osservate: 0,
        in_ritardo: 0,
        minuti_ritardo_tot: 0,
        soppresse: 0,
        cancellate_tot: 0,
        lat_weighted_sum: 0,
        lon_weighted_sum: 0,
        weight_sum: 0
      });
    }

    const a = agg.get(cityKey);
    const corse = toNum(r.corse_osservate);
    const weight = Math.max(1, corse);

    a.corse_osservate += corse;
    a.in_ritardo += toNum(r.in_ritardo);
    a.minuti_ritardo_tot += toNum(r.minuti_ritardo_tot);
    a.soppresse += toNum(r.soppresse);
    const canc = r.cancellate_tot !== undefined && r.cancellate_tot !== "" ? r.cancellate_tot : r.cancellate;
    a.cancellate_tot += toNum(canc);

    a.lat_weighted_sum += toNum(coords.lat) * weight;
    a.lon_weighted_sum += toNum(coords.lon) * weight;
    a.weight_sum += weight;
  }

  const pts = Array.from(agg.values())
    .map((o) => {
      const w = o.weight_sum > 0 ? o.weight_sum : 1;
      const v = mapMetricValue(o);
      return {
        ...o,
        coords: { lat: o.lat_weighted_sum / w, lon: o.lon_weighted_sum / w },
        v
      };
    })
    .filter((o) => Number.isFinite(o.coords.lat) && Number.isFinite(o.coords.lon));

  pts.sort((a, b) => toNum(b.v) - toNum(a.v));
  const top = pts.slice(0, 250);

  const values = top.map((p) => Math.max(0, Number(p.v) || 0));
  const maxValue = values.length ? Math.max(...values) : 0;
  const minRadius = 5;
  const maxRadius = 22;

  const bounds = [];
  for (const p of top) {
    const val = Math.max(0, Number(p.v) || 0);
    const label = p.nome + "<br>" + metricLabel() + ": " + fmtFloat(val);

    const ratio = maxValue > 0 ? Math.sqrt(val / maxValue) : 0;
    const radius = minRadius + ratio * (maxRadius - minRadius);

    const m = L.circleMarker([p.coords.lat, p.coords.lon], {
      radius,
      opacity: 0.9,
      fillOpacity: 0.6
    }).addTo(state.map);

    try {
      m.bindPopup(label);
    } catch {}

    state.markers.push(m);
    bounds.push([p.coords.lat, p.coords.lon]);
  }

  if (bounds.length > 3) {
    try {
      state.map.fitBounds(bounds, { padding: [20, 20] });
    } catch {}
  }

  setTimeout(() => {
    try {
      state.map.invalidateSize();
    } catch {}
  }, 100);
}

function renderTables() {
  renderStationsTable();
  renderODTable();
  renderCitiesTable();
}

function renderAll() {
  renderKPI();
  renderSeries();
  renderHist();
  renderTables();
  renderMap();
}

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

async function loadAll() {
  setMeta("Caricamento dati...");

  const base = await pickDataBase();
  state.dataBase = base;

  const man = await fetchJsonAny(candidateFilePaths(base, "manifest.json"));
  state.manifest = man || safeManifestDefaults();

  const built = state.manifest && state.manifest.built_at_utc ? state.manifest.built_at_utc : "";
  setMeta((built ? "Build: " + built : "Build: sconosciuta") + " | base: " + base);

  const files =
    state.manifest && Array.isArray(state.manifest.gold_files) && state.manifest.gold_files.length
      ? state.manifest.gold_files
      : safeManifestDefaults().gold_files;

  const wanted = uniq([
    ...files,
    "kpi_mese.csv",
    "kpi_mese_categoria.csv",
    "kpi_giorno.csv",
    "kpi_giorno_categoria.csv",
    "hist_mese_categoria.csv",
    "hist_giorno_categoria.csv",
    "stazioni_mese_categoria_nodo.csv",
    "stazioni_giorno_categoria_nodo.csv",
    "od_mese_categoria.csv",
    "od_giorno_categoria.csv",
    "hist_stazioni_mese_categoria_ruolo.csv",
    "hist_stazioni_giorno_categoria_ruolo.csv"
  ]);

  const texts = await Promise.all(wanted.map((f) => fetchTextAny(candidateFilePaths(base, f))));

  const parsed = {};
  for (let i = 0; i < wanted.length; i++) {
    const txt = texts[i];
    parsed[wanted[i]] = txt ? parseCSV(txt) : [];
  }

  state.data.kpiMonth = parsed["kpi_mese.csv"] || [];
  state.data.kpiMonthCat = parsed["kpi_mese_categoria.csv"] || [];
  state.data.kpiDay = parsed["kpi_giorno.csv"] || [];
  state.data.kpiDayCat = parsed["kpi_giorno_categoria.csv"] || [];
  state.data.histMonthCat = parsed["hist_mese_categoria.csv"] || [];
  state.data.histDayCat = parsed["hist_giorno_categoria.csv"] || [];
  state.data.stationsMonthNode = parsed["stazioni_mese_categoria_nodo.csv"] || [];
  state.data.stationsDayNode = parsed["stazioni_giorno_categoria_nodo.csv"] || [];
  state.data.odMonthCat = parsed["od_mese_categoria.csv"] || [];
  state.data.odDayCat = parsed["od_giorno_categoria.csv"] || [];
  state.data.histStationsMonthRuolo = parsed["hist_stazioni_mese_categoria_ruolo.csv"] || [];
  state.data.histStationsDayRuolo = parsed["hist_stazioni_giorno_categoria_ruolo.csv"] || [];

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

    state.stationsRef.set(code, {
      code,
      name,
      lat: Number.isFinite(lat) ? lat : NaN,
      lon: Number.isFinite(lon) ? lon : NaN,
      city
    });
  }

  const capRows = await loadCapoluoghiAnyBase(base);
  state.capoluoghiSet = new Set(
    capRows
      .map((r) => normalizeText(r.citta || r.capoluogo || r.nome || r.city || ""))
      .filter(Boolean)
  );

  initFilters();
  initDayWeekTimeControls();
  initTables();
  initMap();
  ensureHistToggle();

  renderAll();

  const haveAny =
    (state.data.kpiMonthCat && state.data.kpiMonthCat.length) ||
    (state.data.kpiDayCat && state.data.kpiDayCat.length) ||
    (state.data.histMonthCat && state.data.histMonthCat.length);

  const coordCount = Array.from(state.stationsRef.values()).filter((s) => Number.isFinite(s.lat) && Number.isFinite(s.lon)).length;

  const metaExtra =
    " | mese cat: " +
    (state.data.kpiMonthCat ? state.data.kpiMonthCat.length : 0) +
    " | giorno cat: " +
    (state.data.kpiDayCat ? state.data.kpiDayCat.length : 0) +
    " | stazioni dim: " +
    stRows.length +
    " | stazioni coord: " +
    coordCount +
    (stationDimBuilt ? " | stations_dim build: " + stationDimBuilt : "");

  if (!haveAny) setMeta("Errore: non trovo CSV validi. Base: " + base + metaExtra);
  else setMeta((built ? "Build: " + built : "Build: sconosciuta") + " | base: " + base + metaExtra);
}

loadAll().catch((err) => {
  console.error(err);
  setMeta("Errore caricamento dati: " + (err && err.message ? err.message : String(err)));
});
