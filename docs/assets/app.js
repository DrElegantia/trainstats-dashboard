// app.js
/* eslint-disable no-undef */
const state = {
  manifest: null,

  kpiMonthCat: [],
  kpiDayCat: [],
  odMonthCat: [],
  stationsMonthNode: [],
  histMonthCat: [],
  odDayCat: [],
  stationsDayNode: [],
  histDayCat: [],
  kpiDayHourCat: [],
  histDayHourCat: [],

  stationsDim: [],
  stationsRef: new Map(),
  capoluoghi: [],
  capoluoghiSet: new Set(),

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

function $(id) {
  return document.getElementById(id);
}

function toNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

function fmtInt(x) {
  const n = Math.round(toNum(x));
  return n.toLocaleString("it-IT");
}

function uniq(arr) {
  return Array.from(new Set(arr));
}

function safeManifestDefaults() {
  return {
    delay_buckets_minutes: {
      buckets: [0, 5, 10, 15, 30, 60, 999999],
      labels: ["0-4", "5-9", "10-14", "15-29", "30-59", "60+"]
    },
    gold_files: [
      "kpi_mese_categoria.csv",
      "kpi_giorno_categoria.csv",
      "od_mese_categoria.csv",
      "stazioni_mese_categoria_nodo.csv",
      "hist_mese_categoria.csv"
    ]
  };
}

async function fetchText(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`fetch failed ${url} ${r.status}`);
  return await r.text();
}

function parseCsv(text) {
  const lines = text.split(/\r?\n/).filter(l => l.trim().length > 0);
  if (!lines.length) return [];
  const header = lines[0].split(",").map(s => s.trim());
  const out = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(",");
    const row = {};
    header.forEach((h, j) => row[h] = (parts[j] ?? "").trim());
    out.push(row);
  }
  return out;
}

async function fetchCsv(url) {
  const t = await fetchText(url);
  return parseCsv(t);
}

function monthFromDay(isoDay) {
  return String(isoDay || "").slice(0, 7);
}

function passYear(row, keyField) {
  const y = state.filters.year;
  if (y === "all") return true;
  const v = String(row[keyField] || "");
  return v.startsWith(String(y));
}

function passCat(row) {
  const c = state.filters.cat;
  if (c === "all") return true;
  return String(row.categoria || "").trim() === String(c);
}

function passDep(row) {
  const d = state.filters.dep;
  if (d === "all") return true;
  return String(row.cod_partenza || "").trim() === String(d);
}

function passArr(row) {
  const a = state.filters.arr;
  if (a === "all") return true;
  return String(row.cod_arrivo || "").trim() === String(a);
}

function hasDayFilter() {
  const a = String(state.filters.day_from || "").trim();
  const b = String(state.filters.day_to || "").trim();
  return !!(a || b);
}

function passDay(row, keyField) {
  if (!hasDayFilter()) return true;
  const from = String(state.filters.day_from || "").trim();
  const to = String(state.filters.day_to || "").trim();
  const d = String(row[keyField] || "").slice(0, 10);

  if (from && to) return d >= from && d <= to;
  if (from) return d >= from;
  if (to) return d <= to;
  return true;
}

function hasWeekdayFilter() {
  const w = state.filters.weekdays;
  if (!Array.isArray(w) || w.length !== 7) return false;
  return w.some(v => !v);
}

function weekdayIndexFromISO(isoDay) {
  const d = String(isoDay || "").slice(0, 10);
  if (!d) return null;
  const dt = new Date(d + "T00:00:00Z");
  if (isNaN(dt.getTime())) return null;
  const u = dt.getUTCDay();
  return (u + 6) % 7;
}

function passWeekdayISO(isoDay) {
  if (!hasWeekdayFilter()) return true;
  const idx = weekdayIndexFromISO(isoDay);
  if (idx === null) return false;
  const w = state.filters.weekdays;
  return !!w[idx];
}

function passWeekday(row, keyField) {
  const d = String(row[keyField] || "").slice(0, 10);
  return passWeekdayISO(d);
}

function hasTimeFilter() {
  return state.filters.time_all === false;
}

function parseTimeToMinutes(t) {
  const s = String(t || "").trim();
  const m = /^([0-9]{1,2}):([0-9]{2})$/.exec(s);
  if (!m) return null;
  const hh = Math.max(0, Math.min(23, parseInt(m[1], 10)));
  const mm = Math.max(0, Math.min(59, parseInt(m[2], 10)));
  return hh * 60 + mm;
}

function passHour(row, hourField) {
  if (!hasTimeFilter()) return true;
  const h = toNum(row[hourField]);
  if (!isFinite(h)) return false;

  const fromM0 = parseTimeToMinutes(state.filters.time_from || "00:00");
  const toM0 = parseTimeToMinutes(state.filters.time_to || "23:59");
  const fromM = fromM0 === null ? 0 : fromM0;
  const toM = toM0 === null ? (23 * 60 + 59) : toM0;

  const lo = Math.min(fromM, toM);
  const hi = Math.max(fromM, toM);

  const start = h * 60;
  const end = h * 60 + 59;
  return end >= lo && start <= hi;
}

function groupKpiHourToDay(hourRows) {
  const keys = [
    "corse_osservate", "effettuate", "cancellate", "soppresse", "parzialmente_cancellate", "info_mancante",
    "in_orario", "in_ritardo", "in_anticipo",
    "oltre_5", "oltre_10", "oltre_15", "oltre_30", "oltre_60",
    "minuti_ritardo_tot", "minuti_anticipo_tot", "minuti_netti_tot"
  ];

  const by = new Map();

  for (const r of hourRows) {
    const g = String(r.giorno || "").slice(0, 10);
    const c = String(r.categoria || "").trim() || "all";
    if (!g) continue;
    const k = g + "|" + c;
    if (!by.has(k)) {
      const base = { giorno: g, categoria: c };
      keys.forEach(x => base[x] = 0);
      by.set(k, base);
    }
    const a = by.get(k);
    keys.forEach(x => a[x] += toNum(r[x]));
  }

  const out = Array.from(by.values());
  out.sort((a, b) => String(a.giorno).localeCompare(String(b.giorno)) || String(a.categoria).localeCompare(String(b.categoria)));
  return out;
}

function getKpiDailyRowsFiltered() {
  let rows = [];

  if (hasTimeFilter() && Array.isArray(state.kpiDayHourCat) && state.kpiDayHourCat.length > 0) {
    rows = state.kpiDayHourCat;
    if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, "giorno"));
    if (state.filters.cat !== "all") rows = rows.filter(passCat);
    if (hasDayFilter()) rows = rows.filter(r => passDay(r, "giorno"));
    if (hasWeekdayFilter()) rows = rows.filter(r => passWeekday(r, "giorno"));
    rows = rows.filter(r => passHour(r, "ora"));
    return groupKpiHourToDay(rows);
  }

  rows = state.kpiDayCat;

  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, "giorno"));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (hasDayFilter()) rows = rows.filter(r => passDay(r, "giorno"));
  if (hasWeekdayFilter()) rows = rows.filter(r => passWeekday(r, "giorno"));

  return rows;
}

function getKpiMonthlyRowsFiltered() {
  const needDaily = hasDayFilter() || hasWeekdayFilter() || hasTimeFilter();
  if (!needDaily) {
    let m = state.kpiMonthCat;
    if (state.filters.year !== "all") m = m.filter(r => passYear(r, "mese"));
    if (state.filters.cat !== "all") m = m.filter(passCat);
    return m;
  }

  const daily = getKpiDailyRowsFiltered();
  return groupDailyToMonthly(daily);
}

function passMonthFromDayRange(row, keyField) {
  if (!hasDayFilter()) return true;
  const from = String(state.filters.day_from || "").trim();
  const to = String(state.filters.day_to || "").trim();
  const m = String(row[keyField] || "").slice(0, 7);

  const fromM = from ? from.slice(0, 7) : "";
  const toM = to ? to.slice(0, 7) : "";

  if (fromM && toM) return m >= fromM && m <= toM;
  if (fromM) return m >= fromM;
  if (toM) return m <= toM;
  return true;
}

function getHistRowsFiltered() {
  if (state.filters.dep !== "all" || state.filters.arr !== "all") return [];

  if (hasTimeFilter() && Array.isArray(state.histDayHourCat) && state.histDayHourCat.length > 0) {
    let rows = state.histDayHourCat;
    if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, "giorno"));
    if (state.filters.cat !== "all") rows = rows.filter(passCat);
    if (hasDayFilter()) rows = rows.filter(r => passDay(r, "giorno"));
    if (hasWeekdayFilter()) rows = rows.filter(r => passWeekday(r, "giorno"));
    rows = rows.filter(r => passHour(r, "ora"));
    return rows;
  }

  const needDaily = (hasDayFilter() || hasWeekdayFilter()) && Array.isArray(state.histDayCat) && state.histDayCat.length > 0;
  if (needDaily) {
    let rows = state.histDayCat;
    if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, "giorno"));
    if (state.filters.cat !== "all") rows = rows.filter(passCat);
    if (hasDayFilter()) rows = rows.filter(r => passDay(r, "giorno"));
    if (hasWeekdayFilter()) rows = rows.filter(r => passWeekday(r, "giorno"));
    return rows;
  }

  let rows = state.histMonthCat;
  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, "mese"));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (hasDayFilter()) rows = rows.filter(r => passMonthFromDayRange(r, "mese"));

  return rows;
}

async function loadAll() {
  let manifest = null;
  try {
    manifest = JSON.parse(await fetchText("data/manifest.json"));
  } catch (e) {
    manifest = safeManifestDefaults();
  }
  state.manifest = manifest;

  const files = manifest.gold_files || [];
  const base = {};
  for (const f of files) {
    base[f] = await fetchCsv("data/" + f);
  }

  state.kpiMonthCat = base["kpi_mese_categoria.csv"] || [];
  state.kpiDayCat = base["kpi_giorno_categoria.csv"] || [];
  state.odMonthCat = base["od_mese_categoria.csv"] || [];
  state.stationsMonthNode = base["stazioni_mese_categoria_nodo.csv"] || [];
  state.histMonthCat = base["hist_mese_categoria.csv"] || [];

  const extraFiles = [
    "od_giorno_categoria.csv",
    "stazioni_giorno_categoria_nodo.csv",
    "hist_giorno_categoria.csv",
    "kpi_giorno_ora_categoria.csv",
    "hist_giorno_ora_categoria.csv"
  ];

  const extraParsed = {};
  for (const f of extraFiles) {
    try {
      extraParsed[f] = await fetchCsv("data/" + f);
    } catch (e) {
      extraParsed[f] = [];
    }
  }

  state.odDayCat = extraParsed["od_giorno_categoria.csv"] || [];
  state.stationsDayNode = extraParsed["stazioni_giorno_categoria_nodo.csv"] || [];
  state.histDayCat = extraParsed["hist_giorno_categoria.csv"] || [];
  state.kpiDayHourCat = extraParsed["kpi_giorno_ora_categoria.csv"] || [];
  state.histDayHourCat = extraParsed["hist_giorno_ora_categoria.csv"] || [];

  state.stationsDim = await fetchCsv("data/stations_dim.csv");
  state.stationsRef = new Map();

  for (const r of state.stationsDim) {
    const code = String(r.cod_stazione || r.codice || r.cod || "").trim();
    if (!code) continue;

    const name = String(r.nome_stazione || r.nome || r.name || "").trim();
    const city = String(r.citta || r.comune || r.city || r.nome_comune || "").trim();
    const lat = toNum(r.lat);
    const lon = toNum(r.lon);

    state.stationsRef.set(code, { code, name, city, lat, lon, raw: r });
  }

  try {
    state.capoluoghi = await fetchCsv("data/capoluoghi_provincia.csv");
  } catch (e) {
    state.capoluoghi = [];
  }
  state.capoluoghiSet = new Set(state.capoluoghi.map(r => String(r.citta || "").trim()).filter(Boolean));

  initSelectors();
  initDayControls();
  initTables();
  initMap();
  initCities();

  renderAll();
}

function initSelectors() {
  const years = uniq(state.kpiMonthCat.map(r => String(r.mese || "").slice(0, 4)).filter(Boolean)).sort();
  const cats = uniq(state.kpiMonthCat.map(r => String(r.categoria || "").trim()).filter(Boolean)).sort();

  const yearSel = $("yearSel");
  const catSel = $("catSel");

  yearSel.innerHTML = "";
  catSel.innerHTML = "";

  const optAllY = document.createElement("option");
  optAllY.value = "all";
  optAllY.innerText = "Tutti";
  yearSel.appendChild(optAllY);

  years.forEach(y => {
    const o = document.createElement("option");
    o.value = y;
    o.innerText = y;
    yearSel.appendChild(o);
  });

  const optAllC = document.createElement("option");
  optAllC.value = "all";
  optAllC.innerText = "Tutte";
  catSel.appendChild(optAllC);

  cats.forEach(c => {
    const o = document.createElement("option");
    o.value = c;
    o.innerText = c;
    catSel.appendChild(o);
  });

  yearSel.onchange = () => {
    state.filters.year = yearSel.value || "all";
    renderAll();
  };

  catSel.onchange = () => {
    state.filters.cat = catSel.value || "all";
    renderAll();
  };
}

function initDayControls() {
  const yearSel = document.getElementById("yearSel");
  if (!yearSel || !yearSel.parentNode) return;

  if (document.getElementById("dayFrom")) return;

  const days = uniq(state.kpiDayCat.map(r => String(r.giorno || "").slice(0, 10)).filter(Boolean)).sort();
  const minDay = days.length ? days[0] : "";
  const maxDay = days.length ? days[days.length - 1] : "";

  const wrap = document.createElement("div");
  wrap.style.display = "flex";
  wrap.style.alignItems = "center";
  wrap.style.gap = "10px";
  wrap.style.margin = "6px 0 0 0";
  wrap.style.flexWrap = "wrap";

  const lab = document.createElement("div");
  lab.innerText = "Giorno";

  const from = document.createElement("input");
  from.type = "date";
  from.id = "dayFrom";
  if (minDay) from.min = minDay;
  if (maxDay) from.max = maxDay;
  from.value = "";

  const to = document.createElement("input");
  to.type = "date";
  to.id = "dayTo";
  if (minDay) to.min = minDay;
  if (maxDay) to.max = maxDay;
  to.value = "";

  const wdLab = document.createElement("div");
  wdLab.innerText = "Giorni";

  const wdWrap = document.createElement("div");
  wdWrap.id = "weekdayWrap";
  wdWrap.style.display = "flex";
  wdWrap.style.alignItems = "center";
  wdWrap.style.gap = "6px";

  const wdLabels = ["Lu", "Ma", "Me", "Gi", "Ve", "Sa", "Do"];
  const ensureWeekdays = () => {
    if (!Array.isArray(state.filters.weekdays) || state.filters.weekdays.length !== 7) {
      state.filters.weekdays = [true, true, true, true, true, true, true];
    }
  };
  ensureWeekdays();

  const refreshWdStyles = () => {
    const btns = wdWrap.querySelectorAll("button[data-wd]");
    btns.forEach(b => {
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
      updateDayNote();
      renderAll();
    };
    wdWrap.appendChild(b);
  });

  refreshWdStyles();

  const timeLab = document.createElement("div");
  timeLab.innerText = "Orario";

  const timeAllWrap = document.createElement("label");
  timeAllWrap.style.display = "inline-flex";
  timeAllWrap.style.alignItems = "center";
  timeAllWrap.style.gap = "6px";
  timeAllWrap.style.cursor = "pointer";

  const timeAll = document.createElement("input");
  timeAll.type = "checkbox";
  timeAll.id = "timeAll";
  timeAll.checked = state.filters.time_all !== false;

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

  const syncTimeDisabled = () => {
    const allDay = timeAll.checked;
    timeFrom.disabled = allDay;
    timeTo.disabled = allDay;
  };
  syncTimeDisabled();

  const note = document.createElement("div");
  note.id = "dayNote";
  note.style.fontSize = "12px";
  note.style.opacity = "0.75";
  note.style.marginLeft = "6px";

  wrap.appendChild(lab);
  wrap.appendChild(from);
  wrap.appendChild(to);
  wrap.appendChild(wdLab);
  wrap.appendChild(wdWrap);
  wrap.appendChild(timeLab);
  wrap.appendChild(timeAllWrap);
  wrap.appendChild(timeFrom);
  wrap.appendChild(timeTo);
  wrap.appendChild(note);

  yearSel.parentNode.appendChild(wrap);

  const apply = () => {
    state.filters.day_from = String(from.value || "").trim();
    state.filters.day_to = String(to.value || "").trim();

    ensureWeekdays();

    state.filters.time_all = !!timeAll.checked;
    state.filters.time_from = String(timeFrom.value || "00:00");
    state.filters.time_to = String(timeTo.value || "23:59");

    syncTimeDisabled();
    refreshWdStyles();
    updateDayNote();
    renderAll();
  };

  from.onchange = apply;
  to.onchange = apply;
  timeAll.onchange = apply;
  timeFrom.onchange = apply;
  timeTo.onchange = apply;

  updateDayNote();
}

function updateDayNote() {
  const el = document.getElementById("dayNote");
  if (!el) return;

  const any = hasDayFilter() || hasWeekdayFilter() || hasTimeFilter();
  if (!any) {
    el.innerText = "";
    return;
  }

  let msg = "Filtri attivi.";

  if (hasWeekdayFilter()) {
    const haveAnyDaily = Array.isArray(state.kpiDayCat) && state.kpiDayCat.length > 0;
    if (!haveAnyDaily) msg += " Filtro giorni della settimana attivo, ma mancano serie giornaliere.";
  }

  if (hasTimeFilter()) {
    const haveKpiH = Array.isArray(state.kpiDayHourCat) && state.kpiDayHourCat.length > 0;
    const haveHistH = Array.isArray(state.histDayHourCat) && state.histDayHourCat.length > 0;
    if (!haveKpiH) msg += " Filtro orario attivo, ma manca kpi_giorno_ora_categoria.csv.";
    if (!haveHistH) msg += " Filtro orario attivo, ma manca hist_giorno_ora_categoria.csv.";
  }

  const haveOdDay = Array.isArray(state.odDayCat) && state.odDayCat.length > 0;
  const haveStDay = Array.isArray(state.stationsDayNode) && state.stationsDayNode.length > 0;
  const haveHistDay = Array.isArray(state.histDayCat) && state.histDayCat.length > 0;

  if ((hasDayFilter() || hasWeekdayFilter()) && !(haveOdDay || haveStDay || haveHistDay)) {
    msg += " Per tabelle, mappa e tratte servono anche OD, stazioni e istogrammi giornalieri.";
  }

  if (hasTimeFilter() && (state.filters.dep !== "all" || state.filters.arr !== "all")) {
    msg += " Con filtri di partenza/arrivo il filtro orario non si applica alle tratte.";
  }

  el.innerText = msg;
}

function groupDailyToMonthly(dailyRows) {
  const by = new Map();
  dailyRows.forEach(r => {
    const m = monthFromDay(r.giorno);
    const c = String(r.categoria || "").trim() || "all";
    const k = m + "|" + c;
    if (!by.has(k)) by.set(k, { mese: m, categoria: c, ...sumRows([]) });
    const agg = by.get(k);
    const s = sumRows([r]);
    Object.keys(s).forEach(x => {
      if (x === "mese" || x === "giorno" || x === "categoria") return;
      agg[x] = toNum(agg[x]) + toNum(s[x]);
    });
  });
  return Array.from(by.values()).sort((a, b) => String(a.mese).localeCompare(String(b.mese)) || String(a.categoria).localeCompare(String(b.categoria)));
}

function sumRows(rows) {
  const out = {
    corse_osservate: 0,
    effettuate: 0,
    cancellate: 0,
    soppresse: 0,
    parzialmente_cancellate: 0,
    info_mancante: 0,
    in_orario: 0,
    in_ritardo: 0,
    in_anticipo: 0,
    oltre_5: 0,
    oltre_10: 0,
    oltre_15: 0,
    oltre_30: 0,
    oltre_60: 0,
    minuti_ritardo_tot: 0,
    minuti_anticipo_tot: 0,
    minuti_netti_tot: 0
  };
  rows.forEach(r => {
    Object.keys(out).forEach(k => out[k] += toNum(r[k]));
  });
  return out;
}

function renderAll() {
  renderKPI();
  renderSeries();
  renderHist();
  renderTables();
  renderMap();
  renderCities();
}

function renderKPI() {
  const kpiTotal = $("kpiTotal");
  const kpiLate = $("kpiLate");
  const kpiLateMin = $("kpiLateMin");
  const kpiCancelled = $("kpiCancelled");
  const kpiSuppressed = $("kpiSuppressed");

  if (!kpiTotal) return;

  if (state.filters.dep !== "all" || state.filters.arr !== "all") {
    const wantDay = (hasDayFilter() || hasWeekdayFilter()) && state.odDayCat.length > 0;
    const odBase = wantDay && state.odDayCat.length ? state.odDayCat : state.odMonthCat;
    const keyField = wantDay && state.odDayCat.length ? "giorno" : "mese";

    let od = odBase;
    if (state.filters.year !== "all") od = od.filter(r => passYear(r, keyField));
    if (state.filters.cat !== "all") od = od.filter(passCat);

    if (wantDay && keyField === "giorno") {
      if (hasDayFilter()) od = od.filter(r => passDay(r, "giorno"));
      if (hasWeekdayFilter()) od = od.filter(r => passWeekday(r, "giorno"));
    }
    if (!wantDay && hasDayFilter()) od = od.filter(r => passMonthFromDayRange(r, "mese"));
    if (!wantDay && hasWeekdayFilter()) od = [];

    if (state.filters.dep !== "all") od = od.filter(passDep);
    if (state.filters.arr !== "all") od = od.filter(passArr);

    const out = sumRows(od);

    kpiTotal.innerText = fmtInt(out.corse_osservate);
    kpiLate.innerText = fmtInt(out.in_ritardo);
    kpiLateMin.innerText = fmtInt(out.minuti_ritardo_tot);
    kpiCancelled.innerText = fmtInt(out.cancellate);
    kpiSuppressed.innerText = fmtInt(out.soppresse);
    return;
  }

  const needDaily = hasDayFilter() || hasWeekdayFilter() || hasTimeFilter();
  if (needDaily) {
    const daily = getKpiDailyRowsFiltered();
    const s = sumRows(daily);
    kpiTotal.innerText = fmtInt(s.corse_osservate);
    kpiLate.innerText = fmtInt(s.in_ritardo);
    kpiLateMin.innerText = fmtInt(s.minuti_ritardo_tot);
    kpiCancelled.innerText = fmtInt(s.cancellate);
    kpiSuppressed.innerText = fmtInt(s.soppresse);
    return;
  }

  let monthly = state.kpiMonthCat;
  if (state.filters.year !== "all") monthly = monthly.filter(r => passYear(r, "mese"));
  if (state.filters.cat !== "all") monthly = monthly.filter(passCat);

  const s = sumRows(monthly);
  kpiTotal.innerText = fmtInt(s.corse_osservate);
  kpiLate.innerText = fmtInt(s.in_ritardo);
  kpiLateMin.innerText = fmtInt(s.minuti_ritardo_tot);
  kpiCancelled.innerText = fmtInt(s.cancellate);
  kpiSuppressed.innerText = fmtInt(s.soppresse);
}

function renderSeries() {
  if (typeof Plotly === "undefined") return;

  const chartEl = document.getElementById("chartDaily");
  if (!chartEl) return;

  const chartMonthEl = document.getElementById("chartMonthly");
  if (!chartMonthEl) return;

  const year = state.filters.year;
  const cat = state.filters.cat;

  const label = (year !== "all" ? year : "Tutti gli anni") + " · " + (cat !== "all" ? cat : "Tutte le categorie");

  const sumKeys = [
    "corse_osservate", "effettuate", "cancellate", "soppresse", "parzialmente_cancellate", "info_mancante",
    "in_orario", "in_ritardo", "in_anticipo",
    "oltre_5", "oltre_10", "oltre_15", "oltre_30", "oltre_60",
    "minuti_ritardo_tot", "minuti_anticipo_tot", "minuti_netti_tot"
  ];

  const groupToDaily = (rows, dayField) => {
    const by = new Map();
    for (const r of rows) {
      const g = String(r[dayField] || "").slice(0, 10);
      if (!g) continue;
      if (!by.has(g)) {
        const base = { giorno: g };
        sumKeys.forEach(k => base[k] = 0);
        by.set(g, base);
      }
      const a = by.get(g);
      sumKeys.forEach(k => a[k] += toNum(r[k]));
    }
    const out = Array.from(by.values());
    out.sort((a, b) => String(a.giorno).localeCompare(String(b.giorno)));
    return out;
  };

  let dailyRows = [];
  let dailyNote = "";

  if (state.filters.dep !== "all" || state.filters.arr !== "all") {
    if (Array.isArray(state.odDayCat) && state.odDayCat.length > 0) {
      let od = state.odDayCat;
      if (year !== "all") od = od.filter(r => passYear(r, "giorno"));
      if (cat !== "all") od = od.filter(passCat);

      if (hasDayFilter()) od = od.filter(r => passDay(r, "giorno"));
      if (hasWeekdayFilter()) od = od.filter(r => passWeekday(r, "giorno"));

      if (state.filters.dep !== "all") od = od.filter(passDep);
      if (state.filters.arr !== "all") od = od.filter(passArr);

      dailyRows = groupToDaily(od, "giorno");
    } else {
      dailyNote = "Serie giornaliera non disponibile (manca od_giorno_categoria.csv).";
    }
  } else {
    dailyRows = getKpiDailyRowsFiltered();
  }

  const xDay = dailyRows.map(r => String(r.giorno || "").slice(0, 10));
  const yLate = dailyRows.map(r => {
    const n = toNum(r.corse_osservate);
    const late = toNum(r.in_ritardo);
    return n > 0 ? (late / n) * 100 : 0;
  });

  Plotly.newPlot("chartDaily", [{
    x: xDay,
    y: yLate,
    mode: "lines",
    name: "Ritardo %"
  }], {
    margin: { t: 20, l: 40, r: 20, b: 60 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e6e9f2" },
    xaxis: { title: label, gridcolor: "rgba(255,255,255,0.08)" },
    yaxis: { title: "% in ritardo", ticksuffix: "%", gridcolor: "rgba(255,255,255,0.08)", rangemode: "tozero" },
    annotations: dailyNote ? [{
      text: dailyNote,
      xref: "paper",
      yref: "paper",
      x: 0.5,
      y: 0.5,
      showarrow: false,
      font: { size: 12 }
    }] : []
  }, { displayModeBar: false });

  let monthlyRows = [];

  if (state.filters.dep !== "all" || state.filters.arr !== "all") {
    if (dailyRows.length && (hasDayFilter() || hasWeekdayFilter())) {
      monthlyRows = groupDailyToMonthly(dailyRows);
    } else {
      let od = state.odMonthCat;
      if (year !== "all") od = od.filter(r => passYear(r, "mese"));
      if (cat !== "all") od = od.filter(passCat);

      if (hasDayFilter()) od = od.filter(r => passMonthFromDayRange(r, "mese"));
      if (hasWeekdayFilter()) od = [];

      if (state.filters.dep !== "all") od = od.filter(passDep);
      if (state.filters.arr !== "all") od = od.filter(passArr);

      monthlyRows = od;
    }
  } else {
    monthlyRows = getKpiMonthlyRowsFiltered();
  }

  const xM = monthlyRows.map(r => String(r.mese || "").slice(0, 7));
  const yM = monthlyRows.map(r => {
    const n = toNum(r.corse_osservate);
    const late = toNum(r.in_ritardo);
    return n > 0 ? (late / n) * 100 : 0;
  });

  Plotly.newPlot("chartMonthly", [{
    x: xM,
    y: yM,
    mode: "lines+markers",
    name: "Ritardo %"
  }], {
    margin: { t: 10, l: 40, r: 20, b: 60 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e6e9f2" },
    xaxis: { title: "Serie mensile", gridcolor: "rgba(255,255,255,0.08)" },
    yaxis: { title: "% in ritardo", ticksuffix: "%", gridcolor: "rgba(255,255,255,0.08)", rangemode: "tozero" }
  }, { displayModeBar: false });
}

function renderHist() {
  if (typeof Plotly === "undefined") return;
  const chartHistEl = document.getElementById("chartHist");
  if (!chartHistEl) return;

  const t = document.getElementById("histModeToggle");
  const mode = t && t.checked ? "pct" : "count";

  const rows = getHistRowsFiltered();

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
  const counts = labels.map(l => byBucket.get(l) || 0);
  const total = counts.reduce((a, b) => a + b, 0);

  const y = mode === "pct"
    ? counts.map(v => total > 0 ? (v / total) * 100 : 0)
    : counts;

  const yTitle = mode === "pct" ? "Percentuale" : "Conteggio";
  const ySuffix = mode === "pct" ? "%" : "";

  Plotly.newPlot("chartHist", [{ x, y, name: mode === "pct" ? "Percentuale" : "Conteggio", type: "bar" }], {
    margin: { t: 10, l: 40, r: 20, b: 90 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e6e9f2" },
    xaxis: { tickangle: -35, gridcolor: "rgba(255,255,255,0.08)" },
    yaxis: { title: yTitle, ticksuffix: ySuffix, gridcolor: "rgba(255,255,255,0.08)" }
  }, { displayModeBar: false });
}

function initTables() {
  const depSel = $("depSel");
  const arrSel = $("arrSel");
  const resetBtn = $("resetFilters");

  const stations = uniq(state.odMonthCat.flatMap(r => [String(r.cod_partenza || "").trim(), String(r.cod_arrivo || "").trim()]).filter(Boolean)).sort();

  depSel.innerHTML = "";
  arrSel.innerHTML = "";

  const o0 = document.createElement("option");
  o0.value = "all";
  o0.innerText = "Tutte";
  depSel.appendChild(o0);

  const o1 = document.createElement("option");
  o1.value = "all";
  o1.innerText = "Tutte";
  arrSel.appendChild(o1);

  stations.forEach(code => {
    const info = state.stationsRef.get(code);
    const label = info && info.name ? `${info.name} (${code})` : code;

    const od = document.createElement("option");
    od.value = code;
    od.innerText = label;
    depSel.appendChild(od);

    const oa = document.createElement("option");
    oa.value = code;
    oa.innerText = label;
    arrSel.appendChild(oa);
  });

  depSel.onchange = () => {
    state.filters.dep = depSel.value || "all";
    renderAll();
  };

  arrSel.onchange = () => {
    state.filters.arr = arrSel.value || "all";
    renderAll();
  };

  resetBtn.onclick = () => {
    state.filters.year = "all";
    state.filters.cat = "all";
    state.filters.dep = "all";
    state.filters.arr = "all";
    state.filters.day_from = "";
    state.filters.day_to = "";
      state.filters.weekdays = [true,true,true,true,true,true,true];
      state.filters.time_all = true;
      state.filters.time_from = "00:00";
      state.filters.time_to = "23:59";

    $("yearSel").value = "all";
    $("catSel").value = "all";
    depSel.value = "all";
    arrSel.value = "all";

    const dayFrom = $("dayFrom");
    const dayTo = $("dayTo");
    if (dayFrom) dayFrom.value = "";
    if (dayTo) dayTo.value = "";

      const weekdayBtns = document.querySelectorAll("#weekdayWrap button[data-wd]");
      if (weekdayBtns && weekdayBtns.length) {
        weekdayBtns.forEach(b => {
          b.style.opacity = "1";
          b.style.borderColor = "rgba(255,255,255,0.85)";
        });
      }

      const timeAll = document.getElementById("timeAll");
      const timeFrom = document.getElementById("timeFrom");
      const timeTo = document.getElementById("timeTo");
      if (timeAll) timeAll.checked = true;
      if (timeFrom) { timeFrom.value = "00:00"; timeFrom.disabled = true; }
      if (timeTo) { timeTo.value = "23:59"; timeTo.disabled = true; }

      updateDayNote();
      renderAll();
    };
}

function renderTables() {
  const stBody = $("stationsTableBody");
  const odBody = $("odTableBody");
  if (!stBody || !odBody) return;

  {
    const useDay = (hasDayFilter() || hasWeekdayFilter()) && state.stationsDayNode.length > 0 && state.filters.dep === "all" && state.filters.arr === "all";

    let st = useDay ? state.stationsDayNode : state.stationsMonthNode;

    if (state.filters.year !== "all") st = st.filter(r => passYear(r, useDay ? "giorno" : "mese"));
    if (state.filters.cat !== "all") st = st.filter(passCat);

    if (useDay) {
      if (hasDayFilter()) st = st.filter(r => passDay(r, "giorno"));
      if (hasWeekdayFilter()) st = st.filter(r => passWeekday(r, "giorno"));
    }
    if (!useDay && hasDayFilter()) st = st.filter(r => passMonthFromDayRange(r, "mese"));
    if (!useDay && hasWeekdayFilter()) st = [];

    st = st.slice().sort((a, b) => (toNum(b.in_ritardo) / Math.max(1, toNum(b.corse_osservate))) - (toNum(a.in_ritardo) / Math.max(1, toNum(a.corse_osservate)))).slice(0, 20);

    stBody.innerHTML = "";
    st.forEach(r => {
      const code = String(r.cod_stazione || "").trim();
      const info = state.stationsRef.get(code);
      const name = info && info.name ? info.name : code;

      const pct = (toNum(r.corse_osservate) > 0) ? (toNum(r.in_ritardo) / toNum(r.corse_osservate)) * 100 : 0;

      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${name}</td>
        <td>${fmtInt(r.corse_osservate)}</td>
        <td>${pct.toFixed(1)}%</td>
      `;
      stBody.appendChild(tr);
    });
  }

  {
    const useDay = (hasDayFilter() || hasWeekdayFilter()) && state.odDayCat.length > 0;

    let od = useDay ? state.odDayCat : state.odMonthCat;

    if (state.filters.year !== "all") od = od.filter(r => passYear(r, useDay ? "giorno" : "mese"));
    if (state.filters.cat !== "all") od = od.filter(passCat);

    if (useDay) {
      if (hasDayFilter()) od = od.filter(r => passDay(r, "giorno"));
      if (hasWeekdayFilter()) od = od.filter(r => passWeekday(r, "giorno"));
    }
    if (!useDay && hasDayFilter()) od = od.filter(r => passMonthFromDayRange(r, "mese"));
    if (!useDay && hasWeekdayFilter()) od = [];

    if (state.filters.dep !== "all") od = od.filter(passDep);
    if (state.filters.arr !== "all") od = od.filter(passArr);

    od = od.slice().sort((a, b) => (toNum(b.in_ritardo) / Math.max(1, toNum(b.corse_osservate))) - (toNum(a.in_ritardo) / Math.max(1, toNum(a.corse_osservate)))).slice(0, 20);

    odBody.innerHTML = "";
    od.forEach(r => {
      const d = String(r.cod_partenza || "").trim();
      const a = String(r.cod_arrivo || "").trim();
      const di = state.stationsRef.get(d);
      const ai = state.stationsRef.get(a);
      const dn = di && di.name ? di.name : d;
      const an = ai && ai.name ? ai.name : a;

      const pct = (toNum(r.corse_osservate) > 0) ? (toNum(r.in_ritardo) / toNum(r.corse_osservate)) * 100 : 0;

      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${dn}</td>
        <td>${an}</td>
        <td>${fmtInt(r.corse_osservate)}</td>
        <td>${pct.toFixed(1)}%</td>
      `;
      odBody.appendChild(tr);
    });
  }
}

function initMap() {
  if (!window.L) return;

  const el = $("map");
  if (!el) return;

  state._map = L.map("map").setView([41.9, 12.5], 6);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 18,
    attribution: "&copy; OpenStreetMap contributors"
  }).addTo(state._map);

  state._mapLayer = L.layerGroup().addTo(state._map);
}

function renderMap() {
  if (!window.L || !state._mapLayer) return;

  state._mapLayer.clearLayers();

  const useDay = (hasDayFilter() || hasWeekdayFilter()) && state.stationsDayNode.length > 0 && state.filters.dep === "all" && state.filters.arr === "all";
  let st = useDay ? state.stationsDayNode : state.stationsMonthNode;

  if (state.filters.year !== "all") st = st.filter(r => passYear(r, useDay ? "giorno" : "mese"));
  if (state.filters.cat !== "all") st = st.filter(passCat);

  if (useDay) {
    if (hasDayFilter()) st = st.filter(r => passDay(r, "giorno"));
    if (hasWeekdayFilter()) st = st.filter(r => passWeekday(r, "giorno"));
  }
  if (!useDay && hasDayFilter()) st = st.filter(r => passMonthFromDayRange(r, "mese"));
  if (!useDay && hasWeekdayFilter()) st = [];

  const top = st.slice().sort((a, b) => (toNum(b.in_ritardo) / Math.max(1, toNum(b.corse_osservate))) - (toNum(a.in_ritardo) / Math.max(1, toNum(a.corse_osservate)))).slice(0, 60);

  top.forEach(r => {
    const code = String(r.cod_stazione || "").trim();
    const info = state.stationsRef.get(code);
    if (!info || !isFinite(info.lat) || !isFinite(info.lon)) return;

    const pct = (toNum(r.corse_osservate) > 0) ? (toNum(r.in_ritardo) / toNum(r.corse_osservate)) * 100 : 0;
    const popup = `${info.name || code}<br/>Corse: ${fmtInt(r.corse_osservate)}<br/>Ritardo: ${pct.toFixed(1)}%`;

    L.circleMarker([info.lat, info.lon], { radius: 6, weight: 1, opacity: 0.8, fillOpacity: 0.35 })
      .bindPopup(popup)
      .addTo(state._mapLayer);
  });
}

function initCities() {
  const t = $("capoluoghiOnlyToggle");
  if (t) t.onchange = () => renderCities();
}

function passCapoluoghiOnly(cityName) {
  const t = $("capoluoghiOnlyToggle");
  if (!t || !t.checked) return true;
  const c = String(cityName || "").trim();
  if (!c) return false;
  return state.capoluoghiSet.has(c);
}

function renderCities() {
  const cityBody = $("citiesTableBody");
  if (!cityBody) return;

  const useDay = (hasDayFilter() || hasWeekdayFilter()) && state.stationsDayNode.length > 0 && state.filters.dep === "all" && state.filters.arr === "all";
  let rows = useDay ? state.stationsDayNode : state.stationsMonthNode;

  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, useDay ? "giorno" : "mese"));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  if (useDay) {
    if (hasDayFilter()) rows = rows.filter(r => passDay(r, "giorno"));
    if (hasWeekdayFilter()) rows = rows.filter(r => passWeekday(r, "giorno"));
  }
  if (!useDay && hasDayFilter()) rows = rows.filter(r => passMonthFromDayRange(r, "mese"));
  if (!useDay && hasWeekdayFilter()) rows = [];

  const cityAgg = new Map();
  rows.forEach(r => {
    const code = String(r.cod_stazione || "").trim();
    const info = state.stationsRef.get(code);
    const city = info && info.city ? info.city : "";
    if (!city) return;
    if (!passCapoluoghiOnly(city)) return;

    if (!cityAgg.has(city)) cityAgg.set(city, { city, corse: 0, late: 0 });
    const a = cityAgg.get(city);
    a.corse += toNum(r.corse_osservate);
    a.late += toNum(r.in_ritardo);
  });

  const arr = Array.from(cityAgg.values()).map(x => ({
    city: x.city,
    corse: x.corse,
    pct: x.corse > 0 ? (x.late / x.corse) * 100 : 0
  })).sort((a, b) => b.pct - a.pct).slice(0, 20);

  cityBody.innerHTML = "";
  arr.forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.city}</td>
      <td>${fmtInt(r.corse)}</td>
      <td>${r.pct.toFixed(1)}%</td>
    `;
    cityBody.appendChild(tr);
  });
}

document.addEventListener("DOMContentLoaded", () => {
  loadAll().catch(err => {
    console.error(err);
    const el = $("errorBox");
    if (el) el.innerText = String(err);
  });
});
