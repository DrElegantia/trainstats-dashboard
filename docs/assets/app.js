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

function parseCSV(text) {
  const t = String(text || "").trim();
  if (!t) return [];
  const lines = t.split(/\r?\n/);
  if (lines.length <= 1) return [];
  const header = lines[0].split(",").map(x => String(x || "").trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const line = String(lines[i] || "");
    if (!line.trim()) continue;
    const cols = splitCSVLine(line);
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

  input.oninput = () => fillStationSelect(selectEl, items, input.value);
}

function safeSetData(table, data) {
  if (!table || typeof table.setData !== "function") return;
  try {
    const p = table.setData(data);
    if (p && typeof p.catch === "function") p.catch(() => {});
  } catch {}
}

function isCapoluogoCity(cityName) {
  if (!state.capoluoghiSet || state.capoluoghiSet.size === 0) return true;
  return state.capoluoghiSet.has(normalizeText(cityName));
}

function hasDayFilter() {
  return !!(state.filters.day_from || state.filters.day_to);
}

function passDay(row, keyField) {
  const from = String(state.filters.day_from || "").trim();
  const to = String(state.filters.day_to || "").trim();
  if (!from && !to) return true;

  const d = String(row[keyField] || "").slice(0, 10);
  if (!d) return false;

  const a = from || to;
  const b = to || from;

  const lo = a <= b ? a : b;
  const hi = a <= b ? b : a;

  return d >= lo && d <= hi;
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
  const u = dt.getUTCDay(); // 0=dom, 1=lun, ... 6=sab
  return (u + 6) % 7; // 0=lun ... 6=dom
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
    "corse_osservate","effettuate","cancellate","soppresse","parzialmente_cancellate","info_mancante",
    "in_orario","in_ritardo","in_anticipo",
    "oltre_5","oltre_10","oltre_15","oltre_30","oltre_60",
    "minuti_ritardo_tot","minuti_anticipo_tot","minuti_netti_tot"
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


function passMonthFromDayRange(row, keyField) {
  if (!hasDayFilter()) return true;

  const from = String(state.filters.day_from || "").trim();
  const to = String(state.filters.day_to || "").trim();
  const a = from || to;
  const b = to || from;

  const lo = (a <= b ? a : b).slice(0, 7);
  const hi = (a <= b ? b : a).slice(0, 7);

  const m = String(row[keyField] || "").slice(0, 7);
  if (!m) return false;

  return m >= lo && m <= hi;
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

  const pct = !!t.checked;
  if (pct) {
    left.classList.remove("active");
    right.classList.add("active");
  } else {
    left.classList.add("active");
    right.classList.remove("active");
  }
}

function initHistModeToggle() {
  const chart = document.getElementById("chartHist");
  if (!chart) return;

  ensureHistToggleStyles();

  let t = document.getElementById("histModeToggle");
  if (t) {
    updateHistToggleUI();
    t.onchange = () => { updateHistToggleUI(); renderHist(); };
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

  t.onchange = () => { updateHistToggleUI(); renderHist(); };
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

  const wdLabels = ["Lu","Ma","Me","Gi","Ve","Sa","Do"];
  const ensureWeekdays = () => {
    if (!Array.isArray(state.filters.weekdays) || state.filters.weekdays.length !== 7) {
      state.filters.weekdays = [true,true,true,true,true,true,true];
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
    const haveAnyDaily = (Array.isArray(state.kpiDayCat) && state.kpiDayCat.length > 0);
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
  for (const r of dailyRows) {
    const g = String(r.giorno || "").slice(0, 10);
    if (!g) continue;
    const m = g.slice(0, 7);
    if (!by.has(m)) by.set(m, []);
    by.get(m).push(r);
  }
  const out = [];
  for (const [m, rs] of by.entries()) {
    const s = sumRows(rs);
    out.push({ mese: m, categoria: state.filters.cat === "all" ? "all" : state.filters.cat, ...s });
  }
  out.sort((a, b) => String(a.mese).localeCompare(String(b.mese)));
  return out;
}

const state = {
  manifest: null,
  kpiDay: [],
  kpiDayCat: [],
  kpiMonth: [],
  kpiMonthCat: [],
  histMonthCat: [],
  histDayCat: [],
  kpiDayHourCat: [],
  histDayHourCat: [],
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
    day_to: "",
    weekdays: [true, true, true, true, true, true, true],
    time_all: true,
    time_from: "00:00",
    time_to: "23:59"
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

  const extraFiles = [
    "od_giorno_categoria.csv",
    "stazioni_giorno_categoria_nodo.csv",
    "hist_giorno_categoria.csv",
    "kpi_giorno_ora_categoria.csv",
    "hist_giorno_ora_categoria.csv"
  ];

  const extraTexts = await Promise.all(extraFiles.map(f => fetchTextOrNull("data/" + f)));
  const extraParsed = {};
  for (let i = 0; i < extraFiles.length; i++) {
    extraParsed[extraFiles[i]] = extraTexts[i] ? parseCSV(extraTexts[i]) : [];
  }

  state.odDayCat = extraParsed["od_giorno_categoria.csv"] || [];
  state.stationsDayNode = extraParsed["stazioni_giorno_categoria_nodo.csv"] || [];
  state.histDayCat = extraParsed["hist_giorno_categoria.csv"] || [];
  state.kpiDayHourCat = extraParsed["kpi_giorno_ora_categoria.csv"] || [];
  state.histDayHourCat = extraParsed["hist_giorno_ora_categoria.csv"] || [];

  const stTxt = await fetchTextOrNull("data/stations_dim.csv");
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

  const capTxt = await fetchTextOrNull("data/capoluoghi_provincia.csv");
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
    setMeta("Errore: non trovo i CSV in site/data. Controlla il deploy e che pubblichi la cartella site completa.");
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
      state.filters.weekdays = [true,true,true,true,true,true,true];
      state.filters.time_all = true;
      state.filters.time_from = "00:00";
      state.filters.time_to = "23:59";

      if (yearSel) yearSel.value = "all";
      if (catSel) catSel.value = "all";
      if (depSel) depSel.value = "all";
      if (arrSel) arrSel.value = "all";

      const depSearch = document.getElementById("depSearch");
      const arrSearch = document.getElementById("arrSearch");
      if (depSearch) depSearch.value = "";
      if (arrSearch) arrSearch.value = "";
      fillStationSelect(depSel, depItems, "");
      fillStationSelect(arrSel, arrItems, "");

      const dayFrom = document.getElementById("dayFrom");
      const dayTo = document.getElementById("dayTo");
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

  const thr = state.manifest && state.manifest.punctuality ? state.manifest.punctuality.on_time_threshold_minutes : 5;
  const noteEl = document.getElementById("noteThreshold");
  if (noteEl) noteEl.innerText = "In orario significa ritardo arrivo tra 0 e " + thr + " minuti. Anticipo è ritardo negativo.";
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
  updateDayNote();

  const kpiTotal = document.getElementById("kpiTotal");
  const kpiLate = document.getElementById("kpiLate");
  const kpiLateMin = document.getElementById("kpiLateMin");
  const kpiCancelled = document.getElementById("kpiCancelled");
  const kpiSuppressed = document.getElementById("kpiSuppressed");
  if (!kpiTotal || !kpiLate || !kpiLateMin || !kpiCancelled || !kpiSuppressed) return;

  const wantDay = (hasDayFilter() || hasWeekdayFilter()) && state.odDayCat.length > 0;

  if (state.filters.dep !== "all" || state.filters.arr !== "all") {
    const odBase = wantDay && state.odDayCat.length ? state.odDayCat : state.odMonthCat;
    const keyField = wantDay && state.odDayCat.length ? "giorno" : "mese";

    let od = odBase;
    if (state.filters.year !== "all") od = od.filter(r => passYear(r, keyField));
    if (state.filters.cat !== "all") od = od.filter(passCat);
    if (wantDay && keyField === "giorno") {
      if (hasDayFilter()) od = od.filter(r => passDay(r, "giorno"));
      if (hasWeekdayFilter()) od = od.filter(r => passWeekday(r, "giorno"));
    }
    if (state.filters.dep !== "all") od = od.filter(passDep);
    if (state.filters.arr !== "all") od = od.filter(passArr);

    const s = sumRows(od);
    kpiTotal.innerText = fmtInt(s.corse_osservate);
    kpiLate.innerText = fmtInt(s.in_ritardo);
    kpiLateMin.innerText = fmtInt(s.minuti_ritardo_tot);
    kpiCancelled.innerText = fmtInt(s.cancellate);
    kpiSuppressed.innerText = fmtInt(s.soppresse);
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

  let rows = state.kpiMonthCat;
  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, "mese"));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);

  const s = sumRows(rows);
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
    "corse_osservate","effettuate","cancellate","soppresse","parzialmente_cancellate","info_mancante",
    "in_orario","in_ritardo","in_anticipo",
    "oltre_5","oltre_10","oltre_15","oltre_30","oltre_60",
    "minuti_ritardo_tot","minuti_anticipo_tot","minuti_netti_tot"
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

  // Serie giornaliera: se ci sono filtri di partenza/arrivo, proviamo a usare OD giornaliero.
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

  // Serie mensile
  let monthlyRows = [];

  if (state.filters.dep !== "all" || state.filters.arr !== "all") {
    // Se abbiamo già la serie giornaliera OD, possiamo aggregare a mesi.
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

  const norm = (s) => String(s || "").replace(/\s+/g, "");
  const canon = new Map();
  labels.forEach(l => canon.set(norm(l), l));

  const byBucket = new Map();
  labels.forEach(l => byBucket.set(l, 0));

  rows.forEach(r => {
    const raw = r.bucket_ritardo_arrivo;
    const key = canon.get(norm(raw));
    if (!key) return;
    const c = toNum(r.count);
    byBucket.set(key, (byBucket.get(key) || 0) + c);
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
    xaxis: { title: \"Ritardo arrivo (min)\", type: \"category\", tickangle: -35, gridcolor: \"rgba(255,255,255,0.08)\" },
    yaxis: { title: yTitle, ticksuffix: ySuffix, gridcolor: "rgba(255,255,255,0.08)" }
  }, { displayModeBar: false });
}


function initTables() {
  if (typeof Tabulator === "undefined") return;

  const stEl = document.getElementById("tableStations");
  if (stEl) {
    state.tables.stations = new Tabulator("#tableStations", {
      layout: "fitColumns",
      height: "360px",
      movableColumns: true,
      placeholder: "Nessun dato per i filtri selezionati",
      columns: [
        { title: "Periodo", field: "mese", sorter: "string" },
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
  }

  const odEl = document.getElementById("tableOD");
  if (odEl) {
    state.tables.od = new Tabulator("#tableOD", {
      layout: "fitColumns",
      height: "360px",
      movableColumns: true,
      placeholder: "Nessun dato per i filtri selezionati",
      columns: [
        { title: "Periodo", field: "mese", sorter: "string" },
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

  const citiesEl = document.getElementById("tableCities");
  if (citiesEl) {
    state.tables.cities = new Tabulator("#tableCities", {
      layout: "fitColumns",
      height: "360px",
      movableColumns: true,
      placeholder: "Nessun dato per i filtri selezionati",
      columns: [
        { title: "Città", field: "citta", sorter: "string" },
        { title: "Treni", field: "corse_osservate", sorter: "number", hozAlign: "right" },
        { title: "In ritardo", field: "in_ritardo", sorter: "number", hozAlign: "right" },
        { title: "Minuti ritardo", field: "minuti_ritardo_tot", sorter: "number", hozAlign: "right" },
        { title: "Cancellati", field: "cancellate", sorter: "number", hozAlign: "right" },
        { title: "Soppressi", field: "soppresse", sorter: "number", hozAlign: "right" },
        { title: "% ritardo", field: "pct_ritardo", sorter: "number", hozAlign: "right", formatter: (c) => fmtFloat(c.getValue()) }
      ]
    });
  }
}

function renderTables() {
  const minN = state.manifest && state.manifest.min_counts ? toNum(state.manifest.min_counts.leaderboard_min_trains) : 20;

  if (state.tables.stations) {
    const useDay = (hasDayFilter() || hasWeekdayFilter()) && state.stationsDayNode.length > 0 && state.filters.dep === "all" && state.filters.arr === "all";
    const base = useDay ? state.stationsDayNode : state.stationsMonthNode;
    const keyField = useDay ? "giorno" : "mese";

    let st = base;
    if (state.filters.year !== "all") st = st.filter(r => passYear(r, keyField));
    if (state.filters.cat !== "all") st = st.filter(passCat);
    if (useDay) {
      if (hasDayFilter()) st = st.filter(r => passDay(r, "giorno"));
      if (hasWeekdayFilter()) st = st.filter(r => passWeekday(r, "giorno"));
    }
    if (!useDay && hasDayFilter()) st = st.filter(r => passMonthFromDayRange(r, "mese"));
  if (!useDay && hasWeekdayFilter()) st = [];
    if (!useDay && hasWeekdayFilter()) st = [];
    if (state.filters.dep !== "all" || state.filters.arr !== "all") st = [];

    st = st.map(r => {
      const n = toNum(r.corse_osservate);
      const late = toNum(r.in_ritardo);
      const pct = n > 0 ? (late / n) * 100 : 0;
      const periodo = useDay ? String(r.giorno || "").slice(0, 10) : String(r.mese || "").slice(0, 7);
      return { ...r, mese: periodo, pct_ritardo: pct };
    }).filter(r => toNum(r.corse_osservate) >= minN);

    safeSetData(state.tables.stations, st);
  }

  if (state.tables.od) {
    const useDay = (hasDayFilter() || hasWeekdayFilter()) && state.odDayCat.length > 0;
    const base = useDay ? state.odDayCat : state.odMonthCat;
    const keyField = useDay ? "giorno" : "mese";

    let od = base;
    if (state.filters.year !== "all") od = od.filter(r => passYear(r, keyField));
    if (state.filters.cat !== "all") od = od.filter(passCat);
    if (useDay) {
      if (hasDayFilter()) od = od.filter(r => passDay(r, "giorno"));
      if (hasWeekdayFilter()) od = od.filter(r => passWeekday(r, "giorno"));
    }
    if (!useDay && hasDayFilter()) od = od.filter(r => passMonthFromDayRange(r, "mese"));
    if (!useDay && hasWeekdayFilter()) od = [];
    if (state.filters.dep !== "all") od = od.filter(passDep);
    if (state.filters.arr !== "all") od = od.filter(passArr);

    od = od.map(r => {
      const n = toNum(r.corse_osservate);
      const late = toNum(r.in_ritardo);
      const pct = n > 0 ? (late / n) * 100 : 0;
      const periodo = useDay ? String(r.giorno || "").slice(0, 10) : String(r.mese || "").slice(0, 7);
      return { ...r, mese: periodo, pct_ritardo: pct };
    }).filter(r => toNum(r.corse_osservate) >= minN);

    safeSetData(state.tables.od, od);
  }
}

function initMap() {
  const mapEl = document.getElementById("map");
  if (!mapEl) return;
  if (typeof L === "undefined") return;

  state.map = L.map("map", { preferCanvas: true }).setView([41.9, 12.5], 6);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 18,
    attribution: "&copy; OpenStreetMap contributors"
  }).addTo(state.map);

  const metricSel = document.getElementById("mapMetricSel");
  if (metricSel) {
    metricSel.onchange = () => {
      renderMap();
      renderCities();
    };
  }
}

function clearMarkers() {
  state.markers.forEach(m => {
    try {
      if (m && typeof m.remove === "function") m.remove();
    } catch {}
  });
  state.markers = [];
}

function renderMap() {
  if (!state.map) return;
  if (typeof L === "undefined") return;

  clearMarkers();

  const metricSel = document.getElementById("mapMetricSel");
  const metric = metricSel ? metricSel.value : "pct_ritardo";
  const minN = state.manifest && state.manifest.min_counts ? toNum(state.manifest.min_counts.leaderboard_min_trains) : 20;

  const useDay = (hasDayFilter() || hasWeekdayFilter()) && state.stationsDayNode.length > 0 && state.filters.dep === "all" && state.filters.arr === "all";
  const base = useDay ? state.stationsDayNode : state.stationsMonthNode;
  const keyField = useDay ? "giorno" : "mese";

  let st = base;
  if (state.filters.year !== "all") st = st.filter(r => passYear(r, keyField));
  if (state.filters.cat !== "all") st = st.filter(passCat);
  if (useDay) {
      if (hasDayFilter()) st = st.filter(r => passDay(r, "giorno"));
      if (hasWeekdayFilter()) st = st.filter(r => passWeekday(r, "giorno"));
    }
  if (!useDay && hasDayFilter()) st = st.filter(r => passMonthFromDayRange(r, "mese"));
  if (!useDay && hasWeekdayFilter()) st = [];
    if (!useDay && hasWeekdayFilter()) st = [];
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
    ? "Alcune stazioni non hanno coordinate e non sono disegnate sulla mappa."
    : "Coordinate stazioni complete per il set filtrato.";
  const noteEl = document.getElementById("mapNote");
  if (noteEl) noteEl.innerText = note;
}

function renderCities() {
  if (!state.tables.cities) return;

  const minN = state.manifest && state.manifest.min_counts ? toNum(state.manifest.min_counts.leaderboard_min_trains) : 20;
  const metricSel = document.getElementById("mapMetricSel");
  let metric = metricSel ? metricSel.value : "pct_ritardo";
  const allowed = ["pct_ritardo","in_ritardo","minuti_ritardo_tot","cancellate","soppresse"];
  if (!allowed.includes(metric)) metric = "pct_ritardo";

  let mode = "network";
  if (state.filters.dep !== "all" && state.filters.arr === "all") mode = "from_dep_rank_arr_city";
  if (state.filters.arr !== "all" && state.filters.dep === "all") mode = "to_arr_rank_dep_city";
  if (state.filters.arr !== "all" && state.filters.dep !== "all") mode = "pair";

  const noteEl = document.getElementById("citiesNote");
  if (noteEl) {
    noteEl.innerText = state.capoluoghiSet && state.capoluoghiSet.size === 0
      ? "capoluoghi_provincia.csv non trovato, classifica su tutte le città presenti."
      : "Classifica limitata ai capoluoghi di provincia.";
  }

  if (mode === "pair") {
    safeSetData(state.tables.cities, []);
    return;
  }

  const agg = new Map();
  const initAgg = (city) => ({
    citta: city,
    corse_osservate: 0,
    in_ritardo: 0,
    minuti_ritardo_tot: 0,
    cancellate: 0,
    soppresse: 0,
    pct_ritardo: 0
  });

  if (mode === "network") {
    const useDay = (hasDayFilter() || hasWeekdayFilter()) && state.stationsDayNode.length > 0 && state.filters.dep === "all" && state.filters.arr === "all";
    const base = useDay ? state.stationsDayNode : state.stationsMonthNode;
    const keyField = useDay ? "giorno" : "mese";

    let rows = base;
    if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, keyField));
    if (state.filters.cat !== "all") rows = rows.filter(passCat);
    if (useDay) {
      if (hasDayFilter()) rows = rows.filter(r => passDay(r, "giorno"));
      if (hasWeekdayFilter()) rows = rows.filter(r => passWeekday(r, "giorno"));
    }
    if (!useDay && hasDayFilter()) rows = rows.filter(r => passMonthFromDayRange(r, "mese"));
    if (!useDay && hasWeekdayFilter()) rows = [];

    for (const r of rows) {
      const n = toNum(r.corse_osservate);
      if (n <= 0) continue;

      const code = String(r.cod_stazione || "").trim();
      const city = stationCity(code, r.nome_stazione);
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
    const useDay = (hasDayFilter() || hasWeekdayFilter()) && state.odDayCat.length > 0;
    const base = useDay ? state.odDayCat : state.odMonthCat;
    const keyField = useDay ? "giorno" : "mese";

    let od = base;
    if (state.filters.year !== "all") od = od.filter(r => passYear(r, keyField));
    if (state.filters.cat !== "all") od = od.filter(passCat);
    if (useDay) {
      if (hasDayFilter()) od = od.filter(r => passDay(r, "giorno"));
      if (hasWeekdayFilter()) od = od.filter(r => passWeekday(r, "giorno"));
    }
    if (!useDay && hasDayFilter()) od = od.filter(r => passMonthFromDayRange(r, "mese"));
    if (!useDay && hasWeekdayFilter()) od = [];
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

loadAll().catch(err => {
  console.error(err);
  setMeta("Errore caricamento dati: " + (err && err.message ? err.message : String(err)));
});
