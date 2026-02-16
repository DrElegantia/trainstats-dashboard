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

  if (msg) {
    try {
      const el = document.getElementById("metaBox");
      if (el) el.innerText = "Errore Promise: " + msg;
    } catch {}
  }
  console.error(e);
});

const state = {
  manifest: null,

  kpiMonthCat: [],
  kpiDayCat: [],
  odMonthCat: [],
  odDayCat: [],
  stationsMonthNode: [],
  stationsDayNode: [],
  histDayCat: [],
  histMonthCat: [],

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
    dow_mask: [1, 2, 3, 4, 5, 6, 7],
    all_day: true,
    time_from: "00:00",
    time_to: "23:59"
  }
};

function detectDelimiter(headerLine) {
  const h = String(headerLine || "");
  const commas = (h.match(/,/g) || []).length;
  const semis = (h.match(/;/g) || []).length;
  if (semis > commas) return ";";
  return ",";
}

function parseCSV(text) {
  const t = String(text || "").trim();
  if (!t) return [];
  const lines = t.split(/\r?\n/);
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

function splitCSVLine(line, delim) {
  const out = [];
  let cur = "";
  let inQ = false;
  const d = delim || ",";
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

function parseSmartNumber(x) {
  if (x === null || typeof x === "undefined") return NaN;
  let s = String(x).trim();
  if (!s) return NaN;

  s = s.replace(/\s+/g, "");
  s = s.replace(/%/g, "");

  const hasDot = s.includes(".");
  const hasComma = s.includes(",");

  if (hasDot && hasComma) {
    const lastDot = s.lastIndexOf(".");
    const lastComma = s.lastIndexOf(",");
    if (lastComma > lastDot) {
      s = s.replace(/\./g, "");
      s = s.replace(/,/g, ".");
    } else {
      s = s.replace(/,/g, "");
    }
  } else if (hasComma && !hasDot) {
    s = s.replace(/,/g, ".");
  } else if (hasDot && !hasComma) {
    const parts = s.split(".");
    const last = parts[parts.length - 1];
    const looksThousands = parts.length > 1 && last.length === 3 && /^\d+$/.test(last) && /^\d+$/.test(parts[0]);
    if (looksThousands) s = s.replace(/\./g, "");
  }

  const v = parseFloat(s);
  return Number.isFinite(v) ? v : NaN;
}

function toNum(x) {
  const v = parseSmartNumber(x);
  return Number.isFinite(v) ? v : 0;
}

function parseFloatIT(v) {
  return parseSmartNumber(v);
}

function normalizeCoords(lat, lon) {
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  if (lat === 0 && lon === 0) return null;
  if (lat < -90 || lat > 90 || lon < -180 || lon > 180) return null;

  const likelySwapped = lat > 0 && lat < 25 && lon > 25 && lon < 55;
  if (likelySwapped) return { lat: lon, lon: lat };
  return { lat, lon };
}

function getNumAny(row, keys) {
  for (const k of keys) {
    if (row && Object.prototype.hasOwnProperty.call(row, k)) {
      const v = row[k];
      if (v !== "" && v !== null && typeof v !== "undefined") return toNum(v);
    }
  }
  return 0;
}

function fmtInt(x) {
  return Math.round(x).toLocaleString("it-IT");
}

function fmtFloat(x) {
  const v = Number(x);
  if (!Number.isFinite(v)) return "";
  return v.toLocaleString("it-IT", { maximumFractionDigits: 2 });
}

function uniq(arr) {
  const s = new Set();
  const out = [];
  (arr || []).forEach(x => {
    const v = String(x || "").trim();
    if (!v) return;
    if (s.has(v)) return;
    s.add(v);
    out.push(v);
  });
  return out;
}

function safeSetData(table, rows) {
  try {
    if (!table) return;
    table.setData(rows || []);
  } catch (e) {
    console.error(e);
  }
}

function normalizeText(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
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

  const fb = String(fallbackStationName || "").trim();
  if (fb) return fb;
  return c;
}

function stationCoords(code) {
  const c = String(code || "").trim();
  const ref = state.stationsRef.get(c);
  if (!ref) return null;
  const lat = parseFloatIT(ref.lat);
  const lon = parseFloatIT(ref.lon);
  return normalizeCoords(lat, lon);
}

function fetchTextOrNull(path) {
  return fetch(path, { cache: "no-store" })
    .then(r => (r.ok ? r.text() : null))
    .catch(() => null);
}

function setMeta(msg) {
  const el = document.getElementById("metaBox");
  if (el) el.innerText = String(msg || "");
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

function hasDayFilter() {
  const hasRange = !!(state.filters.day_from || state.filters.day_to);
  const mask = Array.isArray(state.filters.dow_mask) ? state.filters.dow_mask : [1, 2, 3, 4, 5, 6, 7];
  const hasDow = mask.length > 0 && mask.length < 7;
  const hasTime = state.filters && state.filters.all_day === false;
  return hasRange || hasDow || hasTime;
}

function dateToDow(dateStr) {
  const d = String(dateStr || "").slice(0, 10);
  if (!d) return null;
  const dt = new Date(d + "T00:00:00");
  if (Number.isNaN(dt.getTime())) return null;
  const jsDow = dt.getDay();
  return jsDow === 0 ? 7 : jsDow;
}

function passDay(row, keyField) {
  const d = String(row[keyField] || "").slice(0, 10);
  if (!d) return false;

  const from = String(state.filters.day_from || "").trim();
  const to = String(state.filters.day_to || "").trim();

  if (from || to) {
    const a = from || to;
    const b = to || from;
    const lo = a <= b ? a : b;
    const hi = a <= b ? b : a;
    if (!(d >= lo && d <= hi)) return false;
  }

  const mask = Array.isArray(state.filters.dow_mask) ? state.filters.dow_mask : [1, 2, 3, 4, 5, 6, 7];
  if (mask.length && mask.length < 7) {
    const dow = dateToDow(d);
    if (dow !== null && !mask.includes(dow)) return false;
  }

  return true;
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

function ensureDayControlsStyles() {
  if (document.getElementById("dayControlsStyles")) return;

  const style = document.createElement("style");
  style.id = "dayControlsStyles";
  style.textContent = `
    .dayCtlRow { display:flex; align-items:center; gap:10px; margin:6px 0 0 0; flex-wrap:wrap; }
    .dowWrap { display:flex; align-items:center; gap:6px; flex-wrap:wrap; }
    .dowBtn { border:1px solid rgba(255,255,255,0.18); background:rgba(255,255,255,0.06); color:#e6e9f2; border-radius:999px; padding:3px 8px; font-size:12px; cursor:pointer; user-select:none; }
    .dowBtn.active { background:rgba(255,255,255,0.18); border-color:rgba(255,255,255,0.30); font-weight:600; }
    .timeWrap { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
    .timeWrap input[type="time"] { width:92px; }
    .timeAllDayLabel { font-size:12px; opacity:0.85; display:flex; align-items:center; gap:6px; }
  `;
  document.head.appendChild(style);
}

function initDayControls() {
  const yearSel = document.getElementById("yearSel");
  if (!yearSel || !yearSel.parentNode) return;

  if (document.getElementById("dayFrom")) return;

  ensureDayControlsStyles();

  const days = uniq(state.kpiDayCat.map(r => String(r.giorno || "").slice(0, 10)).filter(Boolean)).sort();
  const minDay = days.length ? days[0] : "";
  const maxDay = days.length ? days[days.length - 1] : "";

  const row1 = document.createElement("div");
  row1.className = "dayCtlRow";

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

  const note = document.createElement("div");
  note.id = "dayNote";
  note.style.fontSize = "12px";
  note.style.opacity = "0.75";
  note.style.marginLeft = "6px";

  row1.appendChild(lab);
  row1.appendChild(from);
  row1.appendChild(to);
  row1.appendChild(note);

  const row2 = document.createElement("div");
  row2.className = "dayCtlRow";

  const dowLab = document.createElement("div");
  dowLab.innerText = "Giorni";

  const dowWrap = document.createElement("div");
  dowWrap.className = "dowWrap";

  const dows = [
    { k: 1, t: "Lu" },
    { k: 2, t: "Ma" },
    { k: 3, t: "Me" },
    { k: 4, t: "Gi" },
    { k: 5, t: "Ve" },
    { k: 6, t: "Sa" },
    { k: 7, t: "Do" }
  ];

  dows.forEach(d => {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "dowBtn active";
    b.dataset.dow = String(d.k);
    b.innerText = d.t;
    b.onclick = () => {
      b.classList.toggle("active");
      const active = Array.from(document.querySelectorAll(".dowBtn.active")).map(x => Number(x.dataset.dow)).filter(Number.isFinite);
      if (active.length === 0) {
        document.querySelectorAll(".dowBtn").forEach(x => x.classList.add("active"));
        state.filters.dow_mask = [1, 2, 3, 4, 5, 6, 7];
      } else {
        state.filters.dow_mask = active.sort((a, b) => a - b);
      }
      renderAll();
    };
    dowWrap.appendChild(b);
  });

  const timeLab = document.createElement("div");
  timeLab.innerText = "Orari";

  const timeWrap = document.createElement("div");
  timeWrap.className = "timeWrap";

  const allDayLabel = document.createElement("label");
  allDayLabel.className = "timeAllDayLabel";

  const allDay = document.createElement("input");
  allDay.type = "checkbox";
  allDay.id = "timeAllDay";
  allDay.checked = true;

  const allDayText = document.createElement("span");
  allDayText.innerText = "Tutta la giornata";

  allDayLabel.appendChild(allDay);
  allDayLabel.appendChild(allDayText);

  const tFrom = document.createElement("input");
  tFrom.type = "time";
  tFrom.id = "timeFrom";
  tFrom.value = "00:00";

  const tTo = document.createElement("input");
  tTo.type = "time";
  tTo.id = "timeTo";
  tTo.value = "23:59";

  timeWrap.appendChild(allDayLabel);
  timeWrap.appendChild(tFrom);
  timeWrap.appendChild(tTo);

  row2.appendChild(dowLab);
  row2.appendChild(dowWrap);
  row2.appendChild(timeLab);
  row2.appendChild(timeWrap);

  yearSel.parentNode.appendChild(row1);
  yearSel.parentNode.appendChild(row2);

  const apply = () => {
    state.filters.day_from = String(from.value || "").trim();
    state.filters.day_to = String(to.value || "").trim();
    state.filters.all_day = !!allDay.checked;
    state.filters.time_from = String(tFrom.value || "00:00");
    state.filters.time_to = String(tTo.value || "23:59");
    renderAll();
  };

  from.onchange = apply;
  to.onchange = apply;
  allDay.onchange = apply;
  tFrom.onchange = apply;
  tTo.onchange = apply;

  updateDayNote();
}

function updateDayNote() {
  const el = document.getElementById("dayNote");
  if (!el) return;

  if (!hasDayFilter()) {
    el.innerText = "";
    return;
  }

  const haveOdDay = Array.isArray(state.odDayCat) && state.odDayCat.length > 0;
  const haveStDay = Array.isArray(state.stationsDayNode) && state.stationsDayNode.length > 0;
  const haveHistDay = Array.isArray(state.histDayCat) && state.histDayCat.length > 0;

  const base = "Filtri giorno attivi.";
  if (haveOdDay || haveStDay || haveHistDay) {
    el.innerText = base;
    return;
  }

  el.innerText = base + " Per tabelle, mappa e tratte serve anche OD e stazioni giornaliere.";
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
    selectEl.appendChild(new Option(it.name, it.code));
  }

  const values = Array.from(selectEl.options).map(o => o.value);
  if (values.includes(cur)) selectEl.value = cur;
  else selectEl.value = "all";
}

function ensureSearchInput(selectEl, id, placeholder, items) {
  if (!selectEl || !selectEl.parentNode) return;
  if (document.getElementById(id)) return;

  const wrap = document.createElement("div");
  wrap.style.display = "flex";
  wrap.style.flexDirection = "column";
  wrap.style.gap = "6px";
  wrap.style.minWidth = "220px";

  const inp = document.createElement("input");
  inp.type = "text";
  inp.id = id;
  inp.placeholder = placeholder;
  inp.autocomplete = "off";

  const parent = selectEl.parentNode;
  parent.replaceChild(wrap, selectEl);

  wrap.appendChild(inp);
  wrap.appendChild(selectEl);

  inp.oninput = () => {
    fillStationSelect(selectEl, items, inp.value || "");
  };
}

function sumRows(rows) {
  const keys = [
    "corse_osservate", "effettuate", "cancellate", "soppresse", "parzialmente_cancellate", "info_mancante",
    "in_orario", "in_ritardo", "in_anticipo",
    "oltre_5", "oltre_10", "oltre_15", "oltre_30", "oltre_60",
    "minuti_ritardo_tot", "minuti_anticipo_tot", "minuti_netti_tot"
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

  const wantDay = hasDayFilter();

  if (state.filters.dep !== "all" || state.filters.arr !== "all") {
    const odBase = wantDay && state.odDayCat.length ? state.odDayCat : state.odMonthCat;
    const keyField = wantDay && state.odDayCat.length ? "giorno" : "mese";

    let od = odBase;
    if (state.filters.year !== "all") od = od.filter(r => passYear(r, keyField));
    if (state.filters.cat !== "all") od = od.filter(passCat);
    if (wantDay && keyField === "giorno") od = od.filter(r => passDay(r, "giorno"));
    if (!wantDay && hasDayFilter() && keyField === "mese") od = od.filter(r => passMonthFromDayRange(r, "mese"));
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

  if (wantDay) {
    let daily = state.kpiDayCat;
    if (state.filters.year !== "all") daily = daily.filter(r => passYear(r, "giorno"));
    if (state.filters.cat !== "all") daily = daily.filter(passCat);
    daily = daily.filter(r => passDay(r, "giorno"));

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

function groupDailyToMonthly(rows) {
  const byM = new Map();
  for (const r of rows) {
    const d = String(r.giorno || "").slice(0, 10);
    if (!d) continue;
    const m = d.slice(0, 7);
    if (!byM.has(m)) byM.set(m, []);
    byM.get(m).push(r);
  }
  const out = [];
  for (const [m, arr] of byM.entries()) {
    const s = sumRows(arr);
    out.push({ mese: m, ...s });
  }
  out.sort((a, b) => String(a.mese).localeCompare(String(b.mese)));
  return out;
}

function seriesFromRows(rows, keyField) {
  const x = [];
  const yPctLate = [];
  const yLate = [];
  const yLateMin = [];
  const yCancel = [];
  const ySupp = [];

  for (const r of rows) {
    const key = String(r[keyField] || "").slice(0, 10);
    if (!key) continue;

    const n = toNum(r.corse_osservate || r.effettuate);
    const late = toNum(r.in_ritardo);
    const lateMin = toNum(r.minuti_ritardo_tot);
    const cancel = toNum(r.cancellate);
    const supp = toNum(r.soppresse);

    x.push(key);
    yPctLate.push(n > 0 ? (late / n) * 100 : 0);
    yLate.push(late);
    yLateMin.push(lateMin);
    yCancel.push(cancel);
    ySupp.push(supp);
  }

  return { x, yPctLate, yLate, yLateMin, yCancel, ySupp };
}

function renderSeries() {
  const chartDaily = document.getElementById("chartDaily");
  const chartMonthly = document.getElementById("chartMonthly");
  if (!chartDaily || !chartMonthly) return;
  if (typeof Plotly === "undefined") return;

  const wantDay = hasDayFilter();

  let daily = state.kpiDayCat;
  if (state.filters.year !== "all") daily = daily.filter(r => passYear(r, "giorno"));
  if (state.filters.cat !== "all") daily = daily.filter(passCat);
  if (wantDay) daily = daily.filter(r => passDay(r, "giorno"));

  daily.sort((a, b) => String(a.giorno).localeCompare(String(b.giorno)));

  let monthly = state.kpiMonthCat;
  if (wantDay) monthly = groupDailyToMonthly(daily);
  if (!wantDay) {
    if (state.filters.year !== "all") monthly = monthly.filter(r => passYear(r, "mese"));
    if (state.filters.cat !== "all") monthly = monthly.filter(passCat);
    if (hasDayFilter()) monthly = monthly.filter(r => passMonthFromDayRange(r, "mese"));
  }

  monthly.sort((a, b) => String(a.mese).localeCompare(String(b.mese)));

  const sD = seriesFromRows(daily, "giorno");
  const sM = seriesFromRows(monthly, "mese");

  const layoutCommon = {
    margin: { l: 55, r: 18, t: 10, b: 40 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e6e9f2" },
    xaxis: { showgrid: true, gridcolor: "rgba(255,255,255,0.12)" },
    yaxis: { showgrid: true, gridcolor: "rgba(255,255,255,0.12)" }
  };

  const dataDaily = [
    { x: sD.x, y: sD.yPctLate, type: "scatter", mode: "lines+markers", name: "% in ritardo" }
  ];
  const layoutDaily = {
    ...layoutCommon,
    yaxis: { ...layoutCommon.yaxis, title: "% in ritardo" }
  };

  const dataMonthly = [
    { x: sM.x, y: sM.yPctLate, type: "scatter", mode: "lines+markers", name: "% in ritardo" }
  ];
  const layoutMonthly = {
    ...layoutCommon,
    yaxis: { ...layoutCommon.yaxis, title: "% in ritardo" }
  };

  Plotly.react(chartDaily, dataDaily, layoutDaily, { displayModeBar: false, responsive: true });
  Plotly.react(chartMonthly, dataMonthly, layoutMonthly, { displayModeBar: false, responsive: true });
}

function buildHistBins() {
  return [
    { k: "<=-60", lo: -1e9, hi: -60 },
    { k: "(-60,-30]", lo: -60, hi: -30 },
    { k: "(-30,-15]", lo: -30, hi: -15 },
    { k: "(-15,-10]", lo: -15, hi: -10 },
    { k: "(-10,-5]", lo: -10, hi: -5 },
    { k: "(-5,-1]", lo: -5, hi: -1 },
    { k: "(-1,0]", lo: -1, hi: 0 },
    { k: "(0,1]", lo: 0, hi: 1 },
    { k: "(1,5]", lo: 1, hi: 5 },
    { k: "(5,10]", lo: 5, hi: 10 },
    { k: "(10,15]", lo: 10, hi: 15 },
    { k: "(15,30]", lo: 15, hi: 30 },
    { k: "(30,60]", lo: 30, hi: 60 },
    { k: "(60,120]", lo: 60, hi: 120 },
    { k: ">120", lo: 120, hi: 1e9 }
  ];
}

function renderHist() {
  const chart = document.getElementById("chartHist");
  if (!chart) return;
  if (typeof Plotly === "undefined") return;

  initHistModeToggle();

  const bins = buildHistBins();
  const wantDay = hasDayFilter();

  const base = wantDay && state.histDayCat.length ? state.histDayCat : state.histMonthCat;
  const keyField = wantDay && state.histDayCat.length ? "giorno" : "mese";

  let rows = base;

  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (wantDay && keyField === "giorno") rows = rows.filter(r => passDay(r, "giorno"));
  if (!wantDay && hasDayFilter() && keyField === "mese") rows = rows.filter(r => passMonthFromDayRange(r, "mese"));
  if (state.filters.dep !== "all" || state.filters.arr !== "all") {
    rows = [];
  }

  const counts = new Array(bins.length).fill(0);

  for (const r of rows) {
    const b = String(r.bin || r.fascia || "").trim();
    const idx = bins.findIndex(x => x.k === b);
    if (idx >= 0) {
      counts[idx] += toNum(r.conteggio || r.count || r.n || 0);
    }
  }

  const total = counts.reduce((a, b) => a + b, 0);
  const t = document.getElementById("histModeToggle");
  const pct = t ? !!t.checked : false;
  const y = pct && total > 0 ? counts.map(c => (c / total) * 100) : counts;

  const data = [
    { x: bins.map(b => b.k), y, type: "bar", name: pct ? "%" : "Conteggi" }
  ];

  const layout = {
    margin: { l: 55, r: 18, t: 10, b: 65 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e6e9f2" },
    xaxis: { tickangle: -35, showgrid: true, gridcolor: "rgba(255,255,255,0.12)" },
    yaxis: { title: pct ? "%" : "Conteggio", showgrid: true, gridcolor: "rgba(255,255,255,0.12)" }
  };

  Plotly.react(chart, data, layout, { displayModeBar: false, responsive: true });
}

function initTables() {
  if (typeof Tabulator === "undefined") return;

  const elStations = document.getElementById("tableStations");
  const elOD = document.getElementById("tableOD");
  const elCities = document.getElementById("tableCities");

  if (elStations && !state.tables.stations) {
    state.tables.stations = new Tabulator(elStations, {
      layout: "fitColumns",
      height: "290px",
      placeholder: "Nessun dato",
      initialSort: [{ column: "pct_ritardo", dir: "desc" }],
      columns: [
        { title: "Stazione", field: "nome_stazione", sorter: "string", widthGrow: 2 },
        { title: "Treni", field: "corse_osservate", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) },
        { title: "In ritardo", field: "in_ritardo", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) },
        { title: "% in ritardo", field: "pct_ritardo", sorter: "number", hozAlign: "right", formatter: cell => fmtFloat(cell.getValue()) + "%" },
        { title: "Minuti", field: "minuti_ritardo_tot", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) },
        { title: "Canc", field: "cancellate", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) },
        { title: "Sopp", field: "soppresse", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) }
      ]
    });
  }

  if (elOD && !state.tables.od) {
    state.tables.od = new Tabulator(elOD, {
      layout: "fitColumns",
      height: "290px",
      placeholder: "Nessun dato",
      initialSort: [{ column: "pct_ritardo", dir: "desc" }],
      columns: [
        { title: "Partenza", field: "nome_partenza", sorter: "string", widthGrow: 2 },
        { title: "Arrivo", field: "nome_arrivo", sorter: "string", widthGrow: 2 },
        { title: "Treni", field: "corse_osservate", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) },
        { title: "In ritardo", field: "in_ritardo", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) },
        { title: "% in ritardo", field: "pct_ritardo", sorter: "number", hozAlign: "right", formatter: cell => fmtFloat(cell.getValue()) + "%" },
        { title: "Minuti", field: "minuti_ritardo_tot", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) },
        { title: "Canc", field: "cancellate", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) },
        { title: "Sopp", field: "soppresse", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) }
      ]
    });
  }

  if (elCities && !state.tables.cities) {
    state.tables.cities = new Tabulator(elCities, {
      layout: "fitColumns",
      height: "290px",
      placeholder: "Nessun dato",
      initialSort: [{ column: "pct_ritardo", dir: "desc" }],
      columns: [
        { title: "Capoluogo", field: "nome", sorter: "string", widthGrow: 2 },
        { title: "Treni", field: "corse_osservate", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) },
        { title: "In ritardo", field: "in_ritardo", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) },
        { title: "% in ritardo", field: "pct_ritardo", sorter: "number", hozAlign: "right", formatter: cell => fmtFloat(cell.getValue()) + "%" },
        { title: "Minuti", field: "minuti_ritardo_tot", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) },
        { title: "Canc", field: "cancellate", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) },
        { title: "Sopp", field: "soppresse", sorter: "number", hozAlign: "right", formatter: cell => fmtInt(cell.getValue()) }
      ]
    });
  }
}

function renderTables() {
  initTables();

  const minN = state.manifest && state.manifest.min_counts ? toNum(state.manifest.min_counts.leaderboard_min_trains) : 20;

  const wantDay = hasDayFilter();
  const haveOdDay = wantDay && state.odDayCat.length > 0;
  const haveStDay = wantDay && state.stationsDayNode.length > 0;

  const odBase = haveOdDay ? state.odDayCat : state.odMonthCat;
  const stBase = haveStDay ? state.stationsDayNode : state.stationsMonthNode;
  const odKey = haveOdDay ? "giorno" : "mese";
  const stKey = haveStDay ? "giorno" : "mese";

  let od = odBase;
  let st = stBase;

  if (state.filters.year !== "all") {
    od = od.filter(r => passYear(r, odKey));
    st = st.filter(r => passYear(r, stKey));
  }
  if (state.filters.cat !== "all") {
    od = od.filter(passCat);
    st = st.filter(passCat);
  }

  if (haveOdDay) od = od.filter(r => passDay(r, "giorno"));
  if (haveStDay) st = st.filter(r => passDay(r, "giorno"));

  if (!haveOdDay && hasDayFilter() && odKey === "mese") od = od.filter(r => passMonthFromDayRange(r, "mese"));
  if (!haveStDay && hasDayFilter() && stKey === "mese") st = st.filter(r => passMonthFromDayRange(r, "mese"));

  if (state.filters.dep !== "all") od = od.filter(passDep);
  if (state.filters.arr !== "all") od = od.filter(passArr);

  if (state.filters.dep !== "all" || state.filters.arr !== "all") {
    st = [];
  }

  const stAgg = new Map();
  for (const r of st) {
    const code = String(r.cod_stazione || "").trim();
    if (!code) continue;
    const n = getNumAny(r, ["corse_osservate", "effettuate", "treni", "n_treni"]);
    if (n <= 0) continue;
    if (!stAgg.has(code)) {
      stAgg.set(code, {
        cod_stazione: code,
        nome_stazione: stationName(code, r.nome_stazione),
        corse_osservate: 0,
        in_ritardo: 0,
        minuti_ritardo_tot: 0,
        cancellate: 0,
        soppresse: 0
      });
    }
    const a = stAgg.get(code);
    a.corse_osservate += n;
    a.in_ritardo += getNumAny(r, ["in_ritardo", "late"]);
    a.minuti_ritardo_tot += getNumAny(r, ["minuti_ritardo_tot", "minuti_ritardo", "delay_minutes_total"]);
    a.cancellate += getNumAny(r, ["cancellate", "cancellati", "cancellate_tot", "cancellati_tot"]);
    a.soppresse += getNumAny(r, ["soppresse", "soppressi", "soppresse_tot", "soppressi_tot"]);
  }

  let stOut = Array.from(stAgg.values());
  stOut.forEach(o => {
    o.pct_ritardo = o.corse_osservate > 0 ? (o.in_ritardo / o.corse_osservate) * 100 : 0;
  });
  stOut = stOut.filter(o => o.corse_osservate >= minN);
  stOut.sort((a, b) => b.pct_ritardo - a.pct_ritardo);
  stOut = stOut.slice(0, 50);

  safeSetData(state.tables.stations, stOut);

  const odAgg = new Map();
  for (const r of od) {
    const dep = String(r.cod_partenza || "").trim();
    const arr = String(r.cod_arrivo || "").trim();
    if (!dep || !arr) continue;
    const key = dep + "||" + arr;
    const n = getNumAny(r, ["corse_osservate", "effettuate", "treni", "n_treni"]);
    if (n <= 0) continue;
    if (!odAgg.has(key)) {
      odAgg.set(key, {
        cod_partenza: dep,
        cod_arrivo: arr,
        nome_partenza: stationName(dep, r.nome_partenza || dep),
        nome_arrivo: stationName(arr, r.nome_arrivo || arr),
        corse_osservate: 0,
        in_ritardo: 0,
        minuti_ritardo_tot: 0,
        cancellate: 0,
        soppresse: 0
      });
    }
    const a = odAgg.get(key);
    a.corse_osservate += n;
    a.in_ritardo += getNumAny(r, ["in_ritardo", "late"]);
    a.minuti_ritardo_tot += getNumAny(r, ["minuti_ritardo_tot", "minuti_ritardo", "delay_minutes_total"]);
    a.cancellate += getNumAny(r, ["cancellate", "cancellati", "cancellate_tot", "cancellati_tot"]);
    a.soppresse += getNumAny(r, ["soppresse", "soppressi", "soppresse_tot", "soppressi_tot"]);
  }

  let odOut = Array.from(odAgg.values());
  odOut.forEach(o => {
    o.pct_ritardo = o.corse_osservate > 0 ? (o.in_ritardo / o.corse_osservate) * 100 : 0;
  });
  odOut = odOut.filter(o => o.corse_osservate >= minN);
  odOut.sort((a, b) => b.pct_ritardo - a.pct_ritardo);
  odOut = odOut.slice(0, 50);

  safeSetData(state.tables.od, odOut);
}

let _mapMetricSelCache = null;

function findMapMetricSelect() {
  if (_mapMetricSelCache) return _mapMetricSelCache;
  const a = document.getElementById("mapMetricSel");
  if (a) { _mapMetricSelCache = a; return a; }
  const b = document.getElementById("metricSel");
  if (b) { _mapMetricSelCache = b; return b; }
  const c = document.getElementById("mapMetric");
  if (c) { _mapMetricSelCache = c; return c; }
  return null;
}

function metricKey() {
  const sel = findMapMetricSelect();
  const raw = sel ? String(sel.value || "") : "pct_ritardo";
  const allowed = ["pct_ritardo", "in_ritardo", "minuti_ritardo_tot", "cancellate", "soppresse"];
  return allowed.includes(raw) ? raw : "pct_ritardo";
}

function metricLabel(k) {
  if (k === "in_ritardo") return "In ritardo";
  if (k === "minuti_ritardo_tot") return "Minuti totali di ritardo";
  if (k === "cancellate") return "Cancellati";
  if (k === "soppresse") return "Soppressi";
  return "% in ritardo";
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

  const metricSel = findMapMetricSelect();
  if (metricSel) {
    metricSel.onchange = () => {
      renderMap();
      renderCities();
    };
  }
}

function clearMarkers() {
  try {
    (state.markers || []).forEach(m => m.remove());
  } catch {}
  state.markers = [];
}

function renderMap() {
  if (!state.map) return;
  if (typeof L === "undefined") return;

  clearMarkers();

  const metric = metricKey();
  const metricName = metricLabel(metric);

  const minN = state.manifest && state.manifest.min_counts
    ? toNum(state.manifest.min_counts.leaderboard_min_trains)
    : 20;

  const wantDay = hasDayFilter() && state.stationsDayNode.length > 0 && state.filters.dep === "all" && state.filters.arr === "all";
  const base = wantDay ? state.stationsDayNode : state.stationsMonthNode;
  const keyField = wantDay ? "giorno" : "mese";

  let rows = base;

  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (wantDay) rows = rows.filter(r => passDay(r, "giorno"));
  if (!wantDay && hasDayFilter()) rows = rows.filter(r => passMonthFromDayRange(r, "mese"));

  if (state.filters.dep !== "all" || state.filters.arr !== "all") rows = [];

  const agg = new Map();

  for (const r of rows) {
    const code = String(r.cod_stazione || "").trim();
    if (!code) continue;

    const n = getNumAny(r, ["corse_osservate", "effettuate", "treni", "n_treni"]);
    if (n <= 0) continue;

    if (!agg.has(code)) {
      agg.set(code, {
        cod_stazione: code,
        nome_stazione: stationName(code, r.nome_stazione),
        corse_osservate: 0,
        in_ritardo: 0,
        minuti_ritardo_tot: 0,
        cancellate: 0,
        soppresse: 0
      });
    }

    const a = agg.get(code);
    a.corse_osservate += n;
    a.in_ritardo += getNumAny(r, ["in_ritardo", "late"]);
    a.minuti_ritardo_tot += getNumAny(r, ["minuti_ritardo_tot", "minuti_ritardo", "delay_minutes_total"]);
    a.cancellate += getNumAny(r, ["cancellate", "cancellati", "cancellate_tot", "cancellati_tot"]);
    a.soppresse += getNumAny(r, ["soppresse", "soppressi", "soppresse_tot", "soppressi_tot"]);
  }

  let missingCoords = 0;
  let drawn = 0;

  const bounds = [];

  for (const a of agg.values()) {
    if (a.corse_osservate < minN) continue;

    const coords = stationCoords(a.cod_stazione);
    if (!coords) {
      missingCoords++;
      continue;
    }

    const pct = a.corse_osservate > 0 ? (a.in_ritardo / a.corse_osservate) * 100 : 0;

    let v = 0;
    if (metric === "pct_ritardo") v = pct;
    if (metric === "in_ritardo") v = a.in_ritardo;
    if (metric === "minuti_ritardo_tot") v = a.minuti_ritardo_tot;
    if (metric === "cancellate") v = a.cancellate;
    if (metric === "soppresse") v = a.soppresse;

    const radius = metric === "pct_ritardo"
      ? Math.max(4, Math.min(18, 4 + (v / 5)))
      : Math.max(4, Math.min(18, Math.sqrt(v + 1)));

    const marker = L.circleMarker([coords.lat, coords.lon], {
      radius,
      weight: 1,
      opacity: 0.9,
      fillOpacity: 0.35
    }).addTo(state.map);

    marker.bindTooltip(
      `<div style="font-size:12px">
        <div><b>${a.nome_stazione}</b> (${a.cod_stazione})</div>
        <div>${metricName}: <b>${metric === "pct_ritardo" ? fmtFloat(v) + "%" : fmtInt(v)}</b></div>
        <div>Treni: ${fmtInt(a.corse_osservate)}</div>
        <div>In ritardo: ${fmtInt(a.in_ritardo)} (${fmtFloat(pct)}%)</div>
        <div>Minuti ritardo: ${fmtInt(a.minuti_ritardo_tot)}</div>
        <div>Cancellati: ${fmtInt(a.cancellate)}</div>
        <div>Soppressi: ${fmtInt(a.soppresse)}</div>
      </div>`
    );

    state.markers.push(marker);

    drawn += 1;
    bounds.push([coords.lat, coords.lon]);
  }

  if (drawn > 0 && bounds.length) {
    try {
      state.map.fitBounds(bounds, { padding: [20, 20], maxZoom: 8 });
    } catch {}
  } else {
    try {
      state.map.setView([41.9, 12.5], 6);
    } catch {}
  }

  const noteEl = document.getElementById("mapNote");
  if (noteEl) {
    if (drawn === 0) {
      noteEl.innerText = "Nessuna stazione da mostrare con i filtri correnti.";
    } else if (missingCoords > 0) {
      noteEl.innerText = "Alcune stazioni non hanno coordinate e non sono disegnate sulla mappa.";
    } else {
      noteEl.innerText = "";
    }
  }
}

function renderCities() {
  if (!state.tables.cities) return;

  const minN = state.manifest && state.manifest.min_counts ? toNum(state.manifest.min_counts.leaderboard_min_trains) : 20;
  const metric = metricKey();

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

  const wantDay = hasDayFilter();

  if (mode === "network") {
    const base = wantDay && state.stationsDayNode.length ? state.stationsDayNode : state.stationsMonthNode;
    const keyField = wantDay && state.stationsDayNode.length ? "giorno" : "mese";

    let rows = base;

    if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, keyField));
    if (state.filters.cat !== "all") rows = rows.filter(passCat);
    if (wantDay && keyField === "giorno") rows = rows.filter(r => passDay(r, "giorno"));
    if (!wantDay && hasDayFilter() && keyField === "mese") rows = rows.filter(r => passMonthFromDayRange(r, "mese"));

    const agg = new Map();

    for (const r of rows) {
      const code = String(r.cod_stazione || "").trim();
      if (!code) continue;

      const city = stationCity(code, stationName(code, ""));
      if (state.capoluoghiSet && state.capoluoghiSet.size > 0) {
        if (!state.capoluoghiSet.has(city)) continue;
      }

      const n = getNumAny(r, ["corse_osservate", "effettuate", "treni", "n_treni"]);
      if (n <= 0) continue;

      if (!agg.has(city)) {
        agg.set(city, {
          nome: city,
          corse_osservate: 0,
          in_ritardo: 0,
          minuti_ritardo_tot: 0,
          cancellate: 0,
          soppresse: 0
        });
      }

      const a = agg.get(city);
      a.corse_osservate += n;
      a.in_ritardo += getNumAny(r, ["in_ritardo", "late"]);
      a.minuti_ritardo_tot += getNumAny(r, ["minuti_ritardo_tot", "minuti_ritardo", "delay_minutes_total"]);
      a.cancellate += getNumAny(r, ["cancellate", "cancellati", "cancellate_tot", "cancellati_tot"]);
      a.soppresse += getNumAny(r, ["soppresse", "soppressi", "soppresse_tot", "soppressi_tot"]);
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
    return;
  }

  const base = wantDay && state.odDayCat.length ? state.odDayCat : state.odMonthCat;
  const keyField = wantDay && state.odDayCat.length ? "giorno" : "mese";

  let rows = base;
  if (state.filters.year !== "all") rows = rows.filter(r => passYear(r, keyField));
  if (state.filters.cat !== "all") rows = rows.filter(passCat);
  if (wantDay && keyField === "giorno") rows = rows.filter(r => passDay(r, "giorno"));
  if (!wantDay && hasDayFilter() && keyField === "mese") rows = rows.filter(r => passMonthFromDayRange(r, "mese"));

  if (mode === "from_dep_rank_arr_city") rows = rows.filter(r => String(r.cod_partenza) === state.filters.dep);
  if (mode === "to_arr_rank_dep_city") rows = rows.filter(r => String(r.cod_arrivo) === state.filters.arr);

  const agg = new Map();

  const groupField = mode === "from_dep_rank_arr_city" ? "cod_arrivo" : "cod_partenza";

  for (const r of rows) {
    const code = String(r[groupField] || "").trim();
    if (!code) continue;

    const city = stationCity(code, stationName(code, ""));
    if (state.capoluoghiSet && state.capoluoghiSet.size > 0) {
      if (!state.capoluoghiSet.has(city)) continue;
    }

    const n = getNumAny(r, ["corse_osservate", "effettuate", "treni", "n_treni"]);
    if (n <= 0) continue;

    if (!agg.has(city)) {
      agg.set(city, {
        nome: city,
        corse_osservate: 0,
        in_ritardo: 0,
        minuti_ritardo_tot: 0,
        cancellate: 0,
        soppresse: 0
      });
    }

    const a = agg.get(city);
    a.corse_osservate += n;
    a.in_ritardo += getNumAny(r, ["in_ritardo", "late"]);
    a.minuti_ritardo_tot += getNumAny(r, ["minuti_ritardo_tot", "minuti_ritardo", "delay_minutes_total"]);
    a.cancellate += getNumAny(r, ["cancellate", "cancellati", "cancellate_tot", "cancellati_tot"]);
    a.soppresse += getNumAny(r, ["soppresse", "soppressi", "soppresse_tot", "soppressi_tot"]);
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

function initFilters() {
  const years = uniq(
    state.kpiMonthCat.map(r => String(r.mese || "").slice(0, 4))
      .concat(state.kpiDayCat.map(r => String(r.giorno || "").slice(0, 4)))
  ).sort();

  const cats = uniq(
    state.kpiMonthCat.map(r => String(r.categoria || "").trim())
      .concat(state.kpiDayCat.map(r => String(r.categoria || "").trim()))
      .filter(Boolean)
  ).sort((a, b) => a.localeCompare(b, "it", { sensitivity: "base" }));

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
      state.filters.dow_mask = [1, 2, 3, 4, 5, 6, 7];
      state.filters.all_day = true;
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

      const dowBtns = document.querySelectorAll(".dowBtn");
      dowBtns.forEach(b => b.classList.add("active"));

      const allDay = document.getElementById("timeAllDay");
      const tFrom = document.getElementById("timeFrom");
      const tTo = document.getElementById("timeTo");
      if (allDay) allDay.checked = true;
      if (tFrom) tFrom.value = "00:00";
      if (tTo) tTo.value = "23:59";

      updateDayNote();
      renderAll();
    };
  }

  const thr = state.manifest && state.manifest.punctuality ? state.manifest.punctuality.on_time_threshold_minutes : 5;
  const noteEl = document.getElementById("noteThreshold");
  if (noteEl) noteEl.innerText = "In orario significa ritardo arrivo tra 0 e " + thr + " minuti. Anticipo è ritardo negativo.";
}

async function loadAll() {
  const files = [
    "manifest.json",
    "kpi_mese_categoria.csv",
    "kpi_giorno_categoria.csv",
    "od_mese_categoria.csv",
    "od_giorno_categoria.csv",
    "stazioni_mese_categoria_nodo.csv",
    "stazioni_giorno_categoria_nodo.csv",
    "hist_giorno_categoria.csv",
    "hist_mese_categoria.csv"
  ];

  const txts = await Promise.all(files.map(f => fetchTextOrNull("data/" + f)));

  const manTxt = txts[0];
  state.manifest = manTxt ? JSON.parse(manTxt) : null;

  const kpiM = txts[1] ? parseCSV(txts[1]) : [];
  const kpiD = txts[2] ? parseCSV(txts[2]) : [];
  const odM = txts[3] ? parseCSV(txts[3]) : [];
  const odD = txts[4] ? parseCSV(txts[4]) : [];
  const stM = txts[5] ? parseCSV(txts[5]) : [];
  const stD = txts[6] ? parseCSV(txts[6]) : [];
  const hD = txts[7] ? parseCSV(txts[7]) : [];
  const hM = txts[8] ? parseCSV(txts[8]) : [];

  state.kpiMonthCat = kpiM;
  state.kpiDayCat = kpiD;
  state.odMonthCat = odM;
  state.odDayCat = odD;
  state.stationsMonthNode = stM;
  state.stationsDayNode = stD;
  state.histDayCat = hD;
  state.histMonthCat = hM;

  const stTxt = await fetchTextOrNull("data/stations_dim.csv");
  const stRows = stTxt ? parseCSV(stTxt) : [];

  state.stationsRef.clear();
  stRows.forEach(r => {
    const code = String(r.cod_stazione || r.codice || r.cod || "").trim();
    if (!code) return;

    const name = String(r.nome_stazione || r.nome_norm || r.nome || "").trim();
    const city = String(r.citta || r.comune || r.city || r.nome_comune || "").trim();

    const lat0 = parseFloatIT(r.lat);
    const lon0 = parseFloatIT(r.lon);
    const c = normalizeCoords(lat0, lon0);

    const lat = c ? c.lat : NaN;
    const lon = c ? c.lon : NaN;

    state.stationsRef.set(code, { code, name, lat, lon, city });
  });

  const capTxt = await fetchTextOrNull("data/capoluoghi_provincia.csv");
  const capRows = capTxt ? parseCSV(capTxt) : [];
  state.capoluoghiSet = new Set(
    capRows.map(r => String(r.citta || r.capoluogo || r.nome || r.comune || "").trim())
      .filter(Boolean)
  );

  initFilters();
  initDayControls();
  initMap();
  renderAll();

  const basePath = state.manifest && state.manifest.base_path ? state.manifest.base_path : "data/";
  const build = state.manifest && state.manifest.build_utc ? state.manifest.build_utc : "";
  setMeta("Build: " + build + " | base: " + basePath);
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
