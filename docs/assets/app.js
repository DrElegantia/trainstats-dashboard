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

function ensureWeekdays() {
  if (!Array.isArray(state.filters.weekdays) || state.filters.weekdays.length !== 7) {
    state.filters.weekdays = [true,true,true,true,true,true,true];
  }
}

function hasWeekdayFilter() {
  ensureWeekdays();
  return state.filters.weekdays.some(x => !x);
}

function hasTimeFilter() {
  const all = state.filters.time_all !== false;
  if (all) return false;
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

function passTime(row) {
  if (!hasTimeFilter()) return true;

  const fromMins = parseTimeToMinutes(state.filters.time_from || "00:00");
  const toMins = parseTimeToMinutes(state.filters.time_to || "23:59");

  const v =
    row.ora ?? row.ora_partenza ?? row.orario ?? row.hh ?? row.hour ?? row.ora_di_partenza ?? "";

  if (v === "" || v === null || typeof v === "undefined") return true;

  const mins = parseTimeToMinutes(v);
  return timeInRange(mins, fromMins, toMins);
}

function hasDayOrWeekdayFilter() {
  return hasDayFilter() || hasWeekdayFilter();
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
    if (d < lo || d > hi) return false;
  }

  if (!passWeekdays(d)) return false;
  if (!passTime(row)) return false;

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

  if (t.checked) {
    left.classList.remove("active");
    right.classList.add("active");
  } else {
    left.classList.add("active");
    right.classList.remove("active");
  }
}

function ensureHistToggle() {
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
  if (!yearSel) return;

  if (document.getElementById("dayFrom")) return;

  const days = uniq(state.kpiDayCat.map(r => String(r.giorno || "").slice(0, 10)).filter(Boolean)).sort();
  const minDay = days.length ? days[0] : "";
  const maxDay = days.length ? days[days.length - 1] : "";

  const ensureExtra = () => {
    let extra = document.getElementById("filtersExtra");
    if (extra) return extra;

    const host =
      yearSel.closest("#filters") ||
      yearSel.closest(".filters") ||
      yearSel.closest(".controls") ||
      yearSel.closest(".filtersRow") ||
      yearSel.closest(".filtersGrid") ||
      yearSel.parentNode;

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
  };

  const extra = ensureExtra();

  const wrap = document.createElement("div");
  wrap.style.display = "flex";
  wrap.style.alignItems = "center";
  wrap.style.gap = "10px";
  wrap.style.flexWrap = "wrap";

  const lab = document.createElement("div");
  lab.innerText = "Giorno";

  const from = document.createElement("input");
  from.type = "date";
  from.id = "dayFrom";
  if (minDay) from.min = minDay;
  if (maxDay) from.max = maxDay;
  from.value = state.filters.day_from || "";

  const to = document.createElement("input");
  to.type = "date";
  to.id = "dayTo";
  if (minDay) to.min = minDay;
  if (maxDay) to.max = maxDay;
  to.value = state.filters.day_to || "";

  wrap.appendChild(lab);
  wrap.appendChild(from);
  wrap.appendChild(to);

  const wdLab = document.createElement("div");
  wdLab.innerText = "Giorni";

  const wdWrap = document.createElement("div");
  wdWrap.id = "weekdayWrap";
  wdWrap.style.display = "flex";
  wdWrap.style.alignItems = "center";
  wdWrap.style.gap = "6px";

  const ensureW = () => {
    if (!Array.isArray(state.filters.weekdays) || state.filters.weekdays.length !== 7) {
      state.filters.weekdays = [true,true,true,true,true,true,true];
    }
  };
  ensureW();

  const wdLabels = ["Lu","Ma","Me","Gi","Ve","Sa","Do"];

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
      ensureW();
      state.filters.weekdays[i] = !state.filters.weekdays[i];
      refreshWdStyles();
      updateDayNote();
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

  const apply = () => {
    state.filters.day_from = String(from.value || "").trim();
    state.filters.day_to = String(to.value || "").trim();
    state.filters.time_all = !!timeAll.checked;
    state.filters.time_from = String(timeFrom.value || "00:00").trim() || "00:00";
    state.filters.time_to = String(timeTo.value || "23:59").trim() || "23:59";
    syncTimeDisabled();
    updateDayNote();
    renderAll();
  };

  from.onchange = apply;
  to.onchange = apply;
  timeAll.onchange = apply;
  timeFrom.onchange = apply;
  timeTo.onchange = apply;

  extra.appendChild(wrap);
  extra.appendChild(wdLab);
  extra.appendChild(wdWrap);
  extra.appendChild(timeLab);
  extra.appendChild(timeAllWrap);
  extra.appendChild(timeFrom);
  extra.appendChild(timeTo);
  extra.appendChild(note);

  updateDayNote();
}

function updateDayNote() {
  const el = document.getElementById("dayNote");
  if (!el) return;

  const dayActive = hasDayFilter();
  const wdActive = hasWeekdayFilter();
  const timeActive = hasTimeFilter();

  if (!dayActive && !wdActive && !timeActive) {
    el.innerText = "";
    return;
  }

  const haveOdDay = Array.isArray(state.odDayCat) && state.odDayCat.length > 0;
  const haveStDay = Array.isArray(state.stationsDayNode) && state.stationsDayNode.length > 0;
  const haveHistDay = Array.isArray(state.histDayCat) && state.histDayCat.length > 0;

  let msg = "";
  if (dayActive || wdActive) msg = "Filtro giorni attivo.";
  if (timeActive) msg = (msg ? msg + " " : "") + "Filtro orario attivo.";

  if (dayActive || wdActive) {
    if (haveOdDay || haveStDay || haveHistDay) {
      el.innerText = msg;
      return;
    }
    el.innerText = msg + " Per tabelle, mappa e tratte servono anche i file giornalieri OD e stazioni.";
    return;
  }

  el.innerText = msg;
}

/* resto del file invariato rispetto alla versione funzionante: groupDailyToMonthly, state, loadAll, initFilters, renderKPI, renderSeries, renderHist, Tabulator, mappa e città */

