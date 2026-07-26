# scripts/build_gold.py
from __future__ import annotations

import os
from datetime import date
from typing import Any, Dict, List, Optional, Set

import numpy as np
import pandas as pd

from .utils import (
    bucketize_delay_series,
    ensure_dir,
    load_yaml,
    normalize_station_name,
)


_station_code_map_cache: Optional[Dict[str, str]] = None


def _build_station_code_map(df: Optional[pd.DataFrame] = None) -> Dict[str, str]:
    """Build a mapping from every station code to a canonical code.

    When the same physical station has multiple codes (e.g. N_xxx from
    name-hashing and S0xxxx from official RFI numbering), pick one
    canonical code per normalized station name so that all gold
    aggregations treat them as the same station.

    Sources: stations_dim.csv (authoritative, all historical codes) plus
    any code→name pairs found in the current dataframe.

    Preference: S-codes (official) > N-codes (synthetic hash).
    """
    global _station_code_map_cache
    if _station_code_map_cache is not None:
        return _station_code_map_cache

    pairs: Dict[str, str] = {}  # code → normalized_name

    # 1. Load from stations_dim.csv (has all historical codes)
    dim_path = os.path.join("data", "gold", "stations_dim.csv")
    if os.path.exists(dim_path):
        try:
            dim = pd.read_csv(dim_path, usecols=["cod_stazione", "nome_stazione"])
            codes = dim["cod_stazione"].astype(str).str.strip()
            names = dim["nome_stazione"].astype(str).str.strip()
            keep = codes.ne("") & names.ne("")
            # normalize_station_name runs once per distinct name rather than
            # once per row (iterrows also boxed every row into a Series).
            norm = {n: normalize_station_name(n) for n in names[keep].unique()}
            pairs.update(zip(codes[keep], names[keep].map(norm)))
        except Exception:
            pass

    # 2. Also collect from current dataframe (catches brand-new codes)
    if df is not None:
        for col_code, col_name in [("cod_partenza", "nome_partenza"),
                                   ("cod_arrivo", "nome_arrivo")]:
            if col_code not in df.columns or col_name not in df.columns:
                continue
            # One row per distinct code instead of a Python loop over every
            # observation: a three-month chunk holds ~750k rows but only a few
            # thousand distinct codes.
            sub = (
                df[[col_code, col_name]]
                .dropna(subset=[col_code])
                .drop_duplicates(subset=[col_code])
            )
            codes = sub[col_code].astype(str).str.strip()
            names = sub[col_name].astype(str).str.strip()
            norm = {n: normalize_station_name(n) for n in names.unique()}
            for code, name in zip(codes, names):
                if code and code not in pairs:
                    pairs[code] = norm[name]

    # Group codes by normalized name
    name_to_codes: Dict[str, List[str]] = {}
    for code, norm_name in pairs.items():
        if not norm_name:
            continue
        name_to_codes.setdefault(norm_name, []).append(code)

    # Pick canonical code per group: prefer S-codes (official RFI)
    code_map: Dict[str, str] = {}
    for _name, codes in name_to_codes.items():
        if len(codes) <= 1:
            continue
        s_codes = sorted(c for c in codes if c.startswith("S"))
        canonical = s_codes[0] if s_codes else codes[0]
        for c in codes:
            if c != canonical:
                code_map[c] = canonical

    _station_code_map_cache = code_map
    return code_map


# Rappresentazioni testuali del "valore assente" prodotte da astype(str) su
# colonne object: vanno collassate, altrimenti diventano gruppi distinti.
_NULL_LABELS = {"", "none", "nan", "nat", "null", "<na>"}


def _clean_label(values: pd.Series) -> pd.Series:
    s = values.astype(str).str.strip()
    return s.where(~s.str.lower().isin(_NULL_LABELS), "")


def collect_silver_code_names() -> pd.DataFrame:
    """Coppie distinte codice/nome stazione su TUTTO lo storico silver.

    La mappa dei codici canonici veniva costruita chunk per chunk, sui soli mesi
    in lavorazione. Un codice sintetico `N_` usato solo nel 2024 e il codice
    ufficiale `S` della stessa stazione usato solo nel 2026 non finivano mai nel
    medesimo gruppo, quindi non venivano uniti: 363 stazioni restavano spezzate
    in due, per 86.275 corse. Serve una vista sull'intero storico.
    """
    rows: List[pd.DataFrame] = []
    for path in list_silver_month_files():
        for code_col, name_col in (("cod_partenza", "nome_partenza"), ("cod_arrivo", "nome_arrivo")):
            try:
                part = pd.read_parquet(path, columns=[code_col, name_col])
            except Exception:
                continue
            part.columns = ["cod", "nome"]
            rows.append(part.dropna(subset=["cod"]).drop_duplicates(subset=["cod"]))

    if not rows:
        return pd.DataFrame(columns=["cod", "nome"])

    out = pd.concat(rows, ignore_index=True)
    out["cod"] = out["cod"].astype(str).str.strip()
    out["nome"] = out["nome"].astype(str).str.strip()
    return out[out["cod"].ne("")].drop_duplicates(subset=["cod"])


def seed_station_code_map_from_history() -> Dict[str, str]:
    """Costruisce la mappa dei codici una volta sola, da tutto lo storico."""
    global _station_code_map_cache
    _station_code_map_cache = None
    pairs = collect_silver_code_names()
    df = pairs.rename(columns={"cod": "cod_partenza", "nome": "nome_partenza"})
    df["cod_arrivo"] = df["cod_partenza"]
    df["nome_arrivo"] = df["nome_partenza"]
    return _build_station_code_map(df)


def _apply_code_map(df: pd.DataFrame, code_map: Dict[str, str]) -> pd.DataFrame:
    """Replace station codes in the dataframe using the canonical mapping."""
    if not code_map:
        return df
    df = df.copy()
    for col in ("cod_partenza", "cod_arrivo", "cod_stazione"):
        if col in df.columns:
            # Dict-based map is resolved in pandas' C layer; the previous
            # lambda paid a Python call for every one of millions of rows.
            df[col] = df[col].map(code_map).fillna(df[col])
    return df


# Columns that are always summed when re-aggregating after code remapping.
# Classe dell'istogramma per le corse che non sono partite. Sta fuori dai
# bucket configurabili in pipeline.yml perche' non e' un intervallo di minuti:
# e' l'assenza della corsa.
ETICHETTA_NON_EFFETTUATE = "non effettuate"

_SUM_COLS = frozenset([
    "corse_osservate", "corse_con_misura", "effettuate", "cancellate", "soppresse",
    "parzialmente_cancellate", "cancellate_tot", "non_effettuate", "info_mancante",
    "in_orario", "in_ritardo", "in_anticipo", "puntuali",
    "oltre_5", "oltre_10", "oltre_15", "oltre_30", "oltre_60",
    "minuti_ritardo_tot", "minuti_anticipo_tot", "minuti_netti_tot",
    "durata_prog_tot", "corse_con_durata",
    "count", "minuti_ritardo", "minuti_anticipo",
])

# Columns that need weighted-average when re-aggregating. The weight is the
# number of usable delay measurements, not the number of observed runs:
# suppressed trains contribute to corse_osservate but carry no delay, so
# weighting by corse_osservate would over-count routes with many cancellations.
_WAVG_COLS = frozenset([
    "ritardo_medio", "ritardo_mediano", "scostamento_medio",
    "scostamento_mediano", "p90", "p95",
])

_WAVG_WEIGHT = "corse_con_misura"


def _reaggregate_remapped(df: pd.DataFrame, key_cols: List[str]) -> pd.DataFrame:
    """Re-aggregate rows that share the same key after station code remapping.

    During the N_→S_ transition period (around Dec 2025), the same station
    may have rows under both code variants. After remapping both to the
    canonical S_ code, those rows share a key and must be merged: counts are
    summed, means/percentiles are weighted-averaged by corse_osservate.
    """
    if df.empty or not df.duplicated(subset=key_cols).any():
        return df

    present_sum = [c for c in _SUM_COLS if c in df.columns]
    present_wavg = [c for c in _WAVG_COLS if c in df.columns]
    weight = _WAVG_WEIGHT if _WAVG_WEIGHT in df.columns else "corse_osservate"
    has_weight = weight in df.columns and len(present_wavg) > 0

    agg_spec: Dict[str, Any] = {c: "sum" for c in present_sum}
    for c in df.columns:
        if c in key_cols or c in agg_spec or c in _WAVG_COLS:
            continue
        agg_spec[c] = "first"

    if has_weight:
        tmp = df.copy()
        for wc in present_wavg:
            wcol = f"__w_{wc}"
            tmp[wcol] = tmp[wc].fillna(0) * tmp[weight].fillna(0)
            agg_spec[wcol] = "sum"
        result = tmp.groupby(key_cols, dropna=False).agg(agg_spec).reset_index()
        safe_w = result[weight].replace(0, float("nan"))
        for wc in present_wavg:
            result[wc] = result[f"__w_{wc}"] / safe_w
            result.drop(columns=[f"__w_{wc}"], inplace=True)
    else:
        for wc in present_wavg:
            if wc in df.columns:
                agg_spec[wc] = "mean"
        result = df.groupby(key_cols, dropna=False).agg(agg_spec).reset_index()

    return result


def list_silver_month_files() -> List[str]:
    root = os.path.join("data", "silver")
    out: List[str] = []
    if not os.path.exists(root):
        return out
    for y in os.listdir(root):
        yp = os.path.join(root, y)
        if not os.path.isdir(yp):
            continue
        for fn in os.listdir(yp):
            if fn.endswith(".parquet"):
                out.append(os.path.join(yp, fn))
    return sorted(out)


def silver_path_for_month_key(month_key: str) -> str:
    y = month_key[:4]
    return os.path.join("data", "silver", y, f"{month_key}.parquet")


def load_silver(months: Optional[List[str]] = None) -> pd.DataFrame:
    if months:
        paths = [silver_path_for_month_key(m) for m in months]
        paths = [p for p in paths if os.path.exists(p)]
    else:
        paths = list_silver_month_files()

    if not paths:
        return pd.DataFrame()

    return pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)


def to_month_key(ts: pd.Series) -> pd.Series:
    return ts.dt.to_period("M").astype(str)



def _get_on_time_threshold(cfg: Dict[str, Any]) -> int:
    p = cfg.get("punctuality", {})
    if "on_time_threshold_minutes" in p:
        return int(p["on_time_threshold_minutes"])
    if "on_time_max_delay_minutes" in p:
        return int(p["on_time_max_delay_minutes"])
    return 4


def _ensure_obs_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "row_id" in df.columns:
        df["_obs_id"] = df["row_id"].astype(str)
        return df
    df = df.reset_index(drop=True)
    df["_obs_id"] = df.index.astype(str)
    return df


def _hour_to_fascia(h: int) -> str:
    """Map hour (0-23) to fascia oraria slot."""
    if 6 <= h < 9:
        return "mattina"
    if 9 <= h < 14:
        return "tarda_mattina"
    if 14 <= h < 18:
        return "pomeriggio"
    if 18 <= h < 22:
        return "sera"
    # 22-23 and 0-5
    return "notte"


# Tolleranza fra data programmata e giorno di rilevazione. Un giorno copre le
# corse notturne, che il gestore pubblica sul foglio del giorno precedente o
# successivo; oltre, la data e' incoerente.
_MAX_SCOSTAMENTO_GIORNI = 2


def _riconcilia_data_programmata(df: pd.DataFrame) -> pd.DataFrame:
    """Riporta al giorno di rilevazione le date programmate incoerenti.

    La sorgente pubblica qualche corsa con una data programmata distante mesi
    dal giorno in cui e' stata rilevata: 2022-09-16 con partenza dichiarata
    2022-01-16, stesso giorno del mese e mese corrotto. Sono 40 righe su 11,4
    milioni, ma finivano nella partizione mensile di un mese che nessun chunk
    riscrive, e sparivano dal gold senza lasciare traccia: silver e gold non
    quadravano piu'.

    L'ora resta plausibile in tutti i casi osservati, quindi si sostituisce la
    sola data, conservando l'orario, e si tiene l'osservazione. Scartarla
    perderebbe una corsa realmente avvenuta per un errore su un campo.
    """
    if "data_riferimento" not in df.columns:
        return df

    rif = pd.to_datetime(df["data_riferimento"], errors="coerce")
    part = df["dt_partenza_prog"]
    scarto = (part.dt.normalize() - rif.dt.normalize()).dt.days.abs()
    incoerenti = scarto.notna() & (scarto > _MAX_SCOSTAMENTO_GIORNI)
    n = int(incoerenti.sum())
    if not n:
        return df

    df = df.copy()
    delta = part[incoerenti].dt.normalize() - rif[incoerenti].dt.normalize()
    df.loc[incoerenti, "dt_partenza_prog"] = part[incoerenti] - delta
    # L'arrivo si sposta della stessa quantita', cosi' la durata non cambia.
    arr = df.loc[incoerenti, "dt_arrivo_prog"]
    df.loc[incoerenti, "dt_arrivo_prog"] = arr - delta
    print(f"  riconciliate {n} date programmate incoerenti col giorno di rilevazione")
    return df


def build_metrics(cfg: Dict[str, Any], df: pd.DataFrame) -> pd.DataFrame:
    thr = _get_on_time_threshold(cfg)
    df = df.copy()

    df = _ensure_obs_id(df)

    required_cols = [
        "_obs_id",
        "categoria",
        "numero_treno",
        "cod_partenza",
        "cod_arrivo",
        "nome_partenza",
        "nome_arrivo",
        "dt_partenza_prog",
        "dt_arrivo_prog",
        "ritardo_partenza_min",
        "ritardo_arrivo_min",
        "stato_corsa",
        "info_mancante",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"silver schema mismatch. missing={missing}. available={list(df.columns)}")

    # astype(str) su una colonna object trasforma None in "None" e NaN in
    # "nan": la categoria mancante finiva cosi' in tre gruppi distinti ("",
    # "None", "nan") che la dashboard mostrava come categorie separate. Qui
    # collassano tutti in stringa vuota.
    df["categoria"] = _clean_label(df["categoria"])
    df["num_treno"] = df["numero_treno"].astype(str).str.strip()

    df["cod_partenza"] = df["cod_partenza"].astype(str).str.strip()
    df["cod_arrivo"] = df["cod_arrivo"].astype(str).str.strip()

    df["nome_partenza"] = df["nome_partenza"].astype(str)
    df["nome_arrivo"] = df["nome_arrivo"].astype(str)

    df["dt_partenza_prog"] = pd.to_datetime(df["dt_partenza_prog"], errors="coerce")
    df["dt_arrivo_prog"] = pd.to_datetime(df["dt_arrivo_prog"], errors="coerce")

    df = _riconcilia_data_programmata(df)

    # Base temporale su cui poggiano mese, anno e tipo di giornata.
    #
    # Di norma e' la partenza programmata, ma i dump del 2020 contengono 398
    # corse che non ne hanno una: quasi tutte soppressioni, che la sorgente
    # pubblica con gli orari azzerati, piu' qualche corsa effettuata di cui
    # resta il solo orario di arrivo. Senza ripiego quelle righe restavano con
    # mese nullo, e il groupby le scartava: sparivano dal gold esattamente come
    # le date incoerenti di 3.11. Si ripiega sull'arrivo programmato e infine
    # sul giorno di rilevazione, che c'e' sempre.
    base = df["dt_partenza_prog"].fillna(df["dt_arrivo_prog"])
    if "data_riferimento" in df.columns:
        base = base.fillna(pd.to_datetime(df["data_riferimento"], errors="coerce"))

    df["mese"] = to_month_key(base)
    df["anno"] = base.dt.year.astype("Int64")

    # tipo_giorno: infrasettimanale (Mon-Fri) vs weekend (Sat-Sun).
    # Both of these ran a Python lambda per row over millions of rows; the
    # boolean mask and the bucketing below do the same work in numpy.
    dow = base.dt.dayofweek  # 0=Mon, 6=Sun
    df["tipo_giorno"] = np.where(dow.ge(5).fillna(False), "weekend", "infrasettimanale")

    # fascia_oraria: time-of-day slot. Qui il ripiego si ferma agli orari veri:
    # il giorno di rilevazione non porta un'ora, e mezzanotte finirebbe per
    # dichiarare "notte" corse di cui non sappiamo nulla. Restano su "mattina"
    # come prima.
    hour = df["dt_partenza_prog"].fillna(df["dt_arrivo_prog"]).dt.hour
    fascia = pd.cut(
        hour,
        bins=[-1, 5, 8, 13, 17, 21, 23],
        labels=["notte", "mattina", "tarda_mattina", "pomeriggio", "sera", "notte_tarda"],
        ordered=False,
    ).astype(object)
    df["fascia_oraria"] = fascia.replace("notte_tarda", "notte").fillna("mattina")

    df["ritardo_partenza_min"] = pd.to_numeric(df["ritardo_partenza_min"], errors="coerce")
    df["ritardo_arrivo_min"] = pd.to_numeric(df["ritardo_arrivo_min"], errors="coerce")

    # Pre-compute stato_corsa flags as int for fast native aggregation
    stato = df["stato_corsa"].astype(str).str.strip()
    df["is_effettuato"] = (stato == "effettuato").astype("int8")
    df["is_cancellato"] = (stato == "cancellato").astype("int8")
    df["is_soppresso"] = (stato == "soppresso").astype("int8")
    df["is_parz_cancellato"] = (stato == "parzialmente_cancellato").astype("int8")
    df["info_mancante_int"] = df["info_mancante"].fillna(False).astype(bool).astype("int8")

    # --- Which arrival-delay measurements may be used at all --------------
    #
    # Two filters, both previously absent.
    #
    # 1. A train that never ran is published with ritardo = 0. That zero was
    #    counted as a punctual arrival, so suppressed trains both inflated
    #    punctuality and pulled the average delay toward zero.
    # 2. The feed occasionally emits physically impossible values. Rare in
    #    aggregate, decisive on routes with few runs: the -85 minute "average
    #    delay" on BOLOGNA CABINA S.DONATO -> RIMINI came from one such record.
    delay_states = set(cfg.get("delay_states", ["effettuato", "parzialmente_cancellato"]))
    ran = stato.isin(delay_states)

    validity = cfg.get("delay_validity", {})
    lo = float(validity.get("min_minutes", -90))
    hi = float(validity.get("max_minutes", 1440))
    in_range = df["ritardo_arrivo_min"].between(lo, hi)

    valid = df["ritardo_arrivo_min"].notna() & ran & in_range

    # Treni con orario di arrivo sistematicamente sbagliato nella sorgente:
    # partono puntuali e arrivano ogni giorno in anticipo dello stesso scarto.
    # Si riconoscono dalla mediana del singolo treno sulla singola tratta.
    sys_thr = validity.get("systematic_early_median_minutes")
    min_runs = int(validity.get("min_runs_for_reference", 5))
    df["orario_arrivo_sospetto"] = 0
    if sys_thr is not None and {"numero_treno", "cod_partenza", "cod_arrivo"} <= set(df.columns):
        key = ["numero_treno", "cod_partenza", "cod_arrivo"]
        dev_for_ref = df["ritardo_arrivo_min"].where(valid)
        grp = dev_for_ref.groupby([df[c] for c in key])
        med = grp.transform("median")
        runs = grp.transform("count")
        suspect = (runs >= min_runs) & (med <= float(sys_thr))
        df["orario_arrivo_sospetto"] = suspect.fillna(False).astype("int8")
        valid = valid & ~suspect.fillna(False)
        n_susp = int(df["orario_arrivo_sospetto"].sum())
        if n_susp:
            print(f"  scartate {n_susp} misure di treni con orario di arrivo sistematicamente anticipato")

    df["delay_valido"] = valid.astype("int8")

    # Signed deviation from timetable, defined only where usable.
    df["scostamento_min"] = df["ritardo_arrivo_min"].where(valid)

    # Official delay: an early arrival is not a negative delay, it is zero
    # delay. Keeping the sign is what produced whole routes with a negative
    # "ritardo medio". The signed series survives as scostamento_min.
    df["ritardo_min"] = df["scostamento_min"].clip(lower=0)

    df["has_delay_arrivo"] = valid
    df["has_delay_partenza"] = df["ritardo_partenza_min"].notna()

    dev = df["scostamento_min"]
    df["arrivo_in_orario"] = (valid & (dev >= 0) & (dev <= thr)).astype("int8")
    df["arrivo_in_ritardo"] = (valid & (dev > thr)).astype("int8")
    df["arrivo_in_anticipo"] = (valid & (dev < 0)).astype("int8")

    # Punctuality in the regulatory sense: arriving early counts as on time.
    # The three buckets above stay mutually exclusive for the histogram, so
    # this ships as its own column rather than being folded into in_orario.
    df["arrivo_puntuale"] = (valid & (dev <= thr)).astype("int8")

    for k in (5, 10, 15, 30, 60):
        df[f"oltre_{k}"] = (valid & (dev >= k)).astype("int8")

    # Durata programmata della corsa. Serve per un indicatore diverso dal
    # ritardo: i minuti che l'orario impiega per chilometro dicono quanto e'
    # lenta la linea, a prescindere da quanto e' puntuale. Una tratta puo'
    # essere sempre in orario e comunque richiedere il doppio del tempo per
    # chilometro di un'altra, e quella e' inefficienza dell'infrastruttura, non
    # del servizio.
    #
    # La finestra scarta le durate impossibili: negative dove gli orari sono
    # incoerenti, oltre le ventiquattro ore dove la data programmata e' quella
    # sbagliata.
    durata = (df["dt_arrivo_prog"] - df["dt_partenza_prog"]).dt.total_seconds() / 60.0
    df["durata_prog_min"] = durata.where((durata > 0) & (durata <= 1440))
    df["ha_durata"] = df["durata_prog_min"].notna()

    df["minuti_ritardo"] = df["ritardo_min"]
    df["minuti_anticipo"] = (-dev).clip(lower=0)
    df["minuti_netti"] = df["minuti_ritardo"].fillna(0) - df["minuti_anticipo"].fillna(0)

    edges = cfg["delay_buckets_minutes"]["edges"]
    labels = cfg["delay_buckets_minutes"]["labels"]
    bucket = bucketize_delay_series(dev, edges, labels)

    # Le corse che non sono partite non hanno uno scostamento, e finivano tutte
    # nel bucket "missing" insieme ai dati mancanti veri. Sull'istogramma
    # sparivano: erano contate nei totali in testa alla pagina ma invisibili
    # nella distribuzione, e chi guardava non aveva modo di vedere dove fossero
    # finite. Ora hanno una classe propria, che la dashboard disegna come ultima
    # barra: una corsa cancellata e' il caso peggiore, non un dato assente.
    non_effettuata = (df["is_cancellato"].astype(bool) | df["is_soppresso"].astype(bool))
    df["bucket_ritardo_arrivo"] = bucket.mask(non_effettuata, ETICHETTA_NON_EFFETTUATE)

    return df


def agg_core(group_cols: List[str], df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby(group_cols, dropna=False, observed=True)

    agg = g.agg(
        corse_osservate=("_obs_id", "count"),
        corse_con_misura=("delay_valido", "sum"),
        # Misure scartate perche' il treno ha un orario di arrivo
        # sistematicamente sbagliato nella sorgente: pubblicata per rendere
        # l'esclusione verificabile invece che invisibile.
        corse_orario_sospetto=("orario_arrivo_sospetto", "sum"),
        effettuate=("is_effettuato", "sum"),
        cancellate=("is_cancellato", "sum"),
        soppresse=("is_soppresso", "sum"),
        parzialmente_cancellate=("is_parz_cancellato", "sum"),
        info_mancante=("info_mancante_int", "sum"),
        in_orario=("arrivo_in_orario", "sum"),
        in_ritardo=("arrivo_in_ritardo", "sum"),
        in_anticipo=("arrivo_in_anticipo", "sum"),
        puntuali=("arrivo_puntuale", "sum"),
        oltre_5=("oltre_5", "sum"),
        oltre_10=("oltre_10", "sum"),
        oltre_15=("oltre_15", "sum"),
        oltre_30=("oltre_30", "sum"),
        oltre_60=("oltre_60", "sum"),
        durata_prog_tot=("durata_prog_min", "sum"),
        corse_con_durata=("ha_durata", "sum"),
        minuti_ritardo_tot=("minuti_ritardo", "sum"),
        minuti_anticipo_tot=("minuti_anticipo", "sum"),
        minuti_netti_tot=("minuti_netti", "sum"),
        # ritardo_medio is the mean of the floored series, so it can never go
        # below zero. scostamento_medio keeps the signed reading for anyone
        # who needs to see how far ahead of timetable a route runs.
        ritardo_medio=("ritardo_min", "mean"),
        ritardo_mediano=("ritardo_min", "median"),
        scostamento_medio=("scostamento_min", "mean"),
        scostamento_mediano=("scostamento_min", "median"),
    )

    # Percentiles need a second pass. It is taken from the same grouper `g` and
    # joined on the group index, so the two frames align by label. The previous
    # version built a *second* groupby and assigned `.values` positionally,
    # which silently hands one station another station's p95 the moment the two
    # passes disagree on ordering. Joining on the index also keeps NaN group
    # keys aligned, which a column merge would drop (categoria is genuinely
    # NaN for part of the history).
    q = g["ritardo_min"].quantile([0.9, 0.95]).unstack()
    q.columns = ["p90", "p95"]
    out = agg.join(q).reset_index()

    # Runs that did not happen at all. cancellate_tot keeps its historical
    # meaning (full + partial) for the dashboard, while non_effettuate is the
    # honest "the train never ran" count, which previously left out soppresse
    # entirely even though suppression is the dominant form in this feed.
    out["non_effettuate"] = out["cancellate"].fillna(0).astype(int) + out["soppresse"].fillna(0).astype(int)
    out["cancellate_tot"] = out["non_effettuate"] + out["parzialmente_cancellate"].fillna(0).astype(int)

    return out


def _station_source(df: pd.DataFrame) -> pd.DataFrame:
    """Long-format view where each run appears once per endpoint it touches.

    Building the station tables from this frame lets every station metric —
    role-level and node-level alike — come straight out of agg_core over the
    underlying observations.

    The node view used to be folded up from the already-aggregated role rows
    with mean-of-means for ritardo_medio, median-of-medians for the median and
    mean-of-p90s for the percentiles. None of those recover the true statistic:
    the unweighted mean let a station with 3 departures count as much as the
    same station's 900 arrivals, and a median of two medians is not a median
    of anything. Aggregating from the observations removes the problem instead
    of trying to correct for it.
    """
    dep = df.copy()
    dep["cod_stazione"] = dep["cod_partenza"]
    dep["nome_stazione"] = dep["nome_partenza"]
    dep["ruolo"] = "partenza"

    arr = df.copy()
    arr["cod_stazione"] = arr["cod_arrivo"]
    arr["nome_stazione"] = arr["nome_arrivo"]
    arr["ruolo"] = "arrivo"

    fuori = pd.concat([dep, arr], ignore_index=True, copy=False)

    # I dump XML del 2019 pubblicano le soppressioni senza la stazione di
    # partenza: 342 corse su 16,5 milioni restano con un capo ignoto. Senza
    # questo filtro finiscono in una stazione dal codice vuoto, che compare
    # nell'elenco della dashboard come una voce senza nome. Si scarta il solo
    # capo ignoto, non la corsa: l'altro estremo continua a contarla, e i
    # totali di kpi_* non sono toccati perche' non passano di qui.
    codice = fuori["cod_stazione"].astype(str).str.strip()
    return fuori[codice.ne("") & ~codice.str.lower().isin(["nan", "none", "null"])].copy()


def _attach_station_names(out: pd.DataFrame, src: pd.DataFrame) -> pd.DataFrame:
    names = (
        src.dropna(subset=["cod_stazione"])
        .drop_duplicates("cod_stazione")
        .set_index("cod_stazione")["nome_stazione"]
    )
    out["nome_stazione"] = out["cod_stazione"].map(names).fillna("")
    return out


def build_gold(cfg: Dict[str, Any], df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}

    # --- Unify station codes (N_ → S_) before any aggregation ---
    code_map = _build_station_code_map(df)
    if code_map:
        df = _apply_code_map(df, code_map)
        print(f"  unified {len(code_map)} station code aliases")

    # --- KPI tables ---
    out["kpi_mese"] = agg_core(["mese"], df)
    out["kpi_mese_categoria"] = agg_core(["mese", "categoria"], df)
    out["kpi_dettaglio"] = agg_core(["mese", "tipo_giorno", "fascia_oraria"], df)
    out["kpi_dettaglio_categoria"] = agg_core(["mese", "tipo_giorno", "fascia_oraria", "categoria"], df)

    # --- Histogram tables ---
    h_m = df.groupby(["mese", "categoria", "bucket_ritardo_arrivo"], dropna=False).agg(
        count=("_obs_id", "count"),
        minuti_ritardo=("minuti_ritardo", "sum"),
        minuti_anticipo=("minuti_anticipo", "sum"),
    ).reset_index()
    out["hist_mese_categoria"] = h_m

    h_det = df.groupby(["mese", "tipo_giorno", "fascia_oraria", "categoria", "bucket_ritardo_arrivo"], dropna=False).agg(
        count=("_obs_id", "count"),
        minuti_ritardo=("minuti_ritardo", "sum"),
        minuti_anticipo=("minuti_anticipo", "sum"),
    ).reset_index()
    out["hist_dettaglio_categoria"] = h_det

    # --- Station-level tables: one long frame shared by histogram and KPI ---
    # Previously the histogram built its own concat of two full copies and the
    # KPI tables built two more renamed views, so the frame was materialised
    # four times over. One shared frame does both.
    hist_station_src = _station_source(df)

    h_st_m = hist_station_src.groupby(
        ["mese", "categoria", "cod_stazione", "ruolo", "bucket_ritardo_arrivo"],
        dropna=False,
    ).agg(
        count=("_obs_id", "count"),
        minuti_ritardo=("minuti_ritardo", "sum"),
        minuti_anticipo=("minuti_anticipo", "sum"),
    ).reset_index()
    out["hist_stazioni_mese_categoria_ruolo"] = h_st_m

    h_st_det = hist_station_src.groupby(
        ["mese", "tipo_giorno", "fascia_oraria", "categoria", "cod_stazione", "ruolo", "bucket_ritardo_arrivo"],
        dropna=False,
    ).agg(
        count=("_obs_id", "count"),
        minuti_ritardo=("minuti_ritardo", "sum"),
        minuti_anticipo=("minuti_anticipo", "sum"),
    ).reset_index()
    out["hist_stazioni_dettaglio_categoria_ruolo"] = h_st_det

    # --- OD tables ---
    part_names = df.dropna(subset=["cod_partenza"]).drop_duplicates("cod_partenza").set_index("cod_partenza")["nome_partenza"]
    arr_names = df.dropna(subset=["cod_arrivo"]).drop_duplicates("cod_arrivo").set_index("cod_arrivo")["nome_arrivo"]

    od_m = agg_core(["mese", "categoria", "cod_partenza", "cod_arrivo"], df)
    od_det = agg_core(["mese", "tipo_giorno", "fascia_oraria", "categoria", "cod_partenza", "cod_arrivo"], df)

    for od in (od_m, od_det):
        od["nome_partenza"] = od["cod_partenza"].map(part_names).fillna("")
        od["nome_arrivo"] = od["cod_arrivo"].map(arr_names).fillna("")

    out["od_mese_categoria"] = od_m
    out["od_dettaglio_categoria"] = od_det

    # --- Station KPI tables ---
    # Role and node views are both aggregated from the observations, so the
    # node figures are true statistics rather than averages of averages.
    m_group = ["mese", "categoria", "cod_stazione"]
    det_group = ["mese", "tipo_giorno", "fascia_oraria", "categoria", "cod_stazione"]

    staz_m_ruolo = agg_core(m_group + ["ruolo"], hist_station_src)
    out["stazioni_mese_categoria_ruolo"] = _attach_station_names(staz_m_ruolo, hist_station_src)

    staz_m_nodo = agg_core(m_group, hist_station_src)
    staz_m_nodo["ruolo"] = "nodo"
    out["stazioni_mese_categoria_nodo"] = _attach_station_names(staz_m_nodo, hist_station_src)

    staz_det_ruolo = agg_core(det_group + ["ruolo"], hist_station_src)
    out["stazioni_dettaglio_categoria_ruolo"] = _attach_station_names(staz_det_ruolo, hist_station_src)

    staz_det_nodo = agg_core(det_group, hist_station_src)
    staz_det_nodo["ruolo"] = "nodo"
    out["stazioni_dettaglio_categoria_nodo"] = _attach_station_names(staz_det_nodo, hist_station_src)

    return out


def gold_keys() -> Dict[str, List[str]]:
    return {
        "kpi_mese": ["mese"],
        "kpi_mese_categoria": ["mese", "categoria"],
        "kpi_dettaglio": ["mese", "tipo_giorno", "fascia_oraria"],
        "kpi_dettaglio_categoria": ["mese", "tipo_giorno", "fascia_oraria", "categoria"],
        "hist_mese_categoria": ["mese", "categoria", "bucket_ritardo_arrivo"],
        "hist_dettaglio_categoria": ["mese", "tipo_giorno", "fascia_oraria", "categoria", "bucket_ritardo_arrivo"],
        "hist_stazioni_mese_categoria_ruolo": ["mese", "categoria", "cod_stazione", "ruolo", "bucket_ritardo_arrivo"],
        "hist_stazioni_dettaglio_categoria_ruolo": ["mese", "tipo_giorno", "fascia_oraria", "categoria", "cod_stazione", "ruolo", "bucket_ritardo_arrivo"],
        "od_mese_categoria": ["mese", "categoria", "cod_partenza", "cod_arrivo"],
        "od_dettaglio_categoria": ["mese", "tipo_giorno", "fascia_oraria", "categoria", "cod_partenza", "cod_arrivo"],
        "stazioni_mese_categoria_ruolo": ["mese", "categoria", "cod_stazione", "ruolo"],
        "stazioni_mese_categoria_nodo": ["mese", "categoria", "cod_stazione"],
        "stazioni_dettaglio_categoria_ruolo": ["mese", "tipo_giorno", "fascia_oraria", "categoria", "cod_stazione", "ruolo"],
        "stazioni_dettaglio_categoria_nodo": ["mese", "tipo_giorno", "fascia_oraria", "categoria", "cod_stazione"],
    }


GOLD_DIR = os.path.join("data", "gold")
PARTS_DIR = os.path.join(GOLD_DIR, "parts")


def partition_path(name: str, mese: str) -> str:
    return os.path.join(PARTS_DIR, name, f"{mese}.parquet")


def _partition_months(name: str) -> List[str]:
    d = os.path.join(PARTS_DIR, name)
    if not os.path.isdir(d):
        return []
    return sorted(f[:-8] for f in os.listdir(d) if f.endswith(".parquet"))


def migrate_csv_to_partitions(name: str) -> bool:
    """Seed month partitions from a legacy monolithic gold CSV, once.

    Lets an existing checkout switch to partitioned storage without a full
    rebuild from bronze.
    """
    csv_path = os.path.join(GOLD_DIR, f"{name}.csv")
    if _partition_months(name) or not os.path.exists(csv_path):
        return False

    try:
        df_old = pd.read_csv(csv_path)
    except Exception as e:
        print(f"  WARNING: could not read legacy {name}.csv ({e})")
        return False

    first_col = str(df_old.columns[0]) if len(df_old.columns) else ""
    if "git-lfs" in first_col:
        print(f"  WARNING: {name}.csv is an LFS pointer, not data — skipping migration")
        return False
    if "mese" not in df_old.columns:
        return False

    ensure_dir(os.path.join(PARTS_DIR, name))
    for mese, part in df_old.groupby("mese", dropna=False):
        part.to_parquet(partition_path(name, str(mese)), index=False)
    print(f"  migrated {name}.csv -> {df_old['mese'].nunique()} month partitions")
    return True


def save_gold_tables(tables: Dict[str, pd.DataFrame], migrate_legacy: bool = False) -> None:
    """Merge each table's new rows into per-month parquet partitions.

    The previous version kept every gold table as one monolithic CSV and, for
    each processed chunk, re-read that whole CSV, concatenated, de-duplicated
    and wrote all of it back. With the largest table at 77 MB and a full
    rebuild running nine chunks, that alone moved well over a gigabyte of CSV
    through the parser for data that had not changed. The nightly run paid the
    same cost to append a single day.

    Partitioning by month makes both cases proportional to what actually
    changed: the nightly run touches one month's partition, a backfill touches
    only the months it rebuilt. It also stops the repository from storing a
    fresh 77 MB blob every night, which is what grew the git history to 6.5 GB.
    """
    keys = gold_keys()
    code_map = _build_station_code_map()

    for name, df_new in tables.items():
        k = keys.get(name)
        if not k:
            continue
        miss = [c for c in k if c not in df_new.columns]
        if miss:
            raise RuntimeError(f"gold table {name} missing key columns {miss}")

        # La migrazione dal CSV monolitico e' esplicita, mai automatica: se
        # scatta durante una ricostruzione completa, le partizioni nascono
        # popolate con i dati vecchi e il merge successivo tiene in vita ogni
        # riga la cui chiave non esiste piu' nel nuovo output. E' esattamente
        # cosi' che una categoria "None" ormai eliminata e' sopravvissuta a un
        # rebuild da zero, gonfiando i totali del 3%.
        if migrate_legacy:
            migrate_csv_to_partitions(name)
        ensure_dir(os.path.join(PARTS_DIR, name))

        for mese, part_new in df_new.groupby("mese", dropna=False):
            path = partition_path(name, str(mese))

            # La partizione del mese viene SOSTITUITA, non fusa con la
            # precedente.
            #
            # Fondere sembrava prudente ma teneva in vita ogni riga la cui
            # chiave non esiste piu' nel nuovo calcolo, perche' il dedup per
            # chiave non puo' cancellare cio' che non ricompare. Cambiando la
            # soglia di validita' dei ritardi, i bucket dell'istogramma che si
            # erano svuotati restavano popolati con i conteggi vecchi: 168.480
            # corse fantasma in classi di anticipo che non dovevano piu'
            # esistere. Lo stesso meccanismo aveva fatto sopravvivere una
            # categoria "None" a un rebuild da zero.
            #
            # La sostituzione e' corretta perche' ogni mese viene ricalcolato
            # per intero dal silver, comprese le corse a cavallo del mese, che
            # arrivano dai mesi adiacenti caricati come contesto in main().
            part_new = part_new.drop_duplicates(subset=k, keep="last").sort_values(k)
            part_new.to_parquet(path, index=False)


def read_gold_table(name: str) -> pd.DataFrame:
    """Reassemble a full gold table from its month partitions."""
    months = _partition_months(name)
    if not months:
        # Il CSV monolitico e' un residuo del formato precedente e puo' essere
        # arbitrariamente vecchio. Leggerlo in silenzio come se fosse il gold
        # corrente e' il modo migliore per analizzare dati stantii senza
        # accorgersene, cosa che e' effettivamente accaduta durante un rebuild.
        csv_path = os.path.join(GOLD_DIR, f"{name}.csv")
        if os.path.exists(csv_path):
            print(
                f"  ATTENZIONE: nessuna partizione per {name}, uso il CSV legacy "
                f"{csv_path}: i dati potrebbero essere stantii"
            )
            return pd.read_csv(csv_path)
        return pd.DataFrame()

    parts = [pd.read_parquet(partition_path(name, m)) for m in months]
    return pd.concat(parts, ignore_index=True)


def gold_table_names() -> List[str]:
    if os.path.isdir(PARTS_DIR):
        names = sorted(
            d for d in os.listdir(PARTS_DIR)
            if os.path.isdir(os.path.join(PARTS_DIR, d))
        )
        if names:
            return names
    return sorted(gold_keys().keys())


def _month_keys_between(d0: date, d1: date) -> List[str]:
    out: Set[str] = set()
    cur = date(d0.year, d0.month, 1)
    end = date(d1.year, d1.month, 1)
    while cur <= end:
        out.add(f"{cur.year:04d}{cur.month:02d}")
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return sorted(out)


def _shift_month(key: str, delta: int) -> str:
    y, m = int(key[:4]), int(key[4:])
    idx = y * 12 + (m - 1) + delta
    return f"{idx // 12:04d}{idx % 12 + 1:02d}"


def _months_with_neighbours(chunk: List[str], available: List[str]) -> List[str]:
    """Il chunk piu' i mesi confinanti, limitati a quelli che esistono davvero."""
    have = set(available)
    out = set(chunk)
    for key in chunk:
        for delta in (-1, 1):
            neighbour = _shift_month(key, delta)
            if neighbour in have:
                out.add(neighbour)
    return sorted(out)


def main(months: Optional[List[str]] = None, chunk_size: int = 3, migrate_legacy: bool = False) -> None:
    cfg = load_yaml("config/pipeline.yml")

    if months is None:
        months = sorted(
            p.rsplit("/", 1)[-1].replace(".parquet", "")
            for p in list_silver_month_files()
        )

    if not months:
        print("no silver found, will not build gold")
        return

    # La mappa dei codici canonici si costruisce una volta, su tutto lo storico:
    # solo cosi' due codici della stessa stazione usati in mesi diversi finiscono
    # nello stesso gruppo. Prima veniva azzerata a ogni chunk e ricostruita sui
    # soli mesi in lavorazione.
    code_map = seed_station_code_map_from_history()
    print(f"  mappa codici stazione: {len(code_map)} alias su tutto lo storico")

    # Process months in chunks to limit memory usage and avoid timeouts.
    # Each chunk's gold tables are merged with existing gold via save_gold_tables.
    all_table_names: Set[str] = set()
    for i in range(0, len(months), chunk_size):
        chunk = months[i : i + chunk_size]
        print(f"  processing chunk {i // chunk_size + 1}: months {chunk}")

        # Il silver del mese M contiene corse la cui partenza cade nel mese
        # adiacente (treni a cavallo della mezzanotte di fine mese), e
        # viceversa. Poiche' la partizione del mese viene sostituita e non fusa,
        # il calcolo di M deve essere completo: si caricano anche i mesi
        # confinanti come contesto e si tengono poi le sole righe di M.
        context = _months_with_neighbours(chunk, months)
        df = load_silver(context)
        if df.empty:
            print(f"  no silver for {chunk}, skipping")
            continue

        dfm = build_metrics(cfg, df)

        # Si scrivono solo i mesi del chunk: quelli di contesto verranno (o sono
        # stati) elaborati dal loro chunk, con il loro contesto.
        month_keys_set = set(chunk)
        mese_keys = dfm["mese"].astype(str).str.replace("-", "")
        dfm = dfm[mese_keys.isin(month_keys_set)].copy()

        if dfm.empty:
            print(f"  no data for {chunk} after month filter, skipping")
            del df, dfm
            continue

        tables = build_gold(cfg, dfm)
        save_gold_tables(tables, migrate_legacy=migrate_legacy)
        all_table_names.update(tables.keys())

        # Free memory
        del df, dfm, tables

    print({"gold_tables": sorted(all_table_names), "months_used": months})


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="*", required=False, help="YYYYMM list. If omitted, rebuilds all history.")
    ap.add_argument("--start", required=False, help="YYYY-MM-DD (optional shortcut to derive months)")
    ap.add_argument("--end", required=False, help="YYYY-MM-DD (optional shortcut to derive months)")
    ap.add_argument("--chunk-size", type=int, default=3, help="Months per processing chunk (default: 3)")
    ap.add_argument("--migrate-legacy", action="store_true",
                    help="Semina le partizioni dai CSV gold monolitici prima di aggiornarle. "
                         "Solo per il primo passaggio al formato partizionato, mai in un rebuild.")
    args = ap.parse_args()

    months_arg: Optional[List[str]] = args.months if args.months else None
    if (not months_arg) and args.start:
        d0 = date.fromisoformat(args.start)
        d1 = date.fromisoformat(args.end) if args.end else d0
        months_arg = _month_keys_between(d0, d1)

    main(months_arg, chunk_size=args.chunk_size, migrate_legacy=args.migrate_legacy)
