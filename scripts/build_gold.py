# scripts/build_gold.py
from __future__ import annotations

import os
from datetime import date
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from .utils import bucketize_delay, ensure_dir, load_yaml, normalize_station_name


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
            dim = pd.read_csv(dim_path)
            for _, row in dim.iterrows():
                code = str(row.get("cod_stazione", "")).strip()
                name = str(row.get("nome_stazione", "")).strip()
                if code and name:
                    pairs[code] = normalize_station_name(name)
        except Exception:
            pass

    # 2. Also collect from current dataframe (catches brand-new codes)
    if df is not None:
        for col_code, col_name in [("cod_partenza", "nome_partenza"),
                                   ("cod_arrivo", "nome_arrivo")]:
            if col_code not in df.columns or col_name not in df.columns:
                continue
            sub = df[[col_code, col_name]].dropna(subset=[col_code])
            for code, name in zip(sub[col_code].astype(str).str.strip(),
                                  sub[col_name].astype(str).str.strip()):
                if code and code not in pairs:
                    pairs[code] = normalize_station_name(name)

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


def _apply_code_map(df: pd.DataFrame, code_map: Dict[str, str]) -> pd.DataFrame:
    """Replace station codes in the dataframe using the canonical mapping."""
    if not code_map:
        return df
    df = df.copy()
    for col in ("cod_partenza", "cod_arrivo", "cod_stazione"):
        if col in df.columns:
            df[col] = df[col].map(lambda c: code_map.get(c, c))
    return df


# Columns that are always summed when re-aggregating after code remapping.
_SUM_COLS = frozenset([
    "corse_osservate", "effettuate", "cancellate", "soppresse",
    "parzialmente_cancellate", "cancellate_tot", "info_mancante",
    "in_orario", "in_ritardo", "in_anticipo",
    "oltre_5", "oltre_10", "oltre_15", "oltre_30", "oltre_60",
    "minuti_ritardo_tot", "minuti_anticipo_tot", "minuti_netti_tot",
    "count", "minuti_ritardo", "minuti_anticipo",
])

# Columns that need weighted-average (by corse_osservate) when re-aggregating.
_WAVG_COLS = frozenset(["ritardo_medio", "ritardo_mediano", "p90", "p95"])


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
    weight = "corse_osservate"
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

    df["categoria"] = df["categoria"].astype(str).str.strip()
    df["num_treno"] = df["numero_treno"].astype(str).str.strip()

    df["cod_partenza"] = df["cod_partenza"].astype(str).str.strip()
    df["cod_arrivo"] = df["cod_arrivo"].astype(str).str.strip()

    df["nome_partenza"] = df["nome_partenza"].astype(str)
    df["nome_arrivo"] = df["nome_arrivo"].astype(str)

    df["dt_partenza_prog"] = pd.to_datetime(df["dt_partenza_prog"], errors="coerce")
    df["dt_arrivo_prog"] = pd.to_datetime(df["dt_arrivo_prog"], errors="coerce")

    df["mese"] = to_month_key(df["dt_partenza_prog"])
    df["anno"] = df["dt_partenza_prog"].dt.year.astype("Int64")

    # tipo_giorno: infrasettimanale (Mon-Fri) vs weekend (Sat-Sun)
    dow = df["dt_partenza_prog"].dt.dayofweek  # 0=Mon, 6=Sun
    df["tipo_giorno"] = dow.map(
        lambda d: "weekend" if (pd.notna(d) and d >= 5) else "infrasettimanale"
    )

    # fascia_oraria: time-of-day slot
    hour = df["dt_partenza_prog"].dt.hour
    df["fascia_oraria"] = hour.map(lambda h: _hour_to_fascia(h) if pd.notna(h) else "mattina")

    df["ritardo_partenza_min"] = pd.to_numeric(df["ritardo_partenza_min"], errors="coerce")
    df["ritardo_arrivo_min"] = pd.to_numeric(df["ritardo_arrivo_min"], errors="coerce")

    df["has_delay_arrivo"] = df["ritardo_arrivo_min"].notna()
    df["has_delay_partenza"] = df["ritardo_partenza_min"].notna()

    df["arrivo_in_orario"] = (df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] <= thr) & (df["ritardo_arrivo_min"] >= 0)).astype("int8")
    df["arrivo_in_ritardo"] = (df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] > thr)).astype("int8")
    df["arrivo_in_anticipo"] = (df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] < 0)).astype("int8")

    df["oltre_5"] = (df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] >= 5)).astype("int8")
    df["oltre_10"] = (df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] >= 10)).astype("int8")
    df["oltre_15"] = (df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] >= 15)).astype("int8")
    df["oltre_30"] = (df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] >= 30)).astype("int8")
    df["oltre_60"] = (df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] >= 60)).astype("int8")

    # Pre-compute stato_corsa flags as int for fast native aggregation
    stato = df["stato_corsa"].astype(str).str.strip()
    df["is_effettuato"] = (stato == "effettuato").astype("int8")
    df["is_cancellato"] = (stato == "cancellato").astype("int8")
    df["is_soppresso"] = (stato == "soppresso").astype("int8")
    df["is_parz_cancellato"] = (stato == "parzialmente_cancellato").astype("int8")
    df["info_mancante_int"] = df["info_mancante"].fillna(False).astype(bool).astype("int8")

    df["minuti_ritardo"] = df["ritardo_arrivo_min"].clip(lower=0)
    df["minuti_anticipo"] = (-df["ritardo_arrivo_min"]).clip(lower=0)
    df["minuti_netti"] = df["minuti_ritardo"].fillna(0) - df["minuti_anticipo"].fillna(0)

    edges = cfg["delay_buckets_minutes"]["edges"]
    labels = cfg["delay_buckets_minutes"]["labels"]

    def buck(x):
        if pd.isna(x):
            return "missing"
        try:
            return bucketize_delay(int(x), edges, labels)
        except Exception:
            return "missing"

    df["bucket_ritardo_arrivo"] = df["ritardo_arrivo_min"].apply(buck)

    return df


def agg_core(group_cols: List[str], df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby(group_cols, dropna=False)

    out = g.agg(
        corse_osservate=("_obs_id", "count"),
        effettuate=("is_effettuato", "sum"),
        cancellate=("is_cancellato", "sum"),
        soppresse=("is_soppresso", "sum"),
        parzialmente_cancellate=("is_parz_cancellato", "sum"),
        info_mancante=("info_mancante_int", "sum"),
        in_orario=("arrivo_in_orario", "sum"),
        in_ritardo=("arrivo_in_ritardo", "sum"),
        in_anticipo=("arrivo_in_anticipo", "sum"),
        oltre_5=("oltre_5", "sum"),
        oltre_10=("oltre_10", "sum"),
        oltre_15=("oltre_15", "sum"),
        oltre_30=("oltre_30", "sum"),
        oltre_60=("oltre_60", "sum"),
        minuti_ritardo_tot=("minuti_ritardo", "sum"),
        minuti_anticipo_tot=("minuti_anticipo", "sum"),
        minuti_netti_tot=("minuti_netti", "sum"),
        ritardo_medio=("ritardo_arrivo_min", "mean"),
        ritardo_mediano=("ritardo_arrivo_min", "median"),
    ).reset_index()

    # Compute quantiles via native groupby().quantile() — much faster than lambdas
    delay_g = df.groupby(group_cols, dropna=False)["ritardo_arrivo_min"]
    out["p90"] = delay_g.quantile(0.9).values
    out["p95"] = delay_g.quantile(0.95).values

    out["cancellate_tot"] = out["cancellate"].fillna(0).astype(int) + out["parzialmente_cancellate"].fillna(0).astype(int)

    return out


def _nodo_from_ruolo(ruolo_df: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
    """Aggregate partenza+arrivo rows into a single 'nodo' view."""
    nodo_group = [c for c in group_cols if c != "ruolo"]
    comb = ruolo_df.groupby(nodo_group, dropna=False).agg(
        corse_osservate=("corse_osservate", "sum"),
        effettuate=("effettuate", "sum"),
        cancellate=("cancellate", "sum"),
        soppresse=("soppresse", "sum"),
        parzialmente_cancellate=("parzialmente_cancellate", "sum"),
        cancellate_tot=("cancellate_tot", "sum"),
        info_mancante=("info_mancante", "sum"),
        in_orario=("in_orario", "sum"),
        in_ritardo=("in_ritardo", "sum"),
        in_anticipo=("in_anticipo", "sum"),
        oltre_5=("oltre_5", "sum"),
        oltre_10=("oltre_10", "sum"),
        oltre_15=("oltre_15", "sum"),
        oltre_30=("oltre_30", "sum"),
        oltre_60=("oltre_60", "sum"),
        minuti_ritardo_tot=("minuti_ritardo_tot", "sum"),
        minuti_anticipo_tot=("minuti_anticipo_tot", "sum"),
        minuti_netti_tot=("minuti_netti_tot", "sum"),
        ritardo_medio=("ritardo_medio", "mean"),
        ritardo_mediano=("ritardo_mediano", "median"),
        p90=("p90", "mean"),
        p95=("p95", "mean"),
    ).reset_index()
    comb["ruolo"] = "nodo"

    name_map = (
        ruolo_df.dropna(subset=["cod_stazione"])
        .drop_duplicates("cod_stazione")
        .set_index("cod_stazione")["nome_stazione"]
    )
    comb["nome_stazione"] = comb["cod_stazione"].map(name_map).fillna("")
    return comb


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

    # --- Station-level histogram: departure and arrival perspectives ---
    dep_hist = df.copy()
    dep_hist["cod_stazione"] = dep_hist["cod_partenza"]
    dep_hist["ruolo"] = "partenza"

    arr_hist = df.copy()
    arr_hist["cod_stazione"] = arr_hist["cod_arrivo"]
    arr_hist["ruolo"] = "arrivo"

    hist_station_src = pd.concat([dep_hist, arr_hist], ignore_index=True)

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
    dep_src = df.rename(columns={"cod_partenza": "cod_stazione"})
    arr_src = df.rename(columns={"cod_arrivo": "cod_stazione"})

    # Monthly station ruolo
    dep_m = agg_core(["mese", "categoria", "cod_stazione"], dep_src)
    dep_m["ruolo"] = "partenza"
    dep_m["nome_stazione"] = dep_m["cod_stazione"].map(part_names).fillna("")

    arr_m = agg_core(["mese", "categoria", "cod_stazione"], arr_src)
    arr_m["ruolo"] = "arrivo"
    arr_m["nome_stazione"] = arr_m["cod_stazione"].map(arr_names).fillna("")

    staz_m_ruolo = pd.concat([dep_m, arr_m], ignore_index=True)
    out["stazioni_mese_categoria_ruolo"] = staz_m_ruolo

    # Monthly station nodo
    out["stazioni_mese_categoria_nodo"] = _nodo_from_ruolo(
        staz_m_ruolo, ["mese", "categoria", "cod_stazione", "ruolo"]
    )

    # Dettaglio station ruolo
    dep_det = agg_core(["mese", "tipo_giorno", "fascia_oraria", "categoria", "cod_stazione"], dep_src)
    dep_det["ruolo"] = "partenza"
    dep_det["nome_stazione"] = dep_det["cod_stazione"].map(part_names).fillna("")

    arr_det = agg_core(["mese", "tipo_giorno", "fascia_oraria", "categoria", "cod_stazione"], arr_src)
    arr_det["ruolo"] = "arrivo"
    arr_det["nome_stazione"] = arr_det["cod_stazione"].map(arr_names).fillna("")

    staz_det_ruolo = pd.concat([dep_det, arr_det], ignore_index=True)
    out["stazioni_dettaglio_categoria_ruolo"] = staz_det_ruolo

    # Dettaglio station nodo
    out["stazioni_dettaglio_categoria_nodo"] = _nodo_from_ruolo(
        staz_det_ruolo, ["mese", "tipo_giorno", "fascia_oraria", "categoria", "cod_stazione", "ruolo"]
    )

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


def save_gold_tables(tables: Dict[str, pd.DataFrame]) -> None:
    out_dir = os.path.join("data", "gold")
    ensure_dir(out_dir)

    keys = gold_keys()
    code_map = _build_station_code_map()

    for name, df_new in tables.items():
        path = os.path.join(out_dir, f"{name}.csv")
        k = keys.get(name)

        if os.path.exists(path):
            try:
                df_old = pd.read_csv(path)
                first_col = str(df_old.columns[0]) if len(df_old.columns) > 0 else ""
                if "git-lfs" in first_col:
                    print(f"  WARNING: {name}.csv is corrupted (LFS pointer as CSV header) — discarding stale data")
                    merged = df_new.copy()
                else:
                    # Unify station codes in old data before merging
                    df_old = _apply_code_map(df_old, code_map)
                    # Re-aggregate old rows whose keys collapsed after remapping
                    # (e.g. transition month Dec 2025 had both N_ and S_ codes)
                    if k:
                        df_old = _reaggregate_remapped(df_old, k)
                    merged = pd.concat([df_old, df_new], ignore_index=True)
            except Exception as e:
                print(f"  WARNING: could not read {name}.csv ({e}) — starting fresh")
                merged = df_new.copy()
        else:
            merged = df_new.copy()

        if k:
            miss = [c for c in k if c not in merged.columns]
            if miss:
                raise RuntimeError(f"gold table {name} missing key columns {miss}")
            merged = merged.drop_duplicates(subset=k, keep="last").sort_values(k)

        merged.to_csv(path, index=False)


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


def main(months: Optional[List[str]] = None, chunk_size: int = 3) -> None:
    cfg = load_yaml("config/pipeline.yml")

    if months is None:
        months = sorted(
            p.rsplit("/", 1)[-1].replace(".parquet", "")
            for p in list_silver_month_files()
        )

    if not months:
        print("no silver found, will not build gold")
        return

    # Process months in chunks to limit memory usage and avoid timeouts.
    # Each chunk's gold tables are merged with existing gold via save_gold_tables.
    all_table_names: Set[str] = set()
    for i in range(0, len(months), chunk_size):
        chunk = months[i : i + chunk_size]
        print(f"  processing chunk {i // chunk_size + 1}: months {chunk}")

        # Reset code_map cache so it picks up any new data written by previous chunks
        global _station_code_map_cache
        _station_code_map_cache = None

        df = load_silver(chunk)
        if df.empty:
            print(f"  no silver for {chunk}, skipping")
            continue

        dfm = build_metrics(cfg, df)

        # Filter to only the requested months.  Silver files for month M may
        # contain edge-case records whose dt_partenza_prog falls in an adjacent
        # month (e.g. trains departing near midnight).  If we let those leak
        # through, save_gold_tables' dedup (keep="last") would overwrite the
        # correct historical gold rows for that adjacent month with a tiny
        # partial slice.
        month_keys_set = set(chunk)
        mese_keys = dfm["mese"].astype(str).str.replace("-", "")
        dfm = dfm[mese_keys.isin(month_keys_set)].copy()

        if dfm.empty:
            print(f"  no data for {chunk} after month filter, skipping")
            del df, dfm
            continue

        tables = build_gold(cfg, dfm)
        save_gold_tables(tables)
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
    args = ap.parse_args()

    months_arg: Optional[List[str]] = args.months if args.months else None
    if (not months_arg) and args.start:
        d0 = date.fromisoformat(args.start)
        d1 = date.fromisoformat(args.end) if args.end else d0
        months_arg = _month_keys_between(d0, d1)

    main(months_arg, chunk_size=args.chunk_size)
