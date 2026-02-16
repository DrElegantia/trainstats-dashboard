# scripts/build_gold.py
from __future__ import annotations

import os
from datetime import date
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from .utils import bucketize_delay, ensure_dir, load_yaml


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


def to_day_key(ts: pd.Series) -> pd.Series:
    return ts.dt.date.astype(str)


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

    df["giorno"] = to_day_key(df["dt_partenza_prog"])
    df["mese"] = to_month_key(df["dt_partenza_prog"])
    df["anno"] = df["dt_partenza_prog"].dt.year.astype("Int64")
    df["ora"] = df["dt_partenza_prog"].dt.strftime("%H:00")

    df["ritardo_partenza_min"] = pd.to_numeric(df["ritardo_partenza_min"], errors="coerce")
    df["ritardo_arrivo_min"] = pd.to_numeric(df["ritardo_arrivo_min"], errors="coerce")

    df["has_delay_arrivo"] = df["ritardo_arrivo_min"].notna()
    df["has_delay_partenza"] = df["ritardo_partenza_min"].notna()

    df["arrivo_in_orario"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] <= thr) & (df["ritardo_arrivo_min"] >= 0)
    df["arrivo_in_ritardo"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] > thr)
    df["arrivo_in_anticipo"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] < 0)

    df["oltre_5"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] >= 5)
    df["oltre_10"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] >= 10)
    df["oltre_15"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] >= 15)
    df["oltre_30"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] >= 30)
    df["oltre_60"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] >= 60)

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
    def q90(x: pd.Series) -> float:
        x = x.dropna()
        if len(x) == 0:
            return float("nan")
        return float(x.quantile(0.9))

    def q95(x: pd.Series) -> float:
        x = x.dropna()
        if len(x) == 0:
            return float("nan")
        return float(x.quantile(0.95))

    g = df.groupby(group_cols, dropna=False)

    out = g.agg(
        corse_osservate=("_obs_id", "count"),
        effettuate=("stato_corsa", lambda s: int((pd.Series(s) == "effettuato").sum())),
        cancellate=("stato_corsa", lambda s: int((pd.Series(s) == "cancellato").sum())),
        soppresse=("stato_corsa", lambda s: int((pd.Series(s) == "soppresso").sum())),
        parzialmente_cancellate=("stato_corsa", lambda s: int((pd.Series(s) == "parzialmente_cancellato").sum())),
        info_mancante=("info_mancante", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
        in_orario=("arrivo_in_orario", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
        in_ritardo=("arrivo_in_ritardo", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
        in_anticipo=("arrivo_in_anticipo", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
        oltre_5=("oltre_5", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
        oltre_10=("oltre_10", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
        oltre_15=("oltre_15", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
        oltre_30=("oltre_30", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
        oltre_60=("oltre_60", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
        minuti_ritardo_tot=("minuti_ritardo", "sum"),
        minuti_anticipo_tot=("minuti_anticipo", "sum"),
        minuti_netti_tot=("minuti_netti", "sum"),
        ritardo_medio=("ritardo_arrivo_min", "mean"),
        ritardo_mediano=("ritardo_arrivo_min", "median"),
        p90=("ritardo_arrivo_min", q90),
        p95=("ritardo_arrivo_min", q95),
    ).reset_index()

    out["cancellate_tot"] = out["cancellate"].fillna(0).astype(int) + out["parzialmente_cancellate"].fillna(0).astype(int)

    return out


def build_gold(cfg: Dict[str, Any], df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}

    out["kpi_giorno_categoria"] = agg_core(["giorno", "ora", "categoria"], df)
    out["kpi_mese_categoria"] = agg_core(["mese", "categoria"], df)
    out["kpi_giorno"] = agg_core(["giorno", "ora"], df)
    out["kpi_mese"] = agg_core(["mese"], df)

    h_m = df.groupby(["mese", "categoria", "bucket_ritardo_arrivo"], dropna=False).agg(
        count=("_obs_id", "count"),
        minuti_ritardo=("minuti_ritardo", "sum"),
        minuti_anticipo=("minuti_anticipo", "sum"),
    ).reset_index()
    out["hist_mese_categoria"] = h_m

    h_d = df.groupby(["giorno", "ora", "categoria", "bucket_ritardo_arrivo"], dropna=False).agg(
        count=("_obs_id", "count"),
        minuti_ritardo=("minuti_ritardo", "sum"),
        minuti_anticipo=("minuti_anticipo", "sum"),
    ).reset_index()
    out["hist_giorno_categoria"] = h_d

    # Station-level histogram: departure and arrival perspectives
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

    h_st_d = hist_station_src.groupby(
        ["giorno", "ora", "categoria", "cod_stazione", "ruolo", "bucket_ritardo_arrivo"],
        dropna=False,
    ).agg(
        count=("_obs_id", "count"),
        minuti_ritardo=("minuti_ritardo", "sum"),
        minuti_anticipo=("minuti_anticipo", "sum"),
    ).reset_index()
    out["hist_stazioni_giorno_categoria_ruolo"] = h_st_d

    od_m = agg_core(["mese", "categoria", "cod_partenza", "cod_arrivo"], df)
    od_d = agg_core(["giorno", "ora", "categoria", "cod_partenza", "cod_arrivo"], df)

    part_names = df.dropna(subset=["cod_partenza"]).drop_duplicates("cod_partenza").set_index("cod_partenza")["nome_partenza"]
    arr_names = df.dropna(subset=["cod_arrivo"]).drop_duplicates("cod_arrivo").set_index("cod_arrivo")["nome_arrivo"]

    for od in (od_m, od_d):
        od["nome_partenza"] = od["cod_partenza"].map(part_names).fillna("")
        od["nome_arrivo"] = od["cod_arrivo"].map(arr_names).fillna("")

    out["od_mese_categoria"] = od_m
    out["od_giorno_categoria"] = od_d

    dep_src = df.rename(columns={"cod_partenza": "cod_stazione"})
    arr_src = df.rename(columns={"cod_arrivo": "cod_stazione"})

    dep_m = agg_core(["mese", "categoria", "cod_stazione"], dep_src)
    dep_m["ruolo"] = "partenza"
    dep_m["nome_stazione"] = dep_m["cod_stazione"].map(part_names).fillna("")

    arr_m = agg_core(["mese", "categoria", "cod_stazione"], arr_src)
    arr_m["ruolo"] = "arrivo"
    arr_m["nome_stazione"] = arr_m["cod_stazione"].map(arr_names).fillna("")

    out["stazioni_mese_categoria_ruolo"] = pd.concat([dep_m, arr_m], ignore_index=True)

    dep_d = agg_core(["giorno", "ora", "categoria", "cod_stazione"], dep_src)
    dep_d["ruolo"] = "partenza"
    dep_d["nome_stazione"] = dep_d["cod_stazione"].map(part_names).fillna("")

    arr_d = agg_core(["giorno", "ora", "categoria", "cod_stazione"], arr_src)
    arr_d["ruolo"] = "arrivo"
    arr_d["nome_stazione"] = arr_d["cod_stazione"].map(arr_names).fillna("")

    out["stazioni_giorno_categoria_ruolo"] = pd.concat([dep_d, arr_d], ignore_index=True)

    combined_m = out["stazioni_mese_categoria_ruolo"]
    comb2_m = combined_m.groupby(["mese", "categoria", "cod_stazione"], dropna=False).agg(
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
    comb2_m["ruolo"] = "nodo"

    name_any_m = combined_m.dropna(subset=["cod_stazione"]).drop_duplicates("cod_stazione").set_index("cod_stazione")["nome_stazione"]
    comb2_m["nome_stazione"] = comb2_m["cod_stazione"].map(name_any_m).fillna("")
    out["stazioni_mese_categoria_nodo"] = comb2_m

    combined_d = out["stazioni_giorno_categoria_ruolo"]
    comb2_d = combined_d.groupby(["giorno", "ora", "categoria", "cod_stazione"], dropna=False).agg(
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
    comb2_d["ruolo"] = "nodo"

    name_any_d = combined_d.dropna(subset=["cod_stazione"]).drop_duplicates("cod_stazione").set_index("cod_stazione")["nome_stazione"]
    comb2_d["nome_stazione"] = comb2_d["cod_stazione"].map(name_any_d).fillna("")
    out["stazioni_giorno_categoria_nodo"] = comb2_d

    return out


def gold_keys() -> Dict[str, List[str]]:
    return {
        "kpi_giorno_categoria": ["giorno", "ora", "categoria"],
        "kpi_mese_categoria": ["mese", "categoria"],
        "kpi_giorno": ["giorno", "ora"],
        "kpi_mese": ["mese"],
        "hist_mese_categoria": ["mese", "categoria", "bucket_ritardo_arrivo"],
        "hist_giorno_categoria": ["giorno", "ora", "categoria", "bucket_ritardo_arrivo"],
        "hist_stazioni_mese_categoria_ruolo": ["mese", "categoria", "cod_stazione", "ruolo", "bucket_ritardo_arrivo"],
        "hist_stazioni_giorno_categoria_ruolo": ["giorno", "ora", "categoria", "cod_stazione", "ruolo", "bucket_ritardo_arrivo"],
        "od_mese_categoria": ["mese", "categoria", "cod_partenza", "cod_arrivo"],
        "od_giorno_categoria": ["giorno", "ora", "categoria", "cod_partenza", "cod_arrivo"],
        "stazioni_mese_categoria_ruolo": ["mese", "categoria", "cod_stazione", "ruolo"],
        "stazioni_mese_categoria_nodo": ["mese", "categoria", "cod_stazione"],  # ✅ CORRETTO - senza ruolo
        "stazioni_giorno_categoria_ruolo": ["giorno", "ora", "categoria", "cod_stazione", "ruolo"],
        "stazioni_giorno_categoria_nodo": ["giorno", "ora", "categoria", "cod_stazione"],  # ✅ CORRETTO - senza ruolo
    }


def save_gold_tables(tables: Dict[str, pd.DataFrame]) -> None:
    out_dir = os.path.join("data", "gold")
    ensure_dir(out_dir)

    keys = gold_keys()

    for name, df_new in tables.items():
        path = os.path.join(out_dir, f"{name}.csv")

        if os.path.exists(path):
            df_old = pd.read_csv(path)
            merged = pd.concat([df_old, df_new], ignore_index=True)
        else:
            merged = df_new.copy()

        k = keys.get(name)
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


def main(months: Optional[List[str]] = None) -> None:
    cfg = load_yaml("config/pipeline.yml")

    df = load_silver(months)
    if df.empty:
        print("no silver found, will not build gold")
        return

    dfm = build_metrics(cfg, df)
    tables = build_gold(cfg, dfm)
    save_gold_tables(tables)

    print({"gold_tables": sorted(list(tables.keys())), "months_used": months or "ALL"})


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="*", required=False, help="YYYYMM list. If omitted, rebuilds all history.")
    ap.add_argument("--start", required=False, help="YYYY-MM-DD (optional shortcut to derive months)")
    ap.add_argument("--end", required=False, help="YYYY-MM-DD (optional shortcut to derive months)")
    args = ap.parse_args()

    months_arg: Optional[List[str]] = args.months if args.months else None
    if (not months_arg) and args.start:
        d0 = date.fromisoformat(args.start)
        d1 = date.fromisoformat(args.end) if args.end else d0
        months_arg = _month_keys_between(d0, d1)

    main(months_arg)
