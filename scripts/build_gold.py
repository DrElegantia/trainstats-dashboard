from __future__ import annotations

import os
from datetime import date
from typing import Any, Dict, Optional, List

import duckdb
import pandas as pd

from scripts.utils import bucketize_delay, ensure_dir, load_yaml


def list_silver_months() -> List[str]:
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


def to_month_key(ts: pd.Series) -> pd.Series:
    return ts.dt.to_period("M").astype(str)


def to_day_key(ts: pd.Series) -> pd.Series:
    return ts.dt.date.astype(str)


def build_metrics(cfg: Dict[str, Any], df: pd.DataFrame) -> pd.DataFrame:
    thr = int(cfg["punctuality"]["on_time_threshold_minutes"])

    df = df.copy()

    df["categoria"] = df["Categoria"].astype(str).str.strip()
    df["num_treno"] = df["Numero treno"].astype(str).str.strip()

    df["cod_partenza"] = df["Codice stazione partenza"].astype(str).str.strip()
    df["cod_arrivo"] = df["Codice stazione arrivo"].astype(str).str.strip()
    df["nome_partenza"] = df["stazione_partenza_nome_norm"].astype(str)
    df["nome_arrivo"] = df["stazione_arrivo_nome_norm"].astype(str)

    df["partenza_prog_dt"] = pd.to_datetime(df["partenza_prog_dt"], errors="coerce")
    df["arrivo_prog_dt"] = pd.to_datetime(df["arrivo_prog_dt"], errors="coerce")

    df["giorno"] = to_day_key(df["partenza_prog_dt"])
    df["mese"] = to_month_key(df["partenza_prog_dt"])
    df["anno"] = df["partenza_prog_dt"].dt.year.astype("Int64")

    df["ritardo_partenza_min"] = pd.to_numeric(df["ritardo_partenza_min"], errors="coerce")
    df["ritardo_arrivo_min"] = pd.to_numeric(df["ritardo_arrivo_min"], errors="coerce")

    df["has_delay_arrivo"] = df["ritardo_arrivo_min"].notna()
    df["has_delay_partenza"] = df["ritardo_partenza_min"].notna()

    df["arrivo_in_orario"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] <= thr) & (df["ritardo_arrivo_min"] >= 0)
    df["arrivo_in_ritardo"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] > thr)
    df["arrivo_in_anticipo"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] < 0)

    df["oltre_5"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] > 5)
    df["oltre_10"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] > 10)
    df["oltre_15"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] > 15)
    df["oltre_30"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] > 30)
    df["oltre_60"] = df["has_delay_arrivo"] & (df["ritardo_arrivo_min"] > 60)

    df["minuti_ritardo"] = df["ritardo_arrivo_min"].clip(lower=0)
    df["minuti_anticipo"] = (-df["ritardo_arrivo_min"]).clip(lower=0)
    df["minuti_netti"] = df["minuti_ritardo"].fillna(0) - df["minuti_anticipo"].fillna(0)

    edges = cfg["delay_buckets_minutes"]["edges"]
    labels = cfg["delay_buckets_minutes"]["labels"]
    df["bucket_ritardo_arrivo"] = df["ritardo_arrivo_min"].apply(lambda x: bucketize_delay(None if pd.isna(x) else int(x), edges, labels))

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
        corse_osservate=("unique_key", "count"),
        effettuate=("stato_corsa", lambda s: int((s == "effettuato").sum())),
        cancellate=("stato_corsa", lambda s: int((s == "cancellato").sum())),
        soppresse=("stato_corsa", lambda s: int((s == "soppresso").sum())),
        parzialmente_cancellate=("stato_corsa", lambda s: int((s == "parzialmente_cancellato").sum())),
        info_mancante=("info_mancante", lambda s: int(pd.Series(s).fillna(False).sum())),
        in_orario=("arrivo_in_orario", lambda s: int(pd.Series(s).fillna(False).sum())),
        in_ritardo=("arrivo_in_ritardo", lambda s: int(pd.Series(s).fillna(False).sum())),
        in_anticipo=("arrivo_in_anticipo", lambda s: int(pd.Series(s).fillna(False).sum())),
        oltre_5=("oltre_5", lambda s: int(pd.Series(s).fillna(False).sum())),
        oltre_10=("oltre_10", lambda s: int(pd.Series(s).fillna(False).sum())),
        oltre_15=("oltre_15", lambda s: int(pd.Series(s).fillna(False).sum())),
        oltre_30=("oltre_30", lambda s: int(pd.Series(s).fillna(False).sum())),
        oltre_60=("oltre_60", lambda s: int(pd.Series(s).fillna(False).sum())),
        minuti_ritardo_tot=("minuti_ritardo", "sum"),
        minuti_anticipo_tot=("minuti_anticipo", "sum"),
        minuti_netti_tot=("minuti_netti", "sum"),
        ritardo_medio=("ritardo_arrivo_min", "mean"),
        ritardo_mediano=("ritardo_arrivo_min", "median"),
        p90=("ritardo_arrivo_min", q90),
        p95=("ritardo_arrivo_min", q95),
    ).reset_index()

    return out


def build_gold(cfg: Dict[str, Any], df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}

    out["kpi_giorno_categoria"] = agg_core(["giorno", "categoria"], df)
    out["kpi_mese_categoria"] = agg_core(["mese", "categoria"], df)
    out["kpi_giorno"] = agg_core(["giorno"], df)
    out["kpi_mese"] = agg_core(["mese"], df)

    h = df.groupby(["mese", "categoria", "bucket_ritardo_arrivo"], dropna=False).agg(
        count=("unique_key", "count"),
        minuti_ritardo=("minuti_ritardo", "sum"),
        minuti_anticipo=("minuti_anticipo", "sum"),
    ).reset_index()
    out["hist_mese_categoria"] = h

    od = agg_core(["mese", "categoria", "cod_partenza", "cod_arrivo"], df)
    od["nome_partenza"] = df.groupby(["cod_partenza"])["nome_partenza"].first().reindex(od["cod_partenza"]).values
    od["nome_arrivo"] = df.groupby(["cod_arrivo"])["nome_arrivo"].first().reindex(od["cod_arrivo"]).values
    out["od_mese_categoria"] = od

    dep = agg_core(["mese", "categoria", "cod_partenza"], df.rename(columns={"cod_partenza": "cod_stazione"}))
    dep["ruolo"] = "partenza"
    dep["nome_stazione"] = df.groupby(["cod_partenza"])["nome_partenza"].first().reindex(dep["cod_stazione"]).values

    arr = agg_core(["mese", "categoria", "cod_arrivo"], df.rename(columns={"cod_arrivo": "cod_stazione"}))
    arr["ruolo"] = "arrivo"
    arr["nome_stazione"] = df.groupby(["cod_arrivo"])["nome_arrivo"].first().reindex(arr["cod_stazione"]).values

    combined = pd.concat([dep, arr], ignore_index=True)
    out["stazioni_mese_categoria_ruolo"] = combined

    comb2 = combined.groupby(["mese", "categoria", "cod_stazione"], dropna=False).agg(
        corse_osservate=("corse_osservate", "sum"),
        effettuate=("effettuate", "sum"),
        cancellate=("cancellate", "sum"),
        soppresse=("soppresse", "sum"),
        parzialmente_cancellate=("parzialmente_cancellate", "sum"),
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
    ).reset_index()
    comb2["ruolo"] = "nodo"
    comb2["nome_stazione"] = combined.groupby(["cod_stazione"])["nome_stazione"].first().reindex(comb2["cod_stazione"]).values
    out["stazioni_mese_categoria_nodo"] = comb2

    return out


def save_gold_tables(tables: Dict[str, pd.DataFrame]) -> None:
    ensure_dir(os.path.join("data", "gold"))
    for name, df in tables.items():
        csv_path = os.path.join("data", "gold", f"{name}.csv")
        df.to_csv(csv_path, index=False)


def main() -> None:
    cfg = load_yaml("config/pipeline.yml")

    silver_files = list_silver_months()
    if not silver_files:
        print("no silver found, will not build gold")
        return

    df = pd.concat([pd.read_parquet(p) for p in silver_files], ignore_index=True)
    dfm = build_metrics(cfg, df)
    tables = build_gold(cfg, dfm)
    save_gold_tables(tables)

    print({"gold_tables": sorted(list(tables.keys()))})


if __name__ == "__main__":
    main()

