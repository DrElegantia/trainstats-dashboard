from __future__ import annotations

import gzip
import os
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .utils import (
    StatusEngine,
    compute_unique_key,
    date_range_inclusive,
    ensure_dir,
    load_json,
    load_yaml,
    normalize_station_name,
    parse_dt_it,
    safe_int,
)


def list_bronze_files_for_range(d0: date, d1: date) -> List[Tuple[date, str, str]]:
    out: List[Tuple[date, str, str]] = []
    for d in date_range_inclusive(d0, d1):
        y = f"{d.year:04d}"
        m = f"{d.month:02d}"
        dd = f"{d.day:02d}"
        root = os.path.join("data", "bronze", y, m)
        csv_gz = os.path.join(root, f"{y}{m}{dd}.csv.gz")
        meta = os.path.join(root, f"{y}{m}{dd}.meta.json")
        if os.path.exists(csv_gz) and os.path.exists(meta):
            out.append((d, csv_gz, meta))
    return out


def silver_path_for_month(d: date) -> str:
    y = f"{d.year:04d}"
    m = f"{d.month:02d}"
    root = os.path.join("data", "silver", y)
    ensure_dir(root)
    return os.path.join(root, f"{y}{m}.parquet")


def read_bronze(csv_gz: str, meta_path: str) -> pd.DataFrame:
    meta = pd.read_json(meta_path, typ="series")
    extracted_at = str(meta.get("extracted_at_utc", ""))

    with gzip.open(csv_gz, "rb") as f:
        df = pd.read_csv(f, dtype=str)

    df["_extracted_at_utc"] = extracted_at
    df["_bronze_path"] = csv_gz
    df["_reference_date"] = str(meta.get("reference_date", ""))
    return df


def canonical_rename(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "Categoria": "categoria",
        "Numero treno": "numero_treno",
        "Codice stazione partenza": "cod_partenza",
        "Nome stazione partenza": "nome_partenza",
        "Ora partenza programmata": "dt_partenza_prog_raw",
        "Ritardo partenza": "ritardo_partenza_raw",
        "Codice stazione arrivo": "cod_arrivo",
        "Nome stazione arrivo": "nome_arrivo",
        "Ora arrivo programmata": "dt_arrivo_prog_raw",
        "Ritardo arrivo": "ritardo_arrivo_raw",
        "Cambi numerazione": "cambi_numerazione",
        "Provvedimenti": "provvedimenti",
        "Variazioni": "variazioni",
        "Stazione estera partenza": "stazione_estera_partenza",
        "Orario estero partenza": "orario_estero_partenza",
        "Stazione estera arrivo": "stazione_estera_arrivo",
        "Orario estero arrivo": "orario_estero_arrivo",
    }
    present = {k: v for k, v in rename_map.items() if k in df.columns}
    return df.rename(columns=present)


def transform(cfg: Dict[str, Any], df: pd.DataFrame) -> pd.DataFrame:
    se = StatusEngine.from_config(cfg)

    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str)

    df = canonical_rename(df)

    required = ["categoria", "numero_treno", "cod_partenza", "cod_arrivo", "dt_partenza_prog_raw", "dt_arrivo_prog_raw"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"silver missing required columns after rename: {missing}. available={list(df.columns)}")

    df["nome_partenza"] = df.get("nome_partenza", "").map(normalize_station_name)
    df["nome_arrivo"] = df.get("nome_arrivo", "").map(normalize_station_name)

    df["ritardo_partenza_min"] = df.get("ritardo_partenza_raw", "").map(safe_int)
    df["ritardo_arrivo_min"] = df.get("ritardo_arrivo_raw", "").map(safe_int)

    df["dt_partenza_prog"] = df["dt_partenza_prog_raw"].map(parse_dt_it)
    df["dt_arrivo_prog"] = df["dt_arrivo_prog_raw"].map(parse_dt_it)

    df["data_riferimento"] = df["_reference_date"]

    df["unique_key"] = df.apply(compute_unique_key, axis=1)

    df["stato_corsa"] = df.apply(se.classify, axis=1)

    df["missing_key"] = df["unique_key"].isna() | (df["unique_key"].astype(str).str.len() == 0)
    df["missing_datetime"] = df["dt_partenza_prog"].isna() | df["dt_arrivo_prog"].isna()

    df["info_mancante"] = df["missing_key"] | df["missing_datetime"]

    df["_extracted_at_utc_ts"] = pd.to_datetime(df["_extracted_at_utc"], errors="coerce", utc=True)
    df = df.sort_values(["unique_key", "_extracted_at_utc_ts"]).drop_duplicates(subset=["unique_key"], keep="last")

    return df


def quality_checks(cfg: Dict[str, Any], df: pd.DataFrame) -> None:
    q = cfg["quality"]
    max_dt_fail = float(q["max_datetime_parse_failure_rate"])
    max_miss_key = float(q["max_missing_key_rate"])

    n = max(1, len(df))
    dt_fail = float(df["missing_datetime"].sum()) / n
    mk_fail = float(df["missing_key"].sum()) / n

    if dt_fail > max_dt_fail:
        raise RuntimeError(f"datetime parse failure rate too high: {dt_fail:.4f} > {max_dt_fail:.4f}")
    if mk_fail > max_miss_key:
        raise RuntimeError(f"missing key rate too high: {mk_fail:.4f} > {max_miss_key:.4f}")


def upsert_month_parquet(path: str, add_df: pd.DataFrame) -> None:
    if os.path.exists(path):
        base = pd.read_parquet(path)
        merged = pd.concat([base, add_df], ignore_index=True)
        merged["_extracted_at_utc_ts"] = pd.to_datetime(merged["_extracted_at_utc"], errors="coerce", utc=True)
        merged = merged.sort_values(["unique_key", "_extracted_at_utc_ts"]).drop_duplicates(subset=["unique_key"], keep="last")
        merged.to_parquet(path, index=False)
    else:
        add_df.to_parquet(path, index=False)


def main(start: str, end: Optional[str] = None) -> None:
    cfg = load_yaml("config/pipeline.yml")
    _schema = load_json("config/schema_expected.json")

    d0 = date.fromisoformat(start)
    d1 = date.fromisoformat(end) if end else d0

    files = list_bronze_files_for_range(d0, d1)
    if not files:
        print("no bronze files found for range")
        return

    by_month: Dict[str, List[pd.DataFrame]] = {}
    for d, csv_gz, meta in files:
        df_b = read_bronze(csv_gz, meta)
        df_s = transform(cfg, df_b)
        quality_checks(cfg, df_s)
        key = f"{d.year:04d}{d.month:02d}"
        by_month.setdefault(key, []).append(df_s)

    for key, parts in by_month.items():
        y = int(key[:4])
        m = int(key[4:])
        p = silver_path_for_month(date(y, m, 1))
        add_df = pd.concat(parts, ignore_index=True)
        upsert_month_parquet(p, add_df)

    print({"silver_updated_months": sorted(list(by_month.keys()))})


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=False, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.start, args.end)
