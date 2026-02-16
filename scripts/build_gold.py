# scripts/build_gold.py
from __future__ import annotations

import os
from typing import Any, Dict, List

import pandas as pd

from .utils import bucketize_delay, ensure_dir, load_yaml


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


def _safe_day_key_from_reference(df: pd.DataFrame) -> pd.Series:
    if "data_riferimento" not in df.columns:
        return pd.Series([None] * len(df), index=df.index, dtype="object")
    s = df["data_riferimento"].astype(str).str.strip()
    ok = s.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    out = pd.Series([None] * len(df), index=df.index, dtype="object")
    out.loc[ok] = s.loc[ok]
    return out


def _safe_month_key_from_reference(df: pd.DataFrame) -> pd.Series:
    d = _safe_day_key_from_reference(df)
    out = pd.Series([None] * len(df), index=df.index, dtype="object")
    ok = d.notna()
    out.loc[ok] = d.loc[ok].str.slice(0, 7)
    return out


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

    giorno_ref = _safe_day_key_from_reference(df)
    mese_ref = _safe_month_key_from_reference(df)

    giorno_dtpart = df["dt_partenza_prog"].dt.date.astype(str)
    mese_dtpart = df["dt_partenza_prog"].dt.to_period("M").astype(str)

    df["giorno"] = giorno_ref.fillna(giorno_dtpart)
    df["mese"] = mese_ref.fillna(mese_dtpart)

    anno_from_giorno = pd.to_datetime(df["giorno"], errors="coerce").dt.year
    df["anno"] = anno_from_giorno.astype("Int64")

    df["dow"] = pd.to_datetime(df["giorno"], errors="coerce").dt.dayofweek.astype("Int64")
    df["ora_partenza"] = df["dt_partenza_prog"].dt.hour.astype("Int64")
    df["minuto_partenza"] = df["dt_partenza_prog"].dt.minute.astype("Int64")

    df["ritardo_partenza_min"] = pd.to_numeric(df["ritardo_partenza_min"], errors="coerce")
    df["ritardo_arrivo_min"] = pd.to_numeric(df["ritardo_arrivo_min"], errors="coerce")

    df["has_delay_arrivo"] = df["ritardo_arrivo_min"].notna()

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

    edges = cfg["delay_buckets_minutes"]["ed]()
