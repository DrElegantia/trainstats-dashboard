# scripts/transform_silver.py
from __future__ import annotations

import ast
import gzip
import os
import re
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .utils import (
    StatusEngine,
    date_range_inclusive,
    ensure_dir,
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
    root = os.path.join("data", "silver", y)
    ensure_dir(root)
    return os.path.join(root, f"{y}{d.month:02d}.parquet")


def read_bronze(csv_gz: str, meta_path: str) -> pd.DataFrame:
    meta = pd.read_json(meta_path, typ="series")
    extracted_at = str(meta.get("extracted_at_utc", ""))

    with gzip.open(csv_gz, "rb") as f:
        df = pd.read_csv(f, dtype=str)

    df["_extracted_at_utc"] = extracted_at
    df["_bronze_path"] = csv_gz
    df["_reference_date"] = str(meta.get("reference_date", ""))
    return df


def _epoch_to_it_string(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s == "" or s == "0":
        return ""
    try:
        ts = pd.to_datetime(int(float(s)), unit="s", utc=True).tz_convert("Europe/Rome")
        return ts.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return ""


def _parse_treni_payload(x: Any) -> List[Dict[str, Any]]:
    if x is None:
        return []
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return []
    try:
        parsed = ast.literal_eval(s)
    except Exception:
        return []
    if isinstance(parsed, list):
        return [r for r in parsed if isinstance(r, dict)]
    return []


def normalize_bronze_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize known bronze layouts into the historical tabular schema."""
    if "Categoria" in df.columns and "Numero treno" in df.columns:
        return df

    if "treni" not in df.columns:
        return df

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        for t in _parse_treni_payload(r.get("treni")):
            rows.append(
                {
                    "Categoria": t.get("c", ""),
                    "Numero treno": t.get("n", ""),
                    "Codice stazione partenza": "",
                    "Nome stazione partenza": t.get("p", ""),
                    "Ora partenza programmata": _epoch_to_it_string(t.get("op")),
                    "Ritardo partenza": t.get("rp", ""),
                    "Codice stazione arrivo": "",
                    "Nome stazione arrivo": t.get("a", ""),
                    "Ora arrivo programmata": _epoch_to_it_string(t.get("oa")),
                    "Ritardo arrivo": t.get("ra", ""),
                    "Cambi numerazione": "",
                    "Provvedimenti": "",
                    "Variazioni": "",
                    "Stazione estera partenza": "",
                    "Orario estero partenza": "",
                    "Stazione estera arrivo": "",
                    "Orario estero arrivo": "",
                    "_extracted_at_utc": r.get("_extracted_at_utc", ""),
                    "_bronze_path": r.get("_bronze_path", ""),
                    "_reference_date": r.get("_reference_date", ""),
                }
            )

    if not rows:
        return df.iloc[0:0].copy()

    return pd.DataFrame(rows)


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


def make_row_id(df: pd.DataFrame) -> pd.Series:
    cols = [
        "_reference_date",
        "categoria",
        "numero_treno",
        "cod_partenza",
        "cod_arrivo",
        "dt_partenza_prog_raw",
        "dt_arrivo_prog_raw",
        "ritardo_partenza_raw",
        "ritardo_arrivo_raw",
        "cambi_numerazione",
        "provvedimenti",
        "variazioni",
    ]
    cols = [c for c in cols if c in df.columns]
    base = df[cols].copy()
    for c in cols:
        base[c] = base[c].astype(str).fillna("")
    return pd.util.hash_pandas_object(base, index=False).astype("uint64").astype(str)


_re_cancel = re.compile(r"\btreno\s+cancellat[oa]\b|\bcancellat[oa]\b", re.IGNORECASE)
_re_supp = re.compile(r"\bsoppresso\b", re.IGNORECASE)
_re_partial = re.compile(
    r"\bfermat[ae]\s+soppress[ae]\b|\bfermate\s+soppresse\b|\bfermata\s+soppressa\b",
    re.IGNORECASE,
)


def _status_fallback_series(df: pd.DataFrame) -> pd.Series:
    prov = df.get("provvedimenti", pd.Series([""] * len(df))).astype(str).fillna("")
    var = df.get("variazioni", pd.Series([""] * len(df))).astype(str).fillna("")
    hay = (prov + " | " + var).astype(str)

    is_partial = hay.str.contains(_re_partial, na=False)
    is_soppresso = prov.str.strip().str.contains(_re_supp, na=False) & (~is_partial)
    is_cancellato = hay.str.contains(_re_cancel, na=False)

    out = pd.Series([""] * len(df), index=df.index, dtype=object)
    out[is_partial] = "parzialmente_cancellato"
    out[is_soppresso] = "soppresso"
    out[is_cancellato] = "cancellato"
    return out


def transform(cfg: Dict[str, Any], df: pd.DataFrame) -> pd.DataFrame:
    se = StatusEngine.from_config(cfg)

    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str)

    df = normalize_bronze_schema(df)
    df = canonical_rename(df)

    required = [
        "categoria",
        "numero_treno",
        "cod_partenza",
        "cod_arrivo",
        "dt_partenza_prog_raw",
        "dt_arrivo_prog_raw",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        path = str(df.get("_bronze_path", pd.Series([""])).iloc[0]) if len(df) else ""
        raise RuntimeError(f"silver missing required columns after rename: {missing} (file={path})")

    df["provvedimenti"] = df.get("provvedimenti", "").astype(str).fillna("")
    df["variazioni"] = df.get("variazioni", "").astype(str).fillna("")

    df["nome_partenza"] = df.get("nome_partenza", "").map(normalize_station_name)
    df["nome_arrivo"] = df.get("nome_arrivo", "").map(normalize_station_name)

    df["ritardo_partenza_min"] = df.get("ritardo_partenza_raw", "").map(safe_int)
    df["ritardo_arrivo_min"] = df.get("ritardo_arrivo_raw", "").map(safe_int)

    df["dt_partenza_prog"] = df["dt_partenza_prog_raw"].map(parse_dt_it)
    df["dt_arrivo_prog"] = df["dt_arrivo_prog_raw"].map(parse_dt_it)

    df["data_riferimento"] = df["_reference_date"]

    df["missing_datetime"] = df["dt_partenza_prog"].isna() | df["dt_arrivo_prog"].isna()
    df["info_mancante"] = df["missing_datetime"]

    stato_cfg = df.apply(se.classify, axis=1).astype("object")
    stato_fb = _status_fallback_series(df)

    stato = stato_cfg.copy()
    strong_fb = stato_fb.isin(["cancellato", "soppresso"])
    stato.loc[strong_fb] = stato_fb.loc[strong_fb]
    weak_fb = (stato_fb == "parzialmente_cancellato") & (stato == "effettuato")
    stato.loc[weak_fb] = stato_fb.loc[weak_fb]
    stato = stato.fillna("effettuato")

    df["stato_corsa"] = stato

    df["_extracted_at_utc_ts"] = pd.to_datetime(df["_extracted_at_utc"], errors="coerce", utc=True)

    df["row_id"] = make_row_id(df)

    df = df.sort_values(["row_id", "_extracted_at_utc_ts"]).drop_duplicates(subset=["row_id"], keep="last")

    return df


def upsert_month_parquet(path: str, add_df: pd.DataFrame) -> None:
    if os.path.exists(path):
        base = pd.read_parquet(path)
        merged = pd.concat([base, add_df], ignore_index=True)

        if "row_id" not in merged.columns:
            merged["row_id"] = make_row_id(merged)

        merged["_extracted_at_utc_ts"] = pd.to_datetime(merged["_extracted_at_utc"], errors="coerce", utc=True)
        merged = merged.sort_values(["row_id", "_extracted_at_utc_ts"]).drop_duplicates(subset=["row_id"], keep="last")
        merged.to_parquet(path, index=False)
    else:
        add_df.to_parquet(path, index=False)


def main(start: str, end: Optional[str] = None) -> None:
    cfg = load_yaml("config/pipeline.yml")

    d0 = date.fromisoformat(start)
    d1 = date.fromisoformat(end) if end else d0

    files = list_bronze_files_for_range(d0, d1)
    if not files:
        print("no bronze files found for range")
        return

    by_month: Dict[str, List[pd.DataFrame]] = {}
    skipped: List[str] = []
    for d, csv_gz, meta in files:
        df_b = read_bronze(csv_gz, meta)
        try:
            df_s = transform(cfg, df_b)
        except RuntimeError as exc:
            skipped.append(f"{csv_gz}: {exc}")
            continue

        if df_s.empty:
            skipped.append(f"{csv_gz}: no rows after normalization")
            continue

        key = f"{d.year:04d}{d.month:02d}"
        by_month.setdefault(key, []).append(df_s)

    if not by_month:
        print({"silver_updated_months": [], "skipped_files": len(skipped)})
        for msg in skipped[:20]:
            print(f"WARN {msg}")
        return

    for key, parts in by_month.items():
        y = int(key[:4])
        m = int(key[4:])
        p = silver_path_for_month(date(y, m, 1))
        add_df = pd.concat(parts, ignore_index=True)
        upsert_month_parquet(p, add_df)

    print({"silver_updated_months": sorted(list(by_month.keys())), "skipped_files": len(skipped)})
    for msg in skipped[:20]:
        print(f"WARN {msg}")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=False, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.start, args.end)
