# scripts/transform_silver.py
from __future__ import annotations

import gzip
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .utils import (
    StatusEngine,
    date_range_inclusive,
    ensure_dir,
    load_yaml,
    normalize_station_name,
    parse_dt_it_series,
    safe_int_series,
)

from .silver_schema import (
    code_from_station_name,
    missing_station_code,
    normalize_bronze_schema,
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
        df = pd.read_csv(f, dtype=str, on_bad_lines="skip")

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
        # Presenti solo nel layout bronze legacy, dove indicano origine e
        # destinazione da orario quando la corsa e' stata limitata.
        "Origine programmata": "origine_programmata",
        "Destinazione programmata": "destinazione_programmata",
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


def _map_over_unique(values: pd.Series, fn) -> pd.Series:
    """Apply fn once per distinct value instead of once per row.

    A daily bronze file holds ~9k rows but only ~2k distinct station names, and
    both normalize_station_name() and code_from_station_name() are pure, so the
    per-row .map() was doing four to five times the necessary regex work.
    """
    s = values if isinstance(values, pd.Series) else pd.Series(values)
    s = s.astype(str)
    if s.empty:
        return s
    lookup = {v: fn(v) for v in s.unique()}
    return s.map(lookup)


def _normalize_names(values: pd.Series) -> pd.Series:
    return _map_over_unique(values, normalize_station_name)


def _codes_from_names(values: pd.Series) -> pd.Series:
    return _map_over_unique(values, code_from_station_name)


# Text columns the status engine reads. Absent ones are created empty so that
# StatusEngine.haystack() always has something to join, whichever bronze
# layout produced the frame.
_STATUS_TEXT_COLUMNS = [
    "provvedimenti",
    "variazioni",
    "cambi_numerazione",
    "stazione_estera_partenza",
    "stazione_estera_arrivo",
]


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

    # La categoria e' testo, e il vuoto e' la stringa vuota, mai un nullo.
    #
    # Nel formato legacy la costruiamo noi e lo e' gia'; in quello nuovo arriva
    # da una colonna CSV, dove la cella vuota diventa NaN. Venti righe su tutto
    # lo storico, e bastavano: il NaN sopravvive al raggruppamento del gold e
    # diventa un gruppo a se', che l'invariante sulle categorie spurie trova. E'
    # cosi' che la pipeline notturna del 31 luglio 2026 e' fallita su entrambi i
    # repo con "trovate ['None']", fermandosi PRIMA di committare: il
    # comportamento voluto, non ha pubblicato dati sbagliati.
    if "categoria" in df.columns:
        df["categoria"] = (df["categoria"].fillna("").astype(str)
                           .replace({"nan": "", "None": "", "<NA>": ""}))

    for c in _STATUS_TEXT_COLUMNS:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).replace("nan", "")
        else:
            df[c] = ""

    # Nomi stazione normalizzati: la mappa cache-ata evita di ripetere la
    # normalizzazione regex su ogni riga quando lo stesso nome ricorre
    # migliaia di volte nello stesso file.
    df["nome_partenza"] = _normalize_names(df.get("nome_partenza", ""))
    df["nome_arrivo"] = _normalize_names(df.get("nome_arrivo", ""))

    # Alcuni bronze (schema "treni") non forniscono codici stazione: deriviamo un codice
    # stabile dal nome per mantenere le aggregazioni station-level in gold/stations_dim.
    miss_dep = df["cod_partenza"].map(missing_station_code)
    miss_arr = df["cod_arrivo"].map(missing_station_code)
    df.loc[miss_dep, "cod_partenza"] = _codes_from_names(df.loc[miss_dep, "nome_partenza"])
    df.loc[miss_arr, "cod_arrivo"] = _codes_from_names(df.loc[miss_arr, "nome_arrivo"])

    df["ritardo_partenza_min"] = safe_int_series(df.get("ritardo_partenza_raw", ""))
    df["ritardo_arrivo_min"] = safe_int_series(df.get("ritardo_arrivo_raw", ""))

    df["dt_partenza_prog"] = parse_dt_it_series(df["dt_partenza_prog_raw"])
    df["dt_arrivo_prog"] = parse_dt_it_series(df["dt_arrivo_prog_raw"])

    df["data_riferimento"] = df["_reference_date"]

    df["missing_datetime"] = df["dt_partenza_prog"].isna() | df["dt_arrivo_prog"].isna()
    df["info_mancante"] = df["missing_datetime"]

    # Una sola classificazione vettoriale sulle colonne canoniche. Prima
    # convivevano due implementazioni: quella da config (che non trovava mai
    # le colonne, perche' cercava le intestazioni maiuscole della sorgente) e
    # un fallback hard-coded che sapeva riconoscere solo "cancellato" e
    # "soppresso", per giunta leggendo le cancellazioni parziali come totali.
    df["stato_corsa"] = se.classify_frame(df)

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


def _transform_one(args: Tuple[Dict[str, Any], str, str]) -> Tuple[str, Optional[pd.DataFrame], str]:
    """Worker entry point: read and transform a single bronze day."""
    cfg, csv_gz, meta = args
    try:
        df_s = transform(cfg, read_bronze(csv_gz, meta))
    except RuntimeError as exc:
        return csv_gz, None, str(exc)
    if df_s.empty:
        return csv_gz, None, "no rows after normalization"
    return csv_gz, df_s, ""


def _default_jobs() -> int:
    return max(1, (os.cpu_count() or 2) - 1)


def main(start: str, end: Optional[str] = None, jobs: Optional[int] = None) -> None:
    cfg = load_yaml("config/pipeline.yml")

    d0 = date.fromisoformat(start)
    d1 = date.fromisoformat(end) if end else d0

    files = list_bronze_files_for_range(d0, d1)
    if not files:
        print("no bronze files found for range")
        return

    # Days are independent of one another, and the legacy bronze layout costs
    # ~4s per day just to ast.literal_eval a 12 MB Python-repr payload (its
    # single quotes and embedded apostrophes rule out the faster json parser).
    # A backfill over the full history is therefore worth spreading across
    # cores; the nightly run has one day to do and stays single-process.
    n_jobs = jobs if jobs is not None else _default_jobs()
    n_jobs = max(1, min(n_jobs, len(files)))

    # Group the day files by target month first, then process one month at a
    # time and flush it before moving on. Holding every transformed day in a
    # dict until the very end meant a full 26-month backfill kept ~6.5M rows
    # of silver resident at once, which is more than a CI runner will give us.
    files_by_month: Dict[str, List[Tuple[date, str, str]]] = {}
    for d, csv_gz, meta in files:
        files_by_month.setdefault(f"{d.year:04d}{d.month:02d}", []).append((d, csv_gz, meta))

    skipped: List[str] = []
    updated: List[str] = []

    # One pool for the whole run, not one per month. Under macOS' spawn start
    # method every worker re-imports pandas on creation, so rebuilding the pool
    # for each of 26 months paid that startup cost 26 times over.
    pool = ProcessPoolExecutor(max_workers=n_jobs) if n_jobs > 1 else None

    try:
        for key in sorted(files_by_month):
            tasks = [(cfg, csv_gz, meta) for _d, csv_gz, meta in files_by_month[key]]
            parts: List[pd.DataFrame] = []

            results = pool.map(_transform_one, tasks) if (pool and len(tasks) > 1) else map(_transform_one, tasks)
            for csv_gz, df_s, err in results:
                if err:
                    skipped.append(f"{csv_gz}: {err}")
                else:
                    parts.append(df_s)

            if not parts:
                continue

            p = silver_path_for_month(date(int(key[:4]), int(key[4:]), 1))
            add_df = pd.concat(parts, ignore_index=True)
            del parts
            upsert_month_parquet(p, add_df)
            del add_df
            updated.append(key)
            print(f"  silver {key} scritto", flush=True)
    finally:
        if pool is not None:
            pool.shutdown()

    print({"silver_updated_months": updated, "skipped_files": len(skipped)})
    for msg in skipped[:20]:
        print(f"WARN {msg}")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=False, help="YYYY-MM-DD")
    ap.add_argument("--jobs", type=int, default=None, help="Processi paralleli (default: core-1)")
    args = ap.parse_args()
    main(args.start, args.end, jobs=args.jobs)
