from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any, Dict, Optional, Tuple

from .utils import (
    date_range_inclusive,
    ensure_dir,
    http_get_with_retry,
    load_json,
    load_yaml,
    read_csv_header_bytes,
    validate_header,
    write_gzip_bytes,
    write_json,
)


def format_di_df(d: date) -> str:
    return d.strftime("%d_%m_%Y")


def build_url(cfg: Dict[str, Any], di: date, df: date) -> str:
    base = cfg["project"]["source_base_url"]
    fields = cfg["project"]["fields"]
    t = cfg["project"]["source_type"]
    di_s = format_di_df(di)
    df_s = format_di_df(df)
    return f"{base}?type={t}&action=show&di={di_s}&df={df_s}&fields={fields}"


def bronze_paths(di: date, df: date) -> Dict[str, str]:
    y = f"{di.year:04d}"
    m = f"{di.month:02d}"
    dd = f"{di.day:02d}"
    root = os.path.join("data", "bronze", y, m)
    ensure_dir(root)

    if di == df:
        tag = f"{y}{m}{dd}"
    else:
        tag = f"{y}{m}{dd}_to_{df.year:04d}{df.month:02d}{df.day:02d}"

    return {
        "csv_gz": os.path.join(root, f"{tag}.csv.gz"),
        "meta": os.path.join(root, f"{tag}.meta.json"),
    }


def infer_date_mode(cfg: Dict[str, Any]) -> str:
    mode = str(cfg.get("project", {}).get("ingest_mode", "")).strip().lower()
    if mode in {"day", "range"}:
        return mode
    return "day"


def fetch_and_validate(url: str, http_cfg: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bytes, str]:
    r = http_get_with_retry(
        url,
        timeout=int(http_cfg["timeout_seconds"]),
        max_retries=int(http_cfg["max_retries"]),
        backoff_factor=int(http_cfg["backoff_factor_seconds"]),
    )
    content = r.content
    header = read_csv_header_bytes(content)
    validate_header(header, schema)
    return content, header


def ingest_one_day(d: date, cfg: Dict[str, Any], schema: Dict[str, Any]) -> None:
    http_cfg = cfg["http"]
    url = build_url(cfg, d, d)
    content, header = fetch_and_validate(url, http_cfg, schema)

    paths = bronze_paths(d, d)
    write_gzip_bytes(paths["csv_gz"], content)

    meta = {
        "reference_date": d.isoformat(),
        "range_start": d.isoformat(),
        "range_end": d.isoformat(),
        "extracted_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source_url": url,
        "bytes": len(content),
        "header": header,
        "mode": "day",
    }
    write_json(paths["meta"], meta)


def ingest_range(d0: date, d1: date, cfg: Dict[str, Any], schema: Dict[str, Any]) -> None:
    http_cfg = cfg["http"]
    url = build_url(cfg, d0, d1)
    content, header = fetch_and_validate(url, http_cfg, schema)

    paths = bronze_paths(d0, d1)
    write_gzip_bytes(paths["csv_gz"], content)

    meta = {
        "reference_date": d0.isoformat(),
        "range_start": d0.isoformat(),
        "range_end": d1.isoformat(),
        "extracted_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source_url": url,
        "bytes": len(content),
        "header": header,
        "mode": "range",
    }
    write_json(paths["meta"], meta)


def main(start: str, end: Optional[str] = None) -> None:
    cfg = load_yaml("config/pipeline.yml")
    schema = load_json("config/schema_expected.json")

    d0 = date.fromisoformat(start)
    d1 = date.fromisoformat(end) if end else d0

    if d1 < d0:
        raise ValueError("end date must be >= start date")

    mode = infer_date_mode(cfg)

    if mode == "range" and d0 != d1:
        ingest_range(d0, d1, cfg, schema)
        print(f"bronze updated for {d0} to {d1} (mode=range)")
        return

    for d in date_range_inclusive(d0, d1):
        ingest_one_day(d, cfg, schema)

    print(f"bronze updated for {d0} to {d1} (mode=day)")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=False, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.start, args.end)
