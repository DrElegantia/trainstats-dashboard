from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any, Dict, Optional

from .utils import (
    ensure_dir,
    format_di_df,
    http_get_with_retry,
    load_json,
    load_yaml,
    read_csv_header_bytes,
    validate_header,
    write_gzip_bytes,
    write_json,
)


def build_url(cfg: Dict[str, Any], d: date) -> str:
    base = cfg["project"]["source_base_url"]
    di = format_di_df(d)
    df = format_di_df(d)
    fields = cfg["project"]["fields"]
    t = cfg["project"]["source_type"]
    return f"{base}?type={t}&action=show&di={di}&df={df}&fields={fields}"


def bronze_paths(d: date) -> Dict[str, str]:
    y = f"{d.year:04d}"
    m = f"{d.month:02d}"
    dd = f"{d.day:02d}"
    root = os.path.join("data", "bronze", y, m)
    ensure_dir(root)
    return {
        "csv_gz": os.path.join(root, f"{y}{m}{dd}.csv.gz"),
        "meta": os.path.join(root, f"{y}{m}{dd}.meta.json"),
    }


def ingest_one(d: date, cfg: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    url = build_url(cfg, d)
    http_cfg = cfg["http"]
    r = http_get_with_retry(
        url,
        timeout=int(http_cfg["timeout_seconds"]),
        max_retries=int(http_cfg["max_retries"]),
        backoff_factor=int(http_cfg["backoff_factor_seconds"]),
    )
    content = r.content

    header = read_csv_header_bytes(content)
    validate_header(header, schema)

    paths = bronze_paths(d)
    write_gzip_bytes(paths["csv_gz"], content)

    meta = {
        "reference_date": d.isoformat(),
        "extracted_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source_url": url,
        "bytes": len(content),
        "header": header,
    }
    write_json(paths["meta"], meta)
    return {"date": d.isoformat(), "path": paths["csv_gz"]}


def main(start: str, end: Optional[str] = None) -> None:
    cfg = load_yaml("config/pipeline.yml")
    schema = load_json("config/schema_expected.json")

    d0 = date.fromisoformat(start)
    d1 = date.fromisoformat(end) if end else d0

    from scripts.utils import date_range_inclusive

    results = []
    for d in date_range_inclusive(d0, d1):
        results.append(ingest_one(d, cfg, schema))

    print({"ingested": results})


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=False, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.start, args.end)

