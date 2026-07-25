from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

import requests


BASE_URL = "https://trainstats.altervista.org/exportcsvint.php"
DEFAULT_FIELDS = ",".join(str(i) for i in range(0, 21))


@dataclass(frozen=True)
class RawSpec:
    tipo: str = "treni"
    fields: str = DEFAULT_FIELDS


def _fmt_di_df(d: date) -> str:
    return d.strftime("%d_%m_%Y")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def daterange_inclusive(d0: date, d1: date) -> Iterable[date]:
    cur = d0
    while cur <= d1:
        yield cur
        cur = cur + timedelta(days=1)


def build_url(di: date, df: date, spec: RawSpec) -> str:
    params = {
        "type": spec.tipo,
        "action": "show",
        "di": _fmt_di_df(di),
        "df": _fmt_di_df(df),
        "fields": spec.fields,
    }
    q = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{BASE_URL}?{q}"


def fetch_raw_csv(di: date, df: date, spec: RawSpec, timeout: int = 60) -> bytes:
    url = build_url(di, df, spec)
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    content = r.content
    if not content or len(content) < 10:
        raise ValueError(f"empty response from source for di={di} df={df}")
    return content


def write_raw(di: date, df: date, content: bytes, spec: RawSpec, out_dir: Path) -> Path:
    _ensure_dir(out_dir)

    tag = f"treni_di={di.isoformat()}_df={df.isoformat()}"
    csv_path = out_dir / f"{tag}.csv"
    meta_path = out_dir / f"{tag}.json"

    csv_path.write_bytes(content)

    meta = {
        "type": spec.tipo,
        "fields": spec.fields,
        "di": di.isoformat(),
        "df": df.isoformat(),
        "fetched_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "source_url": build_url(di, df, spec),
        "bytes": len(content),
        "sha256": _sha256_bytes(content),
        "csv_path": str(csv_path).replace("\\", "/"),
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    return csv_path


def main(start: str, end: str, mode: str = "range", timeout: int = 60) -> None:
    d0 = date.fromisoformat(start)
    d1 = date.fromisoformat(end)
    spec = RawSpec()

    out_dir = Path("data") / "bronze" / "raw" / "treni"

    if mode == "day":
        for d in daterange_inclusive(d0, d1):
            content = fetch_raw_csv(d, d, spec, timeout=timeout)
            path = write_raw(d, d, content, spec, out_dir)
            print({"raw_saved": str(path), "di": d.isoformat(), "df": d.isoformat()})
        return

    content = fetch_raw_csv(d0, d1, spec, timeout=timeout)
    path = write_raw(d0, d1, content, spec, out_dir)
    print({"raw_saved": str(path), "di": d0.isoformat(), "df": d1.isoformat()})


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--mode", choices=["range", "day"], default="range")
    ap.add_argument("--timeout", type=int, default=60)
    args = ap.parse_args()

    main(args.start, args.end, mode=args.mode, timeout=args.timeout)
