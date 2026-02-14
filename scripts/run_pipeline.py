from __future__ import annotations

import subprocess
import sys
from datetime import date, timedelta
from typing import Optional

from .utils import load_yaml, today_in_tz


def call(cmd: list[str]) -> None:
    print(f"\n>>> Running: {' '.join(cmd)}\n", flush=True)
    r = subprocess.run(cmd, check=False)
    if r.returncode != 0:
        raise SystemExit(r.returncode)


def run(start: date, end: date) -> None:
    s = start.isoformat()
    e = end.isoformat()

    call([sys.executable, "-m", "scripts.ingest", "--start", s, "--end", e])
    call([sys.executable, "-m", "scripts.transform_silver", "--start", s, "--end", e])
    call([sys.executable, "-m", "scripts.build_gold"])
    call([sys.executable, "-m", "scripts.build_site"])


def main(start: Optional[str], end: Optional[str]) -> None:
    cfg = load_yaml("config/pipeline.yml")
    tz_name = cfg["project"]["timezone"]

    if start:
        d0 = date.fromisoformat(start)
        d1 = date.fromisoformat(end) if end else d0
    else:
        today = today_in_tz(tz_name)
        d0 = today - timedelta(days=1)
        d1 = d0

    run(d0, d1)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=False, help="YYYY-MM-DD")
    ap.add_argument("--end", required=False, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.start, args.end)
