from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from scripts.utils import load_yaml, today_in_tz


def run(start: date, end: date) -> None:
    import subprocess
    import sys

    def call(args):
        print("running:", " ".join(args))
        r = subprocess.run(args, check=False)
        if r.returncode != 0:
            raise SystemExit(r.returncode)

    call([sys.executable, "scripts/ingest.py", "--start", start.isoformat(), "--end", end.isoformat()])
    call([sys.executable, "scripts/transform_silver.py", "--start", start.isoformat(), "--end", end.isoformat()])
    call([sys.executable, "scripts/build_gold.py"])
    call([sys.executable, "scripts/build_site.py"])


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
