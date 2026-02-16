from __future__ import annotations

import os
import glob
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

from .utils import ensure_dir, load_yaml


GOLD_DIR = os.path.join("data", "gold")
SITE_DATA_DIR = os.path.join("site", "data")


def _list_gold_csv_basenames() -> List[str]:
    paths = glob.glob(os.path.join(GOLD_DIR, "*.csv"))
    names = sorted([os.path.basename(p) for p in paths if os.path.isfile(p)])
    return names


def _copy(src: str, dst: str) -> None:
    ensure_dir(os.path.dirname(dst))
    with open(src, "rb") as fsrc, open(dst, "wb") as fdst:
        fdst.write(fsrc.read())


def main() -> None:
    cfg: Dict[str, Any] = load_yaml("config/pipeline.yml")

    ensure_dir(SITE_DATA_DIR)

    # Dimensioni stazioni e capoluoghi
    try:
        from .build_station_dim import main as build_station_dim_main
        build_station_dim_main()
    except Exception as e:
        print({"warning": "build_station_dim_failed", "error": str(e)})

    # Copia tutti i CSV gold
    gold_files = _list_gold_csv_basenames()
    if not gold_files:
        raise ValueError("no gold csv files found to publish")

    for f in gold_files:
        _copy(os.path.join(GOLD_DIR, f), os.path.join(SITE_DATA_DIR, f))

    # Copia dimensioni se presenti
    dim_src = os.path.join("site", "data", "stations_dim.csv")
    if os.path.exists(dim_src):
        _copy(dim_src, os.path.join(SITE_DATA_DIR, "stations_dim.csv"))

    cap_src = os.path.join("site", "data", "capoluoghi_provincia.csv")
    if os.path.exists(cap_src):
        _copy(cap_src, os.path.join(SITE_DATA_DIR, "capoluoghi_provincia.csv"))

    # Manifest
    built_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    delay_buckets = cfg.get("delay_buckets_minutes", {})
    labels = delay_buckets.get("labels", [])
    if isinstance(labels, list):
        labels = [str(x).replace(" ", "").strip() for x in labels]

    manifest = {
        "built_at_utc": built_at_utc,
        "gold_files": gold_files,
        "punctuality": cfg.get("punctuality", {"on_time_threshold_minutes": 5}),
        "delay_buckets_minutes": {"labels": labels},
        "min_counts": cfg.get("min_counts", {"leaderboard_min_trains": 20}),
    }

    with open(os.path.join(SITE_DATA_DIR, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print({"published_files": len(gold_files), "manifest_written": True, "built_at_utc": built_at_utc})


if __name__ == "__main__":
    main()
