from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from typing import Any, Dict

from .utils import ensure_dir, load_yaml


GOLD_FILES = [
    "kpi_giorno_categoria.csv",
    "kpi_mese_categoria.csv",
    "kpi_giorno.csv",
    "kpi_mese.csv",
    "hist_mese_categoria.csv",
    "od_mese_categoria.csv",
    "stazioni_mese_categoria_ruolo.csv",
    "stazioni_mese_categoria_nodo.csv",
]


def copy_gold_to_site() -> None:
    ensure_dir(os.path.join("site", "data"))
    for fn in GOLD_FILES:
        src = os.path.join("data", "gold", fn)
        dst = os.path.join("site", "data", fn)
        if os.path.exists(src):
            shutil.copyfile(src, dst)


def build_manifest(cfg: Dict[str, Any]) -> None:
    m = {
        "built_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "punctuality": cfg["punctuality"],
        "delay_buckets_minutes": cfg["delay_buckets_minutes"],
        "min_counts": cfg["min_counts"],
        "gold_files": GOLD_FILES,
    }
    with open(os.path.join("site", "data", "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(m, f, ensure_ascii=False, indent=2)


def ensure_demo_if_empty() -> None:
    has_any = any(os.path.exists(os.path.join("data", "gold", fn)) for fn in GOLD_FILES)
    if not has_any:
        from scripts.bootstrap_demo import bootstrap_demo_gold
        bootstrap_demo_gold()


def main() -> None:
    cfg = load_yaml("config/pipeline.yml")
    ensure_demo_if_empty()
    copy_gold_to_site()
    build_manifest(cfg)
    print({"site_data_ready": True})


if __name__ == "__main__":
    main()

