# scripts/build_site.py
from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def build_manifest(gold_dir: Path) -> dict:
    gold_files = sorted([p.name for p in gold_dir.glob("*.csv") if p.is_file()])
    return {
        "built_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "gold_files": gold_files,
    }


def copy_gold_files(gold_dir: Path, target_dir: Path) -> None:
    """Copia i file CSV gold direttamente in docs/data/ (non in una sottocartella)"""
    ensure_dir(target_dir)
    for p in gold_dir.glob("*.csv"):
        if p.is_file():
            shutil.copy2(p, target_dir / p.name)
            print(f"Copied {p.name} -> {target_dir / p.name}")


def copy_root_files(target_dir: Path) -> None:
    """Copia stations_dim.csv e altri file dalla root di data/"""
    ensure_dir(target_dir)
    
    # stations_dim.csv potrebbe essere in data/gold/ o in data/
    stations_paths = [
        Path("data") / "gold" / "stations_dim.csv",
        Path("data") / "stations_dim.csv"
    ]
    
    for src in stations_paths:
        if src.exists():
            shutil.copy2(src, target_dir / "stations_dim.csv")
            print(f"Copied stations_dim.csv from {src}")
            break
    
    # Capoluoghi provincia (supporta sia data/ che data/stations/)
    capoluoghi_paths = [
        Path("data") / "capoluoghi_provincia.csv",
        Path("data") / "stations" / "capoluoghi_provincia.csv",
    ]
    for capoluoghi in capoluoghi_paths:
        if capoluoghi.exists():
            shutil.copy2(capoluoghi, target_dir / "capoluoghi_provincia.csv")
            print(f"Copied capoluoghi_provincia.csv from {capoluoghi}")
            break


def write_manifest(target_dir: Path, manifest: dict) -> None:
    ensure_dir(target_dir)
    manifest_path = target_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Wrote manifest to {manifest_path}")


def main() -> None:
    gold_dir = Path("data") / "gold"
    if not gold_dir.is_dir():
        raise SystemExit("missing data/gold, run scripts.build_gold first")

    manifest = build_manifest(gold_dir)
    
    # Target principale: docs/data
    target = Path("docs") / "data"
    
    copy_gold_files(gold_dir, target)
    copy_root_files(target)
    write_manifest(target, manifest)

    print(
        json.dumps(
            {
                "target": str(target),
                "gold_dir": str(gold_dir),
                "manifest": manifest,
            },
            ensure_ascii=False,
            indent=2
        )
    )


if __name__ == "__main__":
    main()
