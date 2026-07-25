# scripts/measure_payload.py
"""Quanto deve scaricare e parsificare il browser, per scenario d'uso.

Il costo dominante della dashboard non e' la rete ma il parsing: parseCSVAsync
costruisce un oggetto JavaScript per riga, con una property stringa per colonna.
Le metriche che contano sono quindi i byte non compressi e il numero di righe.

    python -m scripts.measure_payload                      # solo il nuovo
    python -m scripts.measure_payload --confronta DIR      # nuovo vs vecchio
"""
from __future__ import annotations

import argparse
import gzip
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Cosa carica la dashboard in ciascuno scenario, secondo la logica di
# ensureDataForCurrentFilters() in docs/assets/app.js.
SCENARIOS: Dict[str, List[str]] = {
    "avvio (nessun filtro)": [
        "kpi_mese.csv",
        "kpi_mese_categoria.csv",
        "hist_mese_categoria.csv",
        "stations_dim.csv",
        "capoluoghi_provincia.csv",
        "station_names.csv",
    ],
    "filtro stazione": [
        "od_mese_categoria.csv",
        "hist_stazioni_mese_categoria_ruolo.csv",
    ],
    "filtro dettaglio": [
        "kpi_dettaglio_categoria.csv",
        "hist_dettaglio_categoria.csv",
        "stazioni_dettaglio_categoria_nodo.csv",
    ],
    "stazione + dettaglio": [
        "od_mese_categoria.csv",
        "hist_stazioni_mese_categoria_ruolo.csv",
        "kpi_dettaglio_categoria.csv",
        "hist_dettaglio_categoria.csv",
        "stazioni_dettaglio_categoria_nodo.csv",
        "od_dettaglio_categoria.csv",
        "hist_stazioni_dettaglio_categoria_ruolo.csv",
    ],
}


def _resolve(base: Path, name: str, year: Optional[str]) -> Optional[Path]:
    """Preferisce lo shard annuale, come fa shardCandidates() nel JS."""
    if year:
        shard = base / name.replace(".csv", f".{year}.csv")
        if shard.exists():
            return shard
    p = base / name
    return p if p.exists() else None


def _measure(path: Path) -> Tuple[int, int, int]:
    raw = path.read_bytes()
    gz = len(gzip.compress(raw, compresslevel=6))
    rows = raw.count(b"\n")
    return len(raw), gz, max(0, rows - 1)


def report(base: Path, year: Optional[str], label: str) -> Dict[str, Tuple[int, int, int]]:
    print(f"\n### {label}" + (f"  (anno selezionato: {year})" if year else "  (tutta la storia)"))
    print(f"{'scenario':<26} {'file':>5} {'MB grezzi':>11} {'MB gzip':>9} {'righe':>12}")
    print("-" * 68)

    totals: Dict[str, Tuple[int, int, int]] = {}
    for scenario, files in SCENARIOS.items():
        raw = gz = rows = n = 0
        for f in files:
            p = _resolve(base, f, year)
            if p is None:
                continue
            r, g, rw = _measure(p)
            raw += r
            gz += g
            rows += rw
            n += 1
        totals[scenario] = (raw, gz, rows)
        print(f"{scenario:<26} {n:>5} {raw / 1e6:>11.1f} {gz / 1e6:>9.1f} {rows:>12,}")
    return totals


def main(base_dir: str, year: Optional[str], confronta: Optional[str]) -> None:
    base = Path(base_dir)
    if not base.is_dir():
        raise SystemExit(f"directory non trovata: {base}")

    new = report(base, year, "Payload attuale")

    if not confronta:
        return

    old_base = Path(confronta)
    if not old_base.is_dir():
        raise SystemExit(f"directory di confronto non trovata: {old_base}")

    # Il baseline non ha shard annuali ne' station_names.csv: si misura com'e'.
    old = report(old_base, None, "Payload baseline")

    print("\n### Variazione")
    print(f"{'scenario':<26} {'MB grezzi':>22} {'righe parsificate':>26}")
    print("-" * 78)
    for scenario in SCENARIOS:
        o_raw, _o_gz, o_rows = old.get(scenario, (0, 0, 0))
        n_raw, _n_gz, n_rows = new.get(scenario, (0, 0, 0))
        d_raw = f"{o_raw / 1e6:.1f} -> {n_raw / 1e6:.1f}"
        d_rows = f"{o_rows:,} -> {n_rows:,}"
        pct = f"{(n_raw - o_raw) / o_raw * 100:+.0f}%" if o_raw else "n/d"
        print(f"{scenario:<26} {d_raw:>16} {pct:>5} {d_rows:>26}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.join("docs", "data"))
    ap.add_argument("--anno", default=None, help="anno selezionato nella dashboard (es. 2026)")
    ap.add_argument("--confronta", default=None, help="directory con i CSV della versione precedente")
    args = ap.parse_args()
    main(args.dir, args.anno, args.confronta)
