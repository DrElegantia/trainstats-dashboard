# scripts/compare_gold.py
"""Confronto fra il gold prodotto dalla pipeline corretta e quello di riferimento.

Serve la verifica di credibilita': ogni scostamento rispetto alla dashboard
precedente deve essere spiegabile con uno dei fix applicati, non con una
regressione. Si usa cosi':

    python -m scripts.compare_gold --baseline /path/al/gold_vecchio

Il baseline e' una directory contenente i CSV gold monolitici della vecchia
pipeline (il commit taggato `baseline-gold` del repo).
"""
from __future__ import annotations

import argparse
import os
from typing import Dict, List, Optional

import pandas as pd

from .build_gold import read_gold_table


def _load_baseline(baseline_dir: str, name: str) -> Optional[pd.DataFrame]:
    path = os.path.join(baseline_dir, f"{name}.csv")
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)


def _weighted(df: pd.DataFrame, col: str, weight: str) -> float:
    if col not in df.columns or weight not in df.columns:
        return float("nan")
    w = pd.to_numeric(df[weight], errors="coerce").fillna(0)
    v = pd.to_numeric(df[col], errors="coerce")
    ok = v.notna() & (w > 0)
    if not ok.any():
        return float("nan")
    return float((v[ok] * w[ok]).sum() / w[ok].sum())


def _share(df: pd.DataFrame, num: str, den: str) -> float:
    if num not in df.columns or den not in df.columns:
        return float("nan")
    n = pd.to_numeric(df[num], errors="coerce").fillna(0).sum()
    d = pd.to_numeric(df[den], errors="coerce").fillna(0).sum()
    return float(100.0 * n / d) if d else float("nan")


def summarize(df: pd.DataFrame) -> Dict[str, float]:
    """Indicatori di alto livello confrontabili fra le due versioni."""
    weight = "corse_con_misura" if "corse_con_misura" in df.columns else "corse_osservate"
    return {
        "righe": float(len(df)),
        "corse_osservate": float(pd.to_numeric(df.get("corse_osservate"), errors="coerce").fillna(0).sum()),
        "ritardo_medio": _weighted(df, "ritardo_medio", weight),
        "pct_in_ritardo": _share(df, "in_ritardo", "corse_osservate"),
        "pct_in_anticipo": _share(df, "in_anticipo", "corse_osservate"),
        "pct_soppresse": _share(df, "soppresse", "corse_osservate"),
        "pct_cancellate_tot": _share(df, "cancellate_tot", "corse_osservate"),
        "righe_ritardo_negativo": float((pd.to_numeric(df.get("ritardo_medio"), errors="coerce") < 0).sum()),
    }


def compare_table(name: str, baseline_dir: str) -> Optional[pd.DataFrame]:
    old = _load_baseline(baseline_dir, name)
    if old is None:
        return None
    new = read_gold_table(name)
    if new.empty:
        return None

    rows = []
    for label, df in (("baseline", old), ("nuovo", new)):
        s = summarize(df)
        s["versione"] = label
        rows.append(s)

    out = pd.DataFrame(rows).set_index("versione")
    out.loc["delta"] = out.loc["nuovo"] - out.loc["baseline"]
    return out


def monthly_delta(name: str, baseline_dir: str) -> Optional[pd.DataFrame]:
    """Confronto mese per mese sulle corse osservate: deve restare invariato.

    Il conteggio delle corse non dipende da nessuno dei fix applicati, quindi
    uno scostamento qui segnalerebbe che si e' perso o duplicato dato grezzo.
    """
    old = _load_baseline(baseline_dir, name)
    if old is None:
        return None
    new = read_gold_table(name)
    if new.empty or "mese" not in old.columns:
        return None

    o = old.groupby("mese")["corse_osservate"].sum().rename("baseline")
    n = new.groupby("mese")["corse_osservate"].sum().rename("nuovo")
    cmp = pd.concat([o, n], axis=1).fillna(0)
    cmp["delta"] = cmp["nuovo"] - cmp["baseline"]
    cmp["delta_pct"] = (100 * cmp["delta"] / cmp["baseline"].replace(0, pd.NA)).round(3)
    return cmp


def main(baseline_dir: str, tables: List[str]) -> None:
    pd.set_option("display.width", 200)

    for name in tables:
        cmp = compare_table(name, baseline_dir)
        print(f"\n{'=' * 78}\n{name}\n{'=' * 78}")
        if cmp is None:
            print("  non confrontabile (tabella assente da un lato)")
            continue
        print(cmp.round(3).to_string())

        md = monthly_delta(name, baseline_dir)
        if md is not None:
            drift = md[md["delta"] != 0]
            if drift.empty:
                print(f"\n  corse osservate per mese: identiche su {len(md)} mesi")
            else:
                print(f"\n  ATTENZIONE: corse osservate cambiate su {len(drift)} mesi")
                print(drift.to_string())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True, help="directory con i CSV gold di riferimento")
    ap.add_argument(
        "--tables",
        nargs="*",
        default=["kpi_mese", "kpi_mese_categoria", "od_mese_categoria", "stazioni_mese_categoria_nodo"],
    )
    args = ap.parse_args()
    main(args.baseline, args.tables)
