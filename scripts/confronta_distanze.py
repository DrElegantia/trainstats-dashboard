# scripts/confronta_distanze.py
"""Misura quanto vale la distanza calcolata su OSM, confrontandola coi GTFS.

Le due fonti sono indipendenti e hanno difetti opposti. I GTFS pubblicano la
distanza percorsa dichiarata dal gestore, quindi sono la verita' di riferimento,
ma coprono un quinto delle tratte. OSM copre molto di piu' ma la distanza e'
ricostruita da noi, come cammino minimo su un grafo di binari, e un cammino
minimo non e' necessariamente il percorso che il treno fa: puo' tagliare per una
linea secondaria dove il servizio passa da quella principale.

Sulle tratte che entrambe coprono lo scarto si puo' misurare, e da quello si
capisce se il dato OSM regge anche dove i GTFS non arrivano. Senza questo
confronto la classifica al chilometro sarebbe costruita su numeri di cui non
sappiamo nulla.

    python -m scripts.confronta_distanze
"""
from __future__ import annotations

import csv
import os
import statistics
from typing import Dict, Tuple

GTFS = os.path.join("data", "stations", "distanze_tratte.csv")
OSM = os.path.join("data", "stations", "distanze_osm.csv")


def _leggi(percorso: str, colonna_km: str = "km") -> Dict[Tuple[str, str], float]:
    if not os.path.exists(percorso):
        return {}
    out = {}
    with open(percorso, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            try:
                out[(r["partenza"], r["arrivo"])] = float(r[colonna_km])
            except (KeyError, TypeError, ValueError):
                continue
    return out


def main() -> None:
    g = _leggi(GTFS)
    o = _leggi(OSM)
    print(f"tratte con distanza dai GTFS: {len(g):,}")
    print(f"tratte con distanza da OSM:   {len(o):,}")

    comuni = sorted(set(g) & set(o))
    print(f"coperte da entrambe:          {len(comuni):,}")
    if not comuni:
        print("\nnessuna tratta in comune: impossibile misurare lo scarto")
        return

    scarti = []
    relativi = []
    for k in comuni:
        d = o[k] - g[k]
        scarti.append(d)
        if g[k] > 0:
            relativi.append(abs(d) / g[k] * 100)

    relativi.sort()
    def pct(q: float) -> float:
        i = min(len(relativi) - 1, max(0, int(q * (len(relativi) - 1))))
        return relativi[i]

    print("\nscarto di OSM rispetto ai GTFS, in percentuale della distanza vera:")
    print(f"  mediano:      {statistics.median(relativi):6.2f}%")
    print(f"  medio:        {statistics.fmean(relativi):6.2f}%")
    for q in (0.75, 0.90, 0.95, 0.99):
        print(f"  {int(q*100)}esimo perc.: {pct(q):6.2f}%")
    entro = lambda s: sum(1 for r in relativi if r <= s) / len(relativi) * 100
    print(f"\n  entro il  2%: {entro(2):5.1f}% delle tratte")
    print(f"  entro il  5%: {entro(5):5.1f}%")
    print(f"  entro il 10%: {entro(10):5.1f}%")
    print(f"  entro il 20%: {entro(20):5.1f}%")

    # Il segno dice se il cammino minimo taglia rispetto al percorso reale:
    # se OSM e' sistematicamente piu' corto, sta usando scorciatoie.
    sotto = sum(1 for d in scarti if d < 0)
    print(f"\n  OSM piu' corto dei GTFS: {sotto/len(scarti):.1%} delle tratte")
    print(f"  scarto mediano con segno: {statistics.median(scarti):+.2f} km")

    peggiori = sorted(comuni, key=lambda k: -abs(o[k] - g[k]))[:10]
    print("\nle 10 tratte con lo scarto assoluto piu' grande:")
    for k in peggiori:
        print(f"  {k[0][:24]:26} -> {k[1][:22]:24} GTFS {g[k]:7.1f} km   OSM {o[k]:7.1f} km")


if __name__ == "__main__":
    main()
