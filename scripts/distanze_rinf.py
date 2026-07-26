# scripts/distanze_rinf.py
"""Distanze fra stazioni dal RINF, il registro ufficiale dell'infrastruttura.

RINF e' il Register of Infrastructure previsto dal regolamento europeo
2019/777: ogni gestore vi dichiara la propria rete divisa in *section of line*,
cioe' tratte fra due punti operativi adiacenti, ognuna con il parametro
`lengthOfSectionOfLine` in chilometri. E' la fonte autorevole per la domanda
"quanto e' lunga questa tratta", e vale per tutta l'infrastruttura italiana, non
solo per quella di RFI.

Le sezioni sono esattamente i lati di un grafo: la distanza fra due stazioni
qualsiasi e' il cammino minimo su quei lati. A differenza del grafo OpenStreetMap
qui non ci sono raccordi merci o scorciatoie mappate come linea, quindi i
cammini non tagliano dove il treno non passa.

Una avvertenza che ERA mette per iscritto: la lunghezza della sezione e' la
distanza teorica fra i punti centrali dei due punti operativi, scelta come valore
medio dei binari presenti. Non e' quindi la sottrazione fra due progressive
chilometriche, che sono un'altra grandezza e stanno nei Fascicoli Linea di RFI.
Per una tabella origine-destinazione va bene la prima.

    python -m scripts.distanze_rinf --scarica    # interroga l'endpoint SPARQL
    python -m scripts.distanze_rinf              # calcola le distanze
"""
from __future__ import annotations

import argparse
import csv
import heapq
import json
import os
import statistics
import urllib.request
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from .utils import normalize_station_name

ENDPOINT = "https://rinf.data.era.europa.eu/api/sparql"
CACHE = os.path.join("data", "stations", "rinf_sezioni.json")
USCITA = os.path.join("data", "stations", "distanze_rinf.csv")

QUERY = """
PREFIX era: <http://data.europa.eu/949/>
SELECT ?nomeA ?nomeB ?km WHERE {
  ?s a era:SectionOfLine ;
     era:inCountry <http://publications.europa.eu/resource/authority/country/ITA> ;
     era:lengthOfSectionOfLine ?km ;
     era:opStart ?a ;
     era:opEnd ?b .
  ?a era:opName ?nomeA .
  ?b era:opName ?nomeB .
}
"""


def scarica(destinazione: str = CACHE) -> int:
    req = urllib.request.Request(
        ENDPOINT, data=QUERY.encode("utf-8"),
        headers={"Content-Type": "application/sparql-query",
                 "Accept": "application/sparql-results+json",
                 "User-Agent": "trainstats-lab/1.0 (distanze tratte)"})
    with urllib.request.urlopen(req, timeout=300) as r:
        risposta = json.load(r)

    sezioni = []
    for b in risposta["results"]["bindings"]:
        try:
            sezioni.append({"a": b["nomeA"]["value"], "b": b["nomeB"]["value"],
                            "km": float(b["km"]["value"])})
        except (KeyError, ValueError):
            continue
    os.makedirs(os.path.dirname(destinazione), exist_ok=True)
    with open(destinazione, "w", encoding="utf-8") as f:
        json.dump(sezioni, f, ensure_ascii=False)
    return len(sezioni)


def lati(percorso: str = CACHE) -> Dict[Tuple[str, str], float]:
    """Sezioni di linea come lati del grafo, per nome normalizzato."""
    with open(percorso, encoding="utf-8") as f:
        sezioni = json.load(f)
    per_coppia: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for s in sezioni:
        a, b = normalize_station_name(s["a"]), normalize_station_name(s["b"])
        if not a or not b or a == b or not (0 < s["km"] < 400):
            continue
        per_coppia[(a, b) if a < b else (b, a)].append(s["km"])
    # Fra due punti operativi possono esserci piu' sezioni dichiarate (binari
    # diversi, gestori diversi): si prende la mediana, non la somma.
    return {k: round(statistics.median(v), 3) for k, v in per_coppia.items()}


def _dijkstra(grafo, partenza, obiettivi: set, limite_km: float = 1800.0):
    dist = {partenza: 0.0}
    coda = [(0.0, partenza)]
    trovati: Dict[str, float] = {}
    restanti = set(obiettivi)
    while coda and restanti:
        d, n = heapq.heappop(coda)
        if d > dist.get(n, float("inf")):
            continue
        if n in restanti:
            trovati[n] = d
            restanti.discard(n)
        if d > limite_km:
            continue
        for v, w in grafo.get(n, ()):
            nd = d + w
            if nd < dist.get(v, float("inf")):
                dist[v] = nd
                heapq.heappush(coda, (nd, v))
    return trovati


def main(scarica_ora: bool) -> None:
    if scarica_ora or not os.path.exists(CACHE):
        print("interrogo il RINF...")
        print(f"sezioni di linea italiane: {scarica():,}")

    l = lati()
    print(f"\nlati del grafo (coppie di punti operativi adiacenti): {len(l):,}")
    nodi = {s for k in l for s in k}
    print(f"punti operativi distinti: {len(nodi):,}")
    print(f"lunghezza sezioni: mediana {statistics.median(l.values()):.2f} km, "
          f"massima {max(l.values()):.1f} km")

    grafo: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
    for (a, b), km in l.items():
        grafo[a].append((b, km))
        grafo[b].append((a, km))

    from .distanze_gtfs import coppie_da_gold
    coppie = coppie_da_gold()
    print(f"\ncoppie da coprire: {len(coppie):,}")

    per_partenza: Dict[str, set] = defaultdict(set)
    for a, b in coppie:
        if a in grafo and b in grafo:
            per_partenza[a].add(b)

    risultati: Dict[Tuple[str, str], float] = {}
    for i, (a, obiettivi) in enumerate(sorted(per_partenza.items()), 1):
        for b, d in _dijkstra(grafo, a, obiettivi).items():
            risultati[(a, b)] = round(d, 3)
        if i % 200 == 0:
            print(f"  {i}/{len(per_partenza)} partenze, {len(risultati):,} tratte")

    print(f"\ncoperte: {len(risultati):,} su {len(coppie):,} "
          f"({len(risultati)/max(len(coppie),1):.1%})")

    os.makedirs(os.path.dirname(USCITA), exist_ok=True)
    with open(USCITA, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["partenza", "arrivo", "km"])
        for (a, b), d in sorted(risultati.items()):
            w.writerow([a, b, d])
    print(f"scritto {USCITA}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scarica", action="store_true")
    args = ap.parse_args()
    main(args.scarica)
