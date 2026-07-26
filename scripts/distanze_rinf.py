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
SELECT ?a ?b ?nomeA ?nomeB ?km WHERE {
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
                            # L'identificativo del punto operativo, che il nome
                            # non sostituisce: in Italia ci sono tre "San Paolo"
                            # e chiamarli tutti allo stesso modo salda pezzi di
                            # rete che non si toccano.
                            "ida": b["a"]["value"], "idb": b["b"]["value"],
                            "km": float(b["km"]["value"])})
        except (KeyError, ValueError):
            continue
    os.makedirs(os.path.dirname(destinazione), exist_ok=True)
    with open(destinazione, "w", encoding="utf-8") as f:
        json.dump(sezioni, f, ensure_ascii=False)
    return len(sezioni)


def lati(percorso: str = CACHE) -> Dict[Tuple[str, str], float]:
    """Sezioni di linea come lati del grafo, per identificativo del punto operativo.

    Il grafo si costruisce sugli identificativi, non sui nomi. Costruirlo sui
    nomi normalizzati produceva saldature fra pezzi di rete che non si toccano:
    esistono un San Paolo vicino a Messina, un San Paolo fra Noto e Rosolini e
    un San Paolo vicino a Taranto, e il nome li fondeva in un unico nodo. Il
    risultato era che la Sicilia risultava collegata alla Puglia via terra, e il
    cammino minimo Messina-Roma passava per Taranto senza attraversare lo
    Stretto: 564 km invece dei circa 900 reali.
    """
    with open(percorso, encoding="utf-8") as f:
        sezioni = json.load(f)
    per_coppia: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for s in sezioni:
        a, b = s.get("ida"), s.get("idb")
        if not a or not b:
            # Cache vecchia, senza identificativi: si ricade sui nomi ed e'
            # meglio dirlo che far finta di niente.
            a, b = normalize_station_name(s["a"]), normalize_station_name(s["b"])
        if not a or not b or a == b or not (0 < s["km"] < 400):
            continue
        per_coppia[(a, b) if a < b else (b, a)].append(s["km"])
    # Fra due punti operativi possono esserci piu' sezioni dichiarate (binari
    # diversi, gestori diversi): si prende la mediana, non la somma.
    fuori = {k: round(statistics.median(v), 3) for k, v in per_coppia.items()}
    return _aggiungi_stretto(fuori, percorso)


# Villa San Giovanni - Messina secondo il prontuario delle distanze FS. Il RINF
# non ha nulla qui, ed e' corretto: lo Stretto non e' una sezione di linea, i
# treni ci passano sopra caricati sulla nave. Senza questo lato la Sicilia e'
# una rete a se' e ogni collegamento con il continente resta senza distanza,
# compresi i quasi 3.500 Roma-Palermo che ci sono nei dati.
_STRETTO_KM = 9.0
_STRETTO = ("MESSINA CENTRALE", "VILLA S GIOVANNI")


def _aggiungi_stretto(lati_: Dict[Tuple[str, str], float], percorso: str
                      ) -> Dict[Tuple[str, str], float]:
    per_id = nomi_per_id(percorso)
    nodi = {s for k in lati_ for s in k}
    capi = []
    for nome in _STRETTO:
        candidati = [i for i, n in per_id.items() if n == nome and i in nodi]
        if len(candidati) != 1:
            # Se il capo e' ambiguo o assente si lascia il grafo com'e': meglio
            # nessuna distanza che una attaccata al nodo sbagliato.
            return lati_
        capi.append(candidati[0])
    a, b = capi
    lati_[(a, b) if a < b else (b, a)] = _STRETTO_KM
    return lati_


def nomi_per_id(percorso: str = CACHE) -> Dict[str, str]:
    """Identificativo del punto operativo -> nome normalizzato."""
    with open(percorso, encoding="utf-8") as f:
        sezioni = json.load(f)
    fuori: Dict[str, str] = {}
    for s in sezioni:
        for chiave, nome in (("ida", "a"), ("idb", "b")):
            i = s.get(chiave)
            if i:
                fuori[i] = normalize_station_name(s[nome])
    return fuori


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

    # Il grafo parla per identificativi, le nostre tratte per nomi: qui si
    # ricuce. Un nome che corrisponde a piu' punti operativi (i tre San Paolo,
    # Varese e Varese Nord) genera piu' partenze e si tiene la distanza minima
    # fra le combinazioni, invece di fondere i nodi come faceva prima.
    per_id = nomi_per_id()
    id_per_nome: Dict[str, list] = defaultdict(list)
    for i, n in per_id.items():
        if n and i in grafo:
            id_per_nome[n].append(i)
    multipli = sum(1 for v in id_per_nome.values() if len(v) > 1)
    print(f"nomi che corrispondono a piu' punti operativi: {multipli}")

    from .distanze_gtfs import coppie_da_gold
    coppie = coppie_da_gold()
    print(f"\ncoppie da coprire: {len(coppie):,}")

    obiettivi_per_nome: Dict[str, set] = defaultdict(set)
    for a, b in coppie:
        if a in id_per_nome and b in id_per_nome:
            obiettivi_per_nome[a].add(b)

    risultati: Dict[Tuple[str, str], float] = {}
    for i, (a, nomi_obiettivo) in enumerate(sorted(obiettivi_per_nome.items()), 1):
        bersagli = {x for n in nomi_obiettivo for x in id_per_nome[n]}
        for partenza in id_per_nome[a]:
            for idb, d in _dijkstra(grafo, partenza, bersagli).items():
                b = per_id.get(idb)
                if not b or b == a:
                    continue
                if d < risultati.get((a, b), float("inf")):
                    risultati[(a, b)] = round(d, 3)
        if i % 200 == 0:
            print(f"  {i}/{len(obiettivi_per_nome)} partenze, {len(risultati):,} tratte")

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
