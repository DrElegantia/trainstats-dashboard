# scripts/distanze_osm.py
"""Lunghezza reale di ogni tratta, calcolata sulla rete ferroviaria di OSM.

I feed GTFS danno i chilometri esatti, ma coprono un quinto delle tratte:
Trenord pubblica 4.358 corse ferroviarie senza nessuna geometria, e 62 feed
italiani su 75 non hanno servizi su ferro. Per avere un dato su *ogni* tratta
serve la rete, non gli orari.

Qui si scarica da OpenStreetMap la geometria dei binari, si costruisce un grafo
dove i lati sono i segmenti di binario con la loro lunghezza, si agganciano le
nostre stazioni al nodo piu' vicino e si calcola il cammino minimo. Il risultato
e' la distanza che il treno percorre davvero, non quella in linea d'aria, che su
una rete che segue le valli sarebbe sistematicamente sottostimata.

L'accuratezza non si dichiara, si misura: le tratte che i GTFS coprono servono
da banco di prova, e il confronto e' nel rapporto finale.

    python -m scripts.distanze_osm --scarica    # scarica i binari (lento, ~10 min)
    python -m scripts.distanze_osm              # calcola le distanze
"""
from __future__ import annotations

import argparse
import csv
import heapq
import json
import math
import os
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

from .utils import normalize_station_name

CACHE_BINARI = os.path.join("data", "stations", "osm_binari.json")
USCITA = os.path.join("data", "stations", "distanze_osm.csv")
# Overpass principale piu' i mirror pubblici. L'istanza principale risponde 504
# sui riquadri densi quando e' sotto carico, e su quattordici riquadri ne
# fallivano sette: senza alternative meta' del nord restava fuori. Si prova il
# riquadro su ogni istanza prima di dichiararlo perso.
ISTANZE = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.osm.ch/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]

# Si tengono i binari su cui circolano i treni del servizio viaggiatori.
# Tram e metropolitane sono reti separate: includerle creerebbe scorciatoie
# inesistenti fra stazioni ferroviarie che si trovano vicine a una fermata tram.
BINARI = "^(rail|narrow_gauge)$"

# L'Italia a riquadri: una sola richiesta su tutto il paese fa scadere Overpass.
#
# Il nord e' spezzato piu' fine del sud perche' e' dove la rete e' piu' densa: i
# due riquadri che coprivano 44-47,2 di latitudine su 6-14 di longitudine
# fallivano entrambi con un timeout, e siccome il download proseguiva senza
# fermarsi mancava tutto il nord-ovest. Il risultato era una rete in cui 917
# stazioni su 1.807 non avevano un binario entro cinquanta chilometri, e nessun
# messaggio diceva che meta' del paese non era stata scaricata.
STRISCE = [
    (35.0, 6.0, 39.5, 19.0),
    (39.5, 6.0, 42.0, 19.0),
    (42.0, 6.0, 44.0, 19.0),
    (44.0, 14.0, 47.2, 19.0),
    # nord-ovest, in riquadri piccoli
    (44.0, 6.0, 45.2, 8.5),
    (44.0, 8.5, 45.2, 10.5),
    (44.0, 10.5, 45.2, 12.5),
    (44.0, 12.5, 45.2, 14.0),
    (45.2, 6.0, 46.2, 8.5),
    (45.2, 8.5, 46.2, 10.5),
    (45.2, 10.5, 46.2, 12.5),
    (45.2, 12.5, 46.2, 14.0),
    (46.2, 6.0, 47.2, 10.0),
    (46.2, 10.0, 47.2, 14.0),
]

_Q = """
[out:json][timeout:300];
way["railway"~"%s"]["service"!~"^(siding|spur|yard|crossover)$"](%s);
out geom;
"""


def _salva(tratti: List[List[Tuple[float, float]]], destinazione: str) -> None:
    os.makedirs(os.path.dirname(destinazione), exist_ok=True)
    tmp = destinazione + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(tratti, f)
    os.replace(tmp, destinazione)


def scarica_binari(destinazione: str = CACHE_BINARI) -> int:
    # Si riparte da quanto e' gia' in cache: se un riquadro fallisce non si
    # perde il resto, e un secondo giro completa invece di ricominciare.
    tratti: List[List[Tuple[float, float]]] = []
    if os.path.exists(destinazione):
        try:
            with open(destinazione, encoding="utf-8") as f:
                tratti = [[tuple(p) for p in t] for t in json.load(f)]
            print(f"  in cache: {len(tratti):,} tratti")
        except Exception:
            tratti = []
    visti = {(t[0], t[-1], len(t)) for t in tratti}
    falliti = []
    for i, bbox in enumerate(STRISCE, 1):
        q = _Q % (BINARI, "%s,%s,%s,%s" % bbox)
        dati = urllib.parse.urlencode({"data": q}).encode()
        risposta = None
        ultimo = ""
        for tentativo, url in enumerate(ISTANZE, 1):
            req = urllib.request.Request(
                url, data=dati,
                headers={"User-Agent": "trainstats-lab/1.0 (distanze tratte)"})
            try:
                with urllib.request.urlopen(req, timeout=420) as r:
                    risposta = json.load(r)
                break
            except Exception as e:
                ultimo = str(e)[:50]
                time.sleep(4 * tentativo)
        if risposta is None:
            print(f"  riquadro {i}: FALLITO su tutte le istanze ({ultimo})")
            falliti.append(i)
            continue
        nuovi = 0
        for e in risposta.get("elements", []):
            geom = e.get("geometry") or []
            # Overpass restituisce a volte voci nulle dentro la geometria, e
            # una sola faceva cadere tutto il download a meta' strada.
            punti = [(g["lat"], g["lon"]) for g in geom
                     if isinstance(g, dict) and "lat" in g and "lon" in g]
            if len(punti) < 2:
                continue
            firma = (punti[0], punti[-1], len(punti))
            if firma in visti:
                continue
            visti.add(firma)
            tratti.append(punti)
            nuovi += 1
        print(f"  riquadro {i}: {nuovi:,} tratti nuovi", flush=True)
        # Si salva dopo ogni riquadro. Con il salvataggio solo alla fine, un
        # errore a meta' strada buttava via tutti i riquadri gia' scaricati.
        if nuovi:
            _salva(tratti, destinazione)
        time.sleep(4)

    _salva(tratti, destinazione)
    if falliti:
        # Un riquadro perso significa una regione senza binari, e senza dirlo il
        # calcolo prosegue producendo distanze mancanti che sembrano tratte
        # inesistenti. Meglio un avviso in faccia.
        print(f"\nATTENZIONE: {len(falliti)} riquadri non scaricati ({falliti}). "
              "Rilanciare con --scarica per completarli: la cache viene riusata.")
    return len(tratti)


def km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


# I punti dei binari si arrotondano a circa undici metri prima di diventare nodi
# del grafo: senza questo due binari che si incontrano restano scollegati per
# frazioni di metro, e la rete si spezza in migliaia di componenti isolate.
_PRECISIONE = 4


def _nodo(lat: float, lon: float) -> Tuple[float, float]:
    return (round(lat, _PRECISIONE), round(lon, _PRECISIONE))


def costruisci_grafo(percorso: str = CACHE_BINARI
                     ) -> Tuple[Dict[Tuple[float, float], List[Tuple[Tuple[float, float], float]]], int]:
    with open(percorso, encoding="utf-8") as f:
        tratti = json.load(f)

    grafo: Dict[Tuple[float, float], List[Tuple[Tuple[float, float], float]]] = defaultdict(list)
    lati = 0
    for punti in tratti:
        prec = None
        for lat, lon in punti:
            n = _nodo(lat, lon)
            if prec is not None and n != prec:
                d = km(prec[0], prec[1], n[0], n[1])
                if d > 0:
                    grafo[prec].append((n, d))
                    grafo[n].append((prec, d))
                    lati += 1
            prec = n
    return grafo, lati


def _griglia(nodi: Iterable[Tuple[float, float]], passo: float = 0.05
             ) -> Dict[Tuple[int, int], List[Tuple[float, float]]]:
    g: Dict[Tuple[int, int], List[Tuple[float, float]]] = defaultdict(list)
    for n in nodi:
        g[(int(n[0] / passo), int(n[1] / passo))].append(n)
    return g


def aggancia_stazioni(grafo, stazioni: Dict[str, Tuple[float, float]],
                      max_km: float = 2.0) -> Dict[str, Tuple[float, float]]:
    """Nodo del grafo piu' vicino a ogni stazione.

    Oltre due chilometri non si aggancia: significa che la stazione non e' sulla
    rete che abbiamo scaricato, e attaccarla al binario piu' vicino qualunque
    esso sia produrrebbe distanze inventate.
    """
    passo = 0.05
    g = _griglia(grafo.keys(), passo)
    out = {}
    for nome, (lat, lon) in stazioni.items():
        ci, cj = int(lat / passo), int(lon / passo)
        migliore, md = None, float("inf")
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                for n in g.get((ci + di, cj + dj), ()):
                    d = km(lat, lon, n[0], n[1])
                    if d < md:
                        migliore, md = n, d
        if migliore is not None and md <= max_km:
            out[nome] = migliore
    return out


def _dijkstra(grafo, partenza, obiettivi: set, limite_km: float = 1600.0):
    dist = {partenza: 0.0}
    coda = [(0.0, partenza)]
    trovati = {}
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


def coordinate_stazioni() -> Dict[str, Tuple[float, float]]:
    import pandas as pd
    p = os.path.join("data", "gold", "stations_dim.csv")
    d = pd.read_csv(p)
    d = d[d["lat"].notna() & d["lon"].notna()]
    out = {}
    for _, r in d.iterrows():
        k = normalize_station_name(str(r["nome_stazione"]))
        if k and k not in out:
            out[k] = (float(r["lat"]), float(r["lon"]))
    return out


def coppie_da_gold() -> List[Tuple[str, str]]:
    from .distanze_gtfs import coppie_da_gold as _c
    return _c()


def ricuci_componenti(grafo, max_metri: float = 60.0) -> int:
    """Unisce i tronchi di binario che in OSM non condividono il nodo di giunzione.

    Il grafo grezzo aveva 390 componenti separate e il 22,6% dei nodi fuori da
    quella principale: non perche' quei binari siano staccati nella realta', ma
    perche' due way di OpenStreetMap che si incontrano non sempre hanno un nodo
    in comune, e l'arrotondamento a undici metri non basta a farle combaciare.

    L'effetto era sistematico e invisibile: Bergamo, Ferrara, Mantova e Varese
    stanno nella componente principale, ma la loro linea diretta e' interrotta a
    meta', quindi ogni cammino minimo girava intorno e produceva distanze
    assurde. Erano 257 tratte, il 2,2% del traffico.

    Qui si collegano i nodi di componenti diverse che distano meno di sessanta
    metri. La soglia e' volutamente stretta: due binari a sessanta metri sono lo
    stesso fascio, mentre a duecento potrebbero essere due linee parallele che
    non si scambiano, e collegarle creerebbe scorciatoie inesistenti.
    """
    from collections import deque

    def componenti():
        visti = set()
        etichetta = {}
        for i, n in enumerate(grafo):
            if n in visti:
                continue
            q = deque([n])
            visti.add(n)
            while q:
                x = q.popleft()
                etichetta[x] = i
                for v, _ in grafo.get(x, ()):
                    if v not in visti:
                        visti.add(v)
                        q.append(v)
        return etichetta

    etichetta = componenti()
    passo = 0.002  # ~200 m: abbastanza per trovare i vicini entro la soglia
    griglia = defaultdict(list)
    for n in grafo:
        griglia[(int(n[0] / passo), int(n[1] / passo))].append(n)

    soglia_km = max_metri / 1000.0
    aggiunti = 0
    for cella, nodi in list(griglia.items()):
        vicini = []
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                vicini.extend(griglia.get((cella[0] + di, cella[1] + dj), ()))
        for a in nodi:
            ea = etichetta.get(a)
            for b in vicini:
                if etichetta.get(b) == ea:
                    continue
                d = km(a[0], a[1], b[0], b[1])
                if d <= soglia_km:
                    grafo[a].append((b, d))
                    grafo[b].append((a, d))
                    aggiunti += 1
                    # Le due componenti ora sono una sola.
                    vecchia = etichetta.get(b)
                    for n, e in etichetta.items():
                        if e == vecchia:
                            etichetta[n] = ea
    return aggiunti


def main(scarica: bool) -> None:
    if scarica or not os.path.exists(CACHE_BINARI):
        print("scarico la geometria dei binari da OpenStreetMap:")
        n = scarica_binari()
        print(f"tratti di binario salvati: {n:,}")

    print("\ncostruisco il grafo della rete:")
    grafo, lati = costruisci_grafo()
    print(f"  nodi: {len(grafo):,}   lati: {lati:,}")
    cuciti = ricuci_componenti(grafo)
    print(f"  giunzioni ricucite fra tronchi vicini: {cuciti:,}")

    stazioni = coordinate_stazioni()
    agganci = aggancia_stazioni(grafo, stazioni)
    print(f"  stazioni con coordinate: {len(stazioni):,}")
    print(f"  agganciate alla rete (entro 2 km): {len(agganci):,}")

    coppie = coppie_da_gold()
    print(f"\ncoppie da coprire: {len(coppie):,}")

    per_partenza: Dict[str, set] = defaultdict(set)
    for a, b in coppie:
        if a in agganci and b in agganci:
            per_partenza[a].add(b)

    inverso: Dict[Tuple[float, float], List[str]] = defaultdict(list)
    for nome, nodo in agganci.items():
        inverso[nodo].append(nome)

    risultati: Dict[Tuple[str, str], float] = {}
    for i, (a, obiettivi) in enumerate(sorted(per_partenza.items()), 1):
        nodi_obiettivo = {agganci[b] for b in obiettivi if b in agganci}
        trovati = _dijkstra(grafo, agganci[a], nodi_obiettivo)
        for nodo, d in trovati.items():
            for b in inverso.get(nodo, ()):
                if b in obiettivi:
                    risultati[(a, b)] = round(d, 3)
        if i % 100 == 0:
            print(f"  {i}/{len(per_partenza)} stazioni di partenza, "
                  f"{len(risultati):,} tratte risolte")

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
