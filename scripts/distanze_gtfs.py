# scripts/distanze_gtfs.py
"""Ricava la lunghezza reale delle tratte dai feed GTFS del trasporto italiano.

I dati di TrainStats hanno origine, destinazione e ritardo, ma non i chilometri:
senza quelli non si puo' dire quanto ritardo accumula una tratta *in rapporto
alla sua lunghezza*, che e' la domanda interessante. Una tratta di 300 km con
dieci minuti di ritardo va meglio di una di 20 km con cinque.

La distanza in linea d'aria sarebbe sbagliata per definizione: la ferrovia
segue le valli. Nei GTFS c'e' invece `shape_dist_traveled`, la distanza
percorsa lungo il tracciato, ed e' esattamente la grandezza che serve.
Verificata contro lunghezze note: Firenze SMN - Pisa 80,7 km (reale ~81),
Firenze SMN - Arezzo 86,9 (reale ~88), Pisa - Livorno 19,0 (reale ~19).

Nessun feed copre l'Italia intera, quindi si scaricano tutti quelli disponibili
e si tiene solo cio' che viaggia su ferro (route_type 2). Le coppie che nessun
feed osserva direttamente si ricavano dal grafo: se A-B e B-C sono note, A-C e'
il cammino minimo. E' cosi' che la copertura passa dalle coppie osservate a
quasi tutta la rete.

    python -m scripts.distanze_gtfs --scarica     # scarica i feed (lento)
    python -m scripts.distanze_gtfs               # ricostruisce le distanze
"""
from __future__ import annotations

import argparse
import csv
import heapq
import io
import json
import os
import statistics
import urllib.request
import zipfile
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

from .utils import normalize_station_name

CATALOGO = "https://bit.ly/catalogs-csv"
CARTELLA_FEED = os.path.join("data", "gtfs")
USCITA = os.path.join("data", "stations", "distanze_tratte.csv")

# route_type GTFS: 2 = ferrovia, 100-117 = ferrovia nella tassonomia estesa.
TIPI_FERRO = {"2"} | {str(n) for n in range(100, 118)}


def _leggi_csv_zip(z: zipfile.ZipFile, nome: str) -> List[dict]:
    if nome not in z.namelist():
        return []
    with z.open(nome) as f:
        testo = io.TextIOWrapper(f, encoding="utf-8-sig", errors="replace")
        return list(csv.DictReader(testo))


def feed_italiani() -> List[Tuple[str, str]]:
    """(nome, url) dei feed GTFS italiani senza autenticazione."""
    with urllib.request.urlopen(CATALOGO, timeout=120) as r:
        righe = list(csv.DictReader(io.TextIOWrapper(r, encoding="utf-8-sig")))
    out = []
    for i, x in enumerate(righe):
        if x.get("location.country_code") != "IT" or x.get("data_type") != "gtfs":
            continue
        if (x.get("urls.authentication_type") or "0") not in ("", "0"):
            continue
        url = x.get("urls.latest") or x.get("urls.direct_download")
        if not url:
            continue
        nome = (x.get("provider") or f"feed{i}").replace("/", "-")[:60]
        out.append((f"{x.get('mdb_source_id', i)}_{nome}", url))
    return out


def scarica_feed(cartella: str = CARTELLA_FEED) -> Dict[str, int]:
    os.makedirs(cartella, exist_ok=True)
    esiti = {"scaricati": 0, "gia presenti": 0, "falliti": 0}
    for nome, url in feed_italiani():
        dest = os.path.join(cartella, nome + ".zip")
        if os.path.exists(dest) and os.path.getsize(dest) > 1000:
            esiti["gia presenti"] += 1
            continue
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "trainstats-lab/1.0 (distanze tratte)"})
            with urllib.request.urlopen(req, timeout=180) as r, open(dest, "wb") as f:
                f.write(r.read())
            esiti["scaricati"] += 1
        except Exception as e:
            esiti["falliti"] += 1
            print(f"  {nome}: {str(e)[:70]}")
    return esiti


def segmenti_da_feed(percorso: str) -> Iterable[Tuple[str, str, float]]:
    """Distanze fra fermate consecutive dei soli servizi su ferro.

    Si usano le coppie consecutive e non tutte le combinazioni: le consecutive
    sono i lati del grafo, e da quelli si ricava qualunque coppia. Prendere
    tutte le combinazioni gonfierebbe il file senza aggiungere informazione.
    """
    try:
        z = zipfile.ZipFile(percorso)
    except Exception:
        return

    with z:
        rotte = _leggi_csv_zip(z, "routes.txt")
        ferro = {r["route_id"] for r in rotte
                 if str(r.get("route_type", "")).strip() in TIPI_FERRO}
        if not ferro:
            return

        viaggi = {t["trip_id"] for t in _leggi_csv_zip(z, "trips.txt")
                  if t.get("route_id") in ferro}
        if not viaggi:
            return

        nomi = {s["stop_id"]: s.get("stop_name", "")
                for s in _leggi_csv_zip(z, "stops.txt")}

        per_viaggio: Dict[str, List[Tuple[int, str, float]]] = defaultdict(list)
        for r in _leggi_csv_zip(z, "stop_times.txt"):
            tid = r.get("trip_id")
            if tid not in viaggi:
                continue
            d = r.get("shape_dist_traveled")
            if d in (None, ""):
                continue
            try:
                seq = int(float(r.get("stop_sequence") or 0))
                dist = float(d)
            except ValueError:
                continue
            chiave = normalize_station_name(nomi.get(r.get("stop_id"), ""))
            if chiave:
                per_viaggio[tid].append((seq, chiave, dist))

    for fermate in per_viaggio.values():
        fermate.sort()
        for (_, a, da), (_, b, db) in zip(fermate, fermate[1:]):
            km = db - da
            # Oltre i 400 km fra due fermate consecutive non e' una tratta
            # ferroviaria italiana: e' un'unita' di misura diversa o un errore.
            if a != b and 0.1 < km < 400:
                yield a, b, km


def costruisci_grafo(cartella: str = CARTELLA_FEED) -> Dict[Tuple[str, str], float]:
    """Lati del grafo: mediana delle distanze osservate per ogni coppia."""
    osservate: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    feed_utili = 0
    for f in sorted(os.listdir(cartella)):
        if not f.endswith(".zip"):
            continue
        trovati = 0
        for a, b, km in segmenti_da_feed(os.path.join(cartella, f)):
            osservate[(a, b) if a < b else (b, a)].append(km)
            trovati += 1
        if trovati:
            feed_utili += 1
            print(f"  {f[:52]:54} {trovati:>7,} segmenti")
    print(f"\nfeed con servizi su ferro: {feed_utili}")
    return {k: round(statistics.median(v), 3) for k, v in osservate.items()}


def _dijkstra(grafo: Dict[str, List[Tuple[str, float]]], partenza: str,
              obiettivi: set, limite_km: float = 1500.0) -> Dict[str, float]:
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


def distanze_per_coppie(lati: Dict[Tuple[str, str], float],
                        coppie: Iterable[Tuple[str, str]]) -> Dict[Tuple[str, str], Tuple[float, str]]:
    """Distanza per ogni coppia richiesta: osservata o dal cammino minimo."""
    grafo: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
    for (a, b), km in lati.items():
        grafo[a].append((b, km))
        grafo[b].append((a, km))

    dirette = {}
    for (a, b), km in lati.items():
        dirette[(a, b)] = km
        dirette[(b, a)] = km

    da_cercare: Dict[str, set] = defaultdict(set)
    out: Dict[Tuple[str, str], Tuple[float, str]] = {}
    for a, b in coppie:
        if not a or not b or a == b:
            continue
        if (a, b) in dirette:
            out[(a, b)] = (dirette[(a, b)], "adiacenti")
        else:
            da_cercare[a].add(b)

    for i, (a, obiettivi) in enumerate(sorted(da_cercare.items()), 1):
        if a not in grafo:
            continue
        for b, km in _dijkstra(grafo, a, obiettivi).items():
            out[(a, b)] = (round(km, 3), "cammino")
        if i % 200 == 0:
            print(f"  cammini calcolati da {i} stazioni")
    return out


def coppie_da_gold() -> List[Tuple[str, str]]:
    """Le coppie origine-destinazione che la dashboard mostra davvero."""
    import glob

    import pandas as pd

    nomi_path = os.path.join("docs", "data", "station_names.csv")
    if not os.path.exists(nomi_path):
        raise SystemExit("manca docs/data/station_names.csv: eseguire prima build_site")
    nomi = pd.read_csv(nomi_path)
    mappa = {str(c): normalize_station_name(str(n))
             for c, n in zip(nomi.iloc[:, 0], nomi.iloc[:, 1])}

    coppie = set()
    for f in sorted(glob.glob(os.path.join("data", "gold", "parts",
                                           "od_mese_categoria", "*.parquet"))):
        d = pd.read_parquet(f, columns=["cod_partenza", "cod_arrivo"])
        for a, b in zip(d["cod_partenza"].astype(str), d["cod_arrivo"].astype(str)):
            na, nb = mappa.get(a), mappa.get(b)
            if na and nb and na != nb:
                coppie.add((na, nb))
    return sorted(coppie)


def main(scarica: bool, cartella: str) -> None:
    if scarica:
        print("scarico i feed GTFS italiani...")
        print(json.dumps(scarica_feed(cartella), ensure_ascii=False, indent=2))

    if not os.path.isdir(cartella):
        raise SystemExit(f"manca {cartella}: eseguire con --scarica")

    print("\nestraggo i segmenti su ferro:")
    lati = costruisci_grafo(cartella)
    print(f"lati del grafo (coppie di fermate consecutive): {len(lati):,}")
    print(f"stazioni raggiungibili: {len({s for k in lati for s in k}):,}")

    coppie = coppie_da_gold()
    print(f"\ncoppie origine-destinazione da coprire: {len(coppie):,}")
    risultati = distanze_per_coppie(lati, coppie)

    diretti = sum(1 for v in risultati.values() if v[1] == "adiacenti")
    print(f"\ncoperte: {len(risultati):,} su {len(coppie):,} "
          f"({len(risultati)/max(len(coppie),1):.1%})")
    print(f"  di cui fermate adiacenti: {diretti:,}")
    print(f"  ricavate dal cammino minimo: {len(risultati) - diretti:,}")

    os.makedirs(os.path.dirname(USCITA), exist_ok=True)
    with open(USCITA, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["partenza", "arrivo", "km", "origine_stima"])
        for (a, b), (km, come) in sorted(risultati.items()):
            w.writerow([a, b, km, come])
    print(f"\nscritto {USCITA}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scarica", action="store_true", help="scarica i feed GTFS")
    ap.add_argument("--cartella", default=CARTELLA_FEED)
    args = ap.parse_args()
    main(args.scarica, args.cartella)
