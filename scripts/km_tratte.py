# scripts/km_tratte.py
"""Tabella finale dei chilometri per tratta, con provenienza e qualita'.

Unisce le due fonti e dichiara per ogni tratta da dove viene il numero e quanto
ci si puo' fidare, invece di consegnare una colonna di chilometri come se
fossero tutti equivalenti.

    ufficiale  somma delle sezioni RINF, il registro dell'infrastruttura
               europeo: scarto mediano 0,05% contro il dato del gestore.
    misurata   la distanza e' letta dai GTFS fra due fermate consecutive:
               e' il dato del gestore, non una nostra ricostruzione.
    stimata    cammino minimo sul grafo dei binari OpenStreetMap. Sulle 250
               tratte verificabili contro i GTFS lo scarto mediano e' 1,18% e
               non ha distorsione (mediana con segno +0,02 km).
    sospetta   il percorso calcolato e' troppo lungo rispetto alla distanza in
               linea d'aria: al grafo manca un collegamento e il cammino gira
               intorno. Grosseto-Montepescali, due stazioni a dodici
               chilometri, risultava 339 km per questo motivo.

Il rapporto col volo d'uccello e' il controllo che permette di scartare i casi
rotti senza avere una seconda fonte: una linea ferroviaria reale e' piu' lunga
della retta, ma non tre volte tanto.

    python -m scripts.km_tratte
"""
from __future__ import annotations

import csv
import math
import os
import statistics
from typing import Dict, Optional, Tuple

from .utils import normalize_station_name

RINF = os.path.join("data", "stations", "distanze_rinf.csv")
GTFS = os.path.join("data", "stations", "distanze_tratte.csv")
PRONTUARIO = os.path.join("data", "stations", "prontuario_distanze.json")
OSM = os.path.join("data", "stations", "distanze_osm.csv")
USCITA = os.path.join("data", "stations", "km_tratte.csv")

# Oltre questo rapporto fra percorso e linea d'aria il cammino sta girando
# intorno a un buco del grafo. Le ferrovie italiane piu' tortuose (la
# Circumetnea, le linee appenniniche) stanno sotto il 2,2; tre volte la retta
# non e' una linea, e' una deviazione.
MAX_RAPPORTO = 3.0
# Sotto la retta e' geometricamente impossibile: e' una scorciatoia falsa nel
# grafo, tipicamente un raccordo merci mappato come linea.
MIN_RAPPORTO = 0.97


def _linea_aria(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    R = 6371.0088
    p1, p2 = math.radians(a[0]), math.radians(b[0])
    dp, dl = math.radians(b[0] - a[0]), math.radians(b[1] - a[1])
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def _coordinate() -> Dict[str, Tuple[float, float]]:
    import pandas as pd
    d = pd.read_csv(os.path.join("data", "gold", "stations_dim.csv"))
    d = d[d["lat"].notna() & d["lon"].notna()]
    out: Dict[str, Tuple[float, float]] = {}
    for _, r in d.iterrows():
        k = normalize_station_name(str(r["nome_stazione"]))
        if k and k not in out:
            out[k] = (float(r["lat"]), float(r["lon"]))
    return out


def _leggi(percorso: str) -> Tuple[Dict[Tuple[str, str], float], Dict[Tuple[str, str], str]]:
    km: Dict[Tuple[str, str], float] = {}
    come: Dict[Tuple[str, str], str] = {}
    if not os.path.exists(percorso):
        return km, come
    with open(percorso, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            try:
                k = (r["partenza"], r["arrivo"])
                km[k] = float(r["km"])
                come[k] = r.get("origine_stima", "")
            except (KeyError, TypeError, ValueError):
                continue
    return km, come


def _prontuario() -> Dict[Tuple[str, str], float]:
    """Progressive chilometriche dal prontuario delle distanze FS.

    Terza fonte indipendente, utile perche' copre la lunga percorrenza dove i
    GTFS regionali non arrivano. Ha due limiti dichiarati dall'autore stesso:
    non e' una pubblicazione ufficiale e i valori sono indicativi. E ne ha uno
    tecnico nostro: le stazioni su linee diramate hanno progressive che
    ripartono dal bivio, e la nota dice che sono quelle sottolineate, ma la
    sottolineatura non sopravvive all'estrazione del testo. Cosi' Napoli Campi
    Flegrei risulta a 866 km da Torre Annunziata invece che a 31.

    Per questo il prontuario non decide da solo: serve a confermare o a smentire,
    e vince solo quando e' l'unico plausibile rispetto alla linea d'aria.
    """
    import json
    if not os.path.exists(PRONTUARIO):
        return {}
    with open(PRONTUARIO, encoding="utf-8") as f:
        dati = json.load(f)
    out = {}
    for k, v in dati.items():
        a, _, b = k.partition("|")
        if a and b:
            out[(a, b)] = float(v)
            out.setdefault((b, a), float(v))
    return out


def main() -> None:
    rinf, _ = _leggi(RINF)
    gtfs, gtfs_come = _leggi(GTFS)
    osm, _ = _leggi(OSM)
    pron = _prontuario()
    coord = _coordinate()

    tutte = sorted(set(rinf) | set(gtfs) | set(osm) | set(pron))
    print(f"tratte con almeno una stima: {len(tutte):,}")

    righe = []
    conta = {"ufficiale": 0, "misurata": 0, "confermata": 0, "stimata": 0,
             "sospetta": 0, "senza linea d'aria": 0}
    for k in tutte:
        a, b = k
        kr, kg, ko, kp = rinf.get(k), gtfs.get(k), osm.get(k), pron.get(k)
        misurata = gtfs_come.get(k) == "adiacenti"

        aria = None
        if a in coord and b in coord:
            aria = _linea_aria(coord[a], coord[b])

        def plausibile(v: Optional[float]) -> bool:
            if v is None or v <= 0:
                return False
            if aria is None or aria <= 0:
                return True
            return MIN_RAPPORTO <= v / aria <= MAX_RAPPORTO

        # Il RINF viene prima di tutto: e' il registro ufficiale
        # dell'infrastruttura previsto dal regolamento europeo 2019/777, dove
        # ogni gestore dichiara la lunghezza di ciascuna sezione fra punti
        # operativi adiacenti. Misurato contro le 254 tratte di cui i GTFS
        # danno la distanza del gestore, lo scarto mediano e' 0,05% e 97,5%
        # delle tratte cade entro il 10%. Nessun'altra fonte ci si avvicina:
        # OSM sta all'1,18% mediano e all'80,4%.
        if kr is not None and plausibile(kr):
            km, fonte = kr, "rinf"
        elif misurata and kg:
            km, fonte = kg, "gtfs"
        else:
            # Fra le fonti ricostruite si preferisce quella che due fonti
            # confermano: due metodi indipendenti che sbagliano nello stesso
            # modo sono improbabili, mentre uno solo che gira intorno a un buco
            # del grafo e' quello che abbiamo visto succedere.
            candidati = [(v, n) for v, n in ((ko, "osm"), (kp, "prontuario"),
                                             (kg, "gtfs_cammino")) if plausibile(v)]
            concordi = [(v, n) for v, n in candidati
                        if any(m is not n and abs(v - w) / max(v, w) <= 0.15
                               for w, m in candidati)]
            scelti = concordi or candidati
            if scelti:
                # Fra i plausibili si tiene il piu' corto: entrambi i metodi
                # possono solo allungare quando manca un pezzo di rete.
                km, fonte = min(scelti, key=lambda x: x[0])
                if concordi:
                    fonte += "+conferma"
            else:
                km, fonte = (ko if ko is not None else (kp if kp is not None else kg)), "incerta"

        if km is None:
            # Nessuna fonte ha prodotto un valore plausibile: la tratta esce,
            # non viene riempita con un numero qualsiasi.
            conta["sospetta"] = conta.get("sospetta", 0) + 1
            righe.append({"partenza": a, "arrivo": b, "km": "", "fonte": "nessuna",
                          "qualita": "sospetta",
                          "km_linea_aria": round(aria, 2) if aria else "",
                          "rapporto": "", "km_rinf": round(kr, 3) if kr else "",
                          "km_gtfs": round(kg, 2) if kg else "",
                          "km_osm": round(ko, 2) if ko else "",
                          "km_prontuario": round(kp, 2) if kp else ""})
            continue

        if fonte == "rinf":
            qualita = "ufficiale"
        elif misurata:
            qualita = "misurata"
        elif "+conferma" in fonte:
            qualita = "confermata"
        elif aria is None or aria <= 0:
            qualita = "stimata"
            conta["senza linea d'aria"] += 1
        else:
            rapporto = km / aria
            if rapporto > MAX_RAPPORTO or rapporto < MIN_RAPPORTO:
                qualita = "sospetta"
                # Se l'altra fonte e' plausibile, si usa quella invece di
                # buttare via la tratta.
                altra = kg if fonte == "osm" else ko
                if altra and MIN_RAPPORTO <= altra / aria <= MAX_RAPPORTO:
                    km = altra
                    fonte = "gtfs" if fonte == "osm" else "osm"
                    qualita = "stimata"
            else:
                qualita = "stimata"

        conta[qualita] = conta.get(qualita, 0) + 1
        righe.append({
            "partenza": a, "arrivo": b, "km": round(km, 2), "fonte": fonte,
            "qualita": qualita,
            "km_linea_aria": round(aria, 2) if aria else "",
            "rapporto": round(km / aria, 2) if aria else "",
            "km_rinf": round(kr, 3) if kr else "",
            "km_gtfs": round(kg, 2) if kg else "",
            "km_osm": round(ko, 2) if ko else "",
            "km_prontuario": round(kp, 2) if kp else "",
        })

    utili = [r for r in righe
             if r["qualita"] in ("ufficiale", "misurata", "confermata", "stimata")]
    print(f"\n  ufficiali dal RINF: {conta.get('ufficiale', 0):,}")
    print(f"  misurate dai GTFS:  {conta['misurata']:,}")
    print(f"  confermate da due fonti: {conta.get('confermata', 0):,}")
    print(f"  stimate su OSM:     {conta['stimata']:,}")
    print(f"  scartate, sospette: {conta['sospetta']:,}")
    print(f"  utilizzabili:       {len(utili):,} ({len(utili)/max(len(righe),1):.1%})")

    rap = [r["rapporto"] for r in utili if r["rapporto"] != ""]
    if rap:
        print(f"\n  rapporto percorso/linea d'aria: mediano {statistics.median(rap):.2f}, "
              f"medio {statistics.fmean(rap):.2f}")
    kms = [r["km"] for r in utili]
    print(f"  lunghezza tratte: mediana {statistics.median(kms):.1f} km, "
          f"massima {max(kms):.0f} km")

    os.makedirs(os.path.dirname(USCITA), exist_ok=True
                ) if os.path.dirname(USCITA) else None
    campi = ["partenza", "arrivo", "km", "fonte", "qualita", "km_linea_aria",
             "rapporto", "km_rinf", "km_gtfs", "km_osm", "km_prontuario"]
    with open(USCITA, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campi)
        w.writeheader()
        w.writerows(righe)
    print(f"\nscritto {USCITA}")


if __name__ == "__main__":
    main()
