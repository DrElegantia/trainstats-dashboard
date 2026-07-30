# scripts/velocita_rete.py
"""La lentezza della rete disegnata sui binari, non sulle linee d'aria.

Una tratta e' una coppia di stazioni, e disegnarla come segmento retto fra i
due capolinea produce una mappa falsa: Roma-Siracusa taglierebbe il Tirreno,
Bari-Reggio le montagne. Qui si disegnano i binari veri, presi dalla geometria
OpenStreetMap gia' usata per misurare le distanze.

Il colore non e' della tratta ma del **pezzo di rete**. Per ogni collegamento
della classifica si ricostruisce il cammino sui binari e si attribuisce a ogni
tratto percorso la velocita' commerciale di quel collegamento, pesata sulle
corse. Un tratto su cui passano treni diversi prende la media di tutti, pesata
su quanti sono: cosi' la Firenze-Roma diretta risulta veloce anche se ci passa
qualche regionale, e una linea appenninica percorsa solo da regionali risulta
lenta com'e'.

Cosa il numero NON e': la velocita' misurata su quel tratto. La sorgente da' i
tempi solo agli estremi della corsa, non alle fermate intermedie, quindi la
velocita' di un tratto e' quella media dell'intero viaggio dei treni che ci
passano. Su una linea omogenea le due cose coincidono; dove un tratto veloce
introduce a uno lento, la media li appiattisce entrambi.

    python -m scripts.velocita_rete

Scrive data/stations/velocita_rete.geojson, che build_site pubblica e la
dashboard disegna a colori.
"""
from __future__ import annotations

import heapq
import json
import math
import os
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from .distanze_osm import (aggancia_stazioni, costruisci_grafo, km,
                           ricuci_componenti)
from .utils import normalize_station_name

INDICATORI = os.path.join("data", "stations", "indicatori_km.csv")
# Si scrive fra i dati versionati, non direttamente in docs/: la CI ricostruisce
# docs/ da zero a ogni pubblicazione e li' la geometria OpenStreetMap non c'e'
# (sono 21 MB di cache rigenerabile, giustamente non versionata). build_site la
# copia, come fa per l'anagrafica delle stazioni.
USCITA = os.path.join("data", "stations", "velocita_rete.geojson")

# Rete di sicurezza, non un filtro attivo: ogni tratta in ingresso ha gia'
# almeno MIN_CORSE corse (indicatori_km ne scarta di meno), e un tratto di
# binario ne raccoglie la somma, quindi oggi non esclude nulla. Resta a
# presidiare il caso in cui quella soglia a monte venga abbassata.
MIN_CORSE_TRATTO = 300

# Quanto il cammino ricostruito su OpenStreetMap puo' discostarsi dai
# chilometri con cui e' stata calcolata la velocita' prima di considerare che
# le due fonti stiano descrivendo viaggi diversi.
MAX_SCARTO_PERCORSO = 0.20
# Tolleranza di semplificazione della geometria, in gradi. Un centesimo di
# grado e' circa 1,1 km in latitudine: troppo. Un millesimo e' 110 metri, che a
# scala nazionale non si distingue e taglia il file di un ordine di grandezza.
TOLLERANZA = 0.001


def _dijkstra_con_percorso(grafo, partenza, obiettivi: set,
                           limite_km: float = 1600.0):
    """Come il Dijkstra delle distanze, ma tiene anche da dove si e' arrivati.

    Serve il cammino, non solo la sua lunghezza: e' il cammino che va disegnato.
    """
    dist = {partenza: 0.0}
    prima: Dict[Tuple[float, float], Tuple[float, float]] = {}
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
                prima[v] = n
                heapq.heappush(coda, (nd, v))
    return trovati, prima


def _risali(prima, partenza, arrivo) -> List[Tuple[float, float]]:
    percorso = [arrivo]
    while percorso[-1] != partenza:
        p = prima.get(percorso[-1])
        if p is None:
            return []
        percorso.append(p)
    return list(reversed(percorso))


def _semplifica(punti: List[Tuple[float, float]], tol: float) -> List[Tuple[float, float]]:
    """Douglas-Peucker, iterativo per non esaurire lo stack sui percorsi lunghi."""
    if len(punti) < 3:
        return punti
    tieni = [False] * len(punti)
    tieni[0] = tieni[-1] = True
    pile = [(0, len(punti) - 1)]
    while pile:
        i, j = pile.pop()
        if j <= i + 1:
            continue
        ax, ay = punti[i]
        bx, by = punti[j]
        dx, dy = bx - ax, by - ay
        quadrato = dx * dx + dy * dy
        peggiore, indice = 0.0, -1
        for k in range(i + 1, j):
            px, py = punti[k]
            if quadrato == 0:
                d = math.hypot(px - ax, py - ay)
            else:
                # Distanza dal SEGMENTO, non dalla retta infinita che lo
                # contiene. Con la retta, un tratto che va e torna sulla stessa
                # direttrice ha tutti i punti allineati e veniva schiacciato sui
                # due estremi: un'andata e ritorno di trentacinque chilometri si
                # riduceva a un segmento di otto metri.
                t = ((px - ax) * dx + (py - ay) * dy) / quadrato
                t = 0.0 if t < 0.0 else (1.0 if t > 1.0 else t)
                d = math.hypot(px - (ax + t * dx), py - (ay + t * dy))
            if d > peggiore:
                peggiore, indice = d, k
        if peggiore > tol and indice > 0:
            tieni[indice] = True
            pile.append((i, indice))
            pile.append((indice, j))
    return [p for p, t in zip(punti, tieni) if t]


def _catene(lati: Dict[Tuple, Tuple[float, float]]) -> List[List[Tuple[float, float]]]:
    """Unisce i lati adiacenti dello stesso colore in un'unica polilinea.

    Senza questo passaggio il file conterrebbe centomila segmentini da due punti
    l'uno, ognuno con la sua intestazione GeoJSON: la geometria peserebbe meno
    dei metadati che la descrivono.
    """
    vicini: Dict[Tuple[float, float], List[Tuple[float, float]]] = defaultdict(list)
    for a, b in lati:
        vicini[a].append(b)
        vicini[b].append(a)

    da_fare = set(lati)
    catene = []
    # Si parte dai capi e dagli incroci: le catene aperte vengono per prime,
    # cosi' restano intere invece di essere spezzate a meta'.
    inizi = [n for n, v in vicini.items() if len(v) != 2]
    for partenza in inizi + [a for a, _ in list(da_fare)]:
        for vicino in list(vicini.get(partenza, ())):
            chiave = (partenza, vicino) if partenza < vicino else (vicino, partenza)
            if chiave not in da_fare:
                continue
            catena = [partenza, vicino]
            da_fare.discard(chiave)
            corrente, precedente = vicino, partenza
            while len(vicini.get(corrente, ())) == 2:
                avanti = [x for x in vicini[corrente] if x != precedente]
                if not avanti:
                    break
                prossimo = avanti[0]
                k = (corrente, prossimo) if corrente < prossimo else (prossimo, corrente)
                if k not in da_fare:
                    break
                da_fare.discard(k)
                catena.append(prossimo)
                corrente, precedente = prossimo, corrente
            catene.append(catena)
    return catene


def main() -> None:
    import pandas as pd

    if not os.path.exists(INDICATORI):
        raise SystemExit(f"manca {INDICATORI}: eseguire prima scripts.indicatori_km")
    tratte = pd.read_csv(INDICATORI)
    tratte = tratte[tratte["km_h_programmati"].notna() & (tratte["corse"] > 0)]
    print(f"tratte da disegnare: {len(tratte):,}")

    print("\ncostruisco il grafo dei binari:")
    grafo, n_lati = costruisci_grafo()
    print(f"  nodi: {len(grafo):,}   lati: {n_lati:,}")
    ricuci_componenti(grafo)

    from .distanze_osm import coordinate_stazioni
    agganci = aggancia_stazioni(grafo, coordinate_stazioni())
    print(f"  stazioni agganciate: {len(agganci):,}")

    # Per ogni lato del grafo: somma di velocita' per corse, e somma di corse.
    peso: Dict[Tuple, float] = defaultdict(float)
    corse: Dict[Tuple, float] = defaultdict(float)

    per_partenza: Dict[str, List] = defaultdict(list)
    fuori_grafo: List[Tuple[str, str, float]] = []
    for _, r in tratte.iterrows():
        a, b = str(r["partenza"]), str(r["arrivo"])
        if a in agganci and b in agganci:
            per_partenza[a].append((b, float(r["km_h_programmati"]),
                                    float(r["corse"]), float(r["km"])))
        else:
            fuori_grafo.append((a, b, float(r["corse"])))

    disegnate = 0
    senza_percorso: List[Tuple[str, str, float]] = []
    incoerenti: List[Tuple[str, str, float, float, float]] = []
    for i, (a, elenco) in enumerate(sorted(per_partenza.items()), 1):
        obiettivi = {agganci[b] for b, _, _, _ in elenco}
        trovati, prima = _dijkstra_con_percorso(grafo, agganci[a], obiettivi)
        for b, kmh, n, km_tratta in elenco:
            percorso = _risali(prima, agganci[a], agganci[b])
            if len(percorso) < 2:
                senza_percorso.append((a, b, n))
                continue
            # Il colore viene dalla velocita', che e' calcolata sui chilometri
            # di km_tratte (in maggioranza RINF); la geometria viene dal cammino
            # minimo su OpenStreetMap. Quando le due fonti non descrivono lo
            # stesso viaggio, dipingere quel cammino significa attribuire una
            # velocita' a binari che quel treno non percorre. Meglio non
            # disegnare la tratta che disegnarla sul percorso sbagliato.
            km_disegnati = trovati.get(agganci[b])
            if km_tratta > 0 and km_disegnati:
                scarto = abs(km_disegnati - km_tratta) / km_tratta
                if scarto > MAX_SCARTO_PERCORSO:
                    incoerenti.append((a, b, km_tratta, km_disegnati, n))
                    continue
            disegnate += 1
            for x, y in zip(percorso, percorso[1:]):
                k = (x, y) if x < y else (y, x)
                peso[k] += kmh * n
                corse[k] += n
        if i % 100 == 0:
            print(f"  {i}/{len(per_partenza)} partenze, {disegnate:,} percorsi tracciati")

    print(f"\npercorsi tracciati: {disegnate:,}")

    # Cio' che non entra in mappa si dice, non si salta in silenzio: prima il
    # totale finale sembrava coprire tutte le tratte della classifica.
    if senza_percorso:
        tot = sum(n for _, _, n in senza_percorso)
        print(f"tratte senza percorso sul grafo: {len(senza_percorso)} ({tot:,.0f} corse)")
        for a, b, n in sorted(senza_percorso, key=lambda x: -x[2])[:5]:
            print(f"    {a} - {b} ({n:,.0f} corse)")
    if incoerenti:
        tot = sum(x[4] for x in incoerenti)
        print(f"tratte escluse perche' il percorso OSM non concorda con i km usati: "
              f"{len(incoerenti)} ({tot:,.0f} corse)")
        for a, b, km, kmo, n in sorted(incoerenti, key=lambda x: -x[4])[:5]:
            print(f"    {a} - {b}: {km:.0f} km usati contro {kmo:.0f} km disegnati ({n:,.0f} corse)")
    if fuori_grafo:
        tot = sum(n for _, _, n in fuori_grafo)
        print(f"tratte con una stazione non agganciata al grafo: {len(fuori_grafo)} ({tot:,.0f} corse)")
        for a, b, n in sorted(fuori_grafo, key=lambda x: -x[2])[:5]:
            print(f"    {a} - {b} ({n:,.0f} corse)")
    print(f"tratti di binario percorsi: {len(corse):,}")

    velocita = {k: peso[k] / corse[k] for k in corse if corse[k] >= MIN_CORSE_TRATTO}
    print(f"tratti con almeno {MIN_CORSE_TRATTO} corse: {len(velocita):,}")

    # Si spezza per classe di velocita' prima di unire le catene: un tratto
    # veloce e uno lento non possono finire nella stessa polilinea, avrebbero un
    # colore solo.
    # Si classifica sul valore ARROTONDATO, che e' quello che il browser legge
    # dal GeoJSON: classificando sul valore pieno, una media di 59,96 finiva in
    # una classe qui e nell'altra nel browser, con la legenda che la contava
    # dalla parte sbagliata.
    def classe(v: float) -> int:
        v = round(v, 1)
        for i, soglia in enumerate((40, 60, 80, 100, 130)):
            if v < soglia:
                return i
        return 5

    per_classe: Dict[int, Dict[Tuple, float]] = defaultdict(dict)
    for k, v in velocita.items():
        per_classe[classe(v)][k] = v

    elementi = []
    for c, lati_classe in sorted(per_classe.items()):
        for catena in _catene(lati_classe):
            punti = _semplifica(catena, TOLLERANZA)
            if len(punti) < 2:
                continue
            vals = []
            pesi = []
            for x, y in zip(catena, catena[1:]):
                k = (x, y) if x < y else (y, x)
                if k in velocita:
                    vals.append(velocita[k])
                    pesi.append(corse[k])
            if not vals:
                continue
            media = sum(v * p for v, p in zip(vals, pesi)) / sum(pesi)
            # Il valore pubblicato deve ricadere nella classe in cui la
            # polilinea e' stata raggruppata: una media di 59,96 arrotondata a
            # 60,0 verrebbe colorata dal browser come la classe successiva.
            kmh = round(media, 1)
            if classe(kmh) != c:
                kmh = math.floor(media * 10) / 10
            elementi.append({
                "type": "Feature",
                "properties": {"kmh": kmh,
                               "corse": int(round(sum(pesi) / len(pesi)))},
                "geometry": {"type": "LineString",
                             "coordinates": [[round(lon, 4), round(lat, 4)]
                                             for lat, lon in punti]},
            })

    geo = {"type": "FeatureCollection", "features": elementi}
    os.makedirs(os.path.dirname(USCITA), exist_ok=True)
    with open(USCITA, "w", encoding="utf-8") as f:
        json.dump(geo, f, ensure_ascii=False, separators=(",", ":"))

    peso_file = os.path.getsize(USCITA) / 1024
    print(f"\npolilinee scritte: {len(elementi):,}")
    print(f"scritto {USCITA} ({peso_file:.0f} KB)")
    v = sorted(velocita.values())
    if v:
        print(f"velocita sui tratti: minima {v[0]:.0f}, mediana {v[len(v)//2]:.0f}, "
              f"massima {v[-1]:.0f} km/h")


if __name__ == "__main__":
    main()
