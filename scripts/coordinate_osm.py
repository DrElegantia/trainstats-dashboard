# scripts/coordinate_osm.py
"""Rifa' l'anagrafica delle coordinate stazione da OpenStreetMap.

Le coordinate venivano da Nominatim interrogato a testo libero, che non cerca
stazioni: cerca qualunque cosa somigli alla stringa. Cercando "Venezia Santa
Lucia" restituisce una frazione chiamata Santa Lucia in provincia di Pordenone,
ed e' esattamente il punto in cui la dashboard disegnava la stazione di
Venezia, 655 km a sud di Venezia Mestre. Non era un caso isolato: 202 stazioni
avevano coordinate sbagliate di oltre 5 km, per 7,5 milioni di corse, fra cui
Napoli Centrale (18 km), Bergamo (45), Parma (40), Siena (48).

Qui si interrogano solo i nodi `railway=station` e `railway=halt` di
OpenStreetMap: il risultato o e' una stazione ferroviaria o non c'e'. Una sola
richiesta scarica tutte le stazioni italiane, invece di una richiesta per nome:
qualche migliaio di interrogazioni fa scadere Overpass e non e' un uso educato
di un servizio gratuito.

    python -m scripts.coordinate_osm --scarica          # aggiorna la cache OSM
    python -m scripts.coordinate_osm --dry-run          # mostra cosa cambierebbe
    python -m scripts.coordinate_osm                    # riscrive stations/stations.csv
"""
from __future__ import annotations

import argparse
import json
import math
import os
import urllib.parse
import urllib.request
from typing import Dict, Optional, Tuple

import pandas as pd

from .utils import normalize_station_name

CACHE_OSM = os.path.join("data", "stations", "osm_stazioni.json")
ANAGRAFICA = os.path.join("stations", "stations.csv")
DIM = os.path.join("data", "gold", "stations_dim.csv")

# Oltre questa distanza fra la coordinata attuale e quella OSM si considera che
# la vecchia fosse sbagliata. Cinque chilometri lasciano passare le differenze
# fra il nodo della stazione e il centroide del fascio binari, che su scali
# grandi arrivano a un paio di chilometri.
SOGLIA_KM = 5.0

QUERY = """
[out:json][timeout:180];
area["ISO3166-1"="IT"][admin_level=2]->.it;
(
  node["railway"~"^(station|halt)$"]["name"](area.it);
  way["railway"~"^(station|halt)$"]["name"](area.it);
);
out center;
"""


def scarica_osm(destinazione: str = CACHE_OSM) -> int:
    dati = urllib.parse.urlencode({"data": QUERY}).encode()
    req = urllib.request.Request(
        "https://overpass-api.de/api/interpreter", data=dati,
        headers={"User-Agent": "trainstats-lab/1.0 (anagrafica stazioni)"})
    with urllib.request.urlopen(req, timeout=300) as r:
        risposta = json.load(r)

    out = []
    for e in risposta.get("elements", []):
        centro = e.get("center") or {}
        lat, lon = e.get("lat") or centro.get("lat"), e.get("lon") or centro.get("lon")
        nome = (e.get("tags") or {}).get("name")
        if lat and lon and nome:
            out.append({"nome": nome, "lat": lat, "lon": lon})

    os.makedirs(os.path.dirname(destinazione), exist_ok=True)
    with open(destinazione, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    return len(out)


def indice_osm(percorso: str = CACHE_OSM) -> Dict[str, Tuple[float, float]]:
    with open(percorso, encoding="utf-8") as f:
        dati = json.load(f)
    idx: Dict[str, Tuple[float, float]] = {}
    for s in dati:
        k = normalize_station_name(s["nome"])
        # Il primo che arriva vince: OSM ha piu' nodi per la stessa stazione
        # (fabbricato viaggiatori, fermata, area), e la differenza fra loro e'
        # di poche decine di metri.
        if k and k not in idx:
            idx[k] = (float(s["lat"]), float(s["lon"]))
    return idx


def distanza_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def _volumi() -> Dict[str, int]:
    """Corse per stazione, per ordinare le correzioni per rilevanza."""
    import glob
    files = sorted(glob.glob(os.path.join("data", "gold", "parts",
                                          "stazioni_mese_categoria_nodo", "*.parquet")))
    if not files:
        return {}
    df = pd.concat([pd.read_parquet(f, columns=["cod_stazione", "corse_osservate"])
                    for f in files], ignore_index=True)
    return df.groupby("cod_stazione")["corse_osservate"].sum().to_dict()


def main(dry_run: bool, scarica: bool, soglia: float) -> None:
    if scarica or not os.path.exists(CACHE_OSM):
        n = scarica_osm()
        print(f"stazioni scaricate da OpenStreetMap: {n}")

    idx = indice_osm()
    print(f"nomi OSM distinti dopo normalizzazione: {len(idx)}")

    if not os.path.exists(ANAGRAFICA):
        raise SystemExit(f"manca {ANAGRAFICA}")
    ana = pd.read_csv(ANAGRAFICA)
    volumi = _volumi()

    corrette = aggiunte = invariate = 0
    senza_corrispondenza = 0
    dettaglio = []

    for i, r in ana.iterrows():
        chiave = normalize_station_name(str(r.get("nome_stazione", "")))
        o = idx.get(chiave)
        if not o:
            senza_corrispondenza += 1
            continue
        lat, lon = r.get("lat"), r.get("lon")
        corse = int(volumi.get(str(r.get("cod_stazione", "")), 0))
        if pd.isna(lat) or pd.isna(lon):
            aggiunte += 1
            dettaglio.append({"nome": r["nome_stazione"], "azione": "aggiunta",
                              "scarto_km": None, "corse": corse})
        else:
            d = distanza_km(float(lat), float(lon), o[0], o[1])
            if d <= soglia:
                invariate += 1
                continue
            corrette += 1
            dettaglio.append({"nome": r["nome_stazione"], "azione": "corretta",
                              "scarto_km": round(d), "corse": corse})
        if not dry_run:
            ana.at[i, "lat"], ana.at[i, "lon"] = o[0], o[1]

    print(f"\ncoordinate corrette (scarto > {soglia:g} km): {corrette}")
    print(f"coordinate aggiunte (prima mancanti):     {aggiunte}")
    print(f"gia' corrette, lasciate come sono:        {invariate}")
    print(f"senza corrispondenza in OSM:              {senza_corrispondenza}")

    if dettaglio:
        d = pd.DataFrame(dettaglio).sort_values("corse", ascending=False)
        sb = d[d["azione"] == "corretta"]
        if len(sb):
            print(f"\nle 12 correzioni piu' rilevanti per traffico:")
            print(sb.head(12).to_string(index=False))
            print(f"\ncorse servite da stazioni riposizionate: {int(sb['corse'].sum()):,}")

    if dry_run:
        print("\ndry-run: nessun file scritto")
        return

    ana.to_csv(ANAGRAFICA, index=False)
    print(f"\nscritto {ANAGRAFICA}")
    print("ricostruire poi l'anagrafica e il sito:")
    print("  python -m scripts.build_station_dim && python -m scripts.build_site")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--scarica", action="store_true", help="riscarica la cache OSM")
    ap.add_argument("--soglia-km", type=float, default=SOGLIA_KM)
    args = ap.parse_args()
    main(args.dry_run, args.scarica, args.soglia_km)
