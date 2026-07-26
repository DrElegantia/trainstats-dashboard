# scripts/indicatori_km.py
"""Tre indicatori per tratta rapportati ai chilometri percorsi.

Il ritardo al chilometro da solo non dice se una linea funziona male: dice
quanto il servizio si scosta dal proprio orario, e un orario abbondante
assorbe i ritardi senza che nessuno li veda. Per capire dove la rete e' lenta
servono affiancati:

    durata al km      minuti che l'orario concede per chilometro. E' la
                      lentezza *strutturale*: binario unico, curve, fermate,
                      incroci. Non dipende da come e' andata quel giorno.
    ritardo al km     minuti che il servizio aggiunge a quell'orario, per
                      chilometro. E' la lentezza *gestionale*.
    velocita'         km/h programmati, cioe' la durata al km letta al
    commerciale       contrario. Stessa informazione, unita' leggibile.

Una linea con durata al km alta e ritardo al km basso e' lenta per come e'
fatta. Una con durata bassa e ritardo alto e' fatta bene e gestita male. Sono
due problemi diversi e chiedono interventi diversi, per questo vanno letti
insieme.

I chilometri arrivano da data/stations/km_tratte.csv, in netta maggioranza dal
RINF, il registro ufficiale dell'infrastruttura europea.

    python -m scripts.indicatori_km [--min-corse 300] [--min-km 30]
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
from typing import Dict, Tuple

import pandas as pd

from .utils import normalize_station_name

KM = os.path.join("data", "stations", "km_tratte.csv")
PARTI = os.path.join("data", "gold", "parts", "od_mese_categoria", "*.parquet")
USCITA = os.path.join("data", "stations", "indicatori_km.csv")

# Sotto questa lunghezza il rapporto per chilometro esplode per costruzione:
# su una navetta di due chilometri i minuti di sosta e di manovra pesano piu'
# del viaggio, e la classifica finisce per premiare le tratte piu' corte invece
# delle piu' lente.
MIN_KM = 30.0
# Con poche corse la media e' un aneddoto.
MIN_CORSE = 300


def _km_per_tratta() -> Dict[Tuple[str, str], Tuple[float, str]]:
    fuori: Dict[Tuple[str, str], Tuple[float, str]] = {}
    with open(KM, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if not r["km"]:
                continue
            if r["qualita"] == "sospetta":
                continue
            v = (float(r["km"]), r["qualita"])
            a, b = r["partenza"], r["arrivo"]
            fuori[(a, b)] = v
            fuori.setdefault((b, a), v)
    return fuori


def tabella(min_corse: int = MIN_CORSE, min_km: float = MIN_KM) -> pd.DataFrame:
    km = _km_per_tratta()

    colonne = ["cod_partenza", "cod_arrivo", "nome_partenza", "nome_arrivo",
               "corse_osservate", "corse_con_misura", "corse_con_durata",
               "durata_prog_tot", "minuti_ritardo_tot", "in_ritardo"]
    pezzi = []
    for p in sorted(glob.glob(PARTI)):
        pezzi.append(pd.read_parquet(p, columns=colonne))
    d = pd.concat(pezzi, ignore_index=True)

    # I nomi sono la chiave verso i chilometri: i codici stazione cambiano nel
    # tempo, i nomi normalizzati no.
    nomi = pd.unique(pd.concat([d["nome_partenza"], d["nome_arrivo"]]).dropna())
    norm = {n: normalize_station_name(str(n)) for n in nomi}
    d["a"] = d["nome_partenza"].map(norm)
    d["b"] = d["nome_arrivo"].map(norm)
    d = d[d["a"].notna() & d["b"].notna() & (d["a"] != d["b"])]

    g = d.groupby(["a", "b"], as_index=False).agg(
        corse=("corse_osservate", "sum"),
        con_misura=("corse_con_misura", "sum"),
        con_durata=("corse_con_durata", "sum"),
        durata_tot=("durata_prog_tot", "sum"),
        ritardo_tot=("minuti_ritardo_tot", "sum"),
        in_ritardo=("in_ritardo", "sum"),
    )

    # La tratta A->B e B->A sono lo stesso pezzo di rete: si sommano, altrimenti
    # ogni linea appare due volte con numeri quasi uguali.
    g["k1"] = g[["a", "b"]].min(axis=1)
    g["k2"] = g[["a", "b"]].max(axis=1)
    g = g.groupby(["k1", "k2"], as_index=False).agg(
        corse=("corse", "sum"), con_misura=("con_misura", "sum"),
        con_durata=("con_durata", "sum"), durata_tot=("durata_tot", "sum"),
        ritardo_tot=("ritardo_tot", "sum"), in_ritardo=("in_ritardo", "sum"))
    g = g.rename(columns={"k1": "partenza", "k2": "arrivo"})

    coppie = list(zip(g["partenza"], g["arrivo"]))
    g["km"] = [km.get(c, (float("nan"), ""))[0] for c in coppie]
    g["qualita_km"] = [km.get(c, (float("nan"), ""))[1] for c in coppie]
    g = g[g["km"].notna() & (g["km"] > 0)]

    g["durata_media_min"] = g["durata_tot"] / g["con_durata"].where(g["con_durata"] > 0)
    g["ritardo_medio_min"] = g["ritardo_tot"] / g["con_misura"].where(g["con_misura"] > 0)
    g["min_per_100km"] = g["durata_media_min"] / g["km"] * 100
    g["ritardo_per_100km"] = g["ritardo_medio_min"] / g["km"] * 100
    g["km_h_programmati"] = g["km"] / (g["durata_media_min"] / 60.0)
    g["km_h_effettivi"] = g["km"] / ((g["durata_media_min"] + g["ritardo_medio_min"]) / 60.0)
    g["pct_ritardo"] = g["in_ritardo"] / g["con_misura"].where(g["con_misura"] > 0) * 100

    g = g[(g["corse"] >= min_corse) & (g["km"] >= min_km) & g["durata_media_min"].notna()]
    return g.sort_values("min_per_100km", ascending=False).reset_index(drop=True)


def _stampa(t: pd.DataFrame, titolo: str, colonna: str, quante: int = 15,
            crescente: bool = False) -> None:
    print(f"\n{titolo}")
    print(f"{'tratta':<48} {'km':>7} {'durata':>7} {'min/100km':>10} "
          f"{'rit/100km':>10} {'km/h':>6} {'corse':>9}")
    for _, r in t.sort_values(colonna, ascending=crescente).head(quante).iterrows():
        nome = f"{r['partenza'].title()} - {r['arrivo'].title()}"[:47]
        print(f"{nome:<48} {r['km']:>7.1f} {r['durata_media_min']:>7.0f} "
              f"{r['min_per_100km']:>10.1f} {r['ritardo_per_100km']:>10.2f} "
              f"{r['km_h_programmati']:>6.1f} {r['corse']:>9,.0f}")


def main(min_corse: int, min_km: float) -> None:
    t = tabella(min_corse, min_km)
    print(f"tratte con almeno {min_corse:,} corse e {min_km:.0f} km: {len(t):,}")

    # Rete di sicurezza: una media commerciale oltre i 200 km/h in Italia non
    # esiste (la piu' alta misurata e' Firenze-Milano a 159), quindi non e' un
    # treno veloce, sono chilometri sbagliati o due stazioni confuse. Era il
    # sintomo di Milano Rogoredo-Palazzolo a 1.070 km/h, con Palazzolo messa in
    # provincia di Napoli. Si segnala, non si nasconde.
    assurde = t[t["km_h_programmati"] > 200]
    if len(assurde):
        print(f"\nATTENZIONE: {len(assurde)} tratte oltre i 200 km/h di media "
              "programmata, cioe' km o stazioni sbagliati:")
        for _, r in assurde.iterrows():
            print(f"  {r['partenza']} - {r['arrivo']}: {r['km']:.0f} km in "
                  f"{r['durata_media_min']:.0f} min = {r['km_h_programmati']:.0f} km/h "
                  f"(km {r['qualita_km']})")
    q = t["qualita_km"].value_counts()
    print("provenienza dei km: " + ", ".join(f"{k} {v:,}" for k, v in q.items()))
    peso = t["corse"].sum()
    print(f"corse coperte: {peso:,.0f}")

    print(f"\nmediana: {t['min_per_100km'].median():.1f} minuti programmati per "
          f"100 km, cioe' {t['km_h_programmati'].median():.1f} km/h; "
          f"ritardo {t['ritardo_per_100km'].median():.2f} minuti per 100 km")

    _stampa(t, "LE PIU' LENTE DI ORARIO (durata al km)", "min_per_100km")
    _stampa(t, "LE PIU' VELOCI DI ORARIO", "min_per_100km", crescente=True)
    _stampa(t, "IL RITARDO PIU' ALTO PER CHILOMETRO", "ritardo_per_100km")

    t.to_csv(USCITA, index=False, float_format="%.4f")
    print(f"\nscritto {USCITA}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-corse", type=int, default=MIN_CORSE)
    ap.add_argument("--min-km", type=float, default=MIN_KM)
    a = ap.parse_args()
    main(a.min_corse, a.min_km)
