# scripts/correggi_stato_fermate.py
"""Ricalcola `stato_fermata` sui parquet gia' estratti, senza rileggere il bronze.

La prima versione di `transform_fermate` prendeva lo stato della fermata dallo
stato del solo arrivo. Alla stazione di origine l'arrivo non esiste e la
sorgente scrive "N", quindi 231.229 prime fermate al mese finivano marcate
"non applicabile" pur avendo tutte un ritardo di partenza valido: sparivano
dalle fermate servite, e con loro meta' del peso delle stazioni di testa.

Riestrarre dal bronze costerebbe un'ora e mezza; non serve, perche' lo stato
corretto si ricava da cio' che il parquet gia' contiene:

    soppressa      resta soppressa, e' l'unico stato che i ritardi non dicono
    regolare       almeno uno fra arrivo e partenza porta una misura
    il resto       come prima, non rilevata o non applicabile

Da lanciare una volta sola sui file prodotti prima della correzione.

    python -m scripts.correggi_stato_fermate
"""
from __future__ import annotations

import argparse
import glob
import os
from typing import List, Optional

import pandas as pd

from .transform_fermate import NON_APPLICABILE, NON_RILEVATA, RADICE, REGOLARE, SOPPRESSA


def correggi(percorso: str) -> tuple:
    d = pd.read_parquet(percorso)
    if "stato_fermata" not in d.columns:
        return 0, 0

    prima = d["stato_fermata"].astype(str)
    ha_misura = d["ritardo_arrivo_min"].notna() | d["ritardo_partenza_min"].notna()

    dopo = prima.copy()
    da_promuovere = ha_misura & prima.ne(SOPPRESSA) & prima.ne(REGOLARE)
    dopo = dopo.mask(da_promuovere, REGOLARE)
    # Chi non ha misure e non era soppresso resta come stava: la distinzione fra
    # "non rilevata" e "non applicabile" viene dal payload e non e' ricostruibile
    # da qui, ma nessuna delle due entra nelle statistiche.
    cambiate = int((dopo != prima).sum())
    if cambiate:
        d["stato_fermata"] = dopo.astype("category")
        d.to_parquet(percorso, index=False, compression="zstd")
    return len(d), cambiate


def main(mesi: Optional[List[str]] = None) -> None:
    file_mese = sorted(glob.glob(os.path.join(RADICE, "*", "*.parquet")))
    if mesi:
        volute = set(mesi)
        file_mese = [f for f in file_mese if os.path.basename(f)[:6] in volute]
    if not file_mese:
        print("nessun parquet delle fermate da correggere")
        return

    tot = corrette = 0
    for f in file_mese:
        righe, cambiate = correggi(f)
        tot += righe
        corrette += cambiate
        stato = f"{cambiate:,} fermate promosse a regolare" if cambiate else "gia' corretto"
        print(f"  {os.path.basename(f)[:6]}: {stato}")
    print({"file": len(file_mese), "righe": tot, "corrette": corrette})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="*")
    a = ap.parse_args()
    main(a.months)
