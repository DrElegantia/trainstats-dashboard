# scripts/completa_categoria.py
"""Completa la categoria mancante usando come sa etichettarsi lo stesso treno.

Dopo aver letto anche il campo `sub` restano 1.770 corse su 18,7 milioni senza
categoria, lo 0,009%. Non e' un difetto di estrazione: su quei record la
sorgente lascia `c` vuoto e non porta affatto la chiave `sub`. Guardando i
numeri di treno sono quasi tutti alta velocita': 9xxx sui percorsi Milano
-Salerno e Milano-Venezia, 8xxx su Roma-Lecce e Roma-Genova.

Contano poco sui totali e pero' danno fastidio in un modo preciso: il menu delle
categorie scarta la voce vuota, quindi quelle corse non sono selezionabili da
nessun filtro e la somma delle categorie non torna mai al totale.

**La regola.** Non si indovina dalla numerazione, che sarebbe una convenzione
nostra: si prende come lo stesso NUMERO DI TRENO e' etichettato altrove nei
dati, cioe' dalla sorgente stessa.

    1. la categoria piu' frequente di quel numero NELLO STESSO MESE
    2. se il mese non ne ha, la piu' frequente su tutto lo storico

L'ordine conta. Il 9% dei numeri di treno porta piu' di una categoria nell'arco
dei sette anni, quindi la regola globale da sola sbaglierebbe: dove entrambe
rispondono concordano nel 95,1% dei casi (661 su 695), e nel restante 5% quella
mensile e' la piu' vicina al vero. Il mese risolve 695 corse, il ripiego globale
altre 634, in tutto 1.329 su 1.770 (75%).

**Cosa resta.** 441 corse su 18,7 milioni, lo 0,0024%, e sappiamo cosa sono:
197 numerate 90.000-100.000, cioe' straordinari; 28 soppresse; percorsi che
compaiono una manciata di volte in sette anni, Benevento-Roma Termini (41),
Fortezza-Milano Centrale (33), Reggio Calabria-Sapri (15). La sorgente non le
etichetta da nessuna parte, in nessun giorno.

E' stata provata anche una terza via, il campo dei cambi di numerazione, che
alcune di queste corse portano: "31334,BERGAMO;31510,PIOLTELLO LIMITO;9605,
MILANO CENTRALE" dice che quel treno diventa il 9605, che sappiamo essere un
Frecciarossa. Sembrava promettente e non lo e': risolve 45 corse sull'intero
storico, ma solo 2 restando dentro il mese. Le altre 43 accostano numeri di
periodi diversi, ed e' cosi' che a un Treviglio-Roma S Pietro veniva assegnato
REG. Regola scartata: quarantatre etichette dubbie non valgono il recupero di
uno 0,0002%.

Restano quindi vuote, e va bene cosi': su quelle non c'e' niente da leggere, e
inventare una categoria sarebbe peggio che ammettere di non saperla.

    python -m scripts.completa_categoria [--dry-run]
"""
from __future__ import annotations

import argparse
import collections
import glob
import os
from typing import Dict, Tuple

import pandas as pd

SORGENTI = [
    os.path.join("data", "silver", "*", "*.parquet"),
    os.path.join("data", "silver_fermate", "*", "*.parquet"),
]
VUOTE = {"", "nan", "None", "<NA>"}


def _chiavi(percorsi) -> Tuple[Dict, Dict]:
    """Come ogni numero di treno viene etichettato, per mese e in assoluto."""
    per_mese: Dict = collections.defaultdict(collections.Counter)
    globale: Dict = collections.defaultdict(collections.Counter)
    for p in percorsi:
        d = pd.read_parquet(p, columns=["data_riferimento", "numero_treno", "categoria"])
        cat = d["categoria"].astype(str)
        num = d["numero_treno"].astype(str).str.strip()
        mese = d["data_riferimento"].astype(str).str.slice(0, 7)
        buone = ~cat.isin(VUOTE)
        for n, c, m in zip(num[buone], cat[buone], mese[buone]):
            per_mese[(n, m)][c] += 1
            globale[n][c] += 1
    return per_mese, globale


def _valida(v) -> bool:
    """Una categoria assegnabile e' una stringa che dice qualcosa.

    Il filtro non e' pleonastico: alla prima passata questo modulo assegnava
    `nan` a 123 righe, cioe' esattamente il vuoto che deve togliere. Filtrare in
    ingresso non basta, perche' basta un valore non-stringa che sfugge al
    confronto con l'insieme dei vuoti per rientrare dall'uscita.
    """
    return isinstance(v, str) and v.strip() not in VUOTE


def _scegli(per_mese, globale, numero: str, mese: str):
    for fonte, c in (("mese", per_mese.get((numero, mese))), ("storico", globale.get(numero))):
        if not c:
            continue
        for v, _ in c.most_common():
            if _valida(v):
                return v, fonte
    return None, None


def main(dry_run: bool = False) -> None:
    percorsi = sorted(p for pat in SORGENTI for p in glob.glob(pat))
    if not percorsi:
        print("nessun silver da completare")
        return

    print(f"leggo le etichette note da {len(percorsi)} file...")
    per_mese, globale = _chiavi(percorsi)

    tot_vuote = riempite = 0
    da_mese = da_storico = 0
    per_categoria: collections.Counter = collections.Counter()

    for p in percorsi:
        d = pd.read_parquet(p)
        if "categoria" not in d.columns:
            continue
        cat = d["categoria"].astype(str)
        vuote = cat.isin(VUOTE)
        if not vuote.any():
            continue
        tot_vuote += int(vuote.sum())

        num = d["numero_treno"].astype(str).str.strip()
        mese = d["data_riferimento"].astype(str).str.slice(0, 7)
        nuovo = cat.copy()
        for i in d.index[vuote]:
            scelta, fonte = _scegli(per_mese, globale, num[i], mese[i])
            if scelta is None:
                continue
            nuovo[i] = scelta
            riempite += 1
            per_categoria[scelta] += 1
            if fonte == "mese":
                da_mese += 1
            else:
                da_storico += 1

        if not dry_run and riempite:
            d["categoria"] = nuovo.astype("category")
            d.to_parquet(p, index=False, compression="zstd")

    print(f"corse senza categoria: {tot_vuote:,}")
    print(f"completate: {riempite:,} ({100 * riempite / max(tot_vuote, 1):.1f}%), "
          f"di cui {da_mese:,} dal mese e {da_storico:,} dallo storico")
    print(f"restano vuote: {tot_vuote - riempite:,}")
    if per_categoria:
        print(f"assegnazioni: {dict(per_categoria.most_common(10))}")
    if dry_run:
        print("(dry-run: nessun file scritto)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    main(a.dry_run)
