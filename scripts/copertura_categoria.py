# scripts/copertura_categoria.py
"""Su quanti GIORNI del mese esiste ciascuna categoria di treno.

La soglia di significativita' della dashboard conta le corse: sotto trenta in un
mese non disegna il punto. Non basta, perche' una categoria puo' avere mille
corse concentrate in un giorno solo.

E' il caso delle Frecce. La sorgente ha due formati di payload: quello vecchio
non le pubblica affatto, quello nuovo si'. Su 2.481 giorni di storico i giorni
in formato nuovo sono sei, e le Frecce esistono **solo** in quei sei, dove sono
circa duecento al giorno, che e' il livello di servizio reale. Risultato: chi
scelgieva "FR - Freccia Rossa" leggeva "52,5% in ritardo a giugno 2026", un
numero preciso e credibile ricavato da un unico giorno, il 28 giugno. Le 202
corse superavano la soglia delle trenta e il punto veniva disegnato.

Questa tabella dice, per ogni mese e categoria, su quanti giorni distinti la
categoria compare e quanti giorni ha il mese nei dati. Il browser puo' cosi'
distinguere "poche corse" da "poche giornate", che sono due difetti diversi:
il primo rende la percentuale instabile, il secondo la rende non
rappresentativa, e la seconda cosa non si corregge aspettando piu' dati.

    python -m scripts.copertura_categoria
"""
from __future__ import annotations

import argparse
import glob
import os
from typing import List, Optional

import pandas as pd

SORGENTE = os.path.join("data", "silver")
USCITA = os.path.join("data", "gold", "copertura_categoria.csv")


def copertura_del_mese(percorso: str) -> pd.DataFrame:
    d = pd.read_parquet(percorso, columns=["data_riferimento", "categoria"])
    if d.empty:
        return pd.DataFrame()
    g = d["data_riferimento"].astype(str)
    mese = g.iloc[0][:7]
    giorni_mese = g.nunique()
    out = (d.assign(_g=g)
             .groupby(d["categoria"].astype(str).fillna(""), observed=True)["_g"]
             .nunique()
             .reset_index())
    out.columns = ["categoria", "giorni_con_dati"]
    out.insert(0, "mese", mese)
    out["giorni_nel_mese"] = giorni_mese
    return out


def main(mesi: Optional[List[str]] = None) -> None:
    file_mese = sorted(glob.glob(os.path.join(SORGENTE, "*", "*.parquet")))
    if mesi:
        volute = set(mesi)
        file_mese = [f for f in file_mese if os.path.basename(f)[:6] in volute]
    if not file_mese:
        print("nessun silver da leggere")
        return

    pezzi = [copertura_del_mese(f) for f in file_mese]
    pezzi = [p for p in pezzi if not p.empty]
    if not pezzi:
        print("nessuna copertura calcolabile")
        return

    d = pd.concat(pezzi, ignore_index=True)

    # Con --months si ricalcolano solo quei mesi, ma il file resta completo: si
    # fondono le righe nuove sopra quelle vecchie invece di sostituire la tabella
    # con il solo mese chiesto. Senza questo, il run notturno (che passa il mese
    # in corso) avrebbe ridotto 813 righe a una decina, e la dashboard avrebbe
    # perso la copertura di tutto lo storico.
    if mesi and os.path.exists(USCITA):
        vecchie = pd.read_csv(USCITA, dtype={"categoria": str})
        vecchie["categoria"] = vecchie["categoria"].fillna("")
        rifatti = set(d["mese"].astype(str))
        vecchie = vecchie[~vecchie["mese"].astype(str).isin(rifatti)]
        d = pd.concat([vecchie, d], ignore_index=True)

    d = d.sort_values(["mese", "categoria"])
    os.makedirs(os.path.dirname(USCITA), exist_ok=True)
    d.to_csv(USCITA, index=False)

    # Le categorie che esistono su meno di un decimo delle giornate: sono quelle
    # su cui la dashboard non deve pronunciarsi, e vale la pena vederle scritte.
    scarse = d[d["giorni_con_dati"] < d["giorni_nel_mese"] * 0.1]
    print(f"{len(d):,} righe, {d['mese'].nunique()} mesi -> {USCITA}")
    if not scarse.empty:
        per_cat = scarse.groupby("categoria").size().sort_values(ascending=False)
        print(f"categorie con copertura sotto il 10% delle giornate, per mese: "
              f"{dict(per_cat.head(8))}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="*")
    a = ap.parse_args()
    main(a.months)
