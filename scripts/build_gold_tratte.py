# scripts/build_gold_tratte.py
"""Le tratte come le percorre un passeggero, non come le pubblica la sorgente.

La tabella origine-destinazione della pipeline principale conta una corsa solo
sulla coppia dei suoi capolinea. Chi cerca "Milano Centrale - Treviglio" trova
allora nove corse in un mese, mentre i treni che partono da Milano Centrale e
fermano a Treviglio sono **599**: le altre cinquecentonovanta non finiscono li',
proseguono per Verona (477) e per Brescia (107). La vista per tratta, cosi',
descrive un servizio che non esiste.

Qui la tratta e' invece ogni coppia ordinata di fermate della stessa corsa. Il
treno 2641 Milano-Verona compare quindi su Milano-Treviglio, Milano-Brescia,
Treviglio-Brescia e su tutte le altre coppie del suo percorso, che e'
esattamente cio' che un passeggero puo' prendere.

**Costo.** Undici fermate e mezzo per corsa fanno una sessantina di coppie a
corsa: 19,5 milioni di righe grezze in un mese. Aggregate pero' le coppie
distinte sono 77.826, perche' i treni ripetono ogni giorno gli stessi percorsi,
e sono quelle che finiscono nel gold. Il dettaglio non viene mai materializzato
tutto insieme: si aggrega un mese alla volta.

**Copertura.** Solo dove esistono le fermate, cioe' dall'11 gennaio 2020 al 26
luglio 2026. Per il 2019 e per i giorni senza dump resta la vista per capolinea.

    python -m scripts.build_gold_tratte [--months 202506 ...]
"""
from __future__ import annotations

import argparse
import glob
import os
from typing import List, Optional

import pandas as pd

from .transform_fermate import REGOLARE, SOPPRESSA
from .utils import ensure_dir, load_yaml

SORGENTE = os.path.join("data", "silver_fermate")
USCITA = os.path.join("data", "gold", "tratte")

# Sotto questo numero di corse nel mese una coppia non entra nel gold. Non e'
# una soglia di significativita' statistica ma di peso: le coppie con una corsa
# sola sono decine di migliaia, pesano quanto tutte le altre insieme e nessuno
# le cerchera' mai. Su 77.826 coppie di un mese ne restano 67.003 con dieci e
# 55.406 con trenta, ed e' trenta perche' la cartella pubblicata deve stare
# dentro il gigabyte che GitHub Pages concede a tutto il sito.
#
# E' anche la stessa soglia con cui la dashboard decide se una percentuale ha
# senso: sotto le trenta osservazioni non disegna il punto della serie, quindi
# pubblicare quelle righe servirebbe solo a farle scaricare.
#
# Si conta sulla coppia e non sulla riga: le 55.406 coppie diventano 67.897
# righe una volta divise per categoria, perche' il 92% ha una sola categoria e
# solo qualche migliaio si divide in due o tre.
MIN_CORSE_MESE = 30


def _soglia() -> int:
    try:
        return int(load_yaml("config/pipeline.yml")["punctuality"]["on_time_threshold_minutes"])
    except Exception:
        return 4


def _finestra() -> tuple:
    try:
        v = load_yaml("config/pipeline.yml")["delay_validity"]
        return float(v.get("min_minutes", -5)), float(v.get("max_minutes", 1440))
    except Exception:
        return -5.0, 1440.0


def coppie_del_mese(percorso: str, soglia: int, lo: float, hi: float) -> pd.DataFrame:
    """Aggrega un mese di fermate nelle coppie ordinate di stazioni."""
    d = pd.read_parquet(percorso, columns=[
        "mese", "data_riferimento", "numero_treno", "origine", "destinazione",
        "seq", "nome_stazione", "stato_fermata", "ritardo_arrivo_min", "categoria",
    ])
    if d.empty:
        return pd.DataFrame()

    mese = str(d["mese"].iloc[0])
    d["nome_stazione"] = d["nome_stazione"].astype(str)
    d["categoria"] = d["categoria"].astype(str)
    chiave = ["data_riferimento", "numero_treno", "origine", "destinazione"]

    # La partenza porta il nome, la posizione e la categoria del treno: cio' che
    # conta per la coppia e' come e' andato l'ARRIVO alla seconda stazione, che
    # e' il momento in cui il passeggero scende.
    da = d[chiave + ["nome_stazione", "seq", "categoria"]].rename(
        columns={"nome_stazione": "da", "seq": "seq_da"})
    a = d[chiave + ["nome_stazione", "seq", "stato_fermata", "ritardo_arrivo_min"]].rename(
        columns={"nome_stazione": "a", "seq": "seq_a"})

    m = da.merge(a, on=chiave, copy=False)
    m = m[m["seq_da"] < m["seq_a"]]
    # Un treno puo' fermare due volte nella stessa stazione: Roma S. Pietro
    # compare come origine e poi di nuovo lungo il percorso, e i circolari
    # senesi partono e arrivano a Siena. La coppia che ne esce, "da Siena a
    # Siena", e' vera nel dato e priva di senso per chi la cerca: erano 99
    # coppie e 3.662 corse.
    m = m[m["da"].astype(str) != m["a"].astype(str)]
    if m.empty:
        return pd.DataFrame()

    stato = m["stato_fermata"].astype(str)
    r = pd.to_numeric(m["ritardo_arrivo_min"], errors="coerce")
    servita = stato.eq(REGOLARE)
    valida = servita & r.notna() & r.between(lo, hi)
    dev = r.where(valida)

    m = m.assign(
        _corse=1,
        _con_misura=valida.astype("int32"),
        _in_ritardo=(valida & (dev > soglia)).astype("int32"),
        _in_orario=(valida & (dev >= 0) & (dev <= soglia)).astype("int32"),
        _in_anticipo=(valida & (dev < 0)).astype("int32"),
        _soppressa=stato.eq(SOPPRESSA).astype("int32"),
        _oltre_5=(valida & (dev >= 5)).astype("int32"),
        _oltre_10=(valida & (dev >= 10)).astype("int32"),
        _oltre_15=(valida & (dev >= 15)).astype("int32"),
        _oltre_30=(valida & (dev >= 30)).astype("int32"),
        _oltre_60=(valida & (dev >= 60)).astype("int32"),
        _minuti=dev.clip(lower=0),
    )

    g = m.groupby(["da", "a", "categoria"], observed=True)
    out = g.agg(
        corse=("_corse", "sum"),
        con_misura=("_con_misura", "sum"),
        in_ritardo=("_in_ritardo", "sum"),
        in_orario=("_in_orario", "sum"),
        in_anticipo=("_in_anticipo", "sum"),
        fermate_soppresse=("_soppressa", "sum"),
        oltre_5=("_oltre_5", "sum"),
        oltre_10=("_oltre_10", "sum"),
        oltre_15=("_oltre_15", "sum"),
        oltre_30=("_oltre_30", "sum"),
        oltre_60=("_oltre_60", "sum"),
        minuti_ritardo_tot=("_minuti", "sum"),
    ).reset_index()

    # La soglia vale sulla coppia, non sulla singola categoria. Applicarla riga
    # per riga toglierebbe l'EuroCity che passa dieci volte al mese su una
    # tratta da diecimila corse: la riga "tutte le categorie" della dashboard,
    # che e' la somma di queste, calerebbe dello 0,7% senza che nulla lo dica.
    # Cosi' invece il totale per coppia resta identico a prima e la categoria e'
    # una scomposizione esatta di quel totale.
    totale = out.groupby(["da", "a"], observed=True)["corse"].transform("sum")
    out = out[totale >= MIN_CORSE_MESE].copy()
    out.insert(0, "mese", mese)
    out["ritardo_medio"] = (out["minuti_ritardo_tot"] / out["con_misura"].where(out["con_misura"] > 0)).round(2)
    return out


def main(mesi: Optional[List[str]] = None) -> None:
    file_mese = sorted(glob.glob(os.path.join(SORGENTE, "*", "*.parquet")))
    if mesi:
        volute = set(mesi)
        file_mese = [f for f in file_mese if os.path.basename(f)[:6] in volute]
    if not file_mese:
        print("nessun silver delle fermate: lanciare prima scripts.transform_fermate")
        return

    ensure_dir(USCITA)
    soglia = _soglia()
    lo, hi = _finestra()
    righe = 0
    for f in file_mese:
        chiave = os.path.basename(f)[:6]
        d = coppie_del_mese(f, soglia, lo, hi)
        if d.empty:
            print(f"  {chiave}: nessuna coppia")
            continue
        d.to_parquet(os.path.join(USCITA, f"{chiave[:4]}-{chiave[4:]}.parquet"),
                     index=False, compression="zstd")
        righe += len(d)
        print(f"  {chiave}: {len(d):,} coppie", flush=True)
    print({"mesi": len(file_mese), "righe": righe, "uscita": USCITA})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="*")
    a = ap.parse_args()
    main(a.months)
