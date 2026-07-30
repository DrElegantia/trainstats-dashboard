# scripts/build_gold_fermate.py
"""Le statistiche per stazione ricavate dalle fermate, non dai capolinea.

La tabella per stazione della pipeline principale conta una corsa solo dove
nasce e dove finisce. E' l'unica cosa che il formato nuovo della sorgente
consente, ma descrive male il paese: nel giugno 2025 Pordenone risulta con
quattro corse, Abbiategrasso con nessuna, Acireale con tredici, mentre le
fermate dicono rispettivamente 1.786, 1.311 e 1.557. Una percentuale di ritardo
calcolata su quattro corse non e' una statistica, ed e' il motivo per cui la
mappa deve tenere una riserva sulle stazioni sotto le trenta corse.

Questo modulo aggrega `data/silver_fermate` per mese, stazione e categoria.
Copre da ottobre 2019 a dicembre 2025: dal 2026 la sorgente non pubblica piu'
le fermate, quindi la tabella non si aggiorna oltre quella data e va letta come
una fotografia chiusa, non come la vista corrente del servizio.

Cosa conta, e cosa no:

    fermate_servite     fermate in cui il treno si e' fermato davvero
    fermate_soppresse   fermate saltate, che la sorgente segna con "S"
    non_rilevate        passaggi non misurati ("n.d."), esclusi dalle medie
    fermate_con_misura  denominatore delle percentuali
    giorni_coperti      giorni del mese che la sorgente ha davvero pubblicato

`fermate_con_misura` e' sempre parecchio sotto `fermate_servite`, 144 milioni
contro 174, e non e' un buco: alla stazione di origine il ritardo in arrivo non
esiste, perche' il treno da li' parte e basta. Il rapporto fra le due colonne
non va quindi letto come copertura del dato.

Le soppressioni di fermata sono la ragione principale per guardare qui: in un
giorno campione compaiono in 202 treni, e in 93 di questi il campo dei
provvedimenti tace. Sono corse che la pipeline per capolinea considera regolari.

    python -m scripts.build_gold_fermate
"""
from __future__ import annotations

import argparse
import calendar
import glob
import os
from typing import List, Optional

import pandas as pd

from .transform_fermate import REGOLARE, SOPPRESSA
from .utils import ensure_dir, load_yaml

SORGENTE = os.path.join("data", "silver_fermate")
USCITA = os.path.join("data", "gold", "fermate")


def _soglia_puntualita() -> int:
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


def aggrega_mese(percorso: str, soglia: int, lo: float, hi: float) -> pd.DataFrame:
    d = pd.read_parquet(percorso, columns=[
        "mese", "categoria", "nome_stazione", "stato_fermata",
        "ritardo_arrivo_min", "prima", "ultima", "data_riferimento",
    ])
    # Quanti giorni del mese la sorgente ha davvero coperto. Dieci mesi su
    # settantadue sono parziali, e due lo sono parecchio: gennaio 2020 e
    # dicembre 2025 hanno ventuno giorni su trentuno, perche' nei primi dieci
    # giorni di dicembre la sorgente pubblicava gia' il formato senza fermate.
    # Senza questo numero il confronto con la pipeline dei capolinea sembra un
    # errore di estrazione: a dicembre 2025 Roma Termini risulta a 13.235
    # contro 19.522, che e' esattamente il rapporto fra ventuno e trentuno.
    giorni = int(d["data_riferimento"].nunique())
    chiave = str(d["mese"].iloc[0]) if len(d) else ""
    attesi = calendar.monthrange(int(chiave[:4]), int(chiave[5:7]))[1] if len(chiave) >= 7 else 0
    d = d.drop(columns=["data_riferimento"])
    # Le chiavi vanno rese stringhe piene prima del raggruppamento. I quantili
    # si riattaccano con un merge sulle colonne, e un NaN non si abbina a se
    # stesso: basterebbe un mese con la categoria nulla per far sparire p90 e
    # p95 di quelle righe senza che nulla lo segnali. Oggi la categoria vuota
    # arriva come stringa vuota (68.039 righe in un mese), ma dipendere da
    # questo e' fragile, e la pipeline principale ha gia' pagato lo stesso
    # errore in una forma diversa.
    d["nome_stazione"] = d["nome_stazione"].astype(str)
    d["categoria"] = d["categoria"].astype(str).replace({"nan": "", "None": "", "<NA>": ""})

    servita = d["stato_fermata"].astype(str).eq(REGOLARE)
    soppressa = d["stato_fermata"].astype(str).eq(SOPPRESSA)
    # Il complemento, non l'uguaglianza a "non rilevata": esiste un quarto stato,
    # "non applicabile", che vale quando ne' l'arrivo ne' la partenza esistono
    # nel payload. Sono 1.603 righe su 201,6 milioni, ma contarle a parte le
    # lasciava fuori da tutte e tre le voci e la somma non tornava mai al totale.
    # Per chi legge la tabella dicono la stessa cosa di "non rilevata": fermata
    # senza misura utilizzabile e non soppressa.
    non_rilevata = ~servita & ~soppressa

    # Il cast a float64 non e' cosmetico. La colonna arriva come Int32
    # nullable, cioe' un array di interi piu' una maschera, e sotto la maschera
    # restano valori mai inizializzati. Media e quantili li rimascherano
    # correttamente, ma il round finale moltiplica per cento tutto l'array
    # prima di guardare la maschera: da li' uscivano sei mesi su settantanove
    # con "overflow encountered in multiply". I numeri erano giusti, il
    # messaggio no, e un log che avvisa a vuoto insegna a non leggere i log.
    # In float64 il posto vuoto e' NaN e non c'e' niente da moltiplicare.
    r = pd.to_numeric(d["ritardo_arrivo_min"], errors="coerce").astype("float64")
    # Stessa finestra di validita' della pipeline principale, cosi' i due mondi
    # scartano le stesse misure impossibili e restano confrontabili.
    valida = servita & r.notna() & r.between(lo, hi)
    dev = r.where(valida)

    d = d.assign(
        _servite=servita.astype("int32"),
        _soppresse=soppressa.astype("int32"),
        _non_rilevate=non_rilevata.astype("int32"),
        _con_misura=valida.astype("int32"),
        _in_ritardo=(valida & (dev > soglia)).astype("int32"),
        _in_orario=(valida & (dev >= 0) & (dev <= soglia)).astype("int32"),
        _in_anticipo=(valida & (dev < 0)).astype("int32"),
        _capolinea=(d["prima"].astype(bool) | d["ultima"].astype(bool)).astype("int32"),
        _ritardo=dev.clip(lower=0),
        _scostamento=dev,
    )

    g = d.groupby(["mese", "nome_stazione", "categoria"], dropna=False, observed=True)
    out = g.agg(
        fermate_totali=("_servite", "size"),
        fermate_servite=("_servite", "sum"),
        fermate_soppresse=("_soppresse", "sum"),
        non_rilevate=("_non_rilevate", "sum"),
        fermate_con_misura=("_con_misura", "sum"),
        in_ritardo=("_in_ritardo", "sum"),
        in_orario=("_in_orario", "sum"),
        in_anticipo=("_in_anticipo", "sum"),
        volte_capolinea=("_capolinea", "sum"),
        ritardo_medio=("_ritardo", "mean"),
        ritardo_mediano=("_ritardo", "median"),
        scostamento_medio=("_scostamento", "mean"),
    ).reset_index()

    q = g["_ritardo"].quantile([0.9, 0.95]).unstack()
    q.columns = ["p90", "p95"]
    out = out.merge(q.reset_index(), on=["mese", "nome_stazione", "categoria"], how="left")

    for c in ("ritardo_medio", "ritardo_mediano", "scostamento_medio", "p90", "p95"):
        out[c] = out[c].round(2)
    out["giorni_coperti"] = giorni
    out["giorni_attesi"] = attesi
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
    soglia = _soglia_puntualita()
    lo, hi = _finestra()
    righe = 0
    for f in file_mese:
        d = aggrega_mese(f, soglia, lo, hi)
        chiave = os.path.basename(f)[:6]
        destinazione = os.path.join(USCITA, f"{chiave[:4]}-{chiave[4:]}.parquet")
        d.to_parquet(destinazione, index=False, compression="zstd")
        righe += len(d)
        print(f"  {chiave}: {len(d):,} righe, {d['nome_stazione'].nunique():,} stazioni")
    print({"mesi": len(file_mese), "righe": righe, "uscita": USCITA})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", nargs="*", help="YYYYMM, tutti se assente")
    a = ap.parse_args()
    main(a.months)
