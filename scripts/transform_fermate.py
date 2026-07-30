# scripts/transform_fermate.py
"""Il livello silver delle singole fermate, non dei soli capolinea.

La pipeline principale tiene di ogni corsa l'origine e la destinazione: e' cio'
che il formato nuovo della sorgente pubblica, ed e' il motivo per cui la
dashboard conosce 317 stazioni su 1.783. Pordenone risulta di transito e con
poche centinaia di corse non perche' ci passino pochi treni, ma perche' nessun
treno ci nasce o ci finisce.

Il formato vecchio, pero', porta dentro il payload l'elenco completo delle
fermate, ciascuna con il proprio ritardo in arrivo e in partenza. Questo modulo
lo estrae. Due limiti da tenere presenti prima di costruirci sopra:

1. **Va dal 1 gennaio 2020 al 30 aprile 2026.** Prima il payload legacy non
   porta il campo `fr` (nel 2019 le chiavi di un treno sono altre e le fermate
   non ci sono); l'API dal 2026 pubblica solo i capolinea, e i mesi da gennaio
   ad aprile 2026 esistono qui perche' importati dai dump giornalieri
   distribuiti a parte dall'autore di TrainStats. Senza nuovi dump il ramo non
   avanza: e' una fotografia, non un sostituto della pipeline giornaliera.
2. **Pesa circa otto volte.** Undici fermate e mezzo per corsa: 66.228 righe in
   un giorno contro 5.586 corse.
3. **Perde circa mezzo punto percentuale di corse.** Il payload di qualche
   corsa arriva senza il campo `fr`: i capolinea ci sono, le fermate no, e
   questo ramo non puo' vederla. Su un giorno campione sono 48 corse su 8.860,
   lo 0,54%. La conta esce a fine esecuzione, perche' a valle riemerge come un
   confronto che non torna (Milano Cadorna: 12.138 fermate contro 12.251 corse
   da capolinea) e senza il numero non e' spiegabile.

Sui valori non numerici di `ra`/`rp`, che la sorgente usa per dire cose diverse:

    "S"     la fermata e' stata soppressa, il treno non ci si e' fermato
    "N"     non applicabile: l'arrivo alla prima fermata, la partenza dall'ultima
    "n.d."  non disponibile, il passaggio non e' stato rilevato

La distinzione conta: "S" e' un disservizio da contare, "n.d." e' un buco di
misura da escludere, e prima erano indistinguibili perche' nessuna delle due
usciva dal payload. In un giorno campione le fermate soppresse compaiono in 202
treni, e in 93 di questi il campo dei provvedimenti non dice nulla: sono corse
che oggi risultano regolari.

    python -m scripts.transform_fermate --start 2025-06-01 --end 2025-06-30
"""
from __future__ import annotations

import argparse
import gzip
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .silver_schema import _categoria, parse_treni_payload
from .transform_silver import list_bronze_files_for_range
from .utils import date_range_inclusive, ensure_dir, normalize_station_name

RADICE = os.path.join("data", "silver_fermate")

# Stato di una fermata, ricavato dai valori non numerici della sorgente.
REGOLARE = "regolare"
SOPPRESSA = "soppressa"
NON_RILEVATA = "non_rilevata"
NON_APPLICABILE = "non_applicabile"

# Nessun ritardo ferroviario plausibile supera una settimana. Oltre non e' una
# misura, e' spazzatura del feed, e lasciarla passare rompe il cast a intero.
_ORIZZONTE_MINUTI = 7 * 24 * 60

_NON_NUMERICI = {
    "S": SOPPRESSA,
    "N": NON_APPLICABILE,
    "n.d.": NON_RILEVATA,
    "": NON_RILEVATA,
    "None": NON_RILEVATA,
}


def _leggi_minuti(v: Any) -> Tuple[Optional[int], str]:
    """Il ritardo in minuti e lo stato che la sorgente gli attribuisce."""
    s = str(v).strip()
    if s in _NON_NUMERICI:
        return None, _NON_NUMERICI[s]
    try:
        return int(float(s)), REGOLARE
    except (TypeError, ValueError):
        return None, NON_RILEVATA


def _stato_fermata(stato_arrivo: str, stato_partenza: str) -> str:
    """Lo stato della fermata, che non e' lo stato di uno dei due movimenti.

    Alla stazione di origine l'arrivo non esiste e la sorgente scrive "N": la
    fermata pero' e' servita, il treno e' li' e riparte con il suo ritardo di
    partenza. Prendere lo stato del solo arrivo marcava 231.229 prime fermate
    come "non applicabile" su un mese, cioe' toglieva dalle fermate servite
    tutte le partenze, che per una stazione di testa come Milano Centrale o
    Roma Termini sono meta' del suo peso. Le 232.718 fermate cosi' marcate
    avevano tutte, senza eccezione, un ritardo di partenza valido.

    La regola: una soppressione vince su tutto, poi basta un movimento misurato
    perche' la fermata sia servita, e "non applicabile" resta solo dove nessuno
    dei due lati esiste.
    """
    if SOPPRESSA in (stato_arrivo, stato_partenza):
        return SOPPRESSA
    if REGOLARE in (stato_arrivo, stato_partenza):
        return REGOLARE
    if NON_RILEVATA in (stato_arrivo, stato_partenza):
        return NON_RILEVATA
    return NON_APPLICABILE


def percorso_mese(d: date) -> str:
    radice = os.path.join(RADICE, f"{d.year:04d}")
    ensure_dir(radice)
    return os.path.join(radice, f"{d.year:04d}{d.month:02d}.parquet")


def fermate_di_un_giorno(csv_gz: str, meta_path: str, giorno: date) -> pd.DataFrame:
    with gzip.open(csv_gz, "rb") as f:
        grezzo = pd.read_csv(f, dtype=str, on_bad_lines="skip")
    if "treni" not in grezzo.columns or grezzo.empty:
        # Formato nuovo: nessuna fermata da estrarre, e non e' un errore.
        return pd.DataFrame()

    righe: List[Dict[str, Any]] = []
    corse = 0
    senza = 0
    for _, r in grezzo.iterrows():
        for t in parse_treni_payload(r.get("treni")):
            corse += 1
            fr = t.get("fr") or []
            if not fr:
                # Circa mezzo punto percentuale delle corse arriva senza elenco
                # fermate: la riga esiste, i capolinea ci sono, il payload no.
                # Sono corse che il ramo dei capolinea conta e questo non puo'
                # contare, ed e' la ragione per cui Milano Cadorna chiude un mese
                # con 12.138 fermate contro 12.251 corse da capolinea. Non e'
                # recuperabile, ma va detto: un ammanco dello 0,5% che sparisce
                # in silenzio diventa, a valle, un confronto che non torna e
                # nessuno sa spiegare.
                senza += 1
                continue
            ultimo = len(fr) - 1
            numero = str(t.get("n", "")).strip()
            # Stessa regola del ramo principale: per l'alta velocita' il campo
            # `c` e' vuoto e la sigla sta in `sub`. Senza questa riga le tratte
            # per fermata avrebbero le Frecce senza categoria mentre i capolinea
            # le hanno, e il filtro darebbe due risposte diverse sulla stessa
            # corsa a seconda della vista.
            categoria = _categoria(t)
            origine = normalize_station_name(str(t.get("p", "")))
            destinazione = normalize_station_name(str(t.get("a", "")))
            for i, f in enumerate(fr):
                ra, stato_a = _leggi_minuti(f.get("ra"))
                rp, stato_p = _leggi_minuti(f.get("rp"))
                stato = _stato_fermata(stato_a, stato_p)
                righe.append({
                    "data_riferimento": giorno.isoformat(),
                    "numero_treno": numero,
                    "categoria": categoria,
                    "origine": origine,
                    "destinazione": destinazione,
                    "seq": i,
                    "prima": i == 0,
                    "ultima": i == ultimo,
                    "nome_stazione": normalize_station_name(str(f.get("n", ""))),
                    "ritardo_arrivo_min": ra,
                    "ritardo_partenza_min": rp,
                    "stato_fermata": stato,
                    "ts_arrivo_prog": f.get("oa") or None,
                    "ts_partenza_prog": f.get("op") or None,
                })

    if not righe:
        return pd.DataFrame()

    d = pd.DataFrame(righe)
    d.attrs["corse"] = corse
    d.attrs["senza_fermate"] = senza
    d["mese"] = f"{giorno.year:04d}-{giorno.month:02d}"
    for c in ("ritardo_arrivo_min", "ritardo_partenza_min"):
        v = pd.to_numeric(d[c], errors="coerce")
        # Fuori orizzonte si butta, come per le date nel silver delle corse. Nel
        # bronze di aprile 2024 compare un rp di 28.964.515.204 minuti, che sono
        # cinquantacinquemila anni: un record solo, e faceva fallire il cast
        # dell'intero mese con "cannot safely cast non-equivalent float64 to
        # int32". Il worker moriva, il mese non veniva scritto, e con la stampa
        # in coda al parallelismo il buco si notava solo contando i file.
        d[c] = v.where(v.abs() <= _ORIZZONTE_MINUTI).astype("Int32")
    for c in ("categoria", "stato_fermata", "nome_stazione", "origine", "destinazione"):
        d[c] = d[c].astype("category")
    return d


def _un_mese(args: Tuple[str, List[Tuple[date, str, str]]]) -> Tuple[str, int, int, int]:
    chiave, file_del_mese = args
    pezzi = [fermate_di_un_giorno(csv_gz, meta, giorno) for giorno, csv_gz, meta in file_del_mese]
    pezzi = [p for p in pezzi if not p.empty]
    if not pezzi:
        return chiave, 0, 0, 0
    corse = sum(int(p.attrs.get("corse", 0)) for p in pezzi)
    senza = sum(int(p.attrs.get("senza_fermate", 0)) for p in pezzi)
    d = pd.concat(pezzi, ignore_index=True)
    # Una corsa puo' comparire in due snapshot dello stesso giorno: la chiave e'
    # il treno con la sua fermata, dentro la sua giornata.
    d = d.drop_duplicates(subset=["data_riferimento", "numero_treno", "origine", "destinazione", "seq"], keep="last")
    giorno = date.fromisoformat(d["data_riferimento"].iloc[0])
    d.to_parquet(percorso_mese(giorno), index=False, compression="zstd")
    return chiave, len(d), corse, senza


def main(start: str, end: Optional[str] = None, jobs: Optional[int] = None) -> None:
    d0 = date.fromisoformat(start)
    d1 = date.fromisoformat(end) if end else d0
    file_totali = list_bronze_files_for_range(d0, d1)
    if not file_totali:
        print("nessun file bronze nell'intervallo")
        return

    per_mese: Dict[str, List[Tuple[date, str, str]]] = {}
    for giorno, csv_gz, meta in file_totali:
        per_mese.setdefault(f"{giorno.year:04d}{giorno.month:02d}", []).append((giorno, csv_gz, meta))

    lavori = sorted(per_mese.items())
    # Tre processi, non uno per core: un mese sta in memoria tutto insieme prima
    # di essere scritto, e sono quasi tre milioni di righe.
    n = jobs or min(3, max(1, (os.cpu_count() or 2) - 1))
    scritti = 0
    corse_tot = 0
    senza_tot = 0
    esiti: Dict[str, int] = {}
    if n > 1 and len(lavori) > 1:
        with ProcessPoolExecutor(max_workers=n) as ex:
            esecuzione = ex.map(_un_mese, lavori)
            for chiave, righe, corse, senza in esecuzione:
                esiti[chiave] = righe
                scritti += righe
                corse_tot += corse
                senza_tot += senza
    else:
        for lavoro in lavori:
            chiave, righe, corse, senza = _un_mese(lavoro)
            esiti[chiave] = righe
            scritti += righe
            corse_tot += corse
            senza_tot += senza

    vuoti = []
    for chiave, _ in lavori:
        righe = esiti.get(chiave, 0)
        print(f"  fermate {chiave}: {righe:,} righe" if righe else f"  fermate {chiave}: nessuna fermata nel payload")
        if not righe:
            vuoti.append(chiave)

    # Un mese senza fermate puo' essere legittimo: fino a dicembre 2019 il
    # payload legacy non porta il campo, e dal 2026 non lo porta piu' nessuno.
    # Ma puo' anche essere un worker morto su un record malformato, ed e' il
    # caso che va detto ad alta voce invece di lasciare un buco che si scopre
    # contando i file, come e' successo alla prima passata su tutto lo storico:
    # diciassette mesi mancanti su settantacinque per un solo ritardo fuori
    # scala, e l'errore invisibile perche' la stampa arrivava dopo.
    if vuoti:
        print(f"ATTENZIONE: {len(vuoti)} mesi senza righe: {vuoti}")
        print("            se cadono fra il 2020 e il 2025 rilanciarli con --jobs 1 e leggere l'errore")

    # L'ammanco strutturale, detto invece che lasciato scoprire a valle.
    if corse_tot:
        quota = 100.0 * senza_tot / corse_tot
        print(f"corse senza elenco fermate: {senza_tot:,} su {corse_tot:,} ({quota:.2f}%), "
              f"invisibili a questo ramo e presenti in quello dei capolinea")
    print({"mesi": len(lavori), "righe": scritti, "mesi_vuoti": len(vuoti),
           "corse_senza_fermate": senza_tot})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=False, help="YYYY-MM-DD")
    ap.add_argument("--jobs", type=int, default=None)
    a = ap.parse_args()
    main(a.start, a.end, a.jobs)
