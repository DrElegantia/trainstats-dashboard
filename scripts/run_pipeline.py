# scripts/run_pipeline.py
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date, timedelta
from typing import Optional, Set

from .utils import load_yaml, parse_cli_date, today_in_tz


def call(cmd: list[str]) -> None:
    print(f"\n>>> Running: {' '.join(cmd)}\n", flush=True)
    r = subprocess.run(cmd, check=False)
    if r.returncode != 0:
        raise SystemExit(r.returncode)


def month_keys_between(d0: date, d1: date) -> list[str]:
    out: Set[str] = set()
    cur = date(d0.year, d0.month, 1)
    end = date(d1.year, d1.month, 1)
    while cur <= end:
        out.add(f"{cur.year:04d}{cur.month:02d}")
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return sorted(out)


def expand_to_month_start(d0: date, d1: date) -> tuple[date, date]:
    """
    Espande l'intervallo per rigenerare il silver dall'inizio del mese coinvolto.

    Questo evita di produrre parquet mensili parziali quando l'ambiente di esecuzione
    non conserva i file silver tra un run e l'altro (es. GitHub Actions).
    """
    start_month = date(d0.year, d0.month, 1)
    return start_month, d1


def silver_path_for_month_key(month_key: str) -> str:
    y = month_key[:4]
    return os.path.join("data", "silver", y, f"{month_key}.parquet")


def ensure_silver_available(months: list[str]) -> None:
    missing = [m for m in months if not os.path.exists(silver_path_for_month_key(m))]
    if missing:
        miss = ", ".join(missing[:6]) + ("..." if len(missing) > 6 else "")
        raise SystemExit(
            "Pipeline stopped: no silver parquet found for months "
            + miss
            + ". Verifica ingest/transform e l'intervallo date richiesto."
        )


def run(start: date, end: date) -> None:
    s = start.isoformat()
    e = end.isoformat()

    # 1. Scarica dati raw
    call([sys.executable, "-m", "scripts.ingest", "--start", s, "--end", e])
    
    # 2. Trasforma in silver (month-to-date per evitare overwrite mensili parziali)
    silver_start, silver_end = expand_to_month_start(start, end)
    call(
        [
            sys.executable,
            "-m",
            "scripts.transform_silver",
            "--start",
            silver_start.isoformat(),
            "--end",
            silver_end.isoformat(),
        ]
    )

    months = month_keys_between(start, end)
    ensure_silver_available(months)

    # 2-bis. Completa le categorie che la sorgente lascia vuote, leggendo come
    # etichetta lo stesso numero di treno altrove.
    #
    # Deve stare QUI, fra il silver e il gold, e non piu' avanti: scrive il
    # silver, e se girasse dopo build_gold il gold resterebbe quello di prima,
    # cioe' il completamento non arriverebbe mai a nessuna tabella. Ci era
    # finito dopo alla prima stesura.
    #
    # Sul mese appena riscritto, perche' il passo 2 rigenera dal bronze il silver
    # del mese in corso: senza, ogni notte ricomparirebbero le corse senza
    # categoria gia' completate, e il difetto tornerebbe un mese alla volta.
    call([sys.executable, "-m", "scripts.completa_categoria", "--months", *months])

    # 3. Costruisci aggregazioni gold
    call([sys.executable, "-m", "scripts.build_gold", "--months", *months])

    # 4. Costruisci dimensione stazioni
    call([sys.executable, "-m", "scripts.build_station_dim"])

    # 4-bis. Su quante giornate del mese esiste ciascuna categoria. Va qui e non
    # fuori dal notturno, al contrario del ramo fermate: legge il silver, che il
    # passo 2 ha appena riscritto, e se resta ferma la dashboard smette di sapere
    # quali percentuali per categoria sono rappresentative del mese.
    call([sys.executable, "-m", "scripts.copertura_categoria", "--months", *months])

    # 5. Copia tutto in docs/data per GitHub Pages
    call([sys.executable, "-m", "scripts.build_site"])

    # Il ramo delle fermate (transform_fermate, build_gold_fermate,
    # build_gold_tratte) NON sta qui, e non e' una dimenticanza. Vive sul campo
    # `fr` del payload legacy, che la sorgente ha smesso di pubblicare: la
    # copertura finisce al 26 luglio 2026 e non avanzera' piu'. Rilanciarlo ogni
    # notte rifarebbe lo stesso identico risultato bruciando un'ora di CI.
    #
    # Perche' allora le tratte compaiono sul sito? Perche' data/gold/tratte/ e'
    # versionato, e build_site qui sopra lo ripubblica a ogni giro. E' una
    # fotografia versionata, non un derivato che si rigenera: se sparisse dal
    # repo la CI non saprebbe ricostruirla, perche' data/silver_fermate/ (1,2 GB)
    # e' ignorato per peso.


def main(start: Optional[str], end: Optional[str]) -> None:
    cfg = load_yaml("config/pipeline.yml")
    tz_name = cfg["project"]["timezone"]

    if start:
        d0 = parse_cli_date(start, 'start date')
        d1 = parse_cli_date(end, 'end date') if end else d0
    else:
        today = today_in_tz(tz_name)
        d0 = today - timedelta(days=1)
        d1 = d0

    run(d0, d1)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=False, help="YYYY-MM-DD")
    ap.add_argument("--end", required=False, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.start, args.end)
