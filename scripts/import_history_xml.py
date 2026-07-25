# scripts/import_history_xml.py
"""Importa nel livello bronze i dump XML piu' vecchi di TrainStats.

Sono i dati del sito precedente, un file XML per giorno da ottobre 2019 a
febbraio 2020: il periodo pre-COVID, che nessun'altra fonte copre. La struttura
e' piatta e non ha nulla in comune con il JSON delle epoche successive:

    <Table1>
      <numTreno>12823</numTreno>
      <stazPart>MODICA</stazPart>      <oraPart>05:03</oraPart>
      <ritardoPart>0</ritardoPart>     <stazArr>AUGUSTA</stazArr>
      <oraArr>07:26</oraArr>           <ritardoArr>1</ritardoArr>
      <provvedimenti />                <deviazioni_limitazioni />
      <categoria>REG</categoria>
    </Table1>

Invece di insegnare alla pipeline un terzo schema, qui l'XML viene tradotto nel
payload JSON che la pipeline gia' conosce, e scritto nel formato bronze
canonico. Da li' in poi non esiste piu' nessuna differenza fra le epoche.

Due dettagli che l'XML non risolve da solo:

- gli orari sono `HH:MM` senza data: la data viene dal nome del file, che e'
  `DD_MM_YYYY.xml`;
- se l'ora di arrivo precede quella di partenza la corsa scavalca la
  mezzanotte, e l'arrivo va portato al giorno dopo. Senza questa correzione la
  durata programmata risulterebbe negativa.

    python -m scripts.import_history_xml --src /percorso/xml --dry-run
    python -m scripts.import_history_xml --src /percorso/xml
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import zipfile
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree

from .import_history import BRONZE_COLUMNS, bronze_paths, to_bronze_csv

try:
    from zoneinfo import ZoneInfo
    _ROMA = ZoneInfo("Europe/Rome")
except Exception:  # pragma: no cover
    _ROMA = None

_NOME_FILE = re.compile(r"(?P<d>\d{2})[_-](?P<m>\d{2})[_-](?P<y>20\d{2})")

# Nomi dei tag XML -> chiavi del payload JSON usato dal resto della pipeline.
_CAMPI = {
    "numTreno": "n",
    "stazPart": "p",
    "stazArr": "a",
    "ritardoPart": "rp",
    "ritardoArr": "ra",
    "provvedimenti": "pr",
    "deviazioni_limitazioni": "dl",
    "categoria": "c",
}


def data_da_nome(nome: str) -> Optional[date]:
    m = _NOME_FILE.search(os.path.basename(nome))
    if not m:
        return None
    try:
        return date(int(m.group("y")), int(m.group("m")), int(m.group("d")))
    except ValueError:
        return None


def _epoch(giorno: date, orario: str, giorno_dopo: bool = False) -> int:
    """`HH:MM` piu' la data del file -> epoch secondi, fuso Europe/Rome."""
    orario = (orario or "").strip()
    if not orario or ":" not in orario:
        return 0
    try:
        h, mi = (int(x) for x in orario.split(":")[:2])
    except ValueError:
        return 0
    d = giorno + timedelta(days=1) if giorno_dopo else giorno
    dt = datetime(d.year, d.month, d.day, h % 24, mi % 60)
    if _ROMA is not None:
        dt = dt.replace(tzinfo=_ROMA)
    else:  # pragma: no cover
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _minuti(orario: str) -> Optional[int]:
    orario = (orario or "").strip()
    if ":" not in orario:
        return None
    try:
        h, mi = (int(x) for x in orario.split(":")[:2])
        return h * 60 + mi
    except ValueError:
        return None


def treni_da_xml(contenuto: bytes, giorno: date) -> List[Dict[str, Any]]:
    radice = ElementTree.fromstring(contenuto)
    treni: List[Dict[str, Any]] = []
    for nodo in radice.iter("Table1"):
        rec: Dict[str, Any] = {}
        for tag, chiave in _CAMPI.items():
            el = nodo.find(tag)
            rec[chiave] = (el.text or "").strip() if el is not None and el.text else ""
        if not rec.get("n"):
            continue

        op = nodo.find("oraPart")
        oa = nodo.find("oraArr")
        s_part = (op.text or "").strip() if op is not None and op.text else ""
        s_arr = (oa.text or "").strip() if oa is not None and oa.text else ""

        m_part, m_arr = _minuti(s_part), _minuti(s_arr)
        scavalca = m_part is not None and m_arr is not None and m_arr < m_part

        rec["op"] = _epoch(giorno, s_part)
        rec["oa"] = _epoch(giorno, s_arr, giorno_dopo=scavalca)
        # Campi che l'XML non porta: restano vuoti, come per le corse in cui la
        # sorgente piu' recente non li valorizza.
        rec.setdefault("cn", "")
        rec["oo"] = ""
        rec["od"] = ""
        treni.append(rec)
    return treni


def _payload(giorno: date, treni: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "giorno": giorno.strftime("%d/%m/%Y"),
        "timeZone": "Europe/Rome",
        "riassunto": "",
        "avvisiRFI": "",
        "avvisiTI": "",
        "treni": treni,
    }


def sorgenti(src: str) -> Iterable[Tuple[str, bytes]]:
    """Restituisce (nome, contenuto) da una cartella di XML o da uno zip."""
    if os.path.isfile(src) and src.lower().endswith(".zip"):
        with zipfile.ZipFile(src) as z:
            for nome in sorted(z.namelist()):
                if nome.lower().endswith(".xml"):
                    yield nome, z.read(nome)
        return
    for root, _dirs, files in os.walk(src):
        for fn in sorted(files):
            if fn.lower().endswith(".xml"):
                with open(os.path.join(root, fn), "rb") as fh:
                    yield fn, fh.read()


def importa_uno(nome: str, contenuto: bytes, overwrite: bool, dry_run: bool) -> Dict[str, Any]:
    giorno = data_da_nome(nome)
    if giorno is None:
        return {"nome": nome, "esito": "data non determinabile"}

    try:
        treni = treni_da_xml(contenuto, giorno)
    except ElementTree.ParseError as e:
        return {"nome": nome, "esito": "xml non valido", "dettaglio": str(e)[:120]}

    if not treni:
        return {"nome": nome, "esito": "nessun treno", "data": giorno.isoformat()}

    csv_gz, meta_path = bronze_paths(giorno)
    if os.path.exists(csv_gz) and not overwrite:
        return {"nome": nome, "esito": "gia presente", "data": giorno.isoformat(), "treni": len(treni)}
    if dry_run:
        return {"nome": nome, "esito": "da importare", "data": giorno.isoformat(), "treni": len(treni)}

    contenuto_csv = to_bronze_csv(_payload(giorno, treni))
    os.makedirs(os.path.dirname(csv_gz), exist_ok=True)
    with open(csv_gz, "wb") as fh:
        with gzip.GzipFile(fileobj=fh, mode="wb", mtime=0) as gz:
            gz.write(contenuto_csv)

    meta = {
        "reference_date": giorno.isoformat(),
        "range_start": giorno.isoformat(),
        "range_end": giorno.isoformat(),
        "extracted_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_url": f"dump_storico_xml:{os.path.basename(nome)}",
        "bytes": len(contenuto_csv),
        "header": BRONZE_COLUMNS,
        "mode": "day",
        "treni": len(treni),
        "sha256": hashlib.sha256(contenuto_csv).hexdigest(),
    }
    with open(meta_path, "w", encoding="utf-8") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)

    return {"nome": nome, "esito": "importato", "data": giorno.isoformat(), "treni": len(treni)}


def main(src: str, overwrite: bool, dry_run: bool, limit: Optional[int]) -> None:
    esiti: Dict[str, int] = {}
    per_mese: Dict[str, int] = {}
    treni_tot = 0
    problemi: List[Dict[str, Any]] = []
    n = 0

    for nome, contenuto in sorgenti(src):
        r = importa_uno(nome, contenuto, overwrite, dry_run)
        esiti[r["esito"]] = esiti.get(r["esito"], 0) + 1
        if r["esito"] in {"importato", "da importare"}:
            per_mese[r["data"][:7]] = per_mese.get(r["data"][:7], 0) + 1
            treni_tot += r.get("treni", 0)
        elif r["esito"] != "gia presente":
            problemi.append(r)
        n += 1
        if limit and n >= limit:
            break

    print(json.dumps({"esiti": esiti, "per_mese": dict(sorted(per_mese.items())),
                      "treni_totali": treni_tot}, ensure_ascii=False, indent=2))
    if problemi:
        print(f"\n{len(problemi)} file non importati, primi 15:")
        for p in problemi[:15]:
            print(f"  {p['esito']}: {p['nome']} {p.get('dettaglio', '')}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="cartella con gli XML, oppure lo zip che li contiene")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    main(args.src, args.overwrite, args.dry_run, args.limit)
