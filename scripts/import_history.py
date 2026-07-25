# scripts/import_history.py
"""Importa nel livello bronze i dump storici giornalieri di TrainStats.

I dump sono un file JSON per giorno, con la stessa struttura del layout bronze
"legacy" che la pipeline gia' conosce (`giorno`, `timeZone`, `riassunto`,
`avvisiRFI`, `avvisiTI`, `treni`). Qui vengono riscritti nel formato bronze
canonico, `data/bronze/YYYY/MM/YYYYMMDD.csv.gz` piu' `.meta.json`, cosi' che il
resto della pipeline non debba sapere nulla della loro provenienza.

Il payload dei treni viene scritto come JSON e non come repr() Python: e' lo
stesso contenuto, ma si parsifica un ordine di grandezza piu' in fretta.

    python -m scripts.import_history --src /percorso/dump --dry-run
    python -m scripts.import_history --src /percorso/dump
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

BRONZE_COLUMNS = ["giorno", "timeZone", "riassunto", "avvisiRFI", "avvisiTI", "treni"]

# I nomi file usano piu' convenzioni a seconda dell'epoca del dump:
#   dati_2023_11_23.json   (anno primo)
#   dati_05_09_2022.json   (giorno primo)
_DATE_PATTERNS = [
    (re.compile(r"(?P<y>20\d{2})[_-](?P<m>\d{2})[_-](?P<d>\d{2})"), ("y", "m", "d")),
    (re.compile(r"(?P<d>\d{2})[_-](?P<m>\d{2})[_-](?P<y>20\d{2})"), ("y", "m", "d")),
]


def date_from_name(name: str) -> Optional[date]:
    for rx, _ in _DATE_PATTERNS:
        m = rx.search(name)
        if not m:
            continue
        try:
            return date(int(m.group("y")), int(m.group("m")), int(m.group("d")))
        except ValueError:
            continue
    return None


def date_from_payload(payload: Dict[str, Any]) -> Optional[date]:
    """La data dichiarata dentro il file, in formato gg/mm/aaaa."""
    g = str(payload.get("giorno", "")).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(g, fmt).date()
        except ValueError:
            continue
    return None


def iter_dumps(src: str) -> Iterable[str]:
    for root, _dirs, files in os.walk(src):
        for fn in sorted(files):
            if fn.lower().endswith(".json"):
                yield os.path.join(root, fn)


def bronze_paths(d: date) -> Tuple[str, str]:
    root = os.path.join("data", "bronze", f"{d.year:04d}", f"{d.month:02d}")
    tag = f"{d.year:04d}{d.month:02d}{d.day:02d}"
    return os.path.join(root, f"{tag}.csv.gz"), os.path.join(root, f"{tag}.meta.json")


def to_bronze_csv(payload: Dict[str, Any]) -> bytes:
    """Una riga con le colonne del layout legacy; i campi annidati come JSON."""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=BRONZE_COLUMNS, quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    row = {}
    for col in BRONZE_COLUMNS:
        v = payload.get(col, "")
        row[col] = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
    w.writerow(row)
    return buf.getvalue().encode("utf-8")


def import_one(path: str, overwrite: bool = False, dry_run: bool = False) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        try:
            payload = json.load(f)
        except ValueError as e:
            return {"path": path, "esito": "json non valido", "dettaglio": str(e)[:120]}

    if not isinstance(payload, dict) or "treni" not in payload:
        return {"path": path, "esito": "schema non riconosciuto",
                "dettaglio": f"chiavi={list(payload)[:6] if isinstance(payload, dict) else type(payload).__name__}"}

    d = date_from_payload(payload) or date_from_name(os.path.basename(path))
    if d is None:
        return {"path": path, "esito": "data non determinabile"}

    # La data nel nome e quella dentro il file devono concordare: se non lo fanno
    # il file e' stato rinominato o duplicato, e attribuirlo al giorno sbagliato
    # falserebbe un mese intero.
    d_name = date_from_name(os.path.basename(path))
    if d_name is not None and d_name != d:
        return {"path": path, "esito": "data incoerente",
                "dettaglio": f"nome={d_name} contenuto={d}"}

    treni = payload.get("treni") or []
    if not isinstance(treni, list) or not treni:
        return {"path": path, "esito": "nessun treno", "data": d.isoformat()}

    csv_gz, meta_path = bronze_paths(d)
    if os.path.exists(csv_gz) and not overwrite:
        return {"path": path, "esito": "gia presente", "data": d.isoformat(), "treni": len(treni)}

    if dry_run:
        return {"path": path, "esito": "da importare", "data": d.isoformat(), "treni": len(treni)}

    content = to_bronze_csv(payload)
    os.makedirs(os.path.dirname(csv_gz), exist_ok=True)
    # mtime fisso: senza questo il gzip cambia a ogni riscrittura e il file
    # risulta modificato in git anche quando i dati sono identici.
    with open(csv_gz, "wb") as fh:
        with gzip.GzipFile(fileobj=fh, mode="wb", mtime=0) as gz:
            gz.write(content)

    meta = {
        "reference_date": d.isoformat(),
        "range_start": d.isoformat(),
        "range_end": d.isoformat(),
        "extracted_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_url": f"dump_storico:{os.path.basename(path)}",
        "source_path": path,
        "bytes": len(content),
        "header": BRONZE_COLUMNS,
        "mode": "day",
        "treni": len(treni),
        "sha256": hashlib.sha256(content).hexdigest(),
    }
    with open(meta_path, "w", encoding="utf-8") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)

    return {"path": path, "esito": "importato", "data": d.isoformat(), "treni": len(treni)}


def main(src: str, overwrite: bool, dry_run: bool, limit: Optional[int]) -> None:
    esiti: Dict[str, int] = {}
    per_anno: Dict[str, int] = {}
    problemi: List[Dict[str, Any]] = []
    n = 0

    for path in iter_dumps(src):
        r = import_one(path, overwrite=overwrite, dry_run=dry_run)
        esiti[r["esito"]] = esiti.get(r["esito"], 0) + 1
        if r["esito"] in {"importato", "da importare"}:
            per_anno[r["data"][:4]] = per_anno.get(r["data"][:4], 0) + 1
        elif r["esito"] not in {"gia presente"}:
            problemi.append(r)
        n += 1
        if limit and n >= limit:
            break

    print(json.dumps({"esiti": esiti, "per_anno": dict(sorted(per_anno.items()))}, ensure_ascii=False, indent=2))
    if problemi:
        print(f"\n{len(problemi)} file non importati, primi 15:")
        for p in problemi[:15]:
            print(f"  {p['esito']}: {os.path.basename(p['path'])} {p.get('dettaglio','')}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="directory con i dump JSON (ricorsiva)")
    ap.add_argument("--overwrite", action="store_true", help="riscrive i bronze gia' presenti")
    ap.add_argument("--dry-run", action="store_true", help="non scrive nulla, riporta solo cosa farebbe")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    main(args.src, args.overwrite, args.dry_run, args.limit)
