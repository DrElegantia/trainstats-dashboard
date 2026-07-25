from __future__ import annotations

import ast
import hashlib
import json
from typing import Any, Dict, List

import pandas as pd

from .utils import normalize_station_name


def epoch_to_it_string(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s == "" or s == "0":
        return ""
    try:
        ts = pd.to_datetime(int(float(s)), unit="s", utc=True).tz_convert("Europe/Rome")
        return ts.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return ""


def parse_treni_payload(x: Any) -> List[Dict[str, Any]]:
    if x is None:
        return []
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return []

    parsed = None
    # I bronze storici contengono il repr() Python della lista (apici singoli),
    # che si legge solo con ast.literal_eval: su un payload da 12 MB significa
    # ~4 secondi, quasi tutti spesi a compilare. I file scritti da
    # scripts.import_history usano invece JSON, che qui viene riconosciuto e
    # letto un ordine di grandezza piu' velocemente.
    if s.startswith("["):
        try:
            parsed = json.loads(s)
        except ValueError:
            parsed = None

    if parsed is None:
        try:
            parsed = ast.literal_eval(s)
        except Exception:
            return []

    if isinstance(parsed, list):
        return [r for r in parsed if isinstance(r, dict)]
    return []


def missing_station_code(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() in {"nan", "none", "null"}


def code_from_station_name(name: Any) -> str:
    n = normalize_station_name(name)
    if not n:
        return ""
    digest = hashlib.sha1(n.encode("utf-8")).hexdigest()[:12].upper()
    return f"N_{digest}"


def epoch_series_to_it_string(values: pd.Series) -> pd.Series:
    """Vectorized epoch -> "dd/mm/YYYY HH:MM" conversion in Europe/Rome.

    epoch_to_it_string() built and formatted one Timestamp per cell, the same
    per-row pandas construction cost that dominated the silver build.
    """
    num = pd.to_numeric(values, errors="coerce")
    num = num.where(num.notna() & (num != 0))
    ts = pd.to_datetime(num, unit="s", utc=True, errors="coerce")
    return ts.dt.tz_convert("Europe/Rome").dt.strftime("%d/%m/%Y %H:%M").fillna("")


# Keys carried by the "treni" payload of the legacy bronze layout:
#   pr       provvedimento on the whole run ("Soppresso")
#   dl       free-text variation ("Treno cancellato da X a Y. Arriva a X")
#   cn       renumbering ("25455,CHIASSO")
#   oo / od  originally scheduled origin / destination when the run was limited
# Only c/n/p/a/op/oa/rp/ra were read, so every status signal in the legacy
# files was thrown away and those months reported exactly zero cancellations.
_PAYLOAD_COLUMNS = {
    "Categoria": "c",
    "Numero treno": "n",
    "Nome stazione partenza": "p",
    "Nome stazione arrivo": "a",
    "Ritardo partenza": "rp",
    "Ritardo arrivo": "ra",
    "Cambi numerazione": "cn",
    "Provvedimenti": "pr",
    "Variazioni": "dl",
    "Origine programmata": "oo",
    "Destinazione programmata": "od",
}

_EMPTY_COLUMNS = [
    "Codice stazione partenza",
    "Codice stazione arrivo",
    "Stazione estera partenza",
    "Orario estero partenza",
    "Stazione estera arrivo",
    "Orario estero arrivo",
]

_CARRIED_COLUMNS = ["_extracted_at_utc", "_bronze_path", "_reference_date"]


def normalize_bronze_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize known bronze layouts into the historical tabular schema."""
    if "Categoria" in df.columns and "Numero treno" in df.columns:
        return df

    if "treni" not in df.columns:
        return df

    # itertuples costruisce una namedtuple, e i nomi che iniziano con underscore
    # non sono identificatori validi: pandas li rinomina in posizionali (_1, _2).
    # Accedere per nome a "_reference_date", "_extracted_at_utc" e "_bronze_path"
    # restituiva quindi sempre la stringa vuota, e la data di riferimento andava
    # persa per ogni file dello schema legacy. Si accede per posizione.
    positions = {c: df.columns.get_loc(c) for c in _CARRIED_COLUMNS if c in df.columns}
    treni_pos = df.columns.get_loc("treni")

    records: List[Dict[str, Any]] = []
    for r in df.itertuples(index=False, name=None):
        carried = {c: r[i] for c, i in positions.items()}
        for t in parse_treni_payload(r[treni_pos]):
            rec = {out: t.get(key, "") for out, key in _PAYLOAD_COLUMNS.items()}
            rec["_op_epoch"] = t.get("op")
            rec["_oa_epoch"] = t.get("oa")
            rec.update(carried)
            records.append(rec)

    if not records:
        return df.iloc[0:0].copy()

    out = pd.DataFrame(records)
    out["Ora partenza programmata"] = epoch_series_to_it_string(out.pop("_op_epoch"))
    out["Ora arrivo programmata"] = epoch_series_to_it_string(out.pop("_oa_epoch"))
    for c in _EMPTY_COLUMNS:
        out[c] = ""
    return out
