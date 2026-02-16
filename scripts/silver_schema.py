from __future__ import annotations

import ast
import hashlib
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


def normalize_bronze_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize known bronze layouts into the historical tabular schema."""
    if "Categoria" in df.columns and "Numero treno" in df.columns:
        return df

    if "treni" not in df.columns:
        return df

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        for t in parse_treni_payload(r.get("treni")):
            rows.append(
                {
                    "Categoria": t.get("c", ""),
                    "Numero treno": t.get("n", ""),
                    "Codice stazione partenza": "",
                    "Nome stazione partenza": t.get("p", ""),
                    "Ora partenza programmata": epoch_to_it_string(t.get("op")),
                    "Ritardo partenza": t.get("rp", ""),
                    "Codice stazione arrivo": "",
                    "Nome stazione arrivo": t.get("a", ""),
                    "Ora arrivo programmata": epoch_to_it_string(t.get("oa")),
                    "Ritardo arrivo": t.get("ra", ""),
                    "Cambi numerazione": "",
                    "Provvedimenti": "",
                    "Variazioni": "",
                    "Stazione estera partenza": "",
                    "Orario estero partenza": "",
                    "Stazione estera arrivo": "",
                    "Orario estero arrivo": "",
                    "_extracted_at_utc": r.get("_extracted_at_utc", ""),
                    "_bronze_path": r.get("_bronze_path", ""),
                    "_reference_date": r.get("_reference_date", ""),
                }
            )

    if not rows:
        return df.iloc[0:0].copy()

    return pd.DataFrame(rows)
