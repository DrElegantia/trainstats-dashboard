from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests
import yaml
from dateutil import tz


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def today_in_tz(tz_name: str) -> date:
    z = tz.gettz(tz_name)
    return datetime.now(tz=z).date()


def date_range_inclusive(d0: date, d1: date) -> Iterable[date]:
    if d1 < d0:
        raise ValueError("end date precedes start date")
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)


def http_get_with_retry(url: str, timeout: int, max_retries: int, backoff_factor: int) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": "trainstats-dashboard-bot/1.0"})
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            time.sleep(backoff_factor * (2 ** attempt))
    raise RuntimeError(f"http request failed after retries: {url}") from last_exc


def read_csv_header_bytes(content: bytes) -> List[str]:
    text = content.decode("utf-8", errors="replace")
    first_line = text.splitlines()[0] if text else ""
    return next(csv.reader([first_line]))


def validate_header(header: List[str], schema: Dict[str, Any]) -> None:
    required = schema.get("required_columns", [])
    expected = schema.get("expected_header", [])
    mode = schema.get("mode", "strict")

    missing_required = [c for c in required if c not in header]
    if missing_required:
        raise ValueError(f"missing required columns: {missing_required}")

    if mode == "strict":
        if header != expected:
            raise ValueError("header mismatch in strict mode")
    elif mode == "prefix":
        if header[: len(expected)] != expected:
            raise ValueError("header mismatch in prefix mode")
    elif mode == "required_only":
        return
    else:
        raise ValueError(f"unknown schema mode: {mode}")


def write_gzip_bytes(path: str, content: bytes) -> None:
    ensure_dir(os.path.dirname(path))
    with gzip.open(path, "wb") as f:
        f.write(content)


def write_json(path: str, obj: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def safe_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def parse_dt_it(x: Any) -> Optional[pd.Timestamp]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    try:
        return pd.to_datetime(s, dayfirst=True, errors="raise")
    except Exception:
        return None


def normalize_station_name(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s


def compute_unique_key(row: pd.Series) -> str:
    parts = [
        str(row.get("Categoria", "")).strip(),
        str(row.get("Numero treno", "")).strip(),
        str(row.get("Codice stazione partenza", "")).strip(),
        str(row.get("Codice stazione arrivo", "")).strip(),
        str(row.get("Ora partenza programmata", "")).strip(),
        str(row.get("Ora arrivo programmata", "")).strip(),
    ]
    return sha1_hex("|".join(parts))


@dataclass
class StatusEngine:
    cancelled_any: List[re.Pattern]
    suppressed_any: List[re.Pattern]
    partial_any: List[re.Pattern]
    text_fields: List[str]

    @staticmethod
    def from_config(cfg: Dict[str, Any]) -> "StatusEngine":
        sr = cfg["status_rules"]
        tf = sr["text_fields"]
        p = sr["patterns"]

        def comp_list(xs: List[str]) -> List[re.Pattern]:
            return [re.compile(x) for x in xs]

        cancelled = comp_list(p["cancelled"]["any"])
        suppressed = comp_list(p["suppressed"]["any"])
        partial = comp_list(p["partial_cancelled"]["any"])
        return StatusEngine(cancelled, suppressed, partial, tf)

    def classify(self, row: pd.Series) -> str:
        texts: List[str] = []
        for f in self.text_fields:
            v = row.get(f, "")
            if v is None:
                continue
            s = str(v).strip()
            if s:
                texts.append(s)
        hay = " | ".join(texts)

        if any(r.search(hay) for r in self.cancelled_any):
            return "cancellato"
        if any(r.search(hay) for r in self.suppressed_any):
            return "soppresso"
        if any(r.search(hay) for r in self.partial_any):
            return "parzialmente_cancellato"
        return "effettuato"


def bucketize_delay(minutes: Optional[int], edges: List[int], labels: List[str]) -> str:
    if minutes is None:
        return "missing"
    for i in range(len(edges) - 1):
        lo = edges[i]

def parse_di_df(s: str) -> date:
    return datetime.strptime(s, "%d_%m_%Y").date()

def format_di_df(d: date) -> str:
    return d.strftime("%d_%m_%Y")

