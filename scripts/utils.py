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


def parse_di_df(s: str) -> date:
    return datetime.strptime(s, "%d_%m_%Y").date()


def parse_cli_date(value: str, field_name: str) -> date:
    """Parse date input with guardrails for common YYYY-DD-MM typos."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        parts = value.split("-")
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            y, m, d = parts
            mm = int(m)
            dd = int(d)
            if mm > 12 and 1 <= dd <= 12:
                fixed = f"{int(y):04d}-{dd:02d}-{mm:02d}"
                try:
                    return date.fromisoformat(fixed)
                except ValueError:
                    pass
                raise SystemExit(
                    f"Invalid {field_name} '{value}'. Did you mean '{fixed}'?"
                ) from exc

        raise SystemExit(
            f"Invalid {field_name} '{value}'. Expected format YYYY-MM-DD."
        ) from exc


def format_di_df(d: date) -> str:
    return d.strftime("%d_%m_%Y")


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


# Abbreviazioni ferroviarie italiane. La sorgente alterna le forme lunghe e
# quelle abbreviate per la stessa stazione ("BOLOGNA C.LE" e "BOLOGNA CENTRALE",
# "FIRENZE S.M.N." e "FIRENZE SANTA MARIA NOVELLA"): senza espanderle il
# raggruppamento per nome le tratta come stazioni diverse, e le statistiche di
# 810.418 corse (il 6,5% del totale) finivano spezzate in due.
_ABBREVIATIONS = [
    # Sigle di citta' usate dalla sorgente solo su alcune stazioni, che percio'
    # finivano contate due volte: "MI P GENOVA" con 77.322 corse e "MILANO
    # PORTA GENOVA" con 11.411 erano la stessa stazione in due voci, e cosi'
    # "NOVATE M SE" con "NOVATE MILANESE" e "PALAZZOLO M SE" con "PALAZZOLO
    # MILANESE". Le sigle valgono solo a inizio nome: "MI" in mezzo a un nome
    # non e' Milano.
    (r"^MI\b", "MILANO"),
    (r"^BS\b", "BRESCIA"),
    (r"\bM\.?\s?SE\b", "MILANESE"),
    # "P" davanti al nome di una porta cittadina: si elencano i casi reali
    # invece di espandere qualunque P, che altrove significa altro (i "P.C."
    # sono posti di comunicazione, non porte).
    (r"\bP\s+(GENOVA|GARIBALDI|GAR|VITTORIA|ROMANA|SUSA|VOLTA)\b", r"PORTA \1"),
    (r"\bGAR\b", "GARIBALDI"),
    (r"\bPIAZ\b", "PIAZZALE"),
    (r"\bSOTT\b", "SOTTERRANEA"),
    (r"\bC\.?\s?L\.?E\.?\b", "CENTRALE"),
    (r"\bCENT\.?\b", "CENTRALE"),
    (r"\bS\.?M\.?N\.?\b", "S MARIA NOVELLA"),
    (r"\bP\.?\s?TA\b", "PORTA"),
    (r"\bP\.?\s?Z(?:Z)?A\b", "PIAZZA"),
    (r"\bP\.?\s?NUOVA\b", "PORTA NUOVA"),
    (r"\bAER\.?\b", "AEROPORTO"),
    (r"\bM\.?\s?MO\b", "MARITTIMO"),
    (r"\bMAR\.?\s?MO\b", "MARITTIMO"),
    (r"\bSCR\.?\b", "SCRIVIA"),
    (r"\bMOV\.?\b", "MOVIMENTO"),
    # "Reggio di Calabria Centrale" e "Reggio Calabria Centrale" sono la stessa
    # stazione scritta in due modi, e restavano due identita' separate per
    # 218.826 corse: la piu' grande ancora spezzata dopo la sistemazione degli
    # accenti. Il "di" fra due parti del nome non distingue niente, e sulle
    # 3.359 stazioni dell'anagrafica questo e' l'unico caso in cui toglierlo
    # unisce due voci: non rischia di fondere stazioni diverse.
    (r"\bDI\b", " "),
    # Tutte le forme di "San/Santa/Santo/Santi" collassano su un unico token.
    # La sorgente scrive "VENEZIA S.LUCIA" e "VENEZIA SANTA LUCIA" per la stessa
    # stazione, e da "S." non si puo' dedurre se valga SAN o SANTA: unificarle
    # e' l'unico modo di non spezzarle.
    (r"\bSS\.?\b", "S"),
    (r"\bSANTI\b", "S"),
    (r"\bSANTA\b", "S"),
    (r"\bSANTO\b", "S"),
    (r"\bSANT'", "S "),
    (r"\bSAN\b", "S"),
    (r"\bS\.", "S "),
    (r"\bF\.?S\.?\b", ""),
]

_ABBREVIATIONS_COMPILED = [(re.compile(p), r) for p, r in _ABBREVIATIONS]


def normalize_station_name(x: Any) -> str:
    """Normalize station name for matching across different naming conventions.

    Handles Trenord/FerrovieNord vs RFI naming differences:
      - "M N CADORNA" -> "MILANO CADORNA"
      - "COMO NORD CAMERLATA" -> "COMO CAMERLATA"
      - "MILANO BOVISA FNM" -> "MILANO BOVISA"
      - "MILANO BOVISA POLITECNICO" -> "MILANO BOVISA"

    e le abbreviazioni ferroviarie:
      - "BOLOGNA C.LE" -> "BOLOGNA CENTRALE"
      - "FIRENZE S.M.N." -> "FIRENZE S MARIA NOVELLA"
      - "VENEZIA S.LUCIA" e "VENEZIA SANTA LUCIA" -> "VENEZIA S LUCIA"
    """
    if x is None:
        return ""
    s = str(x).strip().upper()
    # L'accento finale arriva in tre grafie diverse per la stessa stazione, a
    # seconda di come la sorgente ha convertito il carattere accentato: apice
    # inverso, barra rovesciata, o niente. Sono nomi diversi solo per il
    # computer, e restavano stazioni distinte: SANTHIA` con 48.941 corse,
    # SANTHIA con 6.815 e SANTHIA\ con 5.670 erano tre voci separate. In tutto
    # 32 stazioni spezzate per 139.044 osservazioni, fra cui Cefalu', L'Aquila,
    # Cirie' e Forli'. Ne' l'apice inverso ne' la barra rovesciata compaiono in
    # un nome di stazione reale, quindi si trattano come l'apostrofo.
    s = s.replace("`", "'").replace("\\", "'")
    # I separatori interni alternano spazio, punto e trattino per la stessa
    # stazione ("ALBAIRATE-VERMEZZO" e "ALBAIRATE VERMEZZO").
    s = s.replace("-", " ").replace("'", "' ")
    s = re.sub(r"\s+", " ", s)

    for rx, repl in _ABBREVIATIONS_COMPILED:
        s = rx.sub(repl, s)

    # I punti residui delle abbreviazioni gia' espanse ("S.M.N." lascia il
    # punto finale) non devono distinguere due grafie della stessa stazione.
    s = s.replace("'", "").replace(".", " ")
    s = re.sub(r"\s+", " ", s).strip()

    # Expand "M N" abbreviation (Trenord: Milano Nord)
    s = re.sub(r"^M N\b", "MILANO NORD", s)

    # Strip " FNM" suffix (FerrovieNord Milano)
    s = re.sub(r"\s+FNM$", "", s)

    # Strip " POLITECNICO" suffix (same physical station)
    s = re.sub(r"\s+POLITECNICO$", "", s)

    # Strip "NORD" after known city prefixes (Trenord convention)
    # e.g. "MILANO NORD CADORNA" -> "MILANO CADORNA"
    #      "COMO NORD LAGO" -> "COMO LAGO"
    #      "VARESE NORD" -> "VARESE"
    s = re.sub(r"\b(MILANO|COMO|VARESE)\s+NORD\b", r"\1", s)

    s = re.sub(r"\s+", " ", s).strip()
    return s


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
    """Versione a cella singola, tenuta per i richiami esterni e i test.

    Applica lo stesso orizzonte di `parse_dt_it_series`: senza, questa strada
    resterebbe l'unica per cui l'anno 0 o l'anno 52000 del feed entrano nel
    silver e lo rendono irrileggibile, e la protezione dipenderebbe da quale
    delle due funzioni chiama chi passa di qui.
    """
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    try:
        t = pd.to_datetime(s, dayfirst=True, errors="raise")
    except Exception:
        return None
    if pd.isna(t) or t < _ORIZZONTE_MIN or t >= _ORIZZONTE_MAX:
        return None
    return t


_NULLISH = {"", "nan", "none", "null", "nat", "<na>"}

# Layout emitted by the source for both bronze schemas ("24/07/2026 15:14").
_IT_DT_FORMAT = "%d/%m/%Y %H:%M"


def _blank_mask(s: pd.Series) -> pd.Series:
    """True where the string cell carries no usable value."""
    return s.isna() | s.str.strip().str.lower().isin(_NULLISH)


def parse_dt_it_series(values: pd.Series) -> pd.Series:
    """Vectorized counterpart of parse_dt_it.

    parse_dt_it called pd.to_datetime() once per cell, and each call re-ran
    pandas' format inference on a one-element array. On a single month of
    bronze that accounted for ~85% of the silver build time. Here the whole
    column is parsed with an explicit format, and only the cells that fail
    fall back to the (slow) inferring parser.
    """
    s = values.astype(str)
    blank = _blank_mask(s)
    s = s.str.strip()

    out = pd.to_datetime(s.where(~blank), format=_IT_DT_FORMAT, errors="coerce")

    retry = out.isna() & ~blank
    if retry.any():
        out.loc[retry] = pd.to_datetime(s[retry], dayfirst=True, errors="coerce")

    return entro_orizzonte(out)


# Nessun orario ferroviario di questa sorgente puo' cadere fuori da questa
# finestra. Serve perche' il parser tollerante accetta qualunque cosa somigli a
# una data: nel bronze compaiono l'anno 0, l'anno 1 e l'anno 52000, che non sono
# orari ma spazzatura del feed. Non erano solo inutili. Una volta scritti nel
# silver rendevano quel mese impossibile da rileggere: la conversione a
# nanosecondi va in overflow e faceva morire l'intera trasformazione, quindi il
# silver non era piu' ricostruibile dal bronze.
_ORIZZONTE_MIN = pd.Timestamp("2000-01-01")
_ORIZZONTE_MAX = pd.Timestamp("2100-01-01")


def entro_orizzonte(out: pd.Series) -> pd.Series:
    """Porta a NaT le date fuori da ogni orizzonte plausibile."""
    fuori = out.notna() & ((out < _ORIZZONTE_MIN) | (out >= _ORIZZONTE_MAX))
    if fuori.any():
        out = out.mask(fuori)
    return out


def safe_int_series(values: pd.Series) -> pd.Series:
    """Vectorized counterpart of safe_int, returning a nullable Int64 column.

    The source encodes "no measurement" both as an empty cell and as the
    letter codes N / S / X, which must stay missing rather than collapse to 0.
    """
    s = values.astype(str).str.strip()
    num = pd.to_numeric(s.where(~_blank_mask(values)), errors="coerce")
    return num.round().astype("Int64")


def bucketize_delay_series(
    values: pd.Series, edges: List[int], labels: List[str]
) -> pd.Series:
    """Vectorized delay bucketing, left-open/right-closed like the scalar version.

    The scalar bucketize_delay() truncated through int(), which rounds toward
    zero and therefore moved negative fractional delays into the wrong bucket
    (-0.5 became 0, landing in "(-1,0]" instead of "(-1,0]"'s neighbour).
    Working on the numeric column directly avoids the cast entirely.
    """
    num = pd.to_numeric(values, errors="coerce")
    out = pd.cut(num, bins=edges, labels=labels, right=True, ordered=False)
    return out.astype(object).where(out.notna(), "missing")


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


def _any_pattern(hay: pd.Series, patterns: List[re.Pattern]) -> pd.Series:
    """Vectorized 'does any of these regexes match' over a string column."""
    hit = pd.Series(False, index=hay.index)
    for rx in patterns:
        hit |= hay.str.contains(rx, na=False)
    return hit


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

    def haystack(self, df: pd.DataFrame) -> pd.Series:
        """Join the configured text fields into one searchable column.

        Fields absent from the frame are skipped instead of silently
        contributing an empty string: text_fields is configured with the
        canonical (post-rename) column names, and a mismatch there used to
        make every classification fall through to "effettuato".
        """
        present = [f for f in self.text_fields if f in df.columns]
        if not present:
            raise KeyError(
                "status_rules.text_fields matches no column in the silver frame. "
                f"configured={self.text_fields} available={list(df.columns)}"
            )

        parts = [df[f].fillna("").astype(str) for f in present]
        hay = parts[0]
        for p in parts[1:]:
            hay = hay.str.cat(p, sep=" | ")
        return hay

    def classify_frame(self, df: pd.DataFrame) -> pd.Series:
        """Vectorized classification over the whole frame.

        Precedence matters: a "Treno cancellato da X a Y" note describes a
        train that ran on a shortened route, so it must be read as a partial
        cancellation before the generic "cancellato" pattern claims it as a
        full one.
        """
        hay = self.haystack(df)

        is_suppressed = _any_pattern(hay, self.suppressed_any)
        is_partial = _any_pattern(hay, self.partial_any)
        is_cancelled = _any_pattern(hay, self.cancelled_any)

        out = pd.Series("effettuato", index=df.index, dtype=object)
        out[is_cancelled & ~is_partial] = "cancellato"
        out[is_partial & ~is_suppressed] = "parzialmente_cancellato"
        out[is_suppressed] = "soppresso"
        return out

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
        hi = edges[i + 1]
        if minutes > lo and minutes <= hi:
            return labels[i]
    return "missing"


STATIONS_DIM = os.path.join("data", "gold", "stations_dim.csv")


def _compatibile_per_token(corto: str, lungo: str) -> bool:
    """Vero se `corto` e' la forma abbreviata di `lungo`, token per token.

    "TREVISO C" e' compatibile con "TREVISO CENTRALE", "CHIUSI C T" con
    "CHIUSI CHIANCIANO TERME", "MINTURNO" con "MINTURNO SCAURI". Non lo sono
    "CECCHINA" e "ALBANO LAZIALE", che sono due paesi diversi finiti sulle
    stesse coordinate per un errore della sorgente.
    """
    tc = corto.split()
    tl = lungo.split()
    if not tc or len(tc) > len(tl):
        return False
    return all(tl[i].startswith(tc[i]) for i in range(len(tc)))


def alias_stazioni_da_coordinate(percorso: str = STATIONS_DIM) -> Dict[str, str]:
    """Nomi diversi che sono la stessa stazione, riconosciuti dalle coordinate.

    L'anagrafica contiene la stessa fermata sotto due grafie, una estesa e una
    con le iniziali puntate: "TREVISO CENTRALE" e "TREVISO C", con latitudine e
    longitudine identiche fino alla quinta cifra. Il normalizzatore non poteva
    unificarle, perche' espandere una singola lettera e' ambiguo (la C di
    Centrale, la T di Terme, la M di Militello), e le due grafie restavano due
    tratte distinte: Portogruaro-Treviso compariva due volte, una con i 52 km
    ufficiali del RINF e una con 127 km stimati da OpenStreetMap, e quest'ultima
    risultava la quindicesima tratta piu' veloce d'Italia a 128 km/h.

    Qui l'ambiguita' la scioglie il dato: si fondono solo i nomi che stanno
    sulle stesse coordinate E sono compatibili token per token. Le coordinate da
    sole non bastano, perche' l'anagrafica ha anche coppie di stazioni davvero
    diverse alla stessa posizione.

    Nemmeno le due condizioni insieme bastano, e la terza e' il codice. Nella
    dimensione stazioni LECCO e LECCO MAGGIANICO portano la stessa identica
    coordinata, che e' quella di Maggianico: un errore dell'anagrafica su due
    fermate distanti tre chilometri. Sono compatibili token per token, quindi le
    prime due regole le fondevano, e le 110.022 corse di Lecco finivano sotto il
    nome di una fermata che ne fa 45, con i chilometri di un'altra stazione.
    Quando entrambe le grafie hanno un codice vero della sorgente, e' la
    sorgente stessa a dire che sono due stazioni: nessuna coordinata puo'
    smentirla. I codici sintetici `N_`, che derivano dal nome e non dal feed,
    non contano come prova.
    """
    if not os.path.exists(percorso):
        return {}
    d = pd.read_csv(percorso)
    if not {"nome_stazione", "lat", "lon"} <= set(d.columns):
        return {}
    d = d.dropna(subset=["lat", "lon"])

    per_posizione: Dict[Tuple[float, float], set] = {}
    codici_veri: Dict[str, set] = {}
    ha_codice = "cod_stazione" in d.columns
    for i, (nome, lat, lon) in enumerate(zip(d["nome_stazione"], d["lat"], d["lon"])):
        k = normalize_station_name(str(nome))
        if not k:
            continue
        per_posizione.setdefault((round(float(lat), 5), round(float(lon), 5)), set()).add(k)
        if ha_codice:
            cod = str(d["cod_stazione"].iloc[i]).strip()
            if cod and not cod.startswith("N_"):
                codici_veri.setdefault(k, set()).add(cod)

    alias: Dict[str, str] = {}
    for nomi in per_posizione.values():
        if len(nomi) < 2:
            continue
        # Il nome canonico e' il piu' esteso: e' quello che il RINF usa e quello
        # sotto cui si trovano le distanze ufficiali.
        canonico = max(sorted(nomi), key=len)
        for n in nomi:
            if n == canonico or not _compatibile_per_token(n, canonico):
                continue
            propri = codici_veri.get(n, set())
            altrui = codici_veri.get(canonico, set())
            if propri and altrui and propri != altrui:
                continue
            alias[n] = canonico
    return alias
