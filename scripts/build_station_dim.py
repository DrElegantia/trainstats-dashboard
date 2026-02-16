from __future__ import annotations

import io
import json
import os
import re
import time
import unicodedata
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


STATION_NAME_COORDS_URL = os.environ.get(
    "TRAINSTATS_STATION_NAME_COORDS_URL",
    "https://gist.githubusercontent.com/MarcoBuster/5a142febd4a2032505f4acd20326146c/raw/252fae1074a2766e9940f31dbb57be556987f8fa/Stazioni%2520italiane.csv",
)

STATION_NAME_COORDS_CACHE_PATH = os.path.join("data", "stations", "stazioni_italiane.csv")
GEOCODE_CACHE_PATH = os.path.join("data", "stations", "geocode_cache.json")
STATIONS_REGISTRY_PATH = os.path.join("data", "stations", "stations.csv")


def _ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def _detect_delimiter(sample_line: str) -> str:
    s = (sample_line or "").strip()
    if not s:
        return ","
    comma = s.count(",")
    semi = s.count(";")
    tab = s.count("\t")
    if semi > comma and semi >= tab:
        return ";"
    if tab > comma and tab > semi:
        return "\t"
    return ","


def _read_csv_any(path: str) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        first = f.readline()
    sep = _detect_delimiter(first)
    return pd.read_csv(
        path,
        dtype=str,
        sep=sep,
        keep_default_na=False,
        na_filter=False,
    )


def _norm_col(s: str) -> str:
    t = str(s or "").strip().lower()
    t = t.lstrip("\ufeff")
    t = unicodedata.normalize("NFKD", t)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = re.sub(r"[^a-z0-9]+", "_", t)
    t = re.sub(r"_+", "_", t).strip("_")
    return t


def _norm_station_code(x: Any) -> str:
    s = str(x or "").strip().upper()
    s = s.lstrip("\ufeff")
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"\.0$", "", s)

    m = re.fullmatch(r"([A-Z])0*([0-9]{1,5})", s)
    if m:
        return f"{m.group(1)}{m.group(2).zfill(5)}"

    m2 = re.fullmatch(r"0*([0-9]{1,5})", s)
    if m2:
        return m2.group(1).zfill(5)

    return s


def _parse_float_maybe(x: Any) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    s = s.replace("\u00a0", " ").strip()
    s = s.replace("°", "").replace("'", "").replace('"', "")
    s = s.replace(" ", "")
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        v = float(s)
    except Exception:
        return None
    if not (v == v):
        return None
    return v


def _pick_first_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = {_norm_col(c): c for c in df.columns}
    for cand in candidates:
        k = _norm_col(cand)
        if k in cols:
            return cols[k]
    for cand in candidates:
        k = _norm_col(cand)
        for nk, orig in cols.items():
            if nk == k:
                return orig
    return None


def _site_data_roots() -> List[str]:
    roots: List[str] = []
    roots.append(os.path.join("docs", "data"))
    if os.path.isdir(os.path.join("trainstats-dashboard", "docs")):
        roots.append(os.path.join("trainstats-dashboard", "docs", "data"))
    return roots


def _norm_station_name_key(x: Any) -> str:
    s = str(x or "").strip()
    s = s.lstrip("\ufeff")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = s.replace("`", "'")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _download_text(url: str, timeout_s: int = 40) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "trainstats-dashboard"})
    with urllib.request.urlopen(req, timeout=timeout_s) as r:
        b = r.read()
    return b.decode("utf-8", errors="replace")


def _normalize_csv_text_rows(text: str) -> str:
    s = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not s:
        return ""
    s = re.sub(r"\s+(?=[A-Z]\d{5},)", "\n", s)
    if not s.endswith("\n"):
        s += "\n"
    return s


def _load_station_name_coords() -> Dict[str, Tuple[float, float]]:
    if os.path.exists(STATION_NAME_COORDS_CACHE_PATH) and os.path.getsize(STATION_NAME_COORDS_CACHE_PATH) > 1000:
        raw = open(STATION_NAME_COORDS_CACHE_PATH, "r", encoding="utf-8", errors="replace").read()
    else:
        raw = _download_text(STATION_NAME_COORDS_URL)
        raw = _normalize_csv_text_rows(raw)
        _ensure_dir(os.path.dirname(STATION_NAME_COORDS_CACHE_PATH))
        with open(STATION_NAME_COORDS_CACHE_PATH, "w", encoding="utf-8") as f:
            f.write(raw)

    raw = _normalize_csv_text_rows(raw)
    df = pd.read_csv(io.StringIO(raw), dtype=str, keep_default_na=False, na_filter=False)

    name_col = _pick_first_col(df, ["long_name", "nome_stazione", "nome", "station_name"])
    short_col = _pick_first_col(df, ["short_name"])
    lat_col = _pick_first_col(df, ["latitude", "lat"])
    lon_col = _pick_first_col(df, ["longitude", "lon"])

    if name_col is None or lat_col is None or lon_col is None:
        return {}

    out: Dict[str, Tuple[float, float]] = {}
    for _, r in df.iterrows():
        n1 = _norm_station_name_key(r.get(name_col))
        n2 = _norm_station_name_key(r.get(short_col)) if short_col else ""
        lat = _parse_float_maybe(r.get(lat_col))
        lon = _parse_float_maybe(r.get(lon_col))
        if lat is None or lon is None:
            continue
        if n1:
            out[n1] = (lat, lon)
        if n2 and n2 not in out:
            out[n2] = (lat, lon)
    return out


def _load_gold_station_seed() -> pd.DataFrame:
    gold_candidates = [
        os.path.join("data", "gold", "stazioni_mese_categoria_nodo.csv"),
        os.path.join("data", "gold", "stazioni_mese_categoria_ruolo.csv"),
        os.path.join("data", "gold", "stazioni_giorno_categoria_nodo.csv"),
        os.path.join("data", "gold", "stazioni_giorno_categoria_ruolo.csv"),
    ]
    frames: List[pd.DataFrame] = []
    for p in gold_candidates:
        if os.path.exists(p):
            try:
                frames.append(_read_csv_any(p))
            except Exception:
                pass
    if not frames:
        return pd.DataFrame(columns=["cod_stazione", "nome_stazione"])

    df = pd.concat(frames, ignore_index=True)
    df.columns = [str(c) for c in df.columns]

    col_code = _pick_first_col(df, ["cod_stazione", "codice_stazione", "stazione", "station_code"])
    col_name = _pick_first_col(df, ["nome_stazione", "nome", "stazione_nome", "station_name"])

    if col_code is None:
        return pd.DataFrame(columns=["cod_stazione", "nome_stazione"])

    out = pd.DataFrame()
    out["cod_stazione"] = df[col_code].map(_norm_station_code)
    out["nome_stazione"] = df[col_name].astype(str).str.strip() if col_name else ""
    out = out[out["cod_stazione"] != ""]
    out = out.drop_duplicates(subset=["cod_stazione"]).reset_index(drop=True)
    return out


def _load_od_station_seed() -> pd.DataFrame:
    od_candidates = [
        os.path.join("data", "gold", "od_mese_categoria.csv"),
        os.path.join("data", "gold", "od_giorno_categoria.csv"),
    ]
    frames: List[pd.DataFrame] = []
    for p in od_candidates:
        if os.path.exists(p):
            try:
                frames.append(_read_csv_any(p))
            except Exception:
                pass
    if not frames:
        return pd.DataFrame(columns=["cod_stazione", "nome_stazione"])

    df = pd.concat(frames, ignore_index=True)

    col_dep = _pick_first_col(df, ["cod_partenza", "codice_stazione_partenza", "stazione_partenza"])
    col_arr = _pick_first_col(df, ["cod_arrivo", "codice_stazione_arrivo", "stazione_arrivo"])
    col_dep_name = _pick_first_col(df, ["nome_partenza", "nome_stazione_partenza"])
    col_arr_name = _pick_first_col(df, ["nome_arrivo", "nome_stazione_arrivo"])

    rows: List[pd.DataFrame] = []
    if col_dep:
        tmp = pd.DataFrame({"cod_stazione": df[col_dep].map(_norm_station_code)})
        tmp["nome_stazione"] = df[col_dep_name].astype(str).str.strip() if col_dep_name else ""
        rows.append(tmp)
    if col_arr:
        tmp = pd.DataFrame({"cod_stazione": df[col_arr].map(_norm_station_code)})
        tmp["nome_stazione"] = df[col_arr_name].astype(str).str.strip() if col_arr_name else ""
        rows.append(tmp)

    if not rows:
        return pd.DataFrame(columns=["cod_stazione", "nome_stazione"])

    out = pd.concat(rows, ignore_index=True)
    out = out[out["cod_stazione"] != ""].drop_duplicates(subset=["cod_stazione"]).reset_index(drop=True)
    return out


def _load_station_registry() -> pd.DataFrame:
    if not os.path.exists(STATIONS_REGISTRY_PATH):
        return pd.DataFrame(columns=["cod_stazione", "nome_stazione", "lat", "lon", "citta"])

    df = _read_csv_any(STATIONS_REGISTRY_PATH)
    if df.empty:
        return pd.DataFrame(columns=["cod_stazione", "nome_stazione", "lat", "lon", "citta"])

    df.columns = [str(c) for c in df.columns]

    if "cod_stazione" not in df.columns:
        c = _pick_first_col(df, ["codice", "code", "station_code", "id"])
        if c:
            df = df.rename(columns={c: "cod_stazione"})
    if "nome_stazione" not in df.columns:
        c = _pick_first_col(df, ["nome", "station_name", "stazione", "long_name", "short_name"])
        if c:
            df = df.rename(columns={c: "nome_stazione"})
    if "citta" not in df.columns:
        c = _pick_first_col(df, ["city", "comune", "localita", "località", "città"])
        if c:
            df = df.rename(columns={c: "citta"})
    if "lat" not in df.columns:
        c = _pick_first_col(df, ["latitude", "latitudine"])
        if c:
            df = df.rename(columns={c: "lat"})
    if "lon" not in df.columns:
        c = _pick_first_col(df, ["longitude", "longitudine", "lng"])
        if c:
            df = df.rename(columns={c: "lon"})

    for c in ["cod_stazione", "nome_stazione", "lat", "lon", "citta"]:
        if c not in df.columns:
            df[c] = ""

    out = pd.DataFrame()
    out["cod_stazione"] = df["cod_stazione"].map(_norm_station_code)
    out["nome_stazione"] = df["nome_stazione"].astype(str).str.strip()
    out["citta"] = df["citta"].astype(str).str.strip()
    out["lat"] = df["lat"].map(_parse_float_maybe)
    out["lon"] = df["lon"].map(_parse_float_maybe)

    out = out[out["cod_stazione"] != ""]
    out = out.drop_duplicates(subset=["cod_stazione"]).reset_index(drop=True)
    return out[["cod_stazione", "nome_stazione", "lat", "lon", "citta"]].copy()


def _load_geocode_cache() -> dict:
    if not os.path.exists(GEOCODE_CACHE_PATH):
        return {}
    try:
        with open(GEOCODE_CACHE_PATH, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_geocode_cache(cache: dict) -> None:
    _ensure_dir(os.path.dirname(GEOCODE_CACHE_PATH))
    with open(GEOCODE_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)


def _nominatim_geocode(query: str) -> Optional[Tuple[float, float, str]]:
    base = "https://nominatim.openstreetmap.org/search"
    url = base + "?" + urllib.parse.urlencode(
        {"q": query, "format": "jsonv2", "limit": 1, "countrycodes": "it", "addressdetails": 1}
    )
    req = urllib.request.Request(url, headers={"User-Agent": "trainstats-dashboard"})
    with urllib.request.urlopen(req, timeout=25) as r:
        data = json.loads(r.read().decode("utf-8", errors="replace"))
    if not data:
        return None
    item = data[0]
    lat = _parse_float_maybe(item.get("lat"))
    lon = _parse_float_maybe(item.get("lon"))
    if lat is None or lon is None:
        return None
    city = ""
    try:
        addr = item.get("address") or {}
        city = str(addr.get("city") or addr.get("town") or addr.get("village") or addr.get("municipality") or "")
    except Exception:
        city = ""
    return lat, lon, city


def _fill_missing_coords(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    coords_db: Dict[str, Tuple[float, float]] = {}
    try:
        coords_db = _load_station_name_coords()
    except Exception:
        coords_db = {}

    for i, row in df.iterrows():
        if pd.notna(row.get("lat")) and pd.notna(row.get("lon")):
            continue
        k = _norm_station_name_key(row.get("nome_stazione"))
        hit = coords_db.get(k)
        if hit:
            lat, lon = hit
            df.at[i, "lat"] = lat
            df.at[i, "lon"] = lon

    if os.environ.get("TRAINSTATS_DISABLE_GEOCODE", "").strip().lower() in {"1", "true", "yes"}:
        return df

    cache = _load_geocode_cache()

    def from_cache(code: str) -> Optional[Tuple[float, float, str]]:
        obj = cache.get(code)
        if not isinstance(obj, dict):
            return None
        lat = _parse_float_maybe(obj.get("lat"))
        lon = _parse_float_maybe(obj.get("lon"))
        city = str(obj.get("citta") or "")
        if lat is None or lon is None:
            return None
        return lat, lon, city

    changed = False

    for i, row in df.iterrows():
        code = str(row.get("cod_stazione") or "").strip()
        if not code:
            continue
        if pd.notna(row.get("lat")) and pd.notna(row.get("lon")):
            continue
        hit = from_cache(code)
        if hit:
            lat, lon, city = hit
            df.at[i, "lat"] = lat
            df.at[i, "lon"] = lon
            if not str(row.get("citta") or "").strip() and city:
                df.at[i, "citta"] = city

    missing = df[df["lat"].isna() | df["lon"].isna()]
    if len(missing) == 0:
        return df

    for i, row in missing.iterrows():
        code = str(row.get("cod_stazione") or "").strip()
        name = str(row.get("nome_stazione") or "").strip()
        if not code or not name:
            continue

        if isinstance(cache.get(code), dict) and cache[code].get("attempted"):
            continue

        city0 = str(row.get("citta") or "").strip()
        q = f"stazione {name}, Italia"
        if city0 and city0.lower() not in name.lower():
            q = f"stazione {name}, {city0}, Italia"

        coords = None
        try:
            coords = _nominatim_geocode(q)
        except Exception:
            coords = None

        cache[code] = {"attempted": True, "query": q}
        if coords:
            lat, lon, city = coords
            df.at[i, "lat"] = lat
            df.at[i, "lon"] = lon
            if not city0 and city:
                df.at[i, "citta"] = city
            cache[code]["lat"] = lat
            cache[code]["lon"] = lon
            cache[code]["citta"] = str(df.at[i, "citta"] or city or "")
            changed = True

        time.sleep(1.1)

    if changed:
        _save_geocode_cache(cache)

    return df


def _build_stations_dim() -> pd.DataFrame:
    seed_a = _load_gold_station_seed()
    seed_b = _load_od_station_seed()
    seed = pd.concat([seed_a, seed_b], ignore_index=True)
    seed["cod_stazione"] = seed["cod_stazione"].map(_norm_station_code)
    seed = seed.drop_duplicates(subset=["cod_stazione"]).reset_index(drop=True)

    registry = _load_station_registry()

    out = seed.merge(registry, on="cod_stazione", how="left", suffixes=("", "_reg"))

    out["nome_stazione"] = out["nome_stazione"].fillna("")
    if "nome_stazione_reg" in out.columns:
        out["nome_stazione_reg"] = out["nome_stazione_reg"].fillna("")
        out["nome_stazione"] = out["nome_stazione"].where(
            out["nome_stazione"].str.strip() != "",
            out["nome_stazione_reg"],
        )
        out = out.drop(columns=["nome_stazione_reg"])

    out["citta"] = out["citta"].fillna("")
    if "citta_reg" in out.columns:
        out["citta_reg"] = out["citta_reg"].fillna("")
        out["citta"] = out["citta"].where(out["citta"].str.strip() != "", out["citta_reg"])
        out = out.drop(columns=["citta_reg"])

    if "lat_reg" in out.columns:
        out["lat"] = out["lat"].where(out["lat"].notna(), out["lat_reg"])
        out = out.drop(columns=["lat_reg"])
    if "lon_reg" in out.columns:
        out["lon"] = out["lon"].where(out["lon"].notna(), out["lon_reg"])
        out = out.drop(columns=["lon_reg"])

    out["lat"] = out["lat"].map(_parse_float_maybe)
    out["lon"] = out["lon"].map(_parse_float_maybe)

    out = out[["cod_stazione", "nome_stazione", "lat", "lon", "citta"]].copy()
    out = _fill_missing_coords(out)
    out = out.sort_values(["cod_stazione"]).reset_index(drop=True)
    return out


def _fallback_capoluoghi() -> pd.DataFrame:
    fallback = [
        "Aosta",
        "Torino",
        "Genova",
        "Milano",
        "Trento",
        "Venezia",
        "Trieste",
        "Bologna",
        "Firenze",
        "Ancona",
        "Perugia",
        "Roma",
        "L'Aquila",
        "Campobasso",
        "Napoli",
        "Bari",
        "Potenza",
        "Catanzaro",
        "Palermo",
        "Cagliari",
    ]
    return pd.DataFrame({"citta": fallback})


def _write_many(df: pd.DataFrame, rel_paths: List[str]) -> List[str]:
    written: List[str] = []
    for p in rel_paths:
        d = os.path.dirname(p)
        if d:
            _ensure_dir(d)
        df.to_csv(p, index=False)
        written.append(p)
    return written


def main() -> None:
    built_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    stations_dim = _build_stations_dim()
    stations_dim["built_at_utc"] = built_at

    cap = _fallback_capoluoghi()

    site_roots = _site_data_roots()

    station_paths: List[str] = [
        os.path.join("data", "stations_dim.csv"),
        os.path.join("data", "gold", "stations_dim.csv"),
    ]
    cap_paths: List[str] = [
        os.path.join("data", "capoluoghi_provincia.csv"),
        os.path.join("data", "gold", "capoluoghi_provincia.csv"),
    ]

    for root in site_roots:
        station_paths.append(os.path.join(root, "stations_dim.csv"))
        station_paths.append(os.path.join(root, "gold", "stations_dim.csv"))
        cap_paths.append(os.path.join(root, "capoluoghi_provincia.csv"))
        cap_paths.append(os.path.join(root, "gold", "capoluoghi_provincia.csv"))

    out_paths = _write_many(stations_dim, station_paths)
    out_cap_paths = _write_many(cap, cap_paths)

    n_total = int(len(stations_dim))
    n_with_coords = int(stations_dim["lat"].notna().sum()) if "lat" in stations_dim.columns else 0

    print(
        json.dumps(
            {
                "stations_dim_built_at_utc": built_at,
                "stations_dim_rows": n_total,
                "stations_dim_with_coords": n_with_coords,
                "stations_dim_written": out_paths,
                "capoluoghi_written": out_cap_paths,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
