from __future__ import annotations

import os
import re
import json
import unicodedata
from datetime import datetime, timezone
from typing import Optional, Any, Iterable, List

import pandas as pd


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

    rows = []
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
    p = os.path.join("data", "stations", "stations.csv")
    if not os.path.exists(p):
        return pd.DataFrame(columns=["cod_stazione", "nome_stazione", "lat", "lon", "citta"])

    df = _read_csv_any(p)
    df.columns = [str(c) for c in df.columns]

    col_code = _pick_first_col(df, ["cod_stazione", "codice", "codice_stazione", "code", "station_code", "id"])
    col_name = _pick_first_col(df, ["nome_stazione", "nome", "nome_norm", "station_name"])
    col_city = _pick_first_col(
        df,
        [
            "citta",
            "citta_nome",
            "citta_stazione",
            "comune",
            "city",
            "nome_comune",
            "localita",
            "località",
            "città",
        ],
    )

    col_lat = _pick_first_col(df, ["lat", "latitude", "latitudine", "y", "y_wgs84", "lat_wgs84", "wgs84_lat"])
    col_lon = _pick_first_col(df, ["lon", "lng", "long", "longitude", "longitudine", "x", "x_wgs84", "lon_wgs84", "wgs84_lon"])

    if col_code is None:
        raise ValueError("data/stations/stations.csv must contain a station code column")

    out = pd.DataFrame()
    out["cod_stazione"] = df[col_code].map(_norm_station_code)
    out["nome_stazione"] = df[col_name].astype(str).str.strip() if col_name else ""
    out["citta"] = df[col_city].astype(str).str.strip() if col_city else ""

    out["lat"] = df[col_lat].map(_parse_float_maybe) if col_lat else None
    out["lon"] = df[col_lon].map(_parse_float_maybe) if col_lon else None

    out = out[out["cod_stazione"] != ""]
    out = out.drop_duplicates(subset=["cod_stazione"]).reset_index(drop=True)
    return out


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
        out["nome_stazione"] = out["nome_stazione"].where(out["nome_stazione"].str.strip() != "", out["nome_stazione_reg"])
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

    n_total = int(len(stations_dim))
    n_with_coords = int(stations_dim["lat"].notna().sum() if "lat" in stations_dim.columns else 0)

    cap = _fallback_capoluoghi()

    out_paths = _write_many(
        stations_dim,
        [
            os.path.join("data", "stations_dim.csv"),
            os.path.join("data", "gold", "stations_dim.csv"),
        ],
    )

    cap_paths = _write_many(
        cap,
        [
            os.path.join("data", "capoluoghi_provincia.csv"),
            os.path.join("data", "gold", "capoluoghi_provincia.csv"),
        ],
    )

    print(
        json.dumps(
            {
                "stations_dim_built_at_utc": built_at,
                "stations_dim_rows": n_total,
                "stations_dim_with_coords": n_with_coords,
                "stations_dim_written": out_paths,
                "capoluoghi_written": cap_paths,
            }
        )
    )


if __name__ == "__main__":
    main()
