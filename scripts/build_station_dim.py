# scripts/build_station_dim.py
from __future__ import annotations

import io
import os
from typing import Dict, Any, Optional, Tuple

import pandas as pd

from .utils import ensure_dir, http_get_with_retry, load_yaml


ISTAT_COMUNI_URL = "https://www.istat.it/storage/codici-unita-amministrative/Elenco-comuni-italiani.csv"


def _read_csv_if_exists(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, dtype=str)
    except Exception:
        return None


def load_station_registry_or_empty() -> pd.DataFrame:
    if not REGISTRY_PATH.exists():
        print("Registry non trovato:", REGISTRY_PATH)
        return pd.DataFrame(columns=["cod_stazione", "nome_stazione", "lat", "lon", "citta"])

    df = pd.read_csv(REGISTRY_PATH)

    def _first_col(cands):
        for c in cands:
            if c in df.columns:
                return c
        return None

    code_col = _first_col(["cod_stazione", "codice", "cod", "code", "station_code", "id_stazione", "id"])
    name_col = _first_col(["nome_stazione", "nome", "station_name", "name", "descrizione", "denominazione"])
    lat_col = _first_col(["lat", "latitude", "latitudine", "y", "LAT", "LATITUDINE"])
    lon_col = _first_col(["lon", "lng", "longitude", "longitudine", "x", "LON", "LONGITUDINE"])
    city_col = _first_col(["citta", "comune", "city", "municipality", "town", "localita", "località", "provincia_capoluogo"])

    if code_col and code_col != "cod_stazione":
        df = df.rename(columns={code_col: "cod_stazione"})
    if name_col and name_col != "nome_stazione":
        df = df.rename(columns={name_col: "nome_stazione"})
    if lat_col and lat_col != "lat":
        df = df.rename(columns={lat_col: "lat"})
    if lon_col and lon_col != "lon":
        df = df.rename(columns={lon_col: "lon"})
    if city_col and city_col != "citta":
        df = df.rename(columns={city_col: "citta"})

    for c in ["cod_stazione", "nome_stazione", "citta"]:
        if c not in df.columns:
            df[c] = ""

    if "lat" not in df.columns:
        df["lat"] = pd.NA
    if "lon" not in df.columns:
        df["lon"] = pd.NA

    df["cod_stazione"] = df["cod_stazione"].astype(str).str.strip()
    df["nome_stazione"] = df["nome_stazione"].astype(str).str.strip()
    df["citta"] = df["citta"].astype(str).str.strip()

    df["nome_stazione"] = df["nome_stazione"].replace({"nan": "", "NaN": "", "None": ""})
    df["citta"] = df["citta"].replace({"nan": "", "NaN": "", "None": ""})

    df["lat"] = pd.to_numeric(df["lat"].astype(str).str.replace(",", ".", regex=False).str.strip(), errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"].astype(str).str.replace(",", ".", regex=False).str.strip(), errors="coerce")

    out = df[["cod_stazione", "nome_stazione", "lat", "lon", "citta"]].copy()
    out = out[out["cod_stazione"].astype(str).str.len() > 0]
    out = out.drop_duplicates(subset=["cod_stazione"], keep="first")

    return out



    except Exception as e:
        fallback = [
            "Aosta","Torino","Genova","Milano","Trento","Venezia","Trieste","Bologna","Firenze","Ancona",
            "Perugia","Roma","L'Aquila","Campobasso","Napoli","Bari","Potenza","Catanzaro","Palermo","Cagliari"
        ]
        out = pd.DataFrame({"citta": fallback}).drop_duplicates().sort_values("citta").reset_index(drop=True)
        ensure_dir(os.path.dirname(local_path))
        out.to_csv(local_path, index=False)
        print({"capoluoghi_source": "fallback", "warning": str(e)})
        return out


def _load_station_seed_from_gold() -> Tuple[pd.DataFrame, str]:
    p1 = os.path.join("data", "gold", "stazioni_mese_categoria_nodo.csv")
    p2 = os.path.join("data", "gold", "stazioni_mese_categoria_ruolo.csv")
    p3 = os.path.join("data", "gold", "od_mese_categoria.csv")

    df = _read_csv_if_exists(p1)
    if df is not None and len(df) > 0:
        if "cod_stazione" in df.columns and "nome_stazione" in df.columns:
            out = df[["cod_stazione", "nome_stazione"]].drop_duplicates()
            out["cod_stazione"] = out["cod_stazione"].astype(str).str.strip()
            out["nome_stazione"] = out["nome_stazione"].astype(str).fillna("").str.strip()
            out = out[out["cod_stazione"] != ""]
            return out, "stazioni_mese_categoria_nodo"

    df = _read_csv_if_exists(p2)
    if df is not None and len(df) > 0:
        if "cod_stazione" in df.columns and "nome_stazione" in df.columns:
            out = df[["cod_stazione", "nome_stazione"]].drop_duplicates()
            out["cod_stazione"] = out["cod_stazione"].astype(str).str.strip()
            out["nome_stazione"] = out["nome_stazione"].astype(str).fillna("").str.strip()
            out = out[out["cod_stazione"] != ""]
            return out, "stazioni_mese_categoria_ruolo"

    df = _read_csv_if_exists(p3)
    if df is not None and len(df) > 0:
        needed = {"cod_partenza","nome_partenza","cod_arrivo","nome_arrivo"}
        if needed.issubset(set(df.columns)):
            a = df[["cod_partenza","nome_partenza"]].rename(columns={"cod_partenza":"cod_stazione","nome_partenza":"nome_stazione"})
            b = df[["cod_arrivo","nome_arrivo"]].rename(columns={"cod_arrivo":"cod_stazione","nome_arrivo":"nome_stazione"})
            out = pd.concat([a, b], ignore_index=True)
            out["cod_stazione"] = out["cod_stazione"].astype(str).str.strip()
            out["nome_stazione"] = out["nome_stazione"].astype(str).fillna("").str.strip()
            out = out[out["cod_stazione"] != ""].drop_duplicates(subset=["cod_stazione"], keep="last")
            return out, "od_mese_categoria"

    empty = pd.DataFrame({"cod_stazione": [], "nome_stazione": []})
    return empty, "none"


def main() -> None:
    cfg: Dict[str, Any] = load_yaml("config/pipeline.yml")

    seed, seed_src = _load_station_seed_from_gold()
    reg = load_station_registry_or_empty()

    joined = seed.merge(reg, left_on="cod_stazione", right_on="cod_stazione", how="left", suffixes=("", "_reg"))

    if "nome_stazione_reg" in joined.columns:
        joined["nome_stazione"] = joined["nome_stazione"].where(joined["nome_stazione"] != "", joined["nome_stazione_reg"])
        joined = joined.drop(columns=["nome_stazione_reg"])

    if "lat" not in joined.columns:
        joined["lat"] = pd.NA
    if "lon" not in joined.columns:
        joined["lon"] = pd.NA
    if "citta" not in joined.columns:
        joined["citta"] = ""

    joined["lat"] = pd.to_numeric(joined["lat"], errors="coerce")
    joined["lon"] = pd.to_numeric(joined["lon"], errors="coerce")
    joined["citta"] = joined["citta"].astype(str).fillna("").str.strip()

    out = joined[["cod_stazione", "nome_stazione", "lat", "lon", "citta"]].copy()

    ensure_dir(os.path.join("site", "data"))
    out.to_csv(os.path.join("site", "data", "stations_dim.csv"), index=False)

    ensure_dir(os.path.join("data", "gold"))
    out.to_csv(os.path.join("data", "gold", "stations_dim.csv"), index=False)

    missing_mask = out["lat"].isna() | out["lon"].isna()
    missing = out.loc[missing_mask, ["cod_stazione", "nome_stazione"]].drop_duplicates()

    missing_path = os.path.join("data", "stations", "stations_unknown.csv")
    ensure_dir(os.path.dirname(missing_path))
    missing.to_csv(missing_path, index=False)

    cap = build_capoluoghi_provincia_csv(cfg)
    ensure_dir(os.path.join("site", "data"))
    cap.to_csv(os.path.join("site", "data", "capoluoghi_provincia.csv"), index=False)

    print(
        {
            "stations_seed_source": seed_src,
            "stations_seed_rows": int(len(seed)),
            "stations_dim_rows": int(len(out)),
            "stations_with_coords": int((~missing_mask).sum()),
            "stations_missing_coords": int(len(missing)),
            "capoluoghi_rows": int(len(cap)),
        }
    )


if __name__ == "__main__":
    main()
