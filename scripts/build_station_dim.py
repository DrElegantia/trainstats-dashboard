# build_station_dim.py
from __future__ import annotations

import io
import os
from typing import Dict, Any, Optional

import pandas as pd

from .utils import ensure_dir, http_get_with_retry, load_yaml


ISTAT_COMUNI_URL = "https://www.istat.it/storage/codici-unita-amministrative/Elenco-comuni-italiani.csv"


def load_gold_station_table() -> pd.DataFrame:
    p = os.path.join("data", "gold", "stazioni_mese_categoria_nodo.csv")
    if not os.path.exists(p):
        raise FileNotFoundError(f"missing gold table: {p}")
    return pd.read_csv(p, dtype=str)


def load_station_registry() -> pd.DataFrame:
    p = os.path.join("data", "stations", "stations.csv")
    if not os.path.exists(p):
        raise FileNotFoundError(f"missing station registry: {p}")
    df = pd.read_csv(p, dtype=str)

    rename_map = {}
    if "codice" in df.columns and "cod_stazione" not in df.columns:
        rename_map["codice"] = "cod_stazione"
    if "nome_norm" in df.columns and "nome_stazione" not in df.columns:
        rename_map["nome_norm"] = "nome_stazione"
    if rename_map:
        df = df.rename(columns=rename_map)

    if "cod_stazione" not in df.columns:
        raise ValueError("station registry must contain 'cod_stazione' (or 'codice')")

    df["cod_stazione"] = df["cod_stazione"].astype(str).str.strip()

    for c in ("lat", "lon"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def _pick_col(cols: list[str], must_contain: str) -> Optional[str]:
    needle = must_contain.lower()
    for c in cols:
        if needle in c.lower():
            return c
    return None


def build_capoluoghi_provincia_csv(cfg: Dict[str, Any]) -> pd.DataFrame:
    local_path = os.path.join("data", "stations", "capoluoghi_provincia.csv")
    if os.path.exists(local_path):
        df = pd.read_csv(local_path, dtype=str)
        if "citta" not in df.columns:
            for alt in ("capoluogo", "nome", "comune", "city"):
                if alt in df.columns:
                    df = df.rename(columns={alt: "citta"})
                    break
        if "citta" not in df.columns:
            raise ValueError(f"{local_path} must contain a 'citta' column")
        df["citta"] = df["citta"].astype(str).str.strip()
        df = df[df["citta"] != ""].drop_duplicates(subset=["citta"]).sort_values("citta")
        return df[["citta"]].reset_index(drop=True)

    net = cfg.get("network", {})
    timeout = int(net.get("timeout_seconds", 20))
    max_retries = int(net.get("max_retries", 3))
    backoff = int(net.get("backoff_factor", 2))

    try:
        r = http_get_with_retry(ISTAT_COMUNI_URL, timeout=timeout, max_retries=max_retries, backoff_factor=backoff)
        raw = r.content.decode("utf-8", errors="replace")
        df = pd.read_csv(io.StringIO(raw), sep=";", dtype=str)
        df.columns = [str(c).strip() for c in df.columns]

        col_city = _pick_col(list(df.columns), "Denominazione in italiano") or _pick_col(list(df.columns), "Denominazione in Italiano")
        col_flag = _pick_col(list(df.columns), "Flag Comune capoluogo di provincia") or _pick_col(list(df.columns), "capoluogo di provincia")

        if not col_city or not col_flag:
            raise ValueError("ISTAT columns not found (city name or capoluogo flag)")

        flag = df[col_flag].astype(str).str.strip()
        is_cap = flag.isin(["1", "SI", "SÌ", "TRUE", "True", "true", "Y", "y"])

        out = df.loc[is_cap, [col_city]].rename(columns={col_city: "citta"})
        out["citta"] = out["citta"].astype(str).str.strip()
        out = out[out["citta"] != ""].drop_duplicates(subset=["citta"]).sort_values("citta").reset_index(drop=True)

        ensure_dir(os.path.dirname(local_path))
        out.to_csv(local_path, index=False)
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


def main() -> None:
    cfg: Dict[str, Any] = load_yaml("config/pipeline.yml")

    gold = load_gold_station_table()
    reg = load_station_registry()

    if "cod_stazione" not in gold.columns or "nome_stazione" not in gold.columns:
        raise ValueError("gold table must contain 'cod_stazione' and 'nome_stazione' columns")

    seen = gold[["cod_stazione", "nome_stazione"]].drop_duplicates().copy()
    seen["cod_stazione"] = seen["cod_stazione"].astype(str).str.strip()

    joined = seen.merge(reg, on="cod_stazione", how="left", suffixes=("", "_reg"))

    out_dir = os.path.join("site", "data")
    ensure_dir(out_dir)
    joined.to_csv(os.path.join(out_dir, "stations_dim.csv"), index=False)

    missing_cols = ["cod_stazione", "nome_stazione"]
    if {"lat", "lon"}.issubset(joined.columns):
        missing_mask = joined["lat"].isna() | joined["lon"].isna()
    else:
        missing_mask = pd.Series(True, index=joined.index)
    missing = joined.loc[missing_mask, missing_cols].drop_duplicates()

    missing_path = os.path.join("data", "stations", "stations_unknown.csv")
    ensure_dir(os.path.dirname(missing_path))
    missing.to_csv(missing_path, index=False)

    cap = build_capoluoghi_provincia_csv(cfg)
    cap.to_csv(os.path.join(out_dir, "capoluoghi_provincia.csv"), index=False)

    print(
        {
            "stations_dim_rows": int(len(joined)),
            "stations_missing_coords": int(len(missing)),
            "capoluoghi_rows": int(len(cap)),
        }
    )


if __name__ == "__main__":
    main()
