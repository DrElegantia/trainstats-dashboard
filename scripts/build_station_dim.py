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
    p = os.path.join("data", "stations", "stations.csv")
    if not os.path.exists(p):
        df = pd.DataFrame({"cod_stazione": pd.Series([], dtype=str)})
        df["nome_stazione"] = pd.Series([], dtype=str)
        df["lat"] = pd.Series([], dtype=float)
        df["lon"] = pd.Series([], dtype=float)
        df["citta"] = pd.Series([], dtype=str)
        return df

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

    if "nome_stazione" not in df.columns:
        df["nome_stazione"] = ""

    for c in ("lat", "lon"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df[c] = pd.Series([pd.NA] * len(df), dtype="float64")

    if "citta" not in df.columns:
        for alt in ("comune", "city", "nome_comune", "municipality"):
            if alt in df.columns:
                df = df.rename(columns={alt: "citta"})
                break
    if "citta" not in df.columns:
        df["citta"] = ""

    df["nome_stazione"] = df["nome_stazione"].astype(str).fillna("").str.strip()
    df["citta"] = df["citta"].astype(str).fillna("").str.strip()

    keep = ["cod_stazione", "nome_stazione", "lat", "lon", "citta"]
    for k in keep:
        if k not in df.columns:
            df[k] = "" if k in ("cod_stazione", "nome_stazione", "citta") else pd.NA

    return df[keep].copy()


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
        r = http_get_with_retry(
            ISTAT_COMUNI_URL,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff,
        )
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
