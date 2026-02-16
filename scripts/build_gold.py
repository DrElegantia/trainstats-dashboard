from __future__ import annotations

import os
import glob
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from .utils import ensure_dir, load_yaml, bucketize_delay


GOLD_DIR = os.path.join("data", "gold")
SILVER_DIR = os.path.join("data", "silver")
STATION_REGISTRY_PATH = os.path.join("data", "stations", "stations.csv")


def list_silver_month_parquets() -> List[str]:
    patterns = [
        os.path.join(SILVER_DIR, "*.parquet"),
        os.path.join(SILVER_DIR, "*", "*.parquet"),
        os.path.join(SILVER_DIR, "*", "*", "*.parquet"),
    ]
    out: List[str] = []
    for pat in patterns:
        out.extend(glob.glob(pat))
    out = [p for p in out if os.path.isfile(p)]
    out = sorted(set(out))
    return out


def load_silver_all() -> pd.DataFrame:
    paths = list_silver_month_parquets()
    if not paths:
        raise ValueError("no silver parquet found")

    frames: List[pd.DataFrame] = []
    for p in paths:
        try:
            frames.append(pd.read_parquet(p))
        except Exception as e:
            print({"warning": "failed_read_silver", "path": p, "error": str(e)})
    if not frames:
        raise ValueError("no readable silver parquet found")

    df = pd.concat(frames, ignore_index=True)
    return df


def load_station_registry() -> pd.DataFrame:
    if not os.path.exists(STATION_REGISTRY_PATH):
        raise FileNotFoundError(f"missing station registry: {STATION_REGISTRY_PATH}")
    df = pd.read_csv(STATION_REGISTRY_PATH, dtype=str)

    rename_map = {}
    if "codice" in df.columns and "cod_stazione" not in df.columns:
        rename_map["codice"] = "cod_stazione"
    if "nome_norm" in df.columns and "nome_stazione" not in df.columns:
        rename_map["nome_norm"] = "nome_stazione"
    if rename_map:
        df = df.rename(columns=rename_map)

    if "cod_stazione" not in df.columns:
        raise ValueError("station registry must contain 'cod_stazione' (or 'codice')")
    if "nome_stazione" not in df.columns:
        # non è obbligatorio, ma aiuta le tabelle
        df["nome_stazione"] = df["cod_stazione"]

    df["cod_stazione"] = df["cod_stazione"].astype(str).str.strip()
    df["nome_stazione"] = df["nome_stazione"].astype(str).str.strip()

    return df[["cod_stazione", "nome_stazione"]].drop_duplicates(subset=["cod_stazione"])


def _normalize_bucket_label(s: str) -> str:
    # rende robusto il matching se in YAML ci sono spazi tipo "(-60, -30]"
    return str(s).replace(" ", "").strip()


def build_metrics(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    required = [
        "categoria",
        "cod_partenza",
        "cod_arrivo",
        "ritardo_partenza_min",
        "ritardo_arrivo_min",
        "tipo_evento",
        "dt_partenza_prog",
        "dt_arrivo_prog",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"silver missing columns: {missing}")

    out = df.copy()

    # Giorno/mese di riferimento: preferiamo data_riferimento se presente (più coerente col backfill giornaliero)
    if "data_riferimento" in out.columns:
        dt_ref = pd.to_datetime(out["data_riferimento"], errors="coerce")
        # fallback su dt_partenza_prog per eventuali record legacy
        dt_ref = dt_ref.fillna(pd.to_datetime(out["dt_partenza_prog"], errors="coerce"))
    else:
        dt_ref = pd.to_datetime(out["dt_partenza_prog"], errors="coerce")

    out["giorno"] = dt_ref.dt.strftime("%Y-%m-%d")
    out["mese"] = dt_ref.dt.to_period("M").astype(str)
    out["anno"] = dt_ref.dt.year.astype("Int64").astype(str)

    # Ora per filtri orari: usiamo l'ora di partenza programmata
    out["ora"] = pd.to_datetime(out["dt_partenza_prog"], errors="coerce").dt.hour.astype("Int64")

    out["categoria"] = out["categoria"].astype(str).str.strip().replace({"": "unknown"})

    # normalizzazione numerica
    out["ritardo_arrivo_min"] = pd.to_numeric(out["ritardo_arrivo_min"], errors="coerce").fillna(0).astype(int)
    out["ritardo_partenza_min"] = pd.to_numeric(out["ritardo_partenza_min"], errors="coerce").fillna(0).astype(int)

    punctuality = cfg.get("punctuality", {})
    on_time_thr = int(punctuality.get("on_time_threshold_minutes", 5))

    delay_buckets = cfg.get("delay_buckets_minutes", {})
    edges = delay_buckets.get("edges")
    labels = delay_buckets.get("labels")
    if not isinstance(edges, list) or not isinstance(labels, list) or len(edges) - 1 != len(labels):
        raise ValueError("config delay_buckets_minutes must contain edges and labels with len(labels)=len(edges)-1")
    labels = [_normalize_bucket_label(x) for x in labels]

    def status_from_minutes(x: int) -> str:
        if x <= -1:
            return "in_anticipo"
        if x <= on_time_thr - 1:
            return "in_orario"
        return "in_ritardo"

    out["stato_arrivo"] = out["ritardo_arrivo_min"].apply(status_from_minutes)

    # bucket istogramma
    out["bucket_ritardo_arrivo"] = out["ritardo_arrivo_min"].apply(
        lambda x: _normalize_bucket_label(bucketize_delay(int(x), edges=edges, labels=labels))
    )

    # flags evento
    out["effettuato"] = (out["tipo_evento"] == "effettuato").astype(int)
    out["cancellato"] = (out["tipo_evento"] == "cancellato").astype(int)
    out["soppresso"] = (out["tipo_evento"] == "soppresso").astype(int)
    out["parzialmente_cancellato"] = (out["tipo_evento"] == "parzialmente_cancellato").astype(int)
    out["info_mancante"] = (out["tipo_evento"] == "info_mancante").astype(int)

    out["corse_osservate"] = 1
    out["in_orario"] = (out["stato_arrivo"] == "in_orario").astype(int)
    out["in_ritardo"] = (out["stato_arrivo"] == "in_ritardo").astype(int)
    out["in_anticipo"] = (out["stato_arrivo"] == "in_anticipo").astype(int)

    # soglie classiche di puntualità (sempre su ritardo arrivo)
    ra = out["ritardo_arrivo_min"]
    out["oltre_5"] = (ra >= 5).astype(int)
    out["oltre_10"] = (ra >= 10).astype(int)
    out["oltre_15"] = (ra >= 15).astype(int)
    out["oltre_30"] = (ra >= 30).astype(int)
    out["oltre_60"] = (ra >= 60).astype(int)

    # minuti: sommiamo separatamente ritardo positivo e anticipo negativo
    out["minuti_ritardo"] = ra.clip(lower=0).astype(int)
    out["minuti_anticipo"] = (-ra.clip(upper=0)).astype(int)
    out["minuti_netti"] = ra.astype(int)

    # codici stazione
    out["cod_partenza"] = out["cod_partenza"].astype(str).str.strip()
    out["cod_arrivo"] = out["cod_arrivo"].astype(str).str.strip()

    return out


def agg_core(g: pd.DataFrame) -> pd.Series:
    return pd.Series(
        {
            "corse_osservate": int(g["corse_osservate"].sum()),
            "effettuate": int(g["effettuato"].sum()),
            "cancellate": int(g["cancellato"].sum()),
            "soppresse": int(g["soppresso"].sum()),
            "parzialmente_cancellate": int(g["parzialmente_cancellato"].sum()),
            "info_mancante": int(g["info_mancante"].sum()),
            "in_orario": int(g["in_orario"].sum()),
            "in_ritardo": int(g["in_ritardo"].sum()),
            "in_anticipo": int(g["in_anticipo"].sum()),
            "oltre_5": int(g["oltre_5"].sum()),
            "oltre_10": int(g["oltre_10"].sum()),
            "oltre_15": int(g["oltre_15"].sum()),
            "oltre_30": int(g["oltre_30"].sum()),
            "oltre_60": int(g["oltre_60"].sum()),
            "minuti_ritardo_tot": int(g["minuti_ritardo"].sum()),
            "minuti_anticipo_tot": int(g["minuti_anticipo"].sum()),
            "minuti_netti_tot": int(g["minuti_netti"].sum()),
        }
    )


def add_station_names(df: pd.DataFrame, stations: pd.DataFrame, code_col: str, name_col_out: str) -> pd.DataFrame:
    if code_col not in df.columns:
        df[name_col_out] = ""
        return df
    m = stations.rename(columns={"cod_stazione": code_col, "nome_stazione": name_col_out})
    out = df.merge(m, on=code_col, how="left")
    out[name_col_out] = out[name_col_out].fillna(out[code_col]).astype(str)
    return out


def main() -> None:
    cfg: Dict[str, Any] = load_yaml("config/pipeline.yml")

    ensure_dir(GOLD_DIR)

    silver = load_silver_all()
    stations = load_station_registry()

    base = build_metrics(silver, cfg)

    # KPI mese
    kpi_mese = base.groupby(["mese"], as_index=False).apply(lambda g: agg_core(g)).reset_index(drop=True)
    kpi_mese.to_csv(os.path.join(GOLD_DIR, "kpi_mese.csv"), index=False)

    kpi_mese_cat = base.groupby(["mese", "categoria"], as_index=False).apply(lambda g: agg_core(g)).reset_index(drop=True)
    kpi_mese_cat.to_csv(os.path.join(GOLD_DIR, "kpi_mese_categoria.csv"), index=False)

    # KPI giorno
    kpi_giorno = base.groupby(["giorno"], as_index=False).apply(lambda g: agg_core(g)).reset_index(drop=True)
    kpi_giorno.to_csv(os.path.join(GOLD_DIR, "kpi_giorno.csv"), index=False)

    kpi_giorno_cat = base.groupby(["giorno", "categoria"], as_index=False).apply(lambda g: agg_core(g)).reset_index(drop=True)
    kpi_giorno_cat.to_csv(os.path.join(GOLD_DIR, "kpi_giorno_categoria.csv"), index=False)

    # KPI giorno ora categoria (per filtri orari)
    kpi_giorno_ora_cat = (
        base.dropna(subset=["ora"])
        .groupby(["giorno", "ora", "categoria"], as_index=False)
        .apply(lambda g: agg_core(g))
        .reset_index(drop=True)
    )
    kpi_giorno_ora_cat.to_csv(os.path.join(GOLD_DIR, "kpi_giorno_ora_categoria.csv"), index=False)

    # Istogrammi
    hist_mese_cat = (
        base.groupby(["mese", "categoria", "bucket_ritardo_arrivo"], as_index=False)["corse_osservate"]
        .sum()
        .rename(columns={"corse_osservate": "count"})
    )
    hist_mese_cat.to_csv(os.path.join(GOLD_DIR, "hist_mese_categoria.csv"), index=False)

    hist_giorno_cat = (
        base.groupby(["giorno", "categoria", "bucket_ritardo_arrivo"], as_index=False)["corse_osservate"]
        .sum()
        .rename(columns={"corse_osservate": "count"})
    )
    hist_giorno_cat.to_csv(os.path.join(GOLD_DIR, "hist_giorno_categoria.csv"), index=False)

    hist_giorno_ora_cat = (
        base.dropna(subset=["ora"])
        .groupby(["giorno", "ora", "categoria", "bucket_ritardo_arrivo"], as_index=False)["corse_osservate"]
        .sum()
        .rename(columns={"corse_osservate": "count"})
    )
    hist_giorno_ora_cat.to_csv(os.path.join(GOLD_DIR, "hist_giorno_ora_categoria.csv"), index=False)

    # Stazioni mese: partenze e arrivi (poi nodo)
    st_dep_m = (
        base.groupby(["mese", "categoria", "cod_partenza"], as_index=False)
        .apply(lambda g: agg_core(g))
        .reset_index(drop=True)
        .rename(columns={"cod_partenza": "cod_stazione"})
    )
    st_dep_m["ruolo"] = "partenza"

    st_arr_m = (
        base.groupby(["mese", "categoria", "cod_arrivo"], as_index=False)
        .apply(lambda g: agg_core(g))
        .reset_index(drop=True)
        .rename(columns={"cod_arrivo": "cod_stazione"})
    )
    st_arr_m["ruolo"] = "arrivo"

    st_nodo_m = pd.concat([st_dep_m, st_arr_m], ignore_index=True)
    st_nodo_m = (
        st_nodo_m.groupby(["mese", "categoria", "cod_stazione"], as_index=False)
        .sum(numeric_only=True)
        .assign(ruolo="nodo")
    )

    st_nodo_m = add_station_names(st_nodo_m, stations, "cod_stazione", "nome_stazione")
    st_nodo_m.to_csv(os.path.join(GOLD_DIR, "stazioni_mese_categoria_nodo.csv"), index=False)

    # Stazioni giorno: nodo
    st_dep_g = (
        base.groupby(["giorno", "categoria", "cod_partenza"], as_index=False)
        .apply(lambda g: agg_core(g))
        .reset_index(drop=True)
        .rename(columns={"cod_partenza": "cod_stazione"})
    )
    st_dep_g["ruolo"] = "partenza"

    st_arr_g = (
        base.groupby(["giorno", "categoria", "cod_arrivo"], as_index=False)
        .apply(lambda g: agg_core(g))
        .reset_index(drop=True)
        .rename(columns={"cod_arrivo": "cod_stazione"})
    )
    st_arr_g["ruolo"] = "arrivo"

    st_nodo_g = pd.concat([st_dep_g, st_arr_g], ignore_index=True)
    st_nodo_g = (
        st_nodo_g.groupby(["giorno", "categoria", "cod_stazione"], as_index=False)
        .sum(numeric_only=True)
        .assign(ruolo="nodo")
    )

    st_nodo_g = add_station_names(st_nodo_g, stations, "cod_stazione", "nome_stazione")
    st_nodo_g.to_csv(os.path.join(GOLD_DIR, "stazioni_giorno_categoria_nodo.csv"), index=False)

    # OD mese
    od_m = (
        base.groupby(["mese", "categoria", "cod_partenza", "cod_arrivo"], as_index=False)
        .apply(lambda g: agg_core(g))
        .reset_index(drop=True)
    )
    od_m = add_station_names(od_m, stations, "cod_partenza", "nome_partenza")
    od_m = add_station_names(od_m, stations, "cod_arrivo", "nome_arrivo")
    od_m.to_csv(os.path.join(GOLD_DIR, "od_mese_categoria.csv"), index=False)

    # OD giorno
    od_g = (
        base.groupby(["giorno", "categoria", "cod_partenza", "cod_arrivo"], as_index=False)
        .apply(lambda g: agg_core(g))
        .reset_index(drop=True)
    )
    od_g = add_station_names(od_g, stations, "cod_partenza", "nome_partenza")
    od_g = add_station_names(od_g, stations, "cod_arrivo", "nome_arrivo")
    od_g.to_csv(os.path.join(GOLD_DIR, "od_giorno_categoria.csv"), index=False)

    print(
        {
            "silver_rows": int(len(silver)),
            "gold_kpi_mese_rows": int(len(kpi_mese)),
            "gold_kpi_giorno_rows": int(len(kpi_giorno)),
            "gold_hist_mese_rows": int(len(hist_mese_cat)),
            "gold_hist_giorno_rows": int(len(hist_giorno_cat)),
            "gold_od_mese_rows": int(len(od_m)),
            "gold_od_giorno_rows": int(len(od_g)),
        }
    )


if __name__ == "__main__":
    main()
