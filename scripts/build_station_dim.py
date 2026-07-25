# scripts/build_station_dim.py
from __future__ import annotations

import os
import re
import time
import argparse
from pathlib import Path
from typing import Dict, Set, Optional, Tuple

import pandas as pd

try:
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut, GeocoderServiceError
    HAS_GEOPY = True
except ImportError:
    HAS_GEOPY = False


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


# ---------------------------------------------------------------------------
# Abbreviazioni ferroviarie italiane: usate per normalizzare i nomi stazione
# in modo che "BOLOGNA C.LE" e "BOLOGNA CENTRALE" producano la stessa chiave.
# ---------------------------------------------------------------------------
_ABBR_MAP = [
    (r"\bC\.LE\.?(?=\s|$)", "CENTRALE"),
    (r"\bC\.L\.E\.?(?=\s|$)", "CENTRALE"),
    (r"\bP\.TA(?=\s|$)", "PORTA"),
    (r"\bP\.ZA(?=\s|$)", "PIAZZA"),
    (r"\bP\.ZZA(?=\s|$)", "PIAZZA"),
    (r"\bS\.M\.N\.?(?=\s|$)", "SANTA MARIA NOVELLA"),
    (r"\bS\.M\.(?=\s|$)", "SANTA MARIA"),
    (r"\bSS\.(?=\s|$)", "SANTI"),
    (r"\bS\.(?=\s?\w)", "SAN "),
    (r"\bF\.S\.(?=\s|$)", ""),
    (r"\bFS(?=\s|$)", ""),
    (r"\bM\.MO(?=\s|$)", "MARITTIMO"),
    (r"\bMAR\.MO(?=\s|$)", "MARITTIMO"),
]


def _normalize_for_match(name: str) -> str:
    """Normalizza il nome stazione per il matching (espande abbreviazioni)."""
    s = str(name).strip().upper()
    for pattern, repl in _ABBR_MAP:
        s = re.sub(pattern, repl, s)
    s = re.sub(r"['\"\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def collect_station_codes() -> Set[str]:
    """Raccoglie tutti i codici stazione unici dai file silver."""
    codes: Set[str] = set()
    silver_root = Path("data") / "silver"
    if not silver_root.exists():
        return codes
    for parquet_file in silver_root.rglob("*.parquet"):
        try:
            df = pd.read_parquet(parquet_file)
            if "cod_partenza" in df.columns:
                codes.update(df["cod_partenza"].dropna().astype(str).unique())
            if "cod_arrivo" in df.columns:
                codes.update(df["cod_arrivo"].dropna().astype(str).unique())
        except Exception as e:
            print(f"Warning: Could not read {parquet_file}: {e}")
    return codes


def collect_station_names() -> Dict[str, str]:
    """Raccoglie i nomi delle stazioni dai file silver."""
    names: Dict[str, str] = {}
    silver_root = Path("data") / "silver"
    if not silver_root.exists():
        return names
    for parquet_file in silver_root.rglob("*.parquet"):
        try:
            df = pd.read_parquet(parquet_file)
            if "cod_partenza" in df.columns and "nome_partenza" in df.columns:
                part_df = df[["cod_partenza", "nome_partenza"]].dropna()
                for _, row in part_df.iterrows():
                    code = str(row["cod_partenza"])
                    name = str(row["nome_partenza"])
                    if code and name and code not in names:
                        names[code] = name
            if "cod_arrivo" in df.columns and "nome_arrivo" in df.columns:
                arr_df = df[["cod_arrivo", "nome_arrivo"]].dropna()
                for _, row in arr_df.iterrows():
                    code = str(row["cod_arrivo"])
                    name = str(row["nome_arrivo"])
                    if code and name and code not in names:
                        names[code] = name
        except Exception as e:
            print(f"Warning: Could not read {parquet_file}: {e}")
    return names


def load_cached_stations() -> pd.DataFrame:
    """Carica il file cache stations/stations.csv come DataFrame."""
    cache_path = Path("stations") / "stations.csv"
    if not cache_path.exists():
        return pd.DataFrame(columns=["cod_stazione", "nome_stazione", "lat", "lon"])
    try:
        return pd.read_csv(cache_path)
    except Exception as e:
        print(f"Warning: Could not read cache {cache_path}: {e}")
        return pd.DataFrame(columns=["cod_stazione", "nome_stazione", "lat", "lon"])


def load_cached_coords(cache_df: pd.DataFrame) -> Dict[str, Tuple[float, float]]:
    """Estrae la mappa codice -> (lat, lon) dal DataFrame cache."""
    coords: Dict[str, Tuple[float, float]] = {}
    for _, row in cache_df.iterrows():
        code = str(row.get("cod_stazione", "")).strip()
        lat = row.get("lat")
        lon = row.get("lon")
        if code and pd.notna(lat) and pd.notna(lon):
            coords[code] = (float(lat), float(lon))
    return coords


def build_name_to_coords(cache_df: pd.DataFrame) -> Dict[str, Tuple[float, float]]:
    """Costruisce un lookup nome_normalizzato -> (lat, lon) dalla cache.

    Permette di risolvere coordinate per stazioni con codici sintetici N_
    (derivati da nome) matchandole alle stazioni note nella cache tramite
    il nome espanso (es. 'BOLOGNA C.LE' -> 'BOLOGNA CENTRALE').
    """
    lookup: Dict[str, Tuple[float, float]] = {}
    for _, row in cache_df.iterrows():
        name = str(row.get("nome_stazione", "")).strip()
        lat = row.get("lat")
        lon = row.get("lon")
        if name and pd.notna(lat) and pd.notna(lon):
            key = _normalize_for_match(name)
            if key and key not in lookup:
                lookup[key] = (float(lat), float(lon))
    return lookup


def save_coords_cache(records: list) -> None:
    """Salva la cache delle coordinate in stations/stations.csv."""
    cache_dir = Path("stations")
    ensure_dir(str(cache_dir))
    cache_path = cache_dir / "stations.csv"
    df = pd.DataFrame(records)
    df.to_csv(cache_path, index=False, encoding="utf-8")
    print(f"Coords cache saved: {cache_path} ({len(df)} stations)")


def clean_station_name(name: str) -> str:
    """Pulisce il nome stazione per migliorare il geocoding."""
    name = name.strip()
    name = re.sub(r'\(S\d+\)', '', name)
    name = name.replace("S.", "San ").replace("S ", "San ")
    name = name.replace("P.", "Porta ").replace("P ", "Porta ")
    return name.strip()


def geocode_station(geolocator, name: str) -> Optional[Tuple[float, float]]:
    """Geocodifica un nome stazione aggiungendo ', Italy' per contesto."""
    if not name:
        return None

    cleaned = clean_station_name(name)
    queries = [
        f"stazione {cleaned}, Italia",
        f"{cleaned}, Italia",
        f"{cleaned}, Italy",
    ]

    for query in queries:
        try:
            location = geolocator.geocode(query, timeout=10)
            if location:
                if 35 <= location.latitude <= 48 and 6 <= location.longitude <= 19:
                    return (round(location.latitude, 6), round(location.longitude, 6))
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"  Geocoding timeout/error for '{query}': {e}")
            time.sleep(2)
        except Exception as e:
            print(f"  Geocoding error for '{query}': {e}")
        time.sleep(1.1)

    return None


def build_station_dim(enable_geocoding: bool = True) -> pd.DataFrame:
    """Costruisce la tabella dimensionale delle stazioni con coordinate.

    Strategia per assegnare le coordinate (in ordine di priorità):
      1. Codice stazione presente nella cache -> coordinate dalla cache
      2. Nome stazione (normalizzato) presente nella cache -> coordinate via
         name-matching (risolve codici sintetici N_ derivati dal nome)
      3. Geocoding online via Nominatim (se geopy è installato)

    Include SEMPRE tutte le stazioni dalla cache, anche se non presenti nei
    file silver correnti, in modo che la dimensione sia completa anche quando
    il workflow processa solo un sottoinsieme di date.
    """
    codes = collect_station_codes()
    names = collect_station_names()

    # Carica cache completa
    cache_df = load_cached_stations()
    cached_coords = load_cached_coords(cache_df)
    name_coords = build_name_to_coords(cache_df)
    print(f"Loaded {len(cached_coords)} cached coordinates, "
          f"{len(name_coords)} name->coords mappings")

    # Prepara geocoder per codici non risolvibili
    codes_unresolved = [
        c for c in codes
        if c not in cached_coords
        and _normalize_for_match(names.get(c, "")) not in name_coords
    ]
    geolocator = None
    if codes_unresolved and enable_geocoding and HAS_GEOPY:
        print(f"Need to geocode {len(codes_unresolved)} stations...")
        geolocator = Nominatim(user_agent="trainstats-dashboard/1.0")
    elif codes_unresolved and enable_geocoding and not HAS_GEOPY:
        print(f"WARNING: geopy not installed, {len(codes_unresolved)} stations "
              "without coordinates")
    elif codes_unresolved and not enable_geocoding:
        print(
            f"Geocoding disabled, leaving {len(codes_unresolved)} stations "
            "without coordinates"
        )

    # ---- Stazioni trovate nel silver corrente ----
    records = []
    geocoded_count = 0
    name_matched_count = 0
    failed_count = 0

    seen_codes: Set[str] = set()

    for code in sorted(codes):
        name = names.get(code, "")
        lat = None
        lon = None

        if code in cached_coords:
            lat, lon = cached_coords[code]
        else:
            # Prova name-matching: espandi abbreviazioni e cerca nella cache
            norm_name = _normalize_for_match(name)
            if norm_name and norm_name in name_coords:
                lat, lon = name_coords[norm_name]
                name_matched_count += 1
            elif geolocator and name:
                print(f"  Geocoding: {code} -> {name}...", end=" ")
                coords = geocode_station(geolocator, name)
                if coords:
                    lat, lon = coords
                    geocoded_count += 1
                    print(f"OK ({lat}, {lon})")
                else:
                    failed_count += 1
                    print("FAILED")

        records.append({
            "cod_stazione": code,
            "nome_stazione": name,
            "lat": lat,
            "lon": lon,
        })
        seen_codes.add(code)

    # ---- Aggiungi TUTTE le stazioni dalla cache non già presenti ----
    # Questo garantisce che la dimensione sia completa anche per run parziali.
    cache_added = 0
    for _, row in cache_df.iterrows():
        code = str(row.get("cod_stazione", "")).strip()
        if code and code not in seen_codes:
            records.append({
                "cod_stazione": code,
                "nome_stazione": str(row.get("nome_stazione", "")),
                "lat": row.get("lat") if pd.notna(row.get("lat")) else None,
                "lon": row.get("lon") if pd.notna(row.get("lon")) else None,
            })
            seen_codes.add(code)
            cache_added += 1

    print(f"\nStation dim summary: {name_matched_count} name-matched, "
          f"{geocoded_count} geocoded, {failed_count} failed, "
          f"{cache_added} from cache (not in silver)")

    # Aggiorna la cache con eventuali nuove coordinate (da geocoding)
    cache_records = [r for r in records if r["lat"] is not None]
    if geocoded_count > 0 and cache_records:
        save_coords_cache(cache_records)

    return pd.DataFrame(records)


def _deduplicate_by_name(df: pd.DataFrame) -> pd.DataFrame:
    """Propaga coordinate tra stazioni con lo stesso nome ma codici diversi.

    NON rimuove righe: mantiene TUTTI i codici stazione nell'output in modo
    che il frontend possa risolvere qualsiasi codice (anche quelli storici
    come N_xxx sostituiti da S0xxxx).

    Per ogni gruppo di stazioni con lo stesso nome normalizzato, se almeno un
    record ha coordinate valide, le propaga a tutti i record del gruppo che
    ne sono privi.
    """
    if df.empty:
        return df

    df = df.copy()
    df["_name_key"] = df["nome_stazione"].apply(_normalize_for_match)

    # Build lookup: normalized name -> best (lat, lon) from any record in group
    best_coords: Dict[str, Tuple[float, float]] = {}
    for _, row in df.iterrows():
        key = row["_name_key"]
        if not key:
            continue
        lat, lon = row.get("lat"), row.get("lon")
        if pd.notna(lat) and pd.notna(lon) and key not in best_coords:
            best_coords[key] = (float(lat), float(lon))

    # Propagate coordinates to records missing them
    propagated = 0
    for idx, row in df.iterrows():
        key = row["_name_key"]
        if key and (pd.isna(row["lat"]) or pd.isna(row["lon"])) and key in best_coords:
            df.at[idx, "lat"] = best_coords[key][0]
            df.at[idx, "lon"] = best_coords[key][1]
            propagated += 1

    if propagated > 0:
        print(f"Coordinate propagation: {propagated} stations received coords from same-name entries")

    return df.drop(columns=["_name_key"]).reset_index(drop=True)


def save_station_dim(df: pd.DataFrame) -> None:
    """Salva la dimensione delle stazioni in data/gold/."""
    gold_dir = Path("data") / "gold"
    ensure_dir(str(gold_dir))

    # Deduplica per nome prima di salvare
    df = _deduplicate_by_name(df)

    output_path = gold_dir / "stations_dim.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")

    coords_count = df["lat"].notna().sum()
    print(f"Station dimension saved: {output_path}")
    print(f"Total stations: {len(df)}, with coordinates: {coords_count}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build station dimension table with optional geocoding"
    )
    parser.add_argument(
        "--disable-geocoding",
        action="store_true",
        help="Do not query Nominatim for unresolved station names",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    # In CI prefer deterministic builds: avoid external geocoding unless explicitly enabled.
    ci_default_disable = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
    enable_geocoding = not (args.disable_geocoding or ci_default_disable)

    if not enable_geocoding:
        print("Running without online geocoding")

    df = build_station_dim(enable_geocoding=enable_geocoding)
    if df.empty:
        print("Warning: No station data found in silver files")
        return
    save_station_dim(df)


if __name__ == "__main__":
    main()
