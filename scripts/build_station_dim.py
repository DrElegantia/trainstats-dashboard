# scripts/build_station_dim.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Set

import pandas as pd


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


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
            continue
    
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
            
            # Partenze
            if "cod_partenza" in df.columns and "nome_partenza" in df.columns:
                part_df = df[["cod_partenza", "nome_partenza"]].dropna()
                for _, row in part_df.iterrows():
                    code = str(row["cod_partenza"])
                    name = str(row["nome_partenza"])
                    if code and name and code not in names:
                        names[code] = name
            
            # Arrivi
            if "cod_arrivo" in df.columns and "nome_arrivo" in df.columns:
                arr_df = df[["cod_arrivo", "nome_arrivo"]].dropna()
                for _, row in arr_df.iterrows():
                    code = str(row["cod_arrivo"])
                    name = str(row["nome_arrivo"])
                    if code and name and code not in names:
                        names[code] = name
                        
        except Exception as e:
            print(f"Warning: Could not read {parquet_file}: {e}")
            continue
    
    return names


def build_station_dim() -> pd.DataFrame:
    """Costruisce la tabella dimensionale delle stazioni."""
    codes = collect_station_codes()
    names = collect_station_names()
    
    if not codes:
        return pd.DataFrame(columns=["cod_stazione", "nome_stazione"])
    
    records = []
    for code in sorted(codes):
        records.append({
            "cod_stazione": code,
            "nome_stazione": names.get(code, "")
        })
    
    return pd.DataFrame(records)


def save_station_dim(df: pd.DataFrame) -> None:
    """Salva la dimensione delle stazioni in data/gold/."""
    gold_dir = Path("data") / "gold"
    ensure_dir(str(gold_dir))
    
    output_path = gold_dir / "stations_dim.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")
    
    print(f"Station dimension saved: {output_path}")
    print(f"Total stations: {len(df)}")


def main() -> None:
    """Entry point per lo script."""
    df = build_station_dim()
    
    if df.empty:
        print("Warning: No station data found in silver files")
        return
    
    save_station_dim(df)


if __name__ == "__main__":
    main()
