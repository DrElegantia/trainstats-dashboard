# scripts/build_site.py
from __future__ import annotations

import glob
import hashlib
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd

from .build_gold import gold_table_names, read_gold_table
from .utils import load_yaml

# Tables large enough that the browser should fetch one year at a time instead
# of the whole history. Each still gets a full-history file for callers that
# want it; the dashboard prefers the per-year shards.
YEAR_SHARDED = {
    "od_mese_categoria",
    "od_dettaglio_categoria",
    "hist_stazioni_mese_categoria_ruolo",
    "hist_stazioni_dettaglio_categoria_ruolo",
    "stazioni_mese_categoria_nodo",
    "stazioni_dettaglio_categoria_nodo",
    "kpi_dettaglio_categoria",
    "hist_dettaglio_categoria",
}

# Per-row station names repeat the same few thousand strings across millions of
# rows. They are published once in station_names.csv and stripped from the fact
# tables, which is the single largest reduction in bytes shipped.
NAME_COLUMNS = ["nome_partenza", "nome_arrivo", "nome_stazione"]

CODE_NAME_PAIRS = [
    ("cod_partenza", "nome_partenza"),
    ("cod_arrivo", "nome_arrivo"),
    ("cod_stazione", "nome_stazione"),
]


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def compact_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Shrink a gold table's CSV footprint without changing what it says.

    Two sources of waste dominate:
      - counts written as floats ("1234.0"), a legacy of pandas promoting
        integer columns whenever a group had no matching rows;
      - means and percentiles written at full float64 precision
        ("3.1666666666666665"), seventeen significant digits for a figure the
        dashboard renders with two decimals.
    """
    out = df.copy()
    for col in out.columns:
        s = out[col]
        if not pd.api.types.is_numeric_dtype(s) or pd.api.types.is_bool_dtype(s):
            continue
        vals = s.dropna()
        if not vals.empty and (vals == vals.round()).all():
            out[col] = s.astype("Int64")
        else:
            out[col] = s.round(2)
    return out


def harvest_station_names(df: pd.DataFrame, pairs: Dict[str, str]) -> None:
    """Accumula le coppie codice -> nome trovate in una tabella."""
    for code_col, name_col in CODE_NAME_PAIRS:
        if code_col not in df.columns or name_col not in df.columns:
            continue
        sub = df[[code_col, name_col]].dropna().drop_duplicates(subset=[code_col])
        for code, name in zip(sub[code_col].astype(str), sub[name_col].astype(str)):
            code, name = code.strip(), name.strip()
            if code and name and code not in pairs:
                pairs[code] = name


def station_names_frame(pairs: Dict[str, str]) -> pd.DataFrame:
    return pd.DataFrame(sorted(pairs.items()), columns=["cod_stazione", "nome_stazione"])


def strip_name_columns(df: pd.DataFrame) -> pd.DataFrame:
    drop = [c for c in NAME_COLUMNS if c in df.columns]
    return df.drop(columns=drop) if drop else df


# Colonne il cui dominio e' un piccolo insieme chiuso di etichette lunghe.
# Su hist_stazioni_dettaglio_categoria_ruolo ogni riga ripeteva
# "infrasettimanale", "tarda_mattina", "partenza" e un'etichetta di bucket
# quotata: 1,18 milioni di volte, per 78 MB di CSV. Pubblicate come indice
# intero, con il codebook nel manifest, la stessa tabella scende a 41 MB.
ENCODED_COLUMNS = ["tipo_giorno", "fascia_oraria", "ruolo", "bucket_ritardo_arrivo"]


def harvest_codebook(df: pd.DataFrame, codebook: Dict[str, List[str]]) -> None:
    """Raccoglie i valori delle colonne categoriche in ordine deterministico.

    L'ordine conta: nei CSV pubblicati queste colonne sono numeri, e il numero
    e' la posizione nel codebook. Prima l'ordine era quello in cui i valori
    comparivano nei dati, quindi bastava una nuova classe dell'istogramma per
    rimescolare tutti gli indici, "arrivo" diventava "partenza" e la dashboard
    mostrava zero corse su qualunque stazione. Ordinando, la stessa build sugli
    stessi dati produce sempre gli stessi indici.
    """
    for col in ENCODED_COLUMNS:
        if col not in df.columns:
            continue
        known = codebook.setdefault(col, [])
        valori = set(known) | {str(v) for v in df[col].dropna().unique()}
        known[:] = sorted(valori)


def encode_frame(df: pd.DataFrame, codebook: Dict[str, List[str]]) -> pd.DataFrame:
    out = df
    for col in ENCODED_COLUMNS:
        if col not in out.columns or col not in codebook:
            continue
        index = {v: i for i, v in enumerate(codebook[col])}
        if out is df:
            out = df.copy()
        out[col] = out[col].astype(str).map(index).astype("Int64")
    return out


def write_table(df: pd.DataFrame, target: Path, name: str) -> List[str]:
    """Write the full table plus, for heavy tables, one file per year."""
    written: List[str] = []

    full = target / f"{name}.csv"
    df.to_csv(full, index=False)
    written.append(full.name)

    if name in YEAR_SHARDED and "mese" in df.columns:
        years = df["mese"].astype(str).str.slice(0, 4)
        for year, part in df.groupby(years, dropna=False):
            if not str(year).isdigit():
                continue
            shard = target / f"{name}.{year}.csv"
            part.to_csv(shard, index=False)
            written.append(shard.name)

    return written


def copy_root_files(target_dir: Path) -> None:
    """Copia stations_dim.csv e capoluoghi dalla root di data/."""
    ensure_dir(target_dir)

    for src in [Path("data") / "gold" / "stations_dim.csv", Path("data") / "stations_dim.csv"]:
        if src.exists():
            shutil.copy2(src, target_dir / "stations_dim.csv")
            print(f"Copied stations_dim.csv from {src}")
            break

    for src in [
        Path("data") / "capoluoghi_provincia.csv",
        Path("data") / "stations" / "capoluoghi_provincia.csv",
    ]:
        if src.exists():
            shutil.copy2(src, target_dir / "capoluoghi_provincia.csv")
            print(f"Copied capoluoghi_provincia.csv from {src}")
            break

    # La classifica per chilometro. E' l'unica tabella pubblicata che non e'
    # filtrabile: i chilometri non dipendono dall'anno o dalla categoria, e la
    # durata media di una tratta calcolata su tre corse non vorrebbe dire nulla.
    # Copre l'intero periodo e lo dichiara nella scheda.
    # La geometria della rete colorata dalla velocita'. Non si ricostruisce
    # qui: nasce dal grafo OpenStreetMap, che e' una cache da 21 MB non
    # versionata e assente in CI. Si copia com'e', come l'anagrafica.
    rete = Path("data") / "stations" / "velocita_rete.geojson"
    if rete.exists():
        shutil.copy2(rete, target_dir / "velocita_rete.geojson")
        print(f"Copied velocita_rete.geojson ({rete.stat().st_size // 1024} KB)")

    scrivi_tratte(target_dir)

    km = Path("data") / "stations" / "indicatori_km.csv"
    if km.exists():
        colonne = ["partenza", "arrivo", "km", "qualita_km", "corse",
                   "durata_media_min", "ritardo_medio_min", "min_per_100km",
                   "ritardo_per_100km", "km_h_programmati", "km_h_effettivi",
                   # Quali tratte attraversano lo Stretto lo sa solo la
                   # pipeline, che ha il grafo della rete: nel browser non c'e'.
                   "attraversa_stretto",
                   # Su quanti mesi poggia la media, e fra quali estremi. Senza
                   # queste tre colonne la classifica mette sulla stessa scala
                   # una tratta osservata per sei anni e una esistita sette mesi
                   # sparsi, e il lettore non ha modo di distinguerle.
                   "mesi", "primo_mese", "ultimo_mese"]
        d = pd.read_csv(km)
        d = d[[c for c in colonne if c in d.columns]]
        # Due decimali bastano: il file scende da 200 a 90 KB.
        d.to_csv(target_dir / "indicatori_km.csv", index=False, float_format="%.2f")
        print(f"Wrote indicatori_km.csv ({len(d)} tratte)")


def slug_stazione(nome: str) -> str:
    """Il nome di una stazione ridotto a un nome di file prevedibile.

    Deve dare lo stesso risultato qui e nel browser, perche' e' l'unico ponte
    fra la stazione scelta nella tendina e il file da scaricare.
    """
    import re
    import unicodedata

    s = unicodedata.normalize("NFKD", str(nome)).encode("ascii", "ignore").decode()
    s = re.sub(r"[^A-Za-z0-9]+", "-", s).strip("-").lower()
    return s or "senza-nome"


def scrivi_tratte(target_dir: Path) -> None:
    """Pubblica le tratte per fermata, un file per stazione di partenza.

    La tabella intera sono cinque milioni di righe: nessun browser la scarica.
    Ma chi guarda una tratta ha gia' scelto da dove parte, e le righe che gli
    servono sono solo quelle: divisa per stazione di partenza, la piu' grossa
    (Roma Termini) sta in meno di due megabyte e tutte le altre in molto meno.

    La colonna `da` non viene scritta: e' costante dentro ogni file, e ripeterla
    su ogni riga costerebbe piu' dei dati.
    """
    sorgente = sorted(glob.glob(os.path.join("data", "gold", "tratte", "*.parquet")))
    if not sorgente:
        return

    d = pd.concat([pd.read_parquet(f) for f in sorgente], ignore_index=True)
    if d.empty:
        return

    cartella = target_dir / "tratte"
    if cartella.exists():
        shutil.rmtree(cartella)
    cartella.mkdir(parents=True, exist_ok=True)

    # Tre colonne del gold non si pubblicano, e su milioni di righe pesano
    # quanto un quinto della cartella:
    #   oltre_5      coincide con in_ritardo, perche' la soglia di puntualita'
    #                e' quattro minuti e i valori sono interi: il browser lo
    #                ricava da se'
    #   in_orario    e' con_misura meno in_ritardo meno in_anticipo, idem
    #   ritardo_medio non serve a nessuno per riga: la media va ripesata sulle
    #                corse ogni volta che si sommano piu' mesi o piu' categorie,
    #                e la dashboard la ottiene dividendo i totali dell'aggregato
    colonne = ["mese", "a", "categoria", "corse", "con_misura", "in_ritardo",
               "in_anticipo", "fermate_soppresse", "oltre_10",
               "oltre_15", "oltre_30", "oltre_60", "minuti_ritardo_tot"]
    colonne = [c for c in colonne if c in d.columns]

    indice = {}
    peso = 0
    for partenza, gruppo in d.groupby("da", sort=False):
        s = slug_stazione(partenza)
        fuori = cartella / f"{s}.csv"
        gruppo[colonne].sort_values(["mese", "a", "categoria"]).to_csv(
            fuori, index=False, float_format="%.2f")
        peso += fuori.stat().st_size
        indice[str(partenza)] = s

    with open(target_dir / "tratte_indice.json", "w", encoding="utf-8") as f:
        json.dump(indice, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Wrote tratte/: {len(indice):,} stazioni di partenza, {peso / 1e6:.0f} MB")


def _quota_misure_scartate():
    """La quota di corse partite la cui misura di arrivo non e' utilizzabile.

    Era scritta a mano sotto l'istogramma ("il 3,5% delle corse effettuate") e
    non corrispondeva piu' al dato. Una percentuale battuta nell'HTML invecchia
    a ogni aggiornamento senza che nessuno se ne accorga, quindi ora si
    ricalcola a ogni build e il browser la legge dal manifest.

    Si conta sull'istogramma, che e' il grafico a cui la frase si riferisce,
    invece che ricombinando i KPI: il bucket "missing" e' per costruzione cio'
    che il grafico non riesce a mostrare, e il denominatore e' tutto il resto
    tranne la barra delle corse non effettuate, che una misura non puo' averla.

    La versione precedente faceva `effettuate - corse_con_misura` e sbagliava
    di 229.627 corse: `corse_con_misura` comprende anche le parzialmente
    cancellate (stanno in `delay_states`), mentre `effettuate` le esclude, e
    numeratore e denominatore parlavano di due popolazioni diverse.
    """
    try:
        from .build_gold import ETICHETTA_NON_EFFETTUATE, ETICHETTA_PARZIALI, read_gold_table

        h = read_gold_table("hist_mese_categoria")
        conteggi = h.groupby("bucket_ritardo_arrivo")["count"].sum()
        scartate = float(conteggi.get("missing", 0.0))
        # Fuori dal denominatore anche le parzialmente cancellate: da quando
        # hanno una barra loro, "missing" contiene solo corse arrivate a
        # destinazione senza una misura utilizzabile, e il rapporto deve
        # confrontarle con le altre arrivate a destinazione.
        partite = float(conteggi.sum()
                        - conteggi.get(ETICHETTA_NON_EFFETTUATE, 0.0)
                        - conteggi.get(ETICHETTA_PARZIALI, 0.0))
        if partite <= 0 or scartate < 0:
            return None
        return round(100.0 * scartate / partite, 2)
    except Exception:
        return None


def main() -> None:
    target = Path("docs") / "data"
    ensure_dir(target)

    names = [n for n in gold_table_names() if n != "stations_dim"]
    if not names:
        raise SystemExit("missing data/gold, run scripts.build_gold first")

    # Due passate deliberate. Tenere tutte le tabelle gold in memoria insieme
    # significherebbe oltre un gigabyte di DataFrame, piu' di quanto un runner
    # CI conceda; qui ogni tabella viene letta, usata e rilasciata. La prima
    # passata raccoglie i nomi, che devono essere completi prima di poterli
    # rimuovere dalle tabelle nella seconda.
    pairs: Dict[str, str] = {}
    codebook: Dict[str, List[str]] = {}
    present: List[str] = []
    for name in names:
        df = read_gold_table(name)
        if df.empty:
            print(f"  skipping {name}: no rows")
            continue
        present.append(name)
        harvest_station_names(df, pairs)
        harvest_codebook(df, codebook)
        del df

    if not present:
        raise SystemExit("nessuna tabella gold con righe: eseguire prima scripts.build_gold")

    names_df = station_names_frame(pairs)
    names_df.to_csv(target / "station_names.csv", index=False)
    print(f"Wrote station_names.csv ({len(names_df)} stazioni)")

    files: List[str] = ["station_names.csv"]
    years: Set[str] = set()
    for name in present:
        compact = compact_frame(encode_frame(strip_name_columns(read_gold_table(name)), codebook))
        if "mese" in compact.columns:
            years.update(compact["mese"].astype(str).str.slice(0, 4).unique())
        written = write_table(compact, target, name)
        files.extend(written)
        print(f"Wrote {name}: {len(compact)} righe in {len(written)} file")
        del compact

    # Le etichette dei bucket vengono dalla configurazione della pipeline, cosi'
    # l'asse dell'istogramma resta allineato ai bucket effettivamente prodotti
    # invece di dipendere dai default hard-coded nel JS.
    try:
        bucket_labels = load_yaml("config/pipeline.yml")["delay_buckets_minutes"]["labels"]
    except Exception:
        bucket_labels = []

    # Le corse cancellate e soppresse hanno una classe propria, che va in coda
    # all'asse: non e' un intervallo di minuti, e' il caso peggiore. Prima
    # finivano nel bucket dei dati mancanti e la distribuzione non le mostrava,
    # pur contandole nei totali in testa alla pagina.
    # In coda vanno due classi, non una. Le parzialmente cancellate sono partite,
    # quindi non stanno con le non effettuate, ma non sono arrivate dove
    # dovevano, quindi il loro scostamento non e' confrontabile: prima finivano
    # nelle barre normali, 207.778 su 211.036, e 52.485 comparivano fra gli
    # arrivi in anticipo perche' fermarsi prima fa arrivare prima.
    from .build_gold import ETICHETTA_NON_EFFETTUATE, ETICHETTA_PARZIALI
    for coda in (ETICHETTA_PARZIALI, ETICHETTA_NON_EFFETTUATE):
        if bucket_labels and coda not in bucket_labels:
            bucket_labels = list(bucket_labels) + [coda]

    manifest = {
        "built_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "gold_files": sorted(f"{n}.csv" for n in present),
        "delay_bucket_labels": bucket_labels,
        # Indice -> etichetta per le colonne codificate. Senza questo il
        # client non sa leggere i CSV, quindi va pubblicato sempre.
        "codebook": codebook,
        "year_sharded": sorted(n for n in present if n in YEAR_SHARDED),
        "years": sorted(y for y in years if y.isdigit()),
        "station_names_file": "station_names.csv",
    }

    quota = _quota_misure_scartate()
    if quota is not None:
        manifest["pct_misure_scartate"] = quota

    copy_root_files(target)
    if (target / "indicatori_km.csv").exists():
        manifest["km_file"] = "indicatori_km.csv"
    if (target / "velocita_rete.geojson").exists():
        manifest["rete_file"] = "velocita_rete.geojson"
    (target / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    stampati = timbra_versione_asset()
    print(json.dumps({"target": str(target), "files": len(files),
                      "html_timbrati": stampati}, ensure_ascii=False))


_ASSET_RX = re.compile(r'(assets/(?:app\.js|style\.css))(\?v=[0-9a-f]+)?')


def timbra_versione_asset(cartella: str = "docs") -> int:
    """Aggiunge alle pagine la versione di app.js e style.css.

    I CSV portano gia' la data di build come versione, ma gli script no: un
    browser poteva tenere in cache app.js vecchio e ricevere index.html nuovo,
    o il contrario. Il sintomo era la mappa che scompariva senza un errore in
    console, e non c'era modo di distinguerlo da un guasto vero.

    La versione e' l'impronta del contenuto, non la data: cosi' l'HTML cambia
    solo quando l'asset cambia davvero, e le build quotidiane non producono
    diff inutili.
    """
    base = Path(cartella)
    impronte = {}
    for nome in ("app.js", "style.css"):
        p = base / "assets" / nome
        if p.exists():
            impronte[nome] = hashlib.sha256(p.read_bytes()).hexdigest()[:10]
    if not impronte:
        return 0

    def sostituisci(m: "re.Match") -> str:
        percorso = m.group(1)
        nome = percorso.rsplit("/", 1)[-1]
        h = impronte.get(nome)
        return percorso + (f"?v={h}" if h else "")

    toccati = 0
    for pagina in sorted(base.glob("*.html")):
        testo = pagina.read_text(encoding="utf-8")
        nuovo = _ASSET_RX.sub(sostituisci, testo)
        if nuovo != testo:
            pagina.write_text(nuovo, encoding="utf-8")
            toccati += 1
    return toccati


if __name__ == "__main__":
    main()
