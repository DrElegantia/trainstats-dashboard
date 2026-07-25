"""Invarianti che devono valere prima di pubblicare.

Ogni controllo e' una proprieta' che era violata da almeno un bug corretto:
se torna a essere violata, quel bug e' rientrato. Nessuno di questi difetti
era visibile a occhio sulla dashboard, ed e' il motivo per cui esiste questo
script: 33 corse perse su 11 milioni non si notano guardando un grafico.

    python -m scripts.verifica_invarianti

Esce con codice 1 se un invariante e' violato, cosi' si puo' usare in CI.
"""
import glob
import os
import sys

import pandas as pd

GOLD = "data/gold/parts"
esiti = []


def check(nome, ok, dettaglio=""):
    esiti.append((nome, bool(ok), dettaglio))
    print(f"{'OK  ' if ok else 'FAIL'}  {nome}" + (f"  -> {dettaglio}" if dettaglio else ""))


def read_table(t):
    files = sorted(glob.glob(f"{GOLD}/{t}/*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


# 1. silver e gold devono contare le stesse corse, mese per mese.
# Il confronto e' limitato ai mesi che hanno un silver sul disco: in CI il
# silver viene ricostruito solo per i mesi toccati dal run, mentre il gold
# porta tutto lo storico, quindi un confronto sui totali fallirebbe sempre.
silver_per_mese = {}
ref_dates = 0
silver_rows = 0
for p in sorted(glob.glob("data/silver/*/*.parquet")):
    d = pd.read_parquet(p, columns=["data_riferimento"])
    mese = os.path.basename(p)[:6]
    silver_per_mese[f"{mese[:4]}-{mese[4:]}"] = len(d)
    silver_rows += len(d)
    ref_dates += d["data_riferimento"].astype(str).str.len().gt(0).sum()

kpi = read_table("kpi_dettaglio")
gold_corse = int(kpi["corse_osservate"].sum())

# I treni a cavallo di mezzanotte spostano qualche corsa fra un mese e il
# successivo, quindi il confronto per mese ha senso solo su un intervallo
# contiguo di mesi presenti in entrambi: si sommano quelli e si confrontano.
mesi_gold = {os.path.basename(p)[:7] for p in glob.glob(f"{GOLD}/kpi_dettaglio/*.parquet")}
comuni = sorted(set(silver_per_mese) & mesi_gold)
if comuni:
    att = sum(silver_per_mese[m] for m in comuni)
    tro = int(kpi[kpi["mese"].astype(str).isin(comuni)]["corse_osservate"].sum())
    # tolleranza: le corse che sconfinano nel primo mese precedente e nell'ultimo
    # successivo non sono confrontabili se quei mesi non sono nell'intervallo
    bordo = len(comuni) < len(mesi_gold)
    ok = (att == tro) if not bordo else abs(att - tro) <= 200
    check("silver == gold (corse osservate)", ok,
          f"silver={att:,} gold={tro:,} delta={tro - att:+,} su {len(comuni)} mesi"
          + ("  (tolleranza di confine attiva)" if bordo else ""))

# 2. la data di riferimento deve esserci su ogni riga (bug itertuples)
check("data_riferimento valorizzata al 100%", ref_dates == silver_rows,
      f"{ref_dates:,}/{silver_rows:,} = {ref_dates/max(silver_rows,1):.2%}")

# 3. nessun ritardo medio negativo, in nessuna tabella
neg_tot = 0
for t in ["kpi_dettaglio", "kpi_mese", "kpi_dettaglio_categoria", "kpi_mese_categoria",
          "stazioni_mese_categoria_ruolo", "stazioni_mese_categoria_nodo",
          "od_mese_categoria", "od_dettaglio_categoria"]:
    df = read_table(t)
    if df.empty or "ritardo_medio" not in df.columns:
        continue
    n = int((pd.to_numeric(df["ritardo_medio"], errors="coerce") < 0).sum())
    neg_tot += n
    if n:
        print(f"      {t}: {n} righe con ritardo_medio < 0")
check("nessun ritardo_medio negativo", neg_tot == 0, f"{neg_tot} righe")

# 4. nessun anticipo oltre 5 minuti sopravvive alla finestra di validita'
hist = read_table("hist_mese_categoria")
if not hist.empty:
    col = "bucket_ritardo_arrivo" if "bucket_ritardo_arrivo" in hist.columns else None
    if col:
        bad = hist[hist[col].astype(str).str.startswith(("-6", "-10", "-15", "-30"))]
        n = int(pd.to_numeric(bad.get("corse_osservate", pd.Series(dtype=float)), errors="coerce").sum() or 0)
        check("nessun anticipo oltre 5 minuti", n == 0, f"{n:,} corse in bucket sotto -5")
        print("      bucket presenti:", sorted(hist[col].astype(str).unique())[:14])

# 5. corse_con_misura non puo' superare corse_osservate
for t in ["kpi_mese", "kpi_mese_categoria", "stazioni_mese_categoria_ruolo", "od_mese_categoria"]:
    df = read_table(t)
    if df.empty or "corse_con_misura" not in df.columns:
        continue
    bad = int((pd.to_numeric(df["corse_con_misura"], errors="coerce")
               > pd.to_numeric(df["corse_osservate"], errors="coerce")).sum())
    check(f"corse_con_misura <= corse_osservate ({t})", bad == 0, f"{bad} righe")

# 6. le cancellazioni non possono essere zero su un mese intero
km = read_table("kpi_mese")
if not km.empty and "non_effettuate" in km.columns:
    zero_months = km.groupby("mese")["non_effettuate"].sum()
    n = int((zero_months == 0).sum())
    check("nessun mese con zero non effettuate", n == 0,
          f"{n} mesi su {len(zero_months)}" + (f" -> {list(zero_months[zero_months==0].index)[:6]}" if n else ""))

# 7. la categoria non deve contenere etichette spurie
kmc = read_table("kpi_mese_categoria")
if not kmc.empty:
    cats = set(kmc["categoria"].astype(str).unique())
    spurie = cats & {"None", "nan", "NaT", "null", "<NA>", "none"}
    check("nessuna categoria spuria", not spurie, f"trovate {sorted(spurie)}" if spurie else f"{len(cats)} categorie")

# 8. il sito deve avere manifest e le tabelle attese
import json
import os
if os.path.exists("docs/data/manifest.json"):
    man = json.load(open("docs/data/manifest.json"))
    files = [f for f in os.listdir("docs/data") if f.endswith(".csv")]
    check("manifest presente e CSV generati", len(files) > 10, f"{len(files)} CSV, chiavi manifest={len(man)}")
    check("station_names.csv pubblicato", os.path.exists("docs/data/station_names.csv"))
else:
    check("manifest presente", False, "docs/data/manifest.json assente")

print()
fails = [n for n, ok, _ in esiti if not ok]
print(f"{len(esiti) - len(fails)}/{len(esiti)} invarianti rispettati")
if fails:
    print("VIOLATI:", ", ".join(fails))
sys.exit(1 if fails else 0)
