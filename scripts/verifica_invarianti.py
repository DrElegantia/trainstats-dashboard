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

# 5-bis. gli addendi dell'indice ritardo devono restare disgiunti.
#
# L'indice somma le corse in ritardo a quelle mancate. `in_ritardo` comprende
# anche le parzialmente cancellate, che stanno gia' in `cancellate_tot`: se la
# dashboard tornasse a usare quella colonna, conterebbe due volte le stesse
# corse, come faceva prima (0,73 punti percentuali). `in_ritardo_effettuate`
# esiste per questo, e deve restare per costruzione non piu' grande di
# `in_ritardo` e disgiunta dalle cancellate.
for t in ["kpi_mese", "kpi_mese_categoria", "od_mese_categoria"]:
    df = read_table(t)
    if df.empty or "in_ritardo_effettuate" not in df.columns:
        continue
    num = lambda c: pd.to_numeric(df[c], errors="coerce").fillna(0)
    troppo = int((num("in_ritardo_effettuate") > num("in_ritardo")).sum())
    oltre = int((num("in_ritardo_effettuate") + num("cancellate_tot") > num("corse_osservate")).sum())
    check(f"indice ritardo con addendi disgiunti ({t})", troppo == 0 and oltre == 0,
          f"{troppo} righe sopra in_ritardo, {oltre} righe oltre le corse osservate")

# 6. le cancellazioni non possono essere zero su un mese intero
km = read_table("kpi_mese")
if not km.empty and "non_effettuate" in km.columns:
    zero_months = km.groupby("mese")["non_effettuate"].sum()
    n = int((zero_months == 0).sum())
    check("nessun mese con zero non effettuate", n == 0,
          f"{n} mesi su {len(zero_months)}" + (f" -> {list(zero_months[zero_months==0].index)[:6]}" if n else ""))

# 6-bis. l'istogramma deve mostrare tutte le corse cancellate, non solo meta'.
#
# Il contatore in testa alla pagina diceva 404.343 fra totali e parziali,
# l'ultima barra ne mostrava 193.307: le 211.036 parzialmente cancellate erano
# spalmate fra le barre di chi era arrivato a destinazione, e 52.485 comparivano
# addirittura fra gli arrivi in anticipo, perche' un treno limitato a meta'
# percorso arriva prima. Le due classi in coda all'asse devono ricomporre
# esattamente cancellate_tot dei KPI.
h = read_table("hist_mese_categoria")
if not h.empty and not km.empty and "cancellate_tot" in km.columns:
    per_classe = h.groupby("bucket_ritardo_arrivo")["count"].sum()
    code = float(per_classe.get("non effettuate", 0)) + float(per_classe.get("parzialmente cancellate", 0))
    atteso = float(km["cancellate_tot"].sum())
    check("istogramma e KPI concordano sulle cancellate", abs(code - atteso) < 1,
          f"istogramma {code:,.0f} contro KPI {atteso:,.0f}")
    # E la somma di tutte le classi deve restare il totale delle corse.
    tot_h = float(per_classe.sum())
    tot_k = float(km["corse_osservate"].sum())
    check("l'istogramma copre tutte le corse osservate", abs(tot_h - tot_k) < 1,
          f"istogramma {tot_h:,.0f} contro KPI {tot_k:,.0f}")

# 6-ter. il ramo delle fermate deve raccontare gli stessi capolinea.
#
# Le due pipeline leggono lo stesso bronze da due strade diverse: una prende
# origine e destinazione dalle colonne, l'altra le ricava dalla prima e
# dall'ultima fermata del payload. Sui capolinea devono dire la stessa cosa, e
# se un giorno divergono e' un errore di estrazione, non una differenza di
# metodo. Il controllo si salta dove il ramo non e' stato costruito: in CI il
# silver delle fermate non esiste, e dal 2026 la sorgente non lo consente piu'.
import os as _os
_mesi_fermate = sorted(glob.glob("data/gold/fermate/*.parquet"))
if _mesi_fermate:
    _peggiore = 0.0
    _dettaglio = ""
    for _f in _mesi_fermate[-3:]:
        _chiave = _os.path.basename(_f)[:7]
        _staz = f"{GOLD}/stazioni_mese_categoria_nodo/{_chiave}.parquet"
        _nomi = "docs/data/station_names.csv"
        if not (_os.path.exists(_staz) and _os.path.exists(_nomi)):
            continue
        _fm = pd.read_parquet(_f)
        # Un mese che la sorgente ha pubblicato a meta' non si puo' confrontare
        # con un mese intero: a dicembre 2025 le fermate coprono ventuno giorni
        # su trentuno, e Roma Termini risultava a 13.235 contro 19.522, cioe'
        # esattamente quel rapporto. Non e' un errore di estrazione, e un
        # controllo che lo chiama errore insegna a ignorare i controlli.
        if "giorni_coperti" in _fm.columns and "giorni_attesi" in _fm.columns:
            _cop, _att = int(_fm["giorni_coperti"].iloc[0]), int(_fm["giorni_attesi"].iloc[0])
            if _att and _cop < _att:
                continue
        _n = pd.read_csv(_nomi)
        _m = dict(zip(_n.iloc[:, 0], _n.iloc[:, 1]))
        _k = pd.read_parquet(_staz)
        _k["nome"] = _k["cod_stazione"].map(_m).astype(str)
        _a = _k.groupby("nome")["corse_osservate"].sum()
        _b = _fm.groupby("nome_stazione")["volte_capolinea"].sum()
        for _st in ["MILANO CENTRALE", "ROMA TERMINI", "NAPOLI CENTRALE"]:
            _x, _y = float(_a.get(_st, 0)), float(_b.get(_st, 0))
            if _x <= 0:
                continue
            _scarto = abs(_y - _x) / _x
            if _scarto > _peggiore:
                _peggiore, _dettaglio = _scarto, f"{_st} {_chiave}: {_y:,.0f} contro {_x:,.0f}"
    if _dettaglio:
        check("fermate e capolinea concordano sui grandi nodi", _peggiore <= 0.05,
              f"scarto massimo {100 * _peggiore:.1f}% ({_dettaglio})")

# 7. la categoria non deve contenere etichette spurie
kmc = read_table("kpi_mese_categoria")
if not kmc.empty:
    cats = set(kmc["categoria"].astype(str).unique())
    spurie = cats & {"None", "nan", "NaT", "null", "<NA>", "none"}
    check("nessuna categoria spuria", not spurie, f"trovate {sorted(spurie)}" if spurie else f"{len(cats)} categorie")

# 8. nessuna stazione di peso deve stare lontana dalla propria rete
#
# Le coordinate arrivano da una fonte esterna abbinata per nome, e l'abbinamento
# puo' sbagliare in modi che nessun altro controllo vede: Venezia S. Lucia e'
# stata disegnata in Basilicata per mesi, Bolzano in Sicilia, Lodi sulla metro
# A di Roma. Qui non si guardano i nomi ma i treni: se una stazione e' lontana
# da ogni stazione con cui e' collegata, la sbagliata e' lei.
#
# La soglia sul volume tiene fuori i bivi e le fermate con tre corse in sette
# anni, dove la rete non e' un riferimento e il segnale e' solo rumore.
try:
    from scripts.coordinate_osm import audit_coerenza_rete
    fuori = audit_coerenza_rete(soglia_km=100)
    gravi = fuori[fuori["corse"] >= 1000] if not fuori.empty else fuori
    n_gravi = 0 if gravi.empty else len(gravi)
    check("nessuna stazione rilevante fuori dalla sua rete", n_gravi == 0,
          (f"{n_gravi} stazioni: " + ", ".join(gravi["nome"].head(5))) if n_gravi
          else f"{0 if fuori.empty else len(fuori)} segnalazioni, tutte sotto le 1.000 corse")
except Exception as e:
    check("audit coordinate eseguibile", False, str(e)[:120])

# 9. il sito deve avere manifest e le tabelle attese
import json
import os
if os.path.exists("docs/data/manifest.json"):
    man = json.load(open("docs/data/manifest.json"))
    files = [f for f in os.listdir("docs/data") if f.endswith(".csv")]
    check("manifest presente e CSV generati", len(files) > 10, f"{len(files)} CSV, chiavi manifest={len(man)}")
    check("station_names.csv pubblicato", os.path.exists("docs/data/station_names.csv"))
else:
    check("manifest presente", False, "docs/data/manifest.json assente")

# 10. il ramo delle tratte per fermata
#
# La dashboard filtra queste righe per categoria e le somma per coppia: due
# cose devono reggere, o mostra numeri sbagliati senza avvisare. Che la soglia
# sia contata sulla coppia e non sulla singola categoria, altrimenti il totale
# "tutte le categorie" perde le corse delle categorie rare senza dirlo. E che
# il vocabolario delle categorie sia lo stesso della vista per capolinea,
# perche' il menu a tendina viene da li': una sigla presente solo qui sarebbe
# una riga che nessun filtro puo' mai selezionare, e resterebbe invisibile.
_tratte = sorted(glob.glob("data/gold/tratte/*.parquet"))
if _tratte:
    _campione = _tratte[-3:]
    _sotto = 0
    _righe = 0
    _cat_tratte = set()
    for _f in _campione:
        _d = pd.read_parquet(_f, columns=["mese", "da", "a", "categoria", "corse"])
        _righe += len(_d)
        _per_coppia = _d.groupby(["da", "a"], observed=True)["corse"].sum()
        _sotto += int((_per_coppia < 30).sum())
        _cat_tratte |= set(_d["categoria"].astype(str).unique())
        if _d[["mese", "da", "a", "categoria"]].duplicated().any():
            _sotto += 1
    check("le tratte rispettano la soglia sulla coppia", _sotto == 0,
          f"{_righe:,} righe su {len(_campione)} mesi, nessuna coppia sotto le 30 corse"
          if _sotto == 0 else f"{_sotto} coppie sotto soglia o chiavi duplicate")

    _kmc = read_table("kpi_mese_categoria")
    if not _kmc.empty:
        _cat_cap = set(_kmc["categoria"].astype(str).fillna("").unique()) | {""}
        _orfane = sorted(c for c in _cat_tratte if c not in _cat_cap and c != "nan")
        check("le categorie delle tratte esistono nel menu", not _orfane,
              f"{len(_cat_tratte)} categorie, tutte selezionabili" if not _orfane
              else f"non selezionabili: {_orfane}")

print()
fails = [n for n, ok, _ in esiti if not ok]
print(f"{len(esiti) - len(fails)}/{len(esiti)} invarianti rispettati")
if fails:
    print("VIOLATI:", ", ".join(fails))
sys.exit(1 if fails else 0)
