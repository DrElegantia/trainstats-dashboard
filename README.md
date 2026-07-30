# TrainStats Dashboard

Dashboard interattiva per visualizzare statistiche sulla puntualità e il servizio ferroviario in Italia, basata sui dati pubblicati da [TrainStats](https://trainstats.altervista.org/).

Ogni giorno una GitHub Action scarica il CSV giornaliero, lo trasforma attraverso una pipeline bronze/silver/gold e pubblica una dashboard statica fruibile qui [Dashboard Treni](https://www.umbertobertonelli.it/ritardo-treni/)

## Architettura dati

Il progetto segue il pattern **medallion** su tre livelli:

| Livello | Contenuto | Formato |
|---------|-----------|---------|
| **Bronze** | CSV sorgente giornaliero grezzo + metadati | `.csv.gz` |
| **Silver** | Dati normalizzati, tipizzati, deduplicati, con chiave deterministica | Parquet mensile |
| **Gold** | Aggregazioni pronte per la dashboard (KPI, istogrammi, stazioni, O/D) | Parquet partizionato per mese |
| **Sito** | CSV serviti a `docs/data/`, rigenerati dal gold | CSV (+ shard annuali) |

Il gold vive in `data/gold/parts/<tabella>/<YYYY-MM>.parquet`: il run notturno
riscrive una sola partizione invece dell'intero storico. I CSV in `docs/data/`
sono un artefatto derivato, mai versionato.

La dashboard (`docs/`) carica solo i CSV, senza rielaborare lo storico nel
browser, e per le tabelle piu' pesanti preferisce lo shard dell'anno
selezionato al file di tutta la storia.

### Il ramo delle fermate

Accanto alla pipeline principale, che conosce di ogni corsa solo l'origine e la
destinazione, esiste un secondo ramo che scende alle singole fermate:

    python -m scripts.transform_fermate --start 2020-01-01 --end 2025-12-31
    python -m scripts.build_gold_fermate

Serve perche' contare le corse ai soli capolinea descrive male il paese. La
sorgente vede **1.783 stazioni come fermata e 317 come capolinea**: nel giugno
2025 Pordenone risulta con quattro corse, Abbiategrasso con nessuna, Acireale
con tredici, mentre le fermate ne contano rispettivamente 1.786, 1.311 e 1.557.
E' anche la ragione per cui la mappa deve tenere una riserva sulle stazioni
sotto le trenta corse: sulle fermate, 1.778 stazioni su 2.261 superano quella
soglia da sole.

In piu' il payload segna le **fermate soppresse** (`ra = "S"`), cioe' le fermate
che il treno salta. In un giorno campione compaiono in 202 treni, e in 93 di
questi il campo dei provvedimenti tace: per la pipeline dei capolinea quelle
corse sono regolari.

**Copertura e limiti.** Le fermate coprono **2.377 giorni, dall'11 gennaio 2020
al 26 luglio 2026**, con dodici giorni scoperti: dieci mancano anche al bronze
perche' la sorgente non li ha mai pubblicati (30/03/2020, 12/04/2020,
28/07/2020, 25-28/02/2021, 30/03/2021, 13/02/2022, 06/03/2022), uno ha il dump
corrotto all'origine (28/06/2026, lo zip non ha la central directory e non se
ne recupera nulla) e uno non e' mai stato distribuito (07/07/2026). Per quei due
giorni il bronze resta quello dell'API, completo sui capolinea.

Prima dell'11 gennaio 2020 il campo `fr` non esiste: i payload del 2019 e dei
primi dieci giorni del 2020 hanno chiavi diverse e non portano le fermate.
Nessuna delle fonti trovate le contiene, quindi per quel periodo la vista per
fermata non e' ricostruibile.

I mesi dal 2026 non arrivano dall'API, che pubblica solo i capolinea, ma dai
dump giornalieri che l'autore di TrainStats distribuisce a parte, importati con
`scripts.import_history --overwrite`. Attenzione a come sono organizzati: fino
ad aprile 2026 stanno in sottocartelle mensili, da maggio sono sciolti nella
radice della condivisione, e lo zip che Dropbox genera per l'intera cartella
**contiene solo le sottocartelle**. Scaricando quello si perdono gli ultimi tre
mesi senza accorgersene.

Attenzione a quell'import: `transform_silver` fonde con il silver gia' presente
e la chiave di deduplicazione nasce dai campi grezzi, che nei due formati sono
scritti in modo diverso. Riscrivendo il bronze senza cancellare il silver dei
mesi toccati, la stessa corsa entra due volte e i conteggi raddoppiano
esattamente. Cancellare i parquet di quei mesi prima di rilanciare, e
ricostruire il gold anche del mese successivo, perche' le corse a cavallo della
mezzanotte di fine mese vengono contate li'.

Il ramo resta comunque una fotografia che non si aggiorna da sola: senza nuovi
dump non puo' seguire la dashboard giornaliera.
E pesa circa otto volte, undici fermate e mezzo per corsa: 2,8 milioni di righe
in un mese, 17 MB di parquet.

Un mese sta in memoria tutto insieme prima di essere scritto, quindi il
parallelismo va tenuto basso: la prima passata con sette processi ha perso
diciassette mesi su settantacinque uscendo con codice zero. Ora il default e'
tre e i mesi senza righe vengono elencati alla fine.

## Dataset gold

| File | Descrizione |
|------|-------------|
| `kpi_*.csv` | KPI di puntualità, ritardo medio, anticipo, treni soppressi |
| `hist_*.csv` | Distribuzione ritardi in classi configurabili |
| `stazioni_*.csv` | Statistiche per stazione, ruolo (partenza/arrivo) e nodo |
| `od_*.csv` | Statistiche per coppia origine-destinazione |
| `stations_dim.csv` | Anagrafica stazioni con coordinate |
| `station_names.csv` | Codice stazione -> nome, pubblicato una volta sola |

Ogni dataset esiste in versione `dettaglio` (giornaliera) e `mese` (mensile), suddiviso per categoria di treno.

Le tabelle dei fatti non contengono piu' le colonne `nome_partenza`,
`nome_arrivo`, `nome_stazione`: ripetevano le stesse poche migliaia di stringhe
su milioni di righe. I nomi arrivano da `station_names.csv`.

### Metriche di ritardo

| Colonna | Significato |
|---------|-------------|
| `ritardo_medio` | Ritardo medio con **floor a zero**: un arrivo in anticipo vale zero, non un valore negativo. Per costruzione non e' mai negativo. |
| `scostamento_medio` | Scarto medio **con segno** rispetto all'orario. Negativo dove la tratta viaggia mediamente in anticipo. |
| `corse_osservate` | Corse osservate, incluse quelle non effettuate. |
| `corse_con_misura` | Corse con una misura di ritardo utilizzabile: **e' il denominatore corretto** per le percentuali di puntualita'. |
| `puntuali` | Arrivi entro soglia, contando anche gli anticipi. |
| `in_orario` / `in_anticipo` / `in_ritardo` | Tre classi mutuamente esclusive, per l'istogramma. |
| `in_ritardo_effettuate` | Come `in_ritardo`, ma solo per le corse fatte per intero. Serve a sommare i ritardi alle mancate corse senza contare due volte una parzialmente cancellata arrivata tardi, che sta in `in_ritardo` e in `cancellate_tot`. Non e' pubblicata nelle tabelle per stazione, che non alimentano quell'indice. |
| `non_effettuate` | Cancellate + soppresse. |
| `cancellate_tot` | Non effettuate + parzialmente cancellate. |

Un treno soppresso viene pubblicato dalla sorgente con `ritardo = 0`: viene
escluso dalle statistiche di ritardo (`delay_states` in `config/pipeline.yml`),
altrimenti una tratta risulterebbe tanto piu' puntuale quanto piu' viene
cancellata. Vedi [DIAGNOSI.md](DIAGNOSI.md) per il dettaglio.

Le parole della sorgente vanno lette per intero. "Percorso deviato con fermate
soppresse" e' un treno che ha viaggiato saltando qualche fermata, non un treno
mai partito, e "Percorso deviato con fermate straordinarie" e' un treno che ha
fatto tutto il percorso con una fermata in piu': nessuno dei due e' una
cancellazione, e trattarli come tali gonfiava del 7,9% il contatore dei
cancellati.

Il manifest pubblica anche `pct_misure_scartate`, la quota di corse partite la
cui misura di arrivo non e' utilizzabile. La dashboard la legge da li' invece di
tenerla scritta nell'HTML, dove invecchiava a ogni aggiornamento.

## Configurazione

`config/pipeline.yml` contiene tutti i parametri senza modificare codice:

- **Soglia puntualità** — minuti entro cui un treno è considerato "in orario"
- **Classi istogramma** — bucket per la distribuzione del ritardo arrivo
- **Regole stato corsa** — pattern regex per classificare: effettuato, cancellato, soppresso, parzialmente cancellato. I nomi in `text_fields` sono quelli **canonici** (minuscoli), non le intestazioni della sorgente
- **`delay_validity`** — finestra oltre la quale la misura di ritardo è considerata non attendibile
- **`delay_states`** — stati corsa su cui ha senso calcolare un ritardo
- **Soglia minima numerosità** — per classifiche stazioni nella dashboard

## Esecuzione locale

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**Run giornaliero** (usa la data di ieri, fuso Europe/Rome):
```bash
python scripts/run_pipeline.py
```

**Backfill di un intervallo**:
```bash
python scripts/run_pipeline.py --start 2024-06-01 --end 2024-06-30
```

**Ricostruzione solo gold e sito** (se silver esiste già):
```bash
python -m scripts.build_gold
python -m scripts.build_site
```

**Ricostruzione silver in parallelo** (utile solo per i backfill lunghi: i mesi
con schema bronze legacy costano ~4 s al giorno per il solo parsing del payload):
```bash
python -m scripts.transform_silver --start 2024-06-01 --end 2026-07-24 --jobs 7
```

**Verifica dei numeri contro una versione precedente**:
```bash
python -m scripts.compare_gold --baseline /percorso/ai/csv/gold/vecchi
python -m scripts.measure_payload --confronta /percorso/ai/csv/gold/vecchi
```

## GitHub Actions

Il workflow `daily.yml` esegue ogni giorno:

1. Download CSV giornaliero da TrainStats
2. Ingest bronze e trasformazione silver con dedup
3. Aggiornamento delle sole partizioni gold dei mesi toccati
4. Generazione dei CSV in `docs/data/` con manifest e shard annuali
5. Commit automatico (bronze + partizioni gold) e deploy su GitHub Pages

Per il backfill manuale si usa `workflow_dispatch` con parametri `start_date` e `end_date`.

## Anagrafica stazioni

`stations/stations.csv` contiene le coordinate delle stazioni ed è versionato nel repository. Il geocoding online e disattivato per default: si attiva a mano con `python -m scripts.build_station_dim --enable-geocoding`, come manutenzione, mai nel run notturno. La mappa nella dashboard non si rompe se mancano coordinate: semplicemente non disegna quei marker.

## Troubleshooting

| Problema | Cosa controllare |
|----------|-----------------|
| Header mismatch | `config/schema_expected.json` — valuta modalità `prefix` se la sorgente aggiunge colonne |
| Troppi missing datetime | Formato colonne orario nella sorgente — aggiorna `parse_dt_it` in `scripts/utils.py` |
| Mappa vuota | `stations/stations.csv` — aggiungi coordinate mancanti |
| Dashboard non carica | Verifica che `docs/data/` contenga i CSV gold e `manifest.json` |

## Crediti

I dati provengono da [TrainStats](https://trainstats.altervista.org/). Questo progetto non è affiliato a TrainStats né a Trenitalia/RFI: è un'elaborazione indipendente dei dati pubblicamente disponibili.

## Licenza

Questo progetto è distribuito sotto i termini della **GNU General Public License v3.0**.

Il testo completo e ufficiale della licenza è disponibile qui: https://www.gnu.org/licenses/gpl-3.0.html
