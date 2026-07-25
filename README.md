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
| `non_effettuate` | Cancellate + soppresse. |

Un treno soppresso viene pubblicato dalla sorgente con `ritardo = 0`: viene
escluso dalle statistiche di ritardo (`delay_states` in `config/pipeline.yml`),
altrimenti una tratta risulterebbe tanto piu' puntuale quanto piu' viene
cancellata. Vedi [DIAGNOSI.md](DIAGNOSI.md) per il dettaglio.

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

`stations/stations.csv` contiene le coordinate delle stazioni ed è versionato nel repository. La pipeline non fa geocoding online. La mappa nella dashboard non si rompe se mancano coordinate: semplicemente non disegna quei marker.

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
