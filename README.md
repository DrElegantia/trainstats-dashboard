# TrainStats Dashboard

Dashboard interattiva per visualizzare statistiche sulla puntualità e il servizio ferroviario in Italia, basata sui dati pubblicati da [TrainStats](https://trainstats.altervista.org/).

Ogni giorno una GitHub Action scarica il CSV giornaliero, lo trasforma attraverso una pipeline bronze/silver/gold e pubblica una dashboard statica fruibile qui [Dashboard Treni](https://www.umbertobertonelli.it/ritardo-treni/)

## Architettura dati

Il progetto segue il pattern **medallion** su tre livelli:

| Livello | Contenuto | Formato |
|---------|-----------|---------|
| **Bronze** | CSV sorgente giornaliero grezzo + metadati | `.csv.gz` |
| **Silver** | Dati normalizzati, tipizzati, deduplicati, con chiave deterministica | Parquet |
| **Gold** | Aggregazioni pronte per la dashboard (KPI, istogrammi, stazioni, O/D) | CSV |

La dashboard (`docs/`) carica solo i CSV gold, senza rielaborare lo storico nel browser.

## Dataset gold

| File | Descrizione |
|------|-------------|
| `kpi_*.csv` | KPI di puntualità, ritardo medio, anticipo, treni soppressi |
| `hist_*.csv` | Distribuzione ritardi in classi configurabili |
| `stazioni_*.csv` | Statistiche per stazione, ruolo (partenza/arrivo) e nodo |
| `od_*.csv` | Statistiche per coppia origine-destinazione |
| `stations_dim.csv` | Anagrafica stazioni con coordinate |

Ogni dataset esiste in versione `dettaglio` (giornaliera) e `mese` (mensile), suddiviso per categoria di treno.

## Configurazione

`config/pipeline.yml` contiene tutti i parametri senza modificare codice:

- **Soglia puntualità** — minuti entro cui un treno è considerato "in orario"
- **Classi istogramma** — bucket per la distribuzione del ritardo arrivo
- **Regole stato corsa** — pattern regex per classificare: effettuato, cancellato, soppresso, parzialmente cancellato
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
python scripts/build_gold.py
python scripts/build_site.py
```

## GitHub Actions

Il workflow `daily.yml` esegue ogni giorno:

1. Download CSV giornaliero da TrainStats
2. Ingest bronze e trasformazione silver con dedup
3. Rigenerazione completa gold
4. Copia gold in `docs/data/` con manifest
5. Commit automatico e deploy su GitHub Pages

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
