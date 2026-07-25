# TrainStats: diagnosi e interventi

Laboratorio privato su copia di `trainstats-dashboard-test`, con history pulita.
Il commit taggato `baseline-gold` conserva l'output della pipeline **prima**
degli interventi, per poter confrontare i numeri.

---

## 1. Perche' la dashboard era lenta

### 1.1 Il peso di cio' che il browser scarica e parsifica

Il caricamento iniziale e' leggero (poche decine di KB). Il problema esplode
appena l'utente tocca un filtro stazione o un filtro di dettaglio: a quel punto
partono i download pesanti.

| File | CSV grezzo | Servito (gzip) |
|---|---|---|
| `hist_stazioni_dettaglio_categoria_ruolo.csv` | 77 MB | 6,9 MB |
| `od_dettaglio_categoria.csv` | 45 MB | 10,0 MB |
| `stazioni_dettaglio_categoria_ruolo.csv` | 30 MB | 5,2 MB |
| `stazioni_dettaglio_categoria_nodo.csv` | 18 MB | 5,2 MB |
| `od_mese_categoria.csv` | 11 MB | 2,9 MB |
| **Totale scaricabile** | **~166 MB** | **~28 MB** |

Il costo vero non e' la rete: e' che `parseCSVAsync` costruisce **un oggetto
JavaScript per riga**, con una property stringa per colonna. 166 MB di CSV
diventano diverse centinaia di MB di heap e vari secondi di CPU sul thread
principale.

Tre cause concrete, tutte rimosse:

1. **Colonne nome ripetute su ogni riga.** `nome_partenza`, `nome_arrivo`,
   `nome_stazione` ripetevano le stesse poche migliaia di stringhe su milioni
   di righe. Ora i nomi viaggiano una volta sola in `station_names.csv`.
2. **Float a precisione piena.** Medie e percentili scritti come
   `3.1666666666666665`: diciassette cifre per un numero mostrato con due
   decimali. E conteggi interi scritti come `1234.0`.
3. **Nessuna partizione temporale.** Anche guardando un solo anno il browser
   scaricava tutta la storia. Ora ogni tabella pesante ha anche uno shard per
   anno, e la dashboard lo preferisce quando c'e' un anno selezionato.

### 1.2 Il peso della pipeline

`parse_dt_it` chiamava `pd.to_datetime()` **una volta per cella**, e ogni
chiamata rifaceva l'inferenza del formato su un array di un elemento: da sola
valeva l'85% del tempo di costruzione del silver.

`df.apply(se.classify, axis=1)` classificava lo stato corsa riga per riga.

`save_gold_tables` rileggeva, riconcatenava e riscriveva **per intero** ogni CSV
gold a ogni chunk elaborato. Con la tabella piu' grande a 77 MB e nove chunk in
un rebuild completo, oltre un gigabyte di CSV passava dal parser per dati che
non erano cambiati. Il run notturno pagava lo stesso prezzo per aggiungere un
giorno, e riscriveva ogni notte un blob da 77 MB nel repository: e' cosi' che la
history e' arrivata a 6,5 GB.

`build_station_dim` leggeva due volte per intero tutti i parquet silver e
iterava con `iterrows()` su milioni di righe per estrarne poche migliaia di
coppie codice/nome.

**Risultato misurato su un mese reale (giugno 2026, ~247k righe):**
la costruzione del silver passa da **66,8 s a 4,4 s**.

Il gold non riscrive piu' i CSV monolitici: e' partizionato per mese in
`data/gold/parts/<tabella>/<YYYY-MM>.parquet`, quindi il run notturno tocca una
sola partizione e il repository non accumula piu' un blob nuovo ogni notte.

---

## 2. I ritardi negativi

Questo era il sintomo piu' visibile: **22,3% delle tratte** aveva un
`ritardo_medio` negativo, e il 40% circa dei treni risultava "in anticipo".
Casi come `ROMA TERMINI -> NAPOLI CENTRALE` regionale, 321 corse, media
**-7,5 minuti**, non sono credibili.

### 2.1 Cosa dicono i dati grezzi

Il segno negativo **non nasce nella pipeline**: e' gia' nella sorgente. Ma non e'
un semplice "i treni arrivano in anticipo".

Ricostruendo dai file bronze legacy la progressione del ritardo fermata per
fermata (campo `fr`, che la pipeline scartava), emerge un salto sistematico
all'ultima fermata. Esempio reale, treno 2113 Torino P.Nuova -> Genova
P.Principe:

```
TORINO LINGOTTO 0 -> ASTI 0 -> ALESSANDRIA -3 -> NOVI LIGURE +2
-> ARQUATA SCRIVIA +2 -> RONCO SCR. +3 -> GENOVA P.PRINCIPE -10
```

Su 25.165 treni analizzati:

| | ultima fermata | penultima fermata | mediana lungo il percorso |
|---|---|---|---|
| ritardo medio | 2,68 min | 3,96 min | |
| quota valori negativi | **38,4%** | 23,1% | 15,5% |

Lo scarto mediano fra ultima e penultima fermata e' **-1 minuto**, e cresce con
il numero di fermate. Un treno non recupera 13 minuti nell'ultima tratta: la
misura al capolinea e' distorta verso il basso.

### 2.2 Cosa e' stato cambiato

Tre interventi distinti, nessuno dei quali "nasconde" il dato:

1. **`ritardo_medio` non e' piu' la media con segno.** Un arrivo in anticipo non
   e' un ritardo negativo: e' ritardo zero. E' la convenzione con cui si
   calcolano le statistiche ufficiali di puntualita', ed e' quella che rende la
   metrica interpretabile. Per costruzione `ritardo_medio >= 0` sempre.
2. **Lo scostamento con segno resta pubblicato**, come `scostamento_medio` e
   `scostamento_mediano`. Chi vuole vedere quanto una tratta viaggia in anticipo
   rispetto all'orario ce l'ha, con un nome che dice cosa e'.
3. **Finestra di validita' sulla misura** (`delay_validity` in
   `config/pipeline.yml`, oggi da -90 a +1440 minuti). Scarta solo i valori
   fisicamente impossibili: 4 record su 562.000. Sono rari in aggregato ma
   decisivi sulle tratte con pochissime corse, ed e' esattamente da li' che
   venivano i `-85` di `BOLOGNA CABINA S.DONATO -> RIMINI` (1 sola corsa).

---

## 3. Bug trovati e corretti

### 3.1 Le cancellazioni erano a zero (bug critico)

`config/pipeline.yml` elencava i campi testuali da ispezionare con le
intestazioni **maiuscole della sorgente** (`Provvedimenti`, `Variazioni`), ma la
classificazione girava **dopo** `canonical_rename()`, che le ha gia' rinominate
in minuscolo. `StatusEngine.classify` non trovava mai nulla e restituiva sempre
`effettuato`.

Restava in piedi solo un fallback hard-coded, molto piu' povero dei pattern in
configurazione: ignorava `annullato`, `non effettuato`, `limitato`,
`interrotto`, `deviato`, `prosegue da`.

Ora `StatusEngine.haystack()` **solleva un errore** se nessuno dei campi
configurati esiste nel frame, invece di degradare in silenzio.

### 3.2 Lo schema bronze legacy veniva svuotato dei suoi campi di stato

Il payload `treni` dei file 2024-06 / 2025-11 contiene `pr` (provvedimento),
`dl` (variazione testuale), `cn` (cambio numerazione), `oo`/`od` (origine e
destinazione programmate). `normalize_bronze_schema` ne leggeva solo otto campi
e **scartava tutti quelli di stato**, riempiendo `Provvedimenti` e `Variazioni`
con stringa vuota.

Effetto combinato con 3.1: **cancellazioni 0,00% su tutti i mesi fino a
2025-11**, mostrate come fossero un dato reale.

Dopo il fix, giugno 2024 riporta 1,29% soppressi e 1,53% parziali, in linea con
i mesi recenti dello schema nuovo.

### 3.3 Le cancellazioni parziali contavano come totali

`Treno cancellato da THIENE a SCHIO. Arriva a THIENE` descrive un treno che ha
viaggiato su percorso ridotto. Il fallback lo classificava `cancellato`, cioe'
come se non fosse mai partito. Ora i pattern `partial_cancelled` sono valutati
**prima** di quelli generici e riconoscono la formulazione dominante nel feed.

### 3.4 I treni soppressi contavano come puntuali

Un treno soppresso viene pubblicato con `ritardo = 0`. `arrivo_in_orario`
richiedeva `0 <= ritardo <= 4`: ogni soppressione finiva quindi fra gli arrivi
puntuali, e ogni zero fittizio tirava la media verso il basso.

Ora le statistiche di ritardo si calcolano solo sugli stati in `delay_states`
(effettuato, parzialmente cancellato) e la nuova colonna `corse_con_misura`
espone il denominatore corretto. La dashboard usa quello per la "% in ritardo":
altrimenti una tratta risultava tanto piu' puntuale quanto piu' veniva
cancellata.

### 3.5 La vista "nodo" era una media di medie

`_nodo_from_ruolo` costruiva la vista per nodo a partire dalle righe gia'
aggregate per ruolo, con `mean` su `ritardo_medio`, `median` su
`ritardo_mediano` e `mean` su `p90`/`p95`. Nessuna di queste ricostruisce la
statistica vera: la media non pesata faceva contare 3 partenze quanto 900
arrivi della stessa stazione, e la mediana di due mediane non e' la mediana di
niente.

Ora entrambe le viste (ruolo e nodo) sono aggregate direttamente dalle
osservazioni.

### 3.6 p90/p95 assegnati per posizione

`agg_core` calcolava i percentili con un **secondo** `groupby` e li assegnava
con `.values`, assumendo che i due passaggi producessero righe nello stesso
ordine. Oggi e' vero, ma basta che una colonna di raggruppamento diventi
categorica perche' una stazione riceva il p95 di un'altra, senza alcun errore.

Ora i percentili vengono dallo **stesso** grouper e sono uniti per indice.
Nota: un `merge` sulle colonne non sarebbe andato bene, perche' non allinea le
chiavi NaN, e `categoria` e' genuinamente NaN su parte della storia.

### 3.7 `cancellate_tot` escludeva i soppressi

`cancellate_tot = cancellate + parzialmente_cancellate`, senza `soppresse`,
che in questo feed sono la forma dominante di corsa non effettuata. Aggiunta la
colonna `non_effettuate` (cancellate + soppresse) e corretto il totale.

### 3.8 Codice duplicato che oscurava l'import

`transform_silver.py` importava `normalize_bronze_schema` da `silver_schema.py`
e poi **ridefiniva** una propria copia 70 righe piu' sotto. Vinceva la copia
locale; ogni correzione applicata al modulo condiviso non avrebbe avuto effetto.
Copia locale rimossa.

### 3.9 Uno step del workflow notturno falliva ogni notte

`.github/workflows/daily.yml` eseguiva `python -m scripts.update_stations`, un
modulo che non esiste nel repo. Il `|| true` mascherava il `ModuleNotFoundError`
a ogni run. Step rimosso.

### 3.10 Il backfill teneva tutto in memoria

`transform_silver.main` accumulava ogni giorno trasformato in un dizionario e
scriveva solo alla fine: un backfill completo teneva ~6,5 milioni di righe
residenti insieme, piu' di quanto un runner CI conceda. Ora ogni mese viene
scritto e liberato prima di passare al successivo.

---

## 4. Cosa resta noto e non risolto

- **La distorsione della misura al capolinea e' della sorgente**, non nostra.
  Il flooring a zero la rende innocua sulla metrica pubblicata, ma i valori
  negativi intermedi restano nel dato grezzo. Ricostruire il ritardo dalla
  penultima fermata sarebbe possibile **solo per i mesi legacy** (lo schema
  nuovo non pubblica le fermate intermedie), quindi introdurrebbe una
  discontinuita' metodologica a dicembre 2025. Non l'ho fatto.
- **Il backfill completo da bronze resta lento** (~4 s per giorno sui mesi
  legacy): il payload e' un `repr()` Python da 12 MB che va passato ad
  `ast.literal_eval`, e le virgolette doppie presenti nel testo escludono la
  scorciatoia via `json.loads`. Mitigato parallelizzando sui core disponibili.
  Il run notturno non e' toccato da questo costo.
- **Le tabelle `stazioni_*_ruolo`** (35 MB) vengono costruite e pubblicate ma la
  dashboard non le carica mai.
- **L'email con le segnalazioni di bug** citata da UB non e' raggiungibile: e'
  su `info@umbertobertonelli.it`, che non e' coperto dai connettori Gmail
  disponibili. Le due caselle accessibili e le issue GitHub non la contengono.
