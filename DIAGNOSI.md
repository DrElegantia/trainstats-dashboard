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

### 3.11 Corse perse fra silver e gold per una data programmata incoerente

Emerso solo caricando lo storico, perche' e' un difetto dei dump 2022-2023.

La sorgente pubblica qualche corsa con una data programmata lontana mesi dal
giorno in cui e' stata rilevata, e il giorno del mese resta intatto:

| giorno di rilevazione | partenza dichiarata | treno |
|---|---|---|
| 2022-09-16 | 2022-**01**-16 06:20 | 23507 Bari Centrale - Fasano |
| 2022-10-24 | 2022-**01**-24 02:02 | 4104 Roma Termini - Firenze SMN |
| 2022-11-20 | 2022-**01**-20 13:15 | 16273 Venezia Mestre - Venezia S. Lucia |

E' il campo mese a essere corrotto, non l'orario. Sono **40 righe su 11,4
milioni**, ma il danno non era proporzionale alla quantita': il gold e'
partizionato per mese derivato dalla partenza, e ogni chunk riscrive solo le
partizioni dei mesi che sta lavorando, caricando i mesi adiacenti come semplice
contesto. Una riga che dichiara una partenza a nove mesi di distanza non e' mai
presente quando la sua partizione viene scritta: **spariva dal gold senza
lasciare traccia**, e silver e gold non quadravano piu' (11.369.628 contro
11.369.595). Cinque di quelle righe puntavano a gennaio 2022, un mese per cui
non esiste alcun silver, quindi nemmeno una partizione dove atterrare.

`_riconcilia_data_programmata` riporta la data al giorno di rilevazione
conservando l'orario, e sposta l'arrivo della stessa quantita' cosi' che la
durata programmata non cambi. Le corse notturne pubblicate sul foglio del
giorno precedente o successivo restano intatte: la tolleranza e' di due giorni.

Vale la pena notare come il difetto e' stato trovato. Nessuna dashboard lo
mostrava, nessun numero appariva assurdo: si vedeva solo perche' l'invariante
"silver e gold devono contare le stesse corse" e' verificato a ogni
ricostruzione. Uno scarto di 33 righe su 11 milioni non si nota a occhio.

### 3.12 Le coordinate delle stazioni erano sbagliate su larga scala

Segnalato da un lettore: "le coordinate di Venezia risultano errate". Vere, e
non era un caso isolato.

Venezia S. Lucia, 894.000 corse, stava a `40,56 / 16,66`: in Basilicata, **655
km da Venezia Mestre**, che nella realta' dista nove chilometri. La causa e' il
geocoding via Nominatim interrogato a testo libero, che non cerca stazioni ma
qualunque toponimo somigliante: per "Venezia Santa Lucia" restituisce una
frazione chiamata Santa Lucia in provincia di Pordenone, ed e' esattamente il
punto finito in cache.

La correzione ha richiesto quattro passaggi, ognuno nato dal fallimento del
precedente.

**Primo, la fonte.** Interrogare i nodi `railway=station` di OpenStreetMap
invece della ricerca libera: il risultato o e' una stazione o non c'e'. Prima
passata, 168 stazioni riposizionate per 7,4 milioni di corse, fra cui Napoli
Centrale (18 km), Bergamo (45), Parma (40), Siena (48).

**Secondo, gli omonimi.** OpenStreetMap ha piu' nodi con lo stesso nome e il
primo indice teneva quello che capitava per primo: Lodi ha preso la fermata
della metro A di Roma, Udine e Pistoia sono finite altrove. Un errore
introdotto dalla correzione stessa. Ora la scelta non si basa sul nome ma sulla
rete: fra i candidati si prende quello piu' vicino al baricentro delle stazioni
collegate da treni reali. Una stazione lombarda e' collegata a stazioni
lombarde. Otto stazioni rimesse a posto, fra cui Vittoria che sbagliava di
1.019 km.

**Terzo, i nomi che non combaciano.** Bolzano e' rimasta in Sicilia per un giro
intero perche' nessun nodo OSM si chiama "Bolzano": si chiama "Bolzano -
Bozen". Stessa cosa per Bressanone, Merano, Aosta, Villa Opicina. Ora si
indicizzano anche le componenti delle forme bilingui. E diversi capolinea
stanno all'estero, fuori dall'area italiana interrogata: Stabio in Svizzera,
Modane in Francia. Aggiunte le fasce di confine, da 3.940 a 7.961 nodi.

Restano i casi in cui la sorgente abbrevia o scrive diversamente: "RIVAROLO"
per Rivarolo Canavese, "CASTELLINA CH" per Castellina in Chianti, "CIRE'" per
Ciriè. Si risolvono confrontando parola per parola e, all'ultimo, ignorando gli
accenti. Entrambi i passaggi accettano solo se la rete conferma: "FIERA" trova
un solo nome esteso, "Fiera di Roma", ma i treni di quella stazione vanno tutti
a Palermo, quindi la corrispondenza viene rifiutata.

**Quarto, il controllo che non dipende da nessuna anagrafica.** Tutto quanto
sopra trova errori solo dove OpenStreetMap ha un nome corrispondente. Serviva
un criterio che funzionasse anche senza: `audit_coerenza_rete` non guarda i
nomi, guarda i treni, e segnala le stazioni lontane da **ogni** stazione con
cui sono collegate.

La misura giusta e' la distanza dal vicino piu' prossimo, non dal baricentro
dei vicini. Con il baricentro il controllo bocciava Milano Centrale: e'
collegata a mezza Italia, quindi il centro dei suoi trecento collegamenti cade
in Toscana e la stazione risulta "fuori posto" pur essendo al suo posto. Con il
vicino piu' prossimo il criterio regge, perche' ogni stazione reale ha almeno
un collegamento vicino, la fermata successiva sulla linea.

| | prima | dopo |
|---|---|---|
| Stazioni fuori posto oltre 5 km | 202 | 0 |
| Corse servite da stazioni mal posizionate | 7,5 mln | 0 |
| Stazioni georeferenziate | 1.599 | **3.046** |
| Copertura pesata sul traffico | 97,95% | 98,45% |
| Segnalazioni residue dell'audit | | 2, per 9 corse |

Le due residue sono un bivio da sei corse che OpenStreetMap non censisce e
Riccione, che l'audit segnala ma la cui coordinata combacia con OSM: e' un
falso positivo dovuto ai suoi tre soli collegamenti.

L'audit e' entrato negli invarianti verificati a ogni ricostruzione, con una
soglia di mille corse per non far fallire la pipeline su fermate marginali dove
la rete non e' un riferimento.

### 3.12.1 Il nome esatto vinceva sulla stazione giusta

Emerso costruendo la classifica per chilometro, che ha reso visibile un errore
che la mappa non mostrava: la tratta Milano Rogoredo-Palazzolo risultava di 765
km percorsi in 43 minuti, cioe' 1.070 km/h.

La causa e' l'ordine di precedenza fra i criteri di riconoscimento. La sorgente
tronca i nomi: scrive "PALAZZOLO" per Palazzolo Milanese e "S LAZZARO" per
Reggio San Lazzaro. OpenStreetMap ha un nodo che si chiama esattamente
"Palazzolo", in provincia di Napoli, e il confronto esatto lo accettava senza
mai interrogare i criteri successivi. Il nome combaciava; la stazione era
un'altra.

Il controllo aggiunto ribalta la precedenza: dopo i quattro passaggi sui nomi si
guarda la rete, e se un'altra collocazione plausibile del nome sta dentro il
grappolo di stazioni con cui la nostra condivide treni reali, quella vince sul
nome identico.

Il criterio non e' una soglia sulla distanza. Con una soglia il controllo
spostava **Domodossola** a Milano, dove esiste una stazione Ferrovie Nord con lo
stesso nome e dove va la maggior parte dei suoi treni: 122.397 corse
riposizionate per un criterio che sembrava ragionevole. Domodossola dista 43 km
dalla stazione piu' prossima con cui condivide corse, ed e' al suo posto: lontano
non vuol dire sbagliato.

Il criterio che tiene e' la dominanza. Si sostituisce solo se l'alternativa e'
piu' vicina a **ogni** vicino di quanto lo sia il vicino piu' prossimo
dell'attuale: non "meglio in media", meglio in ogni singolo confronto. Palazzolo
di Napoli sta a 652 km dalla piu' vicina fra Milano Rogoredo e Milano Bovisa,
mentre Palazzolo Milanese sta entro 18 km da entrambe. Per Domodossola la
condizione non si verifica, perche' la Domodossola milanese e' lontanissima da
Verbania e da Premosello, e quindi non viene toccata.

| stazione | era | e' | corse |
|---|---|---|---|
| PALAZZOLO | provincia di Napoli, 652 km dai suoi vicini | Palazzolo Milanese | 3.404 |
| GRANAROLO | Genova, 236 km dai suoi vicini | Granarolo Faentino | 24 |
| S LAZZARO | San Lazzaro di Savena, 66 km | Reggio San Lazzaro | 6.941 |

Il controllo gira in due punti, perche' le coordinate arrivano da percorsi
diversi: dentro `coordinate_per_nomi` per quelle risolte su OpenStreetMap
(Palazzolo, Granarolo) e su tutta l'anagrafica finita per quelle che vengono
dalla cache per codice, che quel modulo non attraversa (San Lazzaro).

L'audit ora giudica anche le stazioni con due soli collegamenti: chiederne tre
lasciava Palazzolo fuori dal controllo, ed era il caso peggiore che avessimo.
Il residuo, censito per intero: 29 stazioni distanti oltre 60 km dal vicino piu'
prossimo, per **736 corse su 18,7 milioni**. Diverse sono corrette per come e'
fatto il servizio, non sbagliate: Reggio Emilia AV Mediopadana ha per vicini
Milano e Bologna, e 116 km dal piu' prossimo sono la sua posizione vera.

### 3.13 I grafici mensili scavalcavano i mesi mancanti

Emerso pubblicando lo storico, e non e' un difetto dei dati ma di come
vengono disegnati.

L'asse di Plotly e' categorico: mette in sequenza i mesi che esistono, e la
linea li congiunge in ordine. Con lo storico caricato, l'asse passava da
`12/23` direttamente a `06/24`, tracciando un andamento **continuo sopra i
cinque mesi assenti**. Chi legge non ha modo di accorgersi che manca qualcosa:
un buco invisibile e' peggio di un buco.

`conMesiMancanti` completa la sequenza fra il primo e l'ultimo mese inserendo
quelli assenti con valore nullo, e Plotly interrompe la linea. I totali non si
muovono: le voci inserite non entrano in nessuna somma, la funzione e' usata
solo dalle due serie mensili.

Il difetto e' strutturale, non legato a questo buco: valeva per qualunque mese
mancante, e vale anche per i due giorni assenti di gennaio 2025, che restano
comunque non segnalati (il grafico e' mensile, e gennaio 2025 esiste).

---

## 4. Verifica di credibilita' contro la dashboard precedente

Confronto fra il gold ricostruito e il commit `baseline-gold`, che contiene
l'output della pipeline precedente.

### 4.1 Cosa deve restare identico, e resta identico

Il numero di corse osservate non dipende da nessuno dei fix applicati.

| | baseline | nuovo | delta |
|---|---|---|---|
| `kpi_mese`, corse osservate | 6.231.474 | 6.231.699 | +225 (+0,004%) |
| mesi con scostamento | | | 5 su 26, tutti sotto 0,04% |

Quei 225 record derivano dal fatto che il baseline e' l'accumulo di run
notturni incrementali, mentre questo e' un rebuild completo in una passata.
Prova: per novembre 2025 il baseline riporta 247.743 corse, ma il silver di
quel mese contiene 247.652 righe e zero corse a cavallo del mese. Le 91 righe
in eccesso del baseline **non sono derivabili dai dati sorgente**.

### 4.2 Un invariante che il baseline violava

Aggregando le stesse osservazioni a granularita' diverse la somma deve
coincidere. Nel baseline non coincideva:

| tabella | baseline | nuovo |
|---|---|---|
| `kpi_mese` | 6.231.474 | 6.231.699 |
| `kpi_mese_categoria` | 6.391.517 | 6.231.699 |
| `od_mese_categoria` | 6.392.510 | 6.231.699 |

Il baseline gonfiava del 2,6% le tabelle per stazione e per coppia O/D. La
causa e' il rimappamento dei codici stazione (`N_` sintetico verso `S` ufficiale,
transizione di dicembre 2025): nel CSV monolitico convivevano una riga con
chiave `N_` e una con chiave `S`, e la ri-aggregazione dopo il rimappamento le
**sommava**.

Verifica puntuale su Milano Centrale, luglio 2024, contando direttamente dal
silver le partenze il cui nome normalizza a `MILANO CENTRALE`:

| | corse |
|---|---|
| **silver (verita' sorgente)** | **6.865** |
| gold nuovo | 6.865 |
| gold baseline | 8.696 (+27%) |

La dashboard precedente sovrastimava il traffico proprio delle stazioni piu'
grandi, quelle che hanno cambiato codice.

### 4.3 Cosa cambia, e perche'

| indicatore | baseline | nuovo | spiegazione |
|---|---|---|---|
| righe con ritardo medio negativo | 17.837 | **0** | media con floor a zero (§2.2) |
| ritardo medio ponderato | 1,91 min | 3,01 min | non sottrae piu' gli anticipi |
| soppresse | 0,60% | 1,90% | recuperati i campi di stato dello schema legacy (§3.2) |
| cancellate totali | 0,43% | 3,47% | idem, piu' la corretta separazione parziali/totali |
| arrivi in orario | 2.642.795 | 2.605.486 | -37.309: i soppressi non contano piu' come puntuali |
| corse con misura | non presente | 6.113.161 | denominatore corretto: 118.538 corse non hanno arrivo misurabile |

Coerenza: `corse_osservate - corse_con_misura = 118.538`, esattamente il numero
di soppresse. Nessuna corsa persa per strada.

Esempi sulle tratte segnalate come impossibili (corse invariate):

| tratta | ritardo medio prima | dopo | scostamento medio |
|---|---|---|---|
| ROMA TERMINI -> NAPOLI CENTRALE (REG) | -2,92 | **2,91** | -2,97 |
| GENOVA BRIGNOLE -> ARQUATA SCRIVIA (REG) | -3,53 | **0,76** | -3,53 |
| BOLOGNA CABINA S.DONATO -> RIMINI (NCL) | -25,51 | **6,79** | -33,75 |

### 4.4 Caricamento, misurato nel browser

Stessa scena su entrambe le versioni (Milano Centrale, infrasettimanale,
mattina, tutta la storia), servite in locale:

| | baseline | nuovo | nuovo, anno selezionato |
|---|---|---|---|
| caricamento dati | 4.254 ms | 2.680 ms | 1.234 ms |
| render | 121 ms | 116 ms | 66 ms |
| byte decodificati | 158 MB | 94 MB | 26 MB |
| righe in memoria | 1.872.841 | 1.887.725 | 520.307 |

Primo render misurato sui siti pubblicati, con il cronometro iniettato prima
del caricamento del documento (misurarlo dopo, da console, conta anche il tempo
morto fra il load e l'esecuzione dello script e restituisce numeri gonfiati):

| | primo render | file | KB |
|---|---|---|---|
| produzione | 1.651 ms | 8 | 279 |
| nuova | **1.164 ms** | 8 | 257 |

In locale, senza latenza di rete, il primo render della nuova e' di 396 ms.

Nella stessa misura emerge il §4.2 a colpo d'occhio: il KPI "corse osservate"
della dashboard in produzione riporta **6.391.517**, quello della nuova
**6.231.699**. Il totale in home era gonfiato di 159.818 corse.

### 4.5 Perche' l'istogramma mostrava anticipi di 30 minuti su Milano-Verona

Segnalazione: sulla distribuzione compaiono anticipi di 30 minuti, e su alcune
tratte non sono marginali.

L'istogramma e i KPI stavano descrivendo **popolazioni diverse**:

| | corse |
|---|---|
| KPI (Milano Centrale -> Verona P.N., REG, infrasettimanale, sera) | 2.117 |
| istogramma, stessa selezione | 12.102 |

L'istogramma per stazione (`hist_stazioni_*`) e' indicizzato per stazione e
ruolo, non per coppia origine-destinazione. Con entrambi gli estremi
selezionati puo' filtrare solo su uno dei due e sceglie l'arrivo: disegnava
quindi tutti gli arrivi a Verona P.N. da qualunque origine, pur mostrando
attivo anche il badge della partenza.

Su Milano Centrale -> Verona P.N. il dato reale e' un minimo di **-8 minuti** e
**zero** corse con anticipo di 15 minuti o piu'. Gli anticipi visibili venivano
da altre origini:

| origine dell'arrivo a Verona P.N. | corse con anticipo >= 15 min |
|---|---|
| VENEZIA S.LUCIA | 232 |
| BOLZANO | 29 |
| altre | 8 |
| **totale** | **269 su 19.058 (1,4%)** |

Il meccanismo su Venezia -> Verona e' misurabile: le corse che risultano molto
in anticipo hanno **durata programmata mediana di 142 minuti**, contro gli
**88 minuti** delle corse normali sulla stessa tratta. La sorgente le orarizza
su un itinerario piu' lungo di quello effettivamente percorso, quindi l'arrivo
cade ~50 minuti prima dell'orario registrato. E' un difetto della sorgente, non
un treno veloce, e non incide su `ritardo_medio` (che ha il floor a zero) ma
popola i bucket negativi della distribuzione.

Corretto: il badge della partenza viene marcato come non applicabile
all'istogramma e sopra al grafico compare l'ambito reale con il numero di corse
su cui e' calcolato.

### 4.6 Stazioni spezzate in due dalle varianti del nome

Emerso guardando le origini degli arrivi a Verona: `VENEZIA S.LUCIA` e
`VENEZIA SANTA LUCIA` venivano contate come due stazioni distinte, cosi' come
`BOLOGNA C.LE` e `BOLOGNA CENTRALE`.

`normalize_station_name` gestiva solo le convenzioni Trenord (`M N`, `FNM`,
`NORD`) e non le abbreviazioni ferroviarie. Una tabella di espansione esisteva
in `build_station_dim.py`, ma non veniva usata dal raggruppamento dei codici.

| causa | stazioni | corse coinvolte |
|---|---|---|
| varianti del nome non unificate | 40 gruppi | **810.418 (6,50%)** |
| codici `N_`/`S` della stessa stazione non uniti | 363 | 86.275 (0,69%) |

I casi piu' grossi: Firenze S.M.N. (272.390 corse divise in due), Bologna
Centrale (253.175), Como San Giovanni (88.758).

Il secondo caso aveva una causa distinta: la mappa dei codici canonici veniva
ricostruita **a ogni chunk di tre mesi**, sui soli mesi in lavorazione. Un
codice sintetico usato solo nel 2024 e il codice ufficiale della stessa
stazione usato solo nel 2026 non finivano mai nello stesso gruppo. Ora la mappa
si costruisce una volta sull'intero storico.

Le abbreviazioni ora espanse: `C.LE`/`CENT.` -> CENTRALE, `S.M.N.` ->
S MARIA NOVELLA, `P.TA` -> PORTA, `P.NUOVA` -> PORTA NUOVA, `AER.` ->
AEROPORTO, `SCR.` -> SCRIVIA, `M.MO` -> MARITTIMO, oltre a trattini e punti
come separatori. Tutte le forme di San/Santa/Santo/Santi collassano su un
unico token, perche' da `S.` non e' deducibile quale valga.

Verificato su 18 coppie note (18 unificate) e su 7 coppie di stazioni
realmente diverse (nessuna falsa unione). La versione JavaScript, che deve
raggruppare le tendine allo stesso modo, e' stata confrontata con quella Python
su tutti i 1.777 nomi presenti: zero divergenze.

### 4.7 Bug dei filtri di dettaglio

Segnalazione: con Milano -> Verona, REG, infrasettimanale, sera tutti gli
indicatori vanno a zero. Riprodotto sulla dashboard in produzione.

I pill di "tipo giornata" e "fascia oraria" nascono **tutti attivi**, perche'
attivo significa "incluso" e il default e' includere tutto. Cliccare "Sera"
per *selezionare* la sera quindi la **escludeva**. E niente impediva di
spegnerli tutti: un gruppo vuoto non seleziona nessuna riga, e l'intera
dashboard leggeva zero senza dire perche'.

Corretto: il primo click su un gruppo interamente attivo **isola** il valore
cliccato, e l'ultimo valore incluso di un gruppo non si puo' spegnere.
Verificato nel browser: Milano Centrale -> Verona P.N., REG -> 12.208 corse;
click su Infrasettimanale -> 8.638; click su Sera -> **2.117**, che coincide
con il calcolo diretto in pandas.

---

## 4.8 Stato finale verificato

Numeri della pipeline dopo tutti gli interventi, con la soglia di validita' a
-5 minuti. Le tabelle dei paragrafi precedenti descrivono stati intermedi.

| | valore |
|---|---|
| righe nel silver | 6.231.961 |
| corse nel gold | **6.231.961** (nessuna riga persa) |
| corse con misura di ritardo utilizzabile | 5.899.298 (94,66%) |
| misure escluse | 332.663 (5,34%) |
| ritardo medio ponderato | 3,121 min |
| arrivi puntuali | 82,48% |
| righe con ritardo medio negativo, in tutto il gold | **0** |
| corse con anticipo superiore a 5 minuti | **0** |
| stazioni distinte | 1.732 (erano 2.152, 420 duplicati uniti) |
| alias di codice stazione unificati | 1.171 |

Invarianti che ora reggono e prima no:

- `silver == gold` sul conteggio corse. Prima 262 righe si perdevano ai bordi
  dei chunk, perche' il calcolo di un mese non caricava i mesi confinanti.
- La somma delle corse coincide fra `kpi_mese`, `kpi_mese_categoria` e
  `od_mese_categoria`. Nel baseline le tabelle per stazione e per coppia O/D
  erano gonfiate del 2,6%.
- `corse_osservate - corse_con_misura` corrisponde esattamente alle misure
  escluse, elencate e contabilizzate.

Distribuzione finale dello scostamento all'arrivo:

| classe | corse | quota |
|---|---|---|
| -5 | 140.323 | 2,38% |
| (-5,-1] | 2.119.830 | 35,93% |
| (-1,0] | 586.197 | 9,94% |
| (0,1] | 723.415 | 12,26% |
| (1,5] | 1.517.838 | 25,73% |
| (5,10] | 441.188 | 7,48% |
| (10,15] | 140.304 | 2,38% |
| (15,30] | 142.872 | 2,42% |
| (30,60] | 64.964 | 1,10% |
| (60,120] | 18.562 | 0,31% |
| > 120 | 3.805 | 0,06% |

Verifica nel browser sulla dashboard pubblicata: Milano Centrale -> Verona
Porta Nuova, REG, infrasettimanale, sera restituisce 2.117 corse osservate e
una distribuzione di 1.972 corse misurate **della tratta**, non dei 12.102
arrivi a Verona da qualunque origine. La mappa disegna 97 marker con cinque
livelli di colore sulla metrica e sei dimensioni sul volume, con legenda.

## 4.9 Verifica della catena di import

Il percorso di ingestione non era mai stato eseguito dopo gli interventi, e
conteneva due bug.

**Il run notturno restava appeso sul geocoding.** `run_pipeline` invocava
`build_station_dim` con il geocoding online attivo. Con 1.304 stazioni senza
coordinate, tre query ciascuna e pause di uno o due secondi fra i tentativi,
una passata completa impiega ore, e non produce nulla: Nominatim risponde 429 a
un uso di questo volume, quindi ogni stazione falliva dopo tre tentativi e
altrettante attese. Il README dichiarava, correttamente nelle intenzioni, che
"la pipeline non fa geocoding online". Ora il geocoding e' opt-in
(`--enable-geocoding`) e si interrompe dopo dieci fallimenti consecutivi.

**Due normalizzazioni divergenti dei nomi stazione.** In `build_station_dim`
viveva una seconda tabella di abbreviazioni, simile ma non identica a quella
condivisa: espandeva `S.` in `SAN`, mentre quella condivisa fa collassare
San/Santa/Santo su un token unico. Con le due divergenti il match per nome delle
coordinate falliva, e cinque stazioni perdevano coordinate che la cache
conteneva. Delegando a `normalize_station_name` le stazioni georeferenziate
passano da 1.317 a **1.598**, con copertura del **97,96%** delle corse.

Esito della verifica:

| verifica | esito |
|---|---|
| `scripts.ingest` da sorgente reale | scaricato e header validato in modalita' strict, 5 s |
| contenuto riscaricato vs precedente | 8.646 righe entrambi, colonne identiche, stessa quota di soppressi |
| `run_pipeline` end-to-end in locale | 5 step, **31,6 s**, exit 0 |
| invarianti dopo il run incrementale | silver = gold = 6.231.961, zero ritardi medi negativi |
| workflow `daily.yml` in CI | **12 step su 12 verdi, 1m50s**, incluso commit e deploy |
| dato pubblicato vs locale | identico: 6.231.961 corse, ritardo medio 3,121 |

Il commit prodotto dalla CI tocca **sei piccoli parquet del solo mese
aggiornato**, non un CSV monolitico da riscrivere per intero: e' la conferma
sul campo che la partizione mensile risolve anche la crescita del repository.

---

## 5. Cosa resta noto e non risolto

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
- **Due giorni mancano alla fonte**: i file bronze del 10 e 11 gennaio 2025
  contengono solo l'intestazione. Rifacendo la stessa GET oggi la sorgente
  restituisce ancora zero righe, quindi il buco e' suo e definitivo. Gennaio
  2025 e' calcolato su 29 giorni, senza che la dashboard lo segnali.

---

## 6. Lo storico dai dump MEGA

### 6.1 Cosa c'e' davvero nell'archivio

`megatools` elencava 645 file: non apriva la cartella `Vecchio sito`, e `ls`
sui link pubblici pretende un account. Interrogando l'API MEGA e decifrando
l'albero con la chiave del link, l'archivio reale e':

| cartella | file | GB | periodo |
|---|---|---|---|
| `dump old ... - XML` (`old.zip`) | 1 | 0,02 | ott 2019 - feb 2020 |
| `dump new 11_01_2020-29_02_2020 - JSON` | 50 | 0,78 | gen - feb 2020 |
| `01-03-2020 31-12-2020` | 303 | 3,37 | mar - dic 2020 |
| `2021 completo` | 360 | 4,77 | 2021 intero |
| `prova` | 99 | 0,92 | duplicati del 2020 |
| `Vecchio sito` (radice) | 44 | 0,65 | gen - 14 feb 2022 |
| `dati precedente database` | 623 | 9,33 | 15 feb 2022 - nov 2023 |
| `01_02_2023 a 31_12_2023` | 334 | 5,23 | feb - dic 2023 |
| radice `Dati TrainStats v2` | 159 | 2,46 | gen - 7 giu 2024 |

**1.973 file, 27,53 GB, 1.600 giornate distinte dall'11 gennaio 2020 al 7
giugno 2024**, quasi senza interruzioni. Il bronze parte dal giugno 2024:
lo storico porterebbe la serie da 2 a 6,5 anni.

### 6.2 Quanto e' riconducibile: misurato, non stimato

Il timore era che i dati divergessero troppo. Sui dump 2022-2023 la divergenza
si e' rivelata modesta.

**Identita' delle stazioni.** Le stazioni storiche sono identificate dal nome,
non da un codice: il payload legacy non porta codici, quindi il silver le
marca al 100% con codici sintetici `N_<hash del nome normalizzato>`.
Normalizzando i nomi, **il 99,46% delle osservazioni storiche trova la
corrispondente stazione odierna**. Le 14 che non la trovano sono casi reali,
lo 0,54% del volume: AOSTA (linea sospesa), ROMA TIBURTINA PIAZZALE EST, e una
decina di fermate minori non piu' servite. La mappa dei codici canonici
preferisce i codici ufficiali `S` a quelli sintetici, quindi le coordinate di
`stations_dim.csv` restano valide e la mappa non si rompe.

**Volumi e composizione.** A parita' di campione, cinque giornate per epoca:

| | storico (2022-2023) | odierno (2024-2026) |
|---|---|---|
| corse | 37.802 | 39.573 |
| stazioni distinte | 449 | 544 |
| REG | 92,2% | 92,0% |
| MET | 2,80% | 2,75% |
| IC | 1,16% | 1,31% |

Settembre 2022 completo da' 7.727 corse al giorno, contro le 7.000-8.600 dei
mesi recenti, con cancellazioni presenti e coerenti (2.488 soppressi, 2.498
parziali su 231.820 corse).

**Schema.** I record treno hanno le stesse chiavi che la pipeline gia' gestisce,
piu' `oaz`/`opz`. Nessuna trasformazione nuova e' stata necessaria oltre
all'importatore.

**Due export si sovrappongono, e coincidono.** `dati precedente database` e
`01_02_2023 a 31_12_2023` coprono entrambi feb-nov 2023. Confrontati riga per
riga su quattro giornate: **zero differenze su ritardi e provvedimenti** sulle
circa 8.300 corse comuni al giorno. Sono due export dello stesso database, uno
con il campo `_id` di MongoDB. La precedenza va al primo, il secondo riempie
novembre e dicembre 2023.

**Una conferma inattesa.** Nei dump storici `ra` e `rp` possono valere la
stringa `'X'`, e compare **esattamente sui treni con `pr='Soppresso'`** (28 su
28 nel campione). E' una conferma indipendente, da una sorgente diversa e da
un'altra epoca, che la correzione descritta in 3.4 e' quella giusta: un treno
soppresso non ha ritardo zero, non ha alcuna misura.

### 6.3 Cosa e' stato importato, e cosa manca

Importate **684 giornate**, dal 15 febbraio 2022 al 31 dicembre 2023. Silver
ricostruito in 69 secondi per 23 mesi. Bronze da 842 MB a 1,6 GB.

Il resto e' fermo su un limite esterno: dopo 14 GB MEGA risponde **509 over
quota** sui download anonimi. Restano 1.016 file, e fra questi i 159 della
radice che coprono **gennaio-maggio 2024**. La copertura attuale ha quindi un
buco di cinque mesi fra il 2023 e il giugno 2024, e la serie estesa **non va
pubblicata prima di averlo colmato**: la dashboard mostrerebbe una
discontinuita' in mezzo al grafico. La quota anonima si sblocca alcune ore dopo
il primo download.

Restano inoltre da analizzare, perche' non ancora scaricabili:

- **2020 e 2021** (713 file): stesso formato JSON apparente, da verificare.
- **`old.zip`**, ottobre 2019 - febbraio 2020: XML, formato diverso da tutti
  gli altri, richiederebbe un secondo importatore.
- **`prova`** (99 file): sembra duplicare il 2020, da usare solo per colmare
  buchi.

### 6.4 Aggiungere lo storico non muove il periodo gia' pubblicato

Il rischio da escludere era che unificare le stazioni su 4,5 anni cambiasse
retroattivamente i numeri dei due anni gia' online. Confronto fra il gold
committato e quello ricostruito con lo storico, limitato ai 26 mesi comuni:

| | pubblicato | con lo storico |
|---|---|---|
| corse, `kpi_mese` | 6.231.961 | 6.231.961 |
| corse, vista stazioni | 12.463.922 | 12.463.922 |
| stazioni distinte | 1.731 | 1.731 |
| mesi con `ritardo_medio` diverso | 0 su 26 | |
| righe comuni con conteggio diverso | 0 su 22.326 | |

Cambia solo l'etichetta di **42 stazioni**, e solo fra codici non ufficiali:
prima portavano un hash `N_<sha1 del nome>`, ora il codice che la sorgente
fornisce per la rete suburbana lombarda (`N00006` Bollate Centro, `N00010`
Cesate, `N00019` Tradate). Nessuna stazione perde un codice ufficiale `S`, e la
copertura della mappa resta invariata (97,95% contro 97,96%). Le stazioni
storiche e quelle odierne finiscono nella stessa identita': Milano Centrale e'
`S01700` su tutti i 49 mesi, con zero nomi associati a piu' di un codice.

---

## 7. I chilometri per tratta e la lentezza della rete

La sorgente dice quanto un treno ritarda, non quanta strada fa. Senza i
chilometri non si distingue una linea lenta da un servizio gestito male, ed e'
la distinzione che serve per dire dove la rete non funziona.

### 7.1 Da dove vengono i chilometri

Quattro fonti provate, in ordine di autorevolezza dichiarata e verificata:

| fonte | copertura | scarto mediano contro il dato del gestore |
|---|---|---|
| **RINF**, registro dell'infrastruttura (Reg. UE 2019/777) | 11.475 tratte | **0,05%**, 95,8% entro il 5% |
| GTFS, `shape_dist_traveled` del gestore | 44 tratte adiacenti | e' il dato del gestore |
| grafo dei binari OpenStreetMap | 6.788 tratte | 1,18% |
| prontuario distanze FS | verifica | 2,05% |

Il RINF divide la rete in *section of line*, tratte fra due punti operativi
adiacenti, ognuna con `lengthOfSectionOfLine` in chilometri: sono i lati di un
grafo, e la distanza fra due stazioni qualsiasi e' il cammino minimo su quei
lati. 3.661 sezioni italiane, 3.476 lati, 3.184 punti operativi.

I GTFS coprono poco perche' quasi nessun gestore pubblica la progressiva: su 75
feed solo 3 la compilano, e Trenord ha 4.358 corse ferroviarie senza alcuna
geometria. Sono comunque il **metro di misura**: sono l'unico dato di distanza
che i gestori pubblichino di persona, e le 254 tratte in cui esiste hanno
permesso di misurare l'errore delle altre fonti invece di dichiararlo.

Una avvertenza che ERA mette per iscritto: la lunghezza della sezione e' la
distanza teorica fra i punti centrali dei due punti operativi, non la
sottrazione fra due progressive chilometriche. Per una tabella
origine-destinazione va bene la prima; per la chilometrica storica e
amministrativa servirebbero i Fascicoli Linea di RFI.

Ogni tratta porta scritto da dove viene il suo numero e quanto ci si puo'
fidare, invece di consegnare una colonna di chilometri come se fossero tutti
equivalenti. Il controllo che scarta i casi rotti senza avere una seconda fonte
e' il rapporto con la linea d'aria: una ferrovia reale e' piu' lunga della
retta, ma non tre volte tanto. Grosseto-Montepescali, due stazioni a dodici
chilometri, risultava 339 km perche' al grafo mancava un raccordo e il cammino
girava intorno.

**Risultato: 18.689 tratte su 18.929 utilizzabili, il 98,64% del traffico, di
cui l'85,94% con la distanza ufficiale del registro.** Le 240 scartate non
vengono riempite con un numero plausibile: restano vuote.

### 7.2 I tre indicatori, e perche' servono tutti e tre

Il ritardo al chilometro da solo non dice se una linea funziona: dice quanto il
servizio si scosta dal **proprio** orario, e un orario abbondante assorbe i
ritardi senza che nessuno li veda. Affiancati:

- **durata programmata al km**: minuti che l'orario concede per chilometro. E'
  la lentezza strutturale, quella del binario unico e delle curve. Non dipende
  da come e' andata quel giorno.
- **ritardo al km**: minuti che il servizio aggiunge a quell'orario. E' la
  lentezza gestionale.
- **velocita' commerciale**: la durata al km letta al contrario, in km/h.
  Stessa informazione, unita' leggibile.

Serve un pavimento sulla lunghezza, altrimenti la classifica misura le manovre
invece del viaggio: senza vincolo il podio delle piu' lente e' fatto di navette
di due chilometri, dove la sosta pesa piu' del percorso. Con almeno 300 corse e
30 km restano 1.203 tratte, per 15,3 milioni di corse.

Mediana: **104,9 minuti programmati per 100 km, cioe' 57,2 km/h**, con 3,31
minuti di ritardo per 100 km. Gli estremi vanno da 25,1 km/h (Eboli-Nocera
Inferiore, 40,8 km in 97 minuti) a 158,7 km/h (Firenze-Milano).

I due problemi sono distinti e si vedono separati. Castellammare di Stabia-Napoli
Campi Flegrei e' fra le piu' lente d'orario (27,3 km/h) con un ritardo fra i piu'
bassi (3,0 minuti per 100 km): quella linea e' lenta per come e' fatta, il
servizio rispetta l'orario che ha. Chiasso-Monza viaggia a 47,3 km/h
programmati ma accumula 55,7 minuti di ritardo per 100 km: l'orario non e' il
problema.

La classifica sta sulla dashboard, nella scheda "Tratte per chilometro
percorso", con le tre metriche selezionabili e il caso peggiore sempre in cima
(per la velocita' commerciale l'ordine si inverte, perche' li' il peggio e' il
valore piu' basso). E' l'unica vista che non risponde ai filtri, e lo dichiara:
i chilometri non dipendono dall'anno o dalla categoria, e una durata media
calcolata su tre corse non vorrebbe dire niente. Il file pubblicato e'
`indicatori_km.csv`, un centinaio di KB.

### 7.2.1 Il grafo RINF saldava la Sicilia alla Puglia

Emerso cercando le tratte che attraversano lo Stretto, e non riguarda solo
quelle.

Il grafo delle sezioni di linea era costruito sui **nomi** normalizzati dei
punti operativi. In Italia ci sono tre San Paolo: uno vicino a Messina, uno fra
Noto e Rosolini, uno vicino a Taranto. Il nome li fondeva in un unico nodo, e da
li' passava un collegamento che non esiste. Il cammino minimo Messina-Roma
usciva dalla Sicilia via Taranto **senza attraversare lo Stretto**: 564 km
invece di circa 900.

Non era un caso isolato ne' solo siciliano. La stessa saldatura su San Benigno
accorciava Savona-Bari di 364 km, e Ventimiglia-Lecce di altrettanto. Sei nomi
corrispondevano a piu' di un punto operativo.

Il grafo ora si costruisce sugli **identificativi** dei punti operativi, che il
RINF pubblica e che il nome non sostituisce. Un nome che corrisponde a piu'
nodi genera piu' partenze e si tiene la distanza minima fra le combinazioni,
invece di fonderli.

| | prima | dopo |
|---|---|---|
| tratte invariate entro lo 0,1% | | 11.157 su 12.171 |
| tratte cambiate oltre il 5% | | **875** |
| correzione massima | | +364 km |
| tratte sparite, esistevano solo per la saldatura | | 174 |

Tolta la saldatura la Sicilia risulta quello che e': una rete separata, 212
punti operativi che non toccano il continente. Il RINF non dichiara lo Stretto
perche' non e' una sezione di linea, e ha ragione: i treni ci passano caricati
sulla nave. Senza un lato li' pero' ogni collegamento fra l'isola e il
continente resta senza distanza, compresi i 3.488 Roma-Palermo che ci sono nei
dati. Il traghettamento e' quindi aggiunto come lato esplicito Villa San
Giovanni-Messina di 9 km, che e' il valore del prontuario FS, dichiarato nel
codice per quello che e': l'unica distanza del grafo che non viene da un
registro di infrastruttura.

### 7.2.2 Reggio Calabria era due stazioni

Trovata dal confronto fra le tratte dello Stretto: Roma-Reggio compariva due
volte con numeri diversi. La sorgente scrive sia "REGGIO CALABRIA CENTRALE"
(167.796 corse) sia "REGGIO DI CALABRIA CENTRALE" (51.030), e le due grafie
erano due identita' separate per **218.826 corse**: la spaccatura piu' grande
rimasta dopo quella degli accenti.

La regola aggiunta toglie il "di" fra due parti del nome. Sulle 3.359 stazioni
dell'anagrafica e' l'unico caso in cui toglierlo unisce due voci, quindi non
rischia di fondere stazioni diverse, e "Diano Marina" e "Ronchi dei Legionari"
restano intatte. Vale in entrambi i normalizzatori, quello della pipeline e
quello del browser, verificati uno contro l'altro.

### 7.2.3 Il costo della traversata, misurato

Il rapporto per chilometro non lo mostra, perche' diluito su seicento
chilometri sparisce. Si vede in un confronto fra due tratte lunghe uguali:

| | km | orario | corse |
|---|---|---|---|
| Roma Termini-Reggio Calabria Centrale | 653,7 | 6h51 | 28.082 |
| Roma Termini-Messina Centrale | 648,6 | 8h53 | 579 |

**Cinque chilometri in meno, 122 minuti in piu'.** Quelle due ore sono manovra,
imbarco, traversata e sbarco. E' l'unico punto della rete dove il tempo non ha
niente a che vedere con la distanza, e sta nella dashboard sotto la classifica
per chilometro, calcolato dai dati pubblicati e non scritto a mano.

**Il confronto va fatto a parita' di lunghezza, altrimenti mente.** La prima
versione di questa scheda rapportava le tratte dello Stretto a tutte le 1.192
pubblicate e concludeva che erano piu' veloci della media: 69 km/h contro 58.
E' vero come numero e falso come conclusione, perche' quella platea e' fatta in
maggioranza di regionali che fermano ovunque, e un intercity di ottocento
chilometri ci fa una figura facile.

Confrontate con le altre tratte lunghe, che e' il paragone che regge:

| tratte oltre 400 km | media pesata sulle corse |
|---|---|
| senza Stretto (70 tratte) | **116,2 km/h** |
| via Stretto (4 tratte) | **69,5 km/h** |

Le quattro stanno **66ª, 68ª, 70ª e 71ª su 74**: sono fra le piu' lente della
lunga percorrenza italiana, non fra le piu' veloci della rete. Milano-Napoli fa
774 km a 151 km/h, Milano-Siracusa ne fa 1.386 a 67. La nave non e' l'unica
causa (la dorsale calabrese e quella sicula sono lente per conto loro) ma i 122
minuti misurati sopra sono la parte che un collegamento fisso eliminerebbe.

Le quattro tratte sono marcate nel file pubblicato con una colonna
`attraversa_stretto`, perche' saperlo richiede il grafo della rete e il browser
non ce l'ha. Riconoscerle dalla geografia non funziona: allo Stretto Messina e
Reggio distano tre chilometri e qualunque soglia su latitudine o longitudine ne
sbaglia una delle due. Decide la componente connessa del RINF, e le coordinate
intervengono solo per le stazioni siciliane che il registro chiama in un altro
modo (Taormina Giardini, Punta Raisi Aeroporto), che altrimenti risultavano
continentali e facevano sembrare che mezza Sicilia interna attraversasse lo
Stretto.

### 7.2.4 La lentezza disegnata sui binari

La classifica dice **quali** tratte sono lente, la mappa dice **dove**. Sono due
domande diverse: dalla classifica non si vede che la lentezza del Mezzogiorno e'
continua da Battipaglia in giu', ne' che il nodo di Milano e' rosso perche' ci
passano soprattutto suburbani.

Le linee sono i binari veri. Una tratta e' una coppia di stazioni, e disegnarla
come segmento retto fra i due capolinea produrrebbe una mappa falsa:
Roma-Siracusa taglierebbe il Tirreno. La geometria viene dal grafo
OpenStreetMap gia' usato per misurare le distanze, e per ogni collegamento si
ricostruisce il cammino e si attribuisce a ogni tratto percorso la velocita' di
quel collegamento, pesata sulle corse. 1.168 percorsi su 1.192 tracciati,
178.399 tratti di binario, ridotti a 932 polilinee da 270 KB unendo i tratti
adiacenti della stessa classe e semplificando la geometria a 110 metri.

**Cosa la mappa misura, e cosa no.** Misura i treni, non la linea: il colore e'
la media pesata delle velocita' commerciali dei treni che percorrono quel
tratto. Dove passano molti regionali il tratto risulta lento anche se
l'infrastruttura non lo e', perche' sono loro a fare il numero delle corse:
Milano-Piacenza risulta a 54 km/h per questo motivo. E la sorgente da' i tempi
solo ai capolinea, non alle fermate intermedie, quindi la velocita' di un
viaggio si spalma uguale su tutto il percorso. Entrambe le avvertenze sono
scritte sotto la mappa, non solo qui: una mappa che promette "dove la rete e'
lenta" e misura altro e' peggio di nessuna mappa, e infatti la scheda si chiama
"Dove i treni vanno piano".

### 7.3 Cosa ha trovato la classifica che la mappa non mostrava

Il rapporto fra chilometri e minuti e' un controllo di plausibilita' che le
metriche precedenti non avevano. Milano Rogoredo-Palazzolo risultava 765 km in
43 minuti, cioe' 1.070 km/h: non un treno veloce, una stazione nel posto
sbagliato (vedi 3.12.1). Il controllo e' rimasto dentro `indicatori_km` come
rete di sicurezza permanente, con la soglia a 200 km/h di media programmata:
oggi la piu' alta e' Firenze-Milano a 158,7 e nessuna tratta la supera.
