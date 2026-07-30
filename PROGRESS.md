# Ricostruzione dopo il fix della categoria alta velocita'

**Perche'.** Il payload legacy tiene la sigla dell'alta velocita' nel campo
`sub`, non in `c`, e leggevamo solo `c`. Ogni Frecciarossa dal 2020 in poi
finiva in "senza categoria": circa mezzo milione di corse su tutto lo storico.
Su giugno 2025, dopo la correzione, il gruppo vuoto scende da 7.569 a 17 e
compaiono FR 6.764, FA 507, FB 281, ECFR 63.

**Secondo guasto, trovato durante la prima ricostruzione.** Qualche record porta
un epoch fuori scala che in secondi cade oltre l'anno 9999; pandas rifiuta
strftime sull'intera colonna e il chiamante scartava il file INTERO. Venti
giorni persi su 2.481, fra cui dieci di gennaio 2020 (82.645 corse). Il silver
vecchio non ne risentiva perche' scritto quando pandas ancora formattava quelle
date: il guasto era invisibile finche' nessuno rifaceva tutto da zero. Corretto
azzerando la sola data impossibile invece di buttare il giorno.

**Ordine.** Il silver va cancellato prima di rigenerarlo: `transform_silver`
fonde con quanto trova, e sovrascrivere senza cancellare raddoppia i conteggi.

- [x] 1. silver capolinea, tutti i mesi (cancellare prima)
- [x] 2. build_gold, tutti i mesi
- [x] 3. transform_fermate, 2020-01 -> 2026-07 (circa un'ora)
- [x] 4. build_gold_fermate
- [x] 5. build_gold_tratte
- [x] 6. copertura_categoria
- [x] 7. build_station_dim
- [x] 8. build_site
- [x] 9. verifica_invarianti
- [ ] 10. commit e push sui tre repo

**Ripresa.** Se il job muore, riprendere dal primo passo non spuntato. I passi
1 e 3 sono idempotenti solo se si cancella prima l'uscita del mese interessato.
