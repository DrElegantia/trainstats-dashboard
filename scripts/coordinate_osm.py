# scripts/coordinate_osm.py
"""Rifa' l'anagrafica delle coordinate stazione da OpenStreetMap.

Le coordinate venivano da Nominatim interrogato a testo libero, che non cerca
stazioni: cerca qualunque cosa somigli alla stringa. Cercando "Venezia Santa
Lucia" restituisce una frazione chiamata Santa Lucia in provincia di Pordenone,
ed e' esattamente il punto in cui la dashboard disegnava la stazione di
Venezia, 655 km a sud di Venezia Mestre. Non era un caso isolato: 202 stazioni
avevano coordinate sbagliate di oltre 5 km, per 7,5 milioni di corse, fra cui
Napoli Centrale (18 km), Bergamo (45), Parma (40), Siena (48).

Qui si interrogano solo i nodi `railway=station` e `railway=halt` di
OpenStreetMap: il risultato o e' una stazione ferroviaria o non c'e'. Una sola
richiesta scarica tutte le stazioni italiane, invece di una richiesta per nome:
qualche migliaio di interrogazioni fa scadere Overpass e non e' un uso educato
di un servizio gratuito.

    python -m scripts.coordinate_osm --scarica          # aggiorna la cache OSM
    python -m scripts.coordinate_osm --dry-run          # mostra cosa cambierebbe
    python -m scripts.coordinate_osm                    # riscrive stations/stations.csv
"""
from __future__ import annotations

import argparse
import json
import math
import os
import urllib.parse
import urllib.request
from typing import Dict, Optional, Tuple

import pandas as pd

from .utils import normalize_station_name

CACHE_OSM = os.path.join("data", "stations", "osm_stazioni.json")
ANAGRAFICA = os.path.join("stations", "stations.csv")
DIM = os.path.join("data", "gold", "stations_dim.csv")

# Oltre questa distanza fra la coordinata attuale e quella OSM si considera che
# la vecchia fosse sbagliata. Cinque chilometri lasciano passare le differenze
# fra il nodo della stazione e il centroide del fascio binari, che su scali
# grandi arrivano a un paio di chilometri.
SOGLIA_KM = 5.0

# Oltre all'Italia servono le fasce di confine: diversi capolinea della rete
# italiana stanno all'estero (Stabio e Chiasso in Svizzera, Modane in Francia,
# il Brennero verso l'Austria, Villa Opicina verso la Slovenia) e senza di
# loro quelle stazioni restano con la coordinata sbagliata di prima. Il
# riquadro unico su tutta l'area viene rifiutato da Overpass, quindi si fanno
# richieste piccole e si uniscono.
AREE = [
    ("Italia", None),
    ("Svizzera e Ticino", (45.6, 5.9, 47.9, 10.6)),
    ("Francia sud-orientale", (43.0, 4.5, 46.6, 7.8)),
    ("Austria e Slovenia", (46.2, 9.5, 48.0, 16.2)),
]

_Q_AREA = """
[out:json][timeout:180];
area["ISO3166-1"="IT"][admin_level=2]->.it;
(
  node["railway"~"^(station|halt)$"]["name"](area.it);
  way["railway"~"^(station|halt)$"]["name"](area.it);
);
out center;
"""

_Q_BBOX = """
[out:json][timeout:180];
(
  node["railway"~"^(station|halt)$"]["name"](%s);
  way["railway"~"^(station|halt)$"]["name"](%s);
);
out center;
"""


def _interroga(query: str) -> list:
    dati = urllib.parse.urlencode({"data": query}).encode()
    req = urllib.request.Request(
        "https://overpass-api.de/api/interpreter", data=dati,
        headers={"User-Agent": "trainstats-lab/1.0 (anagrafica stazioni)"})
    with urllib.request.urlopen(req, timeout=300) as r:
        risposta = json.load(r)
    out = []
    for e in risposta.get("elements", []):
        centro = e.get("center") or {}
        lat, lon = e.get("lat") or centro.get("lat"), e.get("lon") or centro.get("lon")
        nome = (e.get("tags") or {}).get("name")
        if lat and lon and nome:
            out.append({"nome": nome, "lat": float(lat), "lon": float(lon)})
    return out


def scarica_osm(destinazione: str = CACHE_OSM) -> int:
    import time

    visti = set()
    out = []
    for etichetta, bbox in AREE:
        query = _Q_AREA if bbox is None else _Q_BBOX % (
            "%s,%s,%s,%s" % bbox, "%s,%s,%s,%s" % bbox)
        try:
            nodi = _interroga(query)
        except Exception as e:
            print(f"  {etichetta}: fallita ({e})")
            continue
        nuovi = 0
        for n in nodi:
            k = (n["nome"], round(n["lat"], 5), round(n["lon"], 5))
            if k in visti:
                continue
            visti.add(k)
            out.append(n)
            nuovi += 1
        print(f"  {etichetta}: {len(nodi)} nodi, {nuovi} nuovi")
        time.sleep(3)

    os.makedirs(os.path.dirname(destinazione), exist_ok=True)
    with open(destinazione, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    return len(out)


def _senza_accenti(s: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")


def varianti_nome(nome: str) -> list:
    """Il nome OSM piu' le sue componenti bilingui.

    In Alto Adige, Valle d'Aosta, Friuli e nel Carso OpenStreetMap usa la forma
    doppia: "Bolzano - Bozen", "Brixen - Bressanone", "Aosta / Aoste",
    "Aurisina / Nabrezina". Confrontando solo la stringa intera nessuna di
    queste combacia con il nome italiano che usa la sorgente, e la stazione
    resta con la coordinata sbagliata di prima: Bolzano finiva in Sicilia, a
    38,20 / 15,63, perche' nessun nodo OSM si chiamava esattamente "Bolzano".
    """
    grezzi = [nome]
    for sep in (" - ", "/", " – "):
        if sep in nome:
            grezzi.extend(p.strip() for p in nome.split(sep))
    out = []
    for g in grezzi:
        k = normalize_station_name(g)
        if k and k not in out:
            out.append(k)
    return out


def candidati_osm(percorso: str = CACHE_OSM) -> Dict[str, list]:
    """Tutti i nodi OSM per ogni nome normalizzato, varianti comprese."""
    with open(percorso, encoding="utf-8") as f:
        dati = json.load(f)
    per_nome: Dict[str, list] = {}
    for s in dati:
        punto = (float(s["lat"]), float(s["lon"]))
        for k in varianti_nome(s["nome"]):
            per_nome.setdefault(k, []).append(punto)
    return per_nome


def indice_osm(percorso: str = CACHE_OSM) -> Dict[str, Tuple[float, float]]:
    """Nome normalizzato -> coordinate, con i casi ambigui risolti sulla rete.

    Per la maggior parte dei nomi i nodi OSM sono piu' d'uno ma distano poche
    decine di metri: fabbricato viaggiatori, fermata, area della stazione. Li'
    il primo che arriva va benissimo.

    Per una quarantina di nomi invece i candidati sono a centinaia di
    chilometri, perche' lo stesso nome esiste come stazione ferroviaria e come
    fermata di metropolitana: Lodi e' una stazione in Lombardia e una fermata
    della metro A di Roma, e prendendo il primo la dashboard disegnava Lodi
    dentro Roma. Vale anche per Vittoria, Bicocca, Marconi, Repubblica, Dante.

    Il criterio per scegliere non e' il nome ma la rete: si guarda dove stanno
    le stazioni con cui quella e' collegata da treni reali, e si prende il
    candidato piu' vicino a loro. Una stazione lombarda e' collegata a stazioni
    lombarde.
    """
    per_nome = candidati_osm(percorso)
    idx: Dict[str, Tuple[float, float]] = {}
    ambigui: Dict[str, list] = {}

    for k, v in per_nome.items():
        if len(v) == 1:
            idx[k] = v[0]
            continue
        lontani = max(distanza_km(a[0], a[1], b[0], b[1])
                      for i, a in enumerate(v) for b in v[i + 1:])
        if lontani <= 10:
            idx[k] = v[0]
        else:
            ambigui[k] = v

    for k, v in ambigui.items():
        rif = _baricentro_vicini(k, idx)
        if rif is None:
            # Senza contesto di rete non si puo' scegliere meglio del caso:
            # meglio nessuna coordinata che una a mille chilometri.
            continue
        idx[k] = min(v, key=lambda c: distanza_km(rif[0], rif[1], c[0], c[1]))

    return idx


def _combacia_per_token(sorgente: str, osm: str) -> bool:
    """Vero se il nome della sorgente e' il nome OSM abbreviato.

    La sorgente accorcia: "CASTELLINA CH" per "Castellina in Chianti",
    "RIVAROLO" per "Rivarolo Canavese", "S GIUSEPPE DI CAIRO" per la forma
    estesa. Ogni parola della sorgente deve essere l'inizio di una parola OSM,
    nello stesso ordine, e la prima deve combaciare per intero: senza questo
    vincolo "MARINO" pescherebbe "San Marino" e "CAIRO" qualunque cosa cominci
    per Cairo.
    """
    ts, to = sorgente.split(), osm.split()
    if not ts or not to or ts[0] != to[0]:
        return False
    i = 0
    for parola in ts[1:]:
        i += 1
        while i < len(to) and not to[i].startswith(parola):
            i += 1
        if i >= len(to):
            return False
    return True


def risolvi_per_abbreviazione(chiave: str, per_nome: Dict[str, list],
                              risolti: Dict[str, Tuple[float, float]]
                              ) -> Optional[Tuple[float, float]]:
    """Cerca il nome esteso di cui la sorgente ha scritto l'abbreviazione.

    Si accetta solo se il contesto di rete indica un vincitore chiaro: senza
    quello, fra "Genova Rivarolo" e "Rivarolo Canavese" non c'e' modo di
    scegliere, e sbagliare qui vuol dire spostare una stazione di cento
    chilometri.
    """
    candidati = []
    for nome_osm, punti in per_nome.items():
        if _combacia_per_token(chiave, nome_osm):
            candidati.extend(punti)
    if not candidati:
        return None

    rif = _baricentro_vicini(chiave, risolti)
    if rif is None:
        # Un solo candidato senza contesto e' comunque un'ipotesi non
        # verificabile: si accetta solo dove la rete la conferma.
        return None

    scelto = min(candidati, key=lambda c: distanza_km(rif[0], rif[1], c[0], c[1]))
    # Il nome esteso puo' essere quello sbagliato: "FIERA" trova solo "Fiera di
    # Roma", ma i treni di quella stazione vanno tutti a Palermo. Se l'unico
    # candidato e' lontano dalla rete a cui la stazione appartiene, e' un
    # omonimo, non la stazione che cerchiamo.
    if distanza_km(rif[0], rif[1], scelto[0], scelto[1]) > _MAX_SCARTO_RETE_KM:
        return None
    return scelto


# Quanto puo' distare una stazione dal baricentro delle stazioni con cui e'
# collegata prima di considerarla un omonimo. Generoso, perche' i nodi grandi
# hanno collegamenti sparsi su tutto il paese.
_MAX_SCARTO_RETE_KM = 100.0


def risolvi_per_somiglianza(chiave: str, per_nome: Dict[str, list],
                            risolti: Dict[str, Tuple[float, float]]
                            ) -> Optional[Tuple[float, float]]:
    """Ultimo tentativo, per le differenze di grafia.

    La sorgente scrive "CIRE'" dove la stazione si chiama "Ciriè": l'apostrofo
    al posto dell'accento e una lettera persa. Nessun confronto esatto o per
    abbreviazione puo' arrivarci. Qui si accettano nomi molto simili, ma solo
    se la rete conferma: senza quel vincolo la somiglianza fra nomi di paese
    italiani produce accostamenti sbagliati a ripetizione.
    """
    import difflib

    rif = _baricentro_vicini(chiave, risolti)
    if rif is None:
        return None

    # Il confronto ignora gli accenti. "CIRE'" e "CIRIÈ" si somigliano al 67%,
    # sotto qualunque soglia sensata, ma solo perche' la È conta come lettera
    # diversa dalla E: senza accenti salgono all'89% e diventano lo stesso
    # posto, che e' quello che sono.
    senza = {_senza_accenti(n): n for n in per_nome}
    simili = difflib.get_close_matches(_senza_accenti(chiave), list(senza), n=5, cutoff=0.85)
    migliore = None
    for s in simili:
        nome = senza[s]
        for punto in per_nome[nome]:
            d = distanza_km(rif[0], rif[1], punto[0], punto[1])
            if d <= _MAX_SCARTO_RETE_KM and (migliore is None or d < migliore[0]):
                migliore = (d, punto)
    return migliore[1] if migliore else None


def coordinate_per_nomi(nomi: list, percorso: str = CACHE_OSM
                        ) -> Dict[str, Tuple[float, float]]:
    """Risolve una lista di nomi stazione della sorgente in coordinate.

    Quattro passaggi, dal piu' sicuro al piu' incerto: nome identico, nome
    identico a una delle forme bilingui, nome che e' l'abbreviazione di un nome
    esteso, nome molto simile a meno di differenze di grafia. Ognuno si applica
    solo a chi non ha trovato niente nei precedenti, e gli ultimi due chiedono
    conferma alla rete prima di accettare.
    """
    per_nome = candidati_osm(percorso)
    risolti = indice_osm(percorso)
    out: Dict[str, Tuple[float, float]] = {}
    mancanti = []
    for n in nomi:
        k = normalize_station_name(str(n))
        if not k:
            continue
        if k in risolti:
            out[k] = risolti[k]
        else:
            mancanti.append(k)

    residui = []
    for k in mancanti:
        c = risolvi_per_abbreviazione(k, per_nome, risolti)
        if c:
            out[k] = c
        else:
            residui.append(k)

    for k in residui:
        c = risolvi_per_somiglianza(k, per_nome, risolti)
        if c:
            out[k] = c

    out, cambi = ricolloca_per_rete(out, per_nome)
    for c in cambi:
        print(f"  ricollocata {c['nome']} -> {c['verso']}: il vicino piu' "
              f"prossimo era a {c['km_prima']:.0f} km, ora il piu' lontano dei "
              f"{c['vicini']} e' a {c['km_dopo']:.0f} km "
              f"({c['spostamento_km']:.0f} km di spostamento)")
    return out


# Sotto questa distanza dal vicino piu' prossimo la stazione e' plausibilmente al
# suo posto e non si tocca: e' la mediana piu' un margine largo (mediana 11 km,
# novantesimo percentile 29 km sulle 1.466 stazioni valutabili).
_PLAUSIBILE_ENTRO_KM = 30.0
# Quanto lontano puo' stare l'alternativa dal vicino piu' lontano: deve essere
# una stazione dentro il grappolo, non un'altra ipotesi lontana.
_ALTERNATIVA_ENTRO_KM = 35.0
_MIN_VICINI = 2


def _e_troncamento(chiave: str, altro: str) -> bool:
    """Vero se `chiave` e' `altro` a cui la sorgente ha tolto dei pezzi.

    La sorgente scrive "PALAZZOLO" per Palazzolo Milanese e "S LAZZARO" per
    Reggio San Lazzaro: il nome c'e' tutto, gli manca la citta'. I token di
    `chiave` devono comparire in `altro` nello stesso ordine.

    Serve una lunghezza minima perche' con nomi cortissimi il contenimento non
    dice niente: "S" sta in qualunque nome che contenga un santo.
    """
    tk, ta = chiave.split(), altro.split()
    if not tk or len(chiave.replace(" ", "")) < 5 or len(ta) <= len(tk):
        return False
    i = 0
    for t in tk:
        while i < len(ta) and ta[i] != t:
            i += 1
        if i >= len(ta):
            return False
        i += 1
    return True


def ricolloca_per_rete(risolti: Dict[str, Tuple[float, float]],
                       per_nome: Dict[str, list],
                       vicini: Optional[Dict[str, list]] = None
                       ) -> Tuple[Dict[str, Tuple[float, float]], list]:
    """Ultimo controllo: il nome ha vinto, ma la rete dice un'altra cosa.

    I quattro passaggi precedenti scelgono in ordine di sicurezza del *nome*, e
    il nome identico vince su tutto. E' giusto quasi sempre, ma quando la
    sorgente tronca produce l'errore peggiore: esiste un nodo OSM che si chiama
    esattamente "Palazzolo", in provincia di Napoli, e vince sul nodo
    "Palazzolo Milanese" che e' la stazione vera. Il confronto esatto e'
    soddisfatto e nessuno dei controlli successivi viene interrogato.

    Qui il criterio si ribalta: conta la rete, non il nome. Ma non basta dire
    "e' lontana dai suoi vicini", perche' lontano non vuol dire sbagliato:
    Domodossola dista 43 km dalla stazione piu' prossima con cui condivide
    treni, ed e' al suo posto. Con una soglia sulla distanza veniva spostata a
    Milano, dove esiste una stazione Ferrovie Nord che si chiama Domodossola e
    dove va la maggior parte dei suoi treni. Centoventimila corse riposizionate
    per un criterio che sembrava ragionevole.

    Il criterio che tiene e' la dominanza: si sostituisce solo se l'alternativa
    e' piu' vicina a *ogni* vicino di quanto lo sia il vicino piu' prossimo
    dell'attuale. Non "in media meglio": meglio in ogni singolo confronto.
    Palazzolo (Napoli) sta a 653 km dalla piu' vicina fra Milano Rogoredo e
    Milano Bovisa, mentre Palazzolo Milanese sta entro 20 km da entrambe. Per
    Domodossola questo non vale, perche' la Domodossola milanese e' lontanissima
    da Verbania e da Brig, e quindi non viene toccata.
    """
    if vicini is None:
        vicini = _vicini_di_rete()
    if not vicini:
        return risolti, []

    # I vicini si leggono da una fotografia iniziale: altrimenti l'esito
    # dipenderebbe dall'ordine con cui si scorrono i nomi, e una stazione
    # ricollocata cambierebbe il giudizio su quelle esaminate dopo.
    base = dict(risolti)
    cambi = []
    for k, punto in base.items():
        punti_vicini = [base[v] for v in (vicini.get(k) or [])
                        if v in base and v != k]
        if len(punti_vicini) < _MIN_VICINI:
            continue
        d_ora = min(distanza_km(punto[0], punto[1], p[0], p[1]) for p in punti_vicini)
        if d_ora <= _PLAUSIBILE_ENTRO_KM:
            continue

        candidati = []
        for nome_osm, pts in per_nome.items():
            if nome_osm == k:
                continue
            if _combacia_per_token(k, nome_osm) or _e_troncamento(k, nome_osm):
                candidati.extend((p, nome_osm) for p in pts)
        if not candidati:
            continue

        # Il candidato si giudica sul vicino piu' *lontano*: deve stare dentro il
        # grappolo di stazioni con cui la nostra condivide treni, non solo
        # accanto a una di loro.
        def peggior_vicino(c) -> float:
            return max(distanza_km(c[0][0], c[0][1], p[0], p[1]) for p in punti_vicini)

        migliore, nome_scelto = min(candidati, key=peggior_vicino)
        d_peggiore = peggior_vicino((migliore, nome_scelto))
        if d_peggiore > _ALTERNATIVA_ENTRO_KM or d_peggiore >= d_ora:
            continue

        cambi.append({"nome": k, "verso": nome_scelto,
                      "km_prima": d_ora, "km_dopo": d_peggiore,
                      "spostamento_km": distanza_km(punto[0], punto[1],
                                                    migliore[0], migliore[1]),
                      "vicini": len(punti_vicini)})
        risolti[k] = migliore

    return risolti, cambi


def ricolloca_anagrafica(dim: "pd.DataFrame") -> Tuple["pd.DataFrame", list]:
    """Applica il controllo di dominanza all'anagrafica completa del gold.

    Serve un secondo punto di applicazione perche' `coordinate_per_nomi` vede
    solo i nomi che gli vengono passati, e le coordinate dell'anagrafica non
    arrivano tutte da li': molte vengono dalla cache per codice o dal confronto
    di nome fra codici diversi. "PALAZZOLO" e' esattamente uno di questi casi,
    e restava in provincia di Napoli con 3.404 corse verso Milano.

    Lavora sui nomi normalizzati: tutti i codici che condividono un nome si
    spostano insieme, come e' giusto dato che sono la stessa stazione.
    """
    if dim.empty or "lat" not in dim.columns:
        return dim, []

    chiavi = dim["nome_stazione"].astype(str).map(normalize_station_name)
    coord: Dict[str, Tuple[float, float]] = {}
    for k, la, lo in zip(chiavi, dim["lat"], dim["lon"]):
        if k and k not in coord and pd.notna(la) and pd.notna(lo):
            coord[k] = (float(la), float(lo))

    nomi = {str(c): k for c, k in zip(dim["cod_stazione"].astype(str), chiavi)}
    vicini = _vicini_di_rete(nomi)
    if not vicini:
        return dim, []

    _, cambi = ricolloca_per_rete(coord, candidati_osm(), vicini)
    if not cambi:
        return dim, []

    dim = dim.copy()
    for c in cambi:
        nuovo = coord[c["nome"]]
        maschera = chiavi == c["nome"]
        dim.loc[maschera, "lat"] = nuovo[0]
        dim.loc[maschera, "lon"] = nuovo[1]
        c["codici"] = int(maschera.sum())
    return dim, cambi


_VICINI_CACHE: Optional[Dict[str, list]] = None


def _vicini_di_rete(nomi: Optional[Dict[str, str]] = None) -> Dict[str, list]:
    """Per ogni stazione, i nomi normalizzati delle stazioni a cui e' collegata.

    `nomi` permette di passare la corrispondenza codice -> nome da fuori: serve
    quando l'anagrafica del gold non e' ancora stata scritta su disco, cioe'
    proprio mentre la si sta costruendo.
    """
    global _VICINI_CACHE
    da_disco = nomi is None
    if da_disco and _VICINI_CACHE is not None:
        return _VICINI_CACHE

    import glob
    files = sorted(glob.glob(os.path.join("data", "gold", "parts",
                                          "od_mese_categoria", "*.parquet")))
    if not files:
        if da_disco:
            _VICINI_CACHE = {}
        return {}

    if nomi is None:
        nomi = _nomi_stazione()
    coppie: Dict[str, set] = {}
    for f in files:
        try:
            d = pd.read_parquet(f, columns=["cod_partenza", "cod_arrivo"])
        except Exception:
            continue
        for a, b in zip(d["cod_partenza"].astype(str), d["cod_arrivo"].astype(str)):
            na, nb = nomi.get(a), nomi.get(b)
            if not na or not nb or na == nb:
                continue
            coppie.setdefault(na, set()).add(nb)
            coppie.setdefault(nb, set()).add(na)
    fuori = {k: sorted(v) for k, v in coppie.items()}
    if da_disco:
        _VICINI_CACHE = fuori
    return fuori


def _nomi_stazione() -> Dict[str, str]:
    """Codice stazione -> nome normalizzato, dall'anagrafica del gold."""
    if not os.path.exists(DIM):
        return {}
    d = pd.read_csv(DIM, usecols=["cod_stazione", "nome_stazione"])
    return {str(c): normalize_station_name(str(n))
            for c, n in zip(d["cod_stazione"], d["nome_stazione"])}


def _baricentro_vicini(chiave: str, risolti: Dict[str, Tuple[float, float]]
                       ) -> Optional[Tuple[float, float]]:
    vicini = _vicini_di_rete().get(chiave) or []
    punti = [risolti[v] for v in vicini if v in risolti]
    if not punti:
        return None
    return (sum(p[0] for p in punti) / len(punti),
            sum(p[1] for p in punti) / len(punti))


def distanza_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def _volumi() -> Dict[str, int]:
    """Corse per stazione, per ordinare le correzioni per rilevanza."""
    import glob
    files = sorted(glob.glob(os.path.join("data", "gold", "parts",
                                          "stazioni_mese_categoria_nodo", "*.parquet")))
    if not files:
        return {}
    df = pd.concat([pd.read_parquet(f, columns=["cod_stazione", "corse_osservate"])
                    for f in files], ignore_index=True)
    return df.groupby("cod_stazione")["corse_osservate"].sum().to_dict()


def audit_coerenza_rete(soglia_km: float = 100.0) -> pd.DataFrame:
    """Stazioni lontane da ogni stazione con cui sono collegate.

    Il confronto con OpenStreetMap trova solo gli errori dove OSM ha un nodo
    con quel nome. Non trova quelli in cui il nome non combacia affatto, ed e'
    esattamente il caso in cui Bolzano e' rimasta in Sicilia per un giro
    intero: nessun nodo OSM si chiama "Bolzano", si chiama "Bolzano - Bozen".

    Questo controllo non guarda i nomi, guarda i treni. Il criterio non e' la
    distanza dal baricentro dei vicini, che boccia tutti i nodi importanti:
    Milano Centrale e' collegata a mezza Italia, quindi il baricentro dei suoi
    trecento vicini cade in Toscana e la stazione risulta "fuori posto" pur
    essendo al suo posto. Il criterio e' la distanza dal vicino **piu'
    prossimo**: una stazione reale ha sempre almeno un collegamento vicino, la
    prossima fermata sulla linea. Una stazione messa nel posto sbagliato e'
    lontana da tutti i suoi vicini insieme.
    """
    if not os.path.exists(DIM):
        return pd.DataFrame()

    dim = pd.read_csv(DIM)
    dim["chiave"] = dim["nome_stazione"].astype(str).map(normalize_station_name)
    coord = {r["chiave"]: (r["lat"], r["lon"])
             for _, r in dim.iterrows()
             if pd.notna(r["lat"]) and pd.notna(r["lon"])}

    vicini = _vicini_di_rete()
    volumi = _volumi()
    per_codice = dict(zip(dim["cod_stazione"].astype(str), dim["chiave"]))

    righe = []
    for _, r in dim.iterrows():
        k = r["chiave"]
        if k not in coord:
            continue
        punti = [coord[v] for v in (vicini.get(k) or []) if v in coord and v != k]
        if len(punti) < 2:
            # Con un solo collegamento non c'e' contesto per giudicare. Con due
            # ce n'e' abbastanza: Palazzolo aveva esattamente due vicini,
            # entrambi a Milano, ed era in provincia di Napoli. Chiedere tre
            # vicini la lasciava fuori dal controllo.
            continue
        d = min(distanza_km(r["lat"], r["lon"], p[0], p[1]) for p in punti)
        if d > soglia_km:
            corse = sum(v for c, v in volumi.items() if per_codice.get(str(c)) == k)
            righe.append({"nome": r["nome_stazione"], "cod": r["cod_stazione"],
                          "lat": round(r["lat"], 4), "lon": round(r["lon"], 4),
                          "km_dal_piu_vicino": round(d), "vicini": len(punti),
                          "corse": int(corse)})

    if not righe:
        return pd.DataFrame()
    return (pd.DataFrame(righe)
            .drop_duplicates(subset=["nome"])
            .sort_values("corse", ascending=False))


def main(dry_run: bool, scarica: bool, soglia: float) -> None:
    if scarica or not os.path.exists(CACHE_OSM):
        n = scarica_osm()
        print(f"stazioni scaricate da OpenStreetMap: {n}")

    if not os.path.exists(ANAGRAFICA):
        raise SystemExit(f"manca {ANAGRAFICA}")
    ana = pd.read_csv(ANAGRAFICA)

    # Si risolvono i nomi che stanno davvero nell'anagrafica, non tutto OSM:
    # cosi' oltre al confronto esatto entra in gioco quello per abbreviazione,
    # che senza sapere cosa cercare non avrebbe modo di scattare. Serve per le
    # stazioni che la cache aveva gia' riempito con un valore sbagliato, dove
    # il vecchio percorso non arrivava mai a interrogare OSM: "RIVAROLO" aveva
    # la coordinata di Genova Rivarolo invece di Rivarolo Canavese, e nessuno
    # se ne accorgeva perche' una coordinata c'era.
    idx = coordinate_per_nomi(list(ana.get("nome_stazione", pd.Series(dtype=str)).astype(str)))
    print(f"nomi risolti su OpenStreetMap: {len(idx)}")
    volumi = _volumi()

    corrette = aggiunte = invariate = 0
    senza_corrispondenza = 0
    dettaglio = []

    for i, r in ana.iterrows():
        chiave = normalize_station_name(str(r.get("nome_stazione", "")))
        o = idx.get(chiave)
        if not o:
            senza_corrispondenza += 1
            continue
        lat, lon = r.get("lat"), r.get("lon")
        corse = int(volumi.get(str(r.get("cod_stazione", "")), 0))
        if pd.isna(lat) or pd.isna(lon):
            aggiunte += 1
            dettaglio.append({"nome": r["nome_stazione"], "azione": "aggiunta",
                              "scarto_km": None, "corse": corse})
        else:
            d = distanza_km(float(lat), float(lon), o[0], o[1])
            if d <= soglia:
                invariate += 1
                continue
            corrette += 1
            dettaglio.append({"nome": r["nome_stazione"], "azione": "corretta",
                              "scarto_km": round(d), "corse": corse})
        if not dry_run:
            ana.at[i, "lat"], ana.at[i, "lon"] = o[0], o[1]

    print(f"\ncoordinate corrette (scarto > {soglia:g} km): {corrette}")
    print(f"coordinate aggiunte (prima mancanti):     {aggiunte}")
    print(f"gia' corrette, lasciate come sono:        {invariate}")
    print(f"senza corrispondenza in OSM:              {senza_corrispondenza}")

    if dettaglio:
        d = pd.DataFrame(dettaglio).sort_values("corse", ascending=False)
        sb = d[d["azione"] == "corretta"]
        if len(sb):
            print(f"\nle 12 correzioni piu' rilevanti per traffico:")
            print(sb.head(12).to_string(index=False))
            print(f"\ncorse servite da stazioni riposizionate: {int(sb['corse'].sum()):,}")

    if dry_run:
        print("\ndry-run: nessun file scritto")
        return

    ana.to_csv(ANAGRAFICA, index=False)
    print(f"\nscritto {ANAGRAFICA}")
    print("ricostruire poi l'anagrafica e il sito:")
    print("  python -m scripts.build_station_dim && python -m scripts.build_site")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--scarica", action="store_true", help="riscarica la cache OSM")
    ap.add_argument("--soglia-km", type=float, default=SOGLIA_KM)
    args = ap.parse_args()
    main(args.dry_run, args.scarica, args.soglia_km)
