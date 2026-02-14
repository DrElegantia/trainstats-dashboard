from __future__ import annotations

import os
import pandas as pd

from scripts.utils import ensure_dir


def bootstrap_demo_gold() -> None:
    ensure_dir(os.path.join("data", "gold"))

    kpi_mese = pd.DataFrame(
        [
            {
                "mese": "2026-02",
                "corse_osservate": 1000,
                "effettuate": 940,
                "cancellate": 20,
                "soppresse": 30,
                "parzialmente_cancellate": 10,
                "info_mancante": 0,
                "in_orario": 650,
                "in_ritardo": 260,
                "in_anticipo": 30,
                "oltre_5": 200,
                "oltre_10": 140,
                "oltre_15": 90,
                "oltre_30": 40,
                "oltre_60": 10,
                "minuti_ritardo_tot": 22000,
                "minuti_anticipo_tot": 600,
                "minuti_netti_tot": 21400,
                "ritardo_medio": 6.5,
                "ritardo_mediano": 2.0,
                "p90": 18.0,
                "p95": 32.0,
            }
        ]
    )
    kpi_mese.to_csv(os.path.join("data", "gold", "kpi_mese.csv"), index=False)

    kpi_mese_categoria = pd.DataFrame(
        [
            dict(kpi_mese.iloc[0], **{"categoria": "FR"}),
            dict(kpi_mese.iloc[0], **{"categoria": "IC"}),
        ]
    )
    kpi_mese_categoria.loc[0, "corse_osservate"] = 400
    kpi_mese_categoria.loc[1, "corse_osservate"] = 600
    kpi_mese_categoria.to_csv(os.path.join("data", "gold", "kpi_mese_categoria.csv"), index=False)

    kpi_giorno = pd.DataFrame(
        [
            {
                "giorno": "2026-02-13",
                "corse_osservate": 1000,
                "effettuate": 940,
                "cancellate": 20,
                "soppresse": 30,
                "parzialmente_cancellate": 10,
                "info_mancante": 0,
                "in_orario": 650,
                "in_ritardo": 260,
                "in_anticipo": 30,
                "oltre_5": 200,
                "oltre_10": 140,
                "oltre_15": 90,
                "oltre_30": 40,
                "oltre_60": 10,
                "minuti_ritardo_tot": 22000,
                "minuti_anticipo_tot": 600,
                "minuti_netti_tot": 21400,
                "ritardo_medio": 6.5,
                "ritardo_mediano": 2.0,
                "p90": 18.0,
                "p95": 32.0,
            }
        ]
    )
    kpi_giorno.to_csv(os.path.join("data", "gold", "kpi_giorno.csv"), index=False)

    kpi_giorno_categoria = pd.DataFrame(
        [
            dict(kpi_giorno.iloc[0], **{"categoria": "FR"}),
            dict(kpi_giorno.iloc[0], **{"categoria": "IC"}),
        ]
    )
    kpi_giorno_categoria.loc[0, "corse_osservate"] = 400
    kpi_giorno_categoria.loc[1, "corse_osservate"] = 600
    kpi_giorno_categoria.to_csv(os.path.join("data", "gold", "kpi_giorno_categoria.csv"), index=False)

    hist = pd.DataFrame(
        [
            {"mese": "2026-02", "categoria": "FR", "bucket_ritardo_arrivo": "(5,10]", "count": 40, "minuti_ritardo": 320, "minuti_anticipo": 0},
            {"mese": "2026-02", "categoria": "FR", "bucket_ritardo_arrivo": "(10,15]", "count": 25, "minuti_ritardo": 310, "minuti_anticipo": 0},
            {"mese": "2026-02", "categoria": "IC", "bucket_ritardo_arrivo": "(5,10]", "count": 60, "minuti_ritardo": 480, "minuti_anticipo": 0},
            {"mese": "2026-02", "categoria": "IC", "bucket_ritardo_arrivo": "(10,15]", "count": 45, "minuti_ritardo": 560, "minuti_anticipo": 0},
        ]
    )
    hist.to_csv(os.path.join("data", "gold", "hist_mese_categoria.csv"), index=False)

    st = pd.DataFrame(
        [
            {"mese": "2026-02", "categoria": "FR", "cod_stazione": "S01700", "ruolo": "nodo", "nome_stazione": "MILANO CENTRALE", "corse_osservate": 180, "effettuate": 170, "cancellate": 2, "soppresse": 5, "parzialmente_cancellate": 3, "info_mancante": 0, "in_orario": 120, "in_ritardo": 45, "in_anticipo": 5, "oltre_5": 35, "oltre_10": 20, "oltre_15": 10, "oltre_30": 5, "oltre_60": 1, "minuti_ritardo_tot": 3200, "minuti_anticipo_tot": 90, "minuti_netti_tot": 3110},
            {"mese": "2026-02", "categoria": "IC", "cod_stazione": "S08409", "ruolo": "nodo", "nome_stazione": "ROMA TERMINI", "corse_osservate": 220, "effettuate": 200, "cancellate": 6, "soppresse": 10, "parzialmente_cancellate": 4, "info_mancante": 0, "in_orario": 120, "in_ritardo": 70, "in_anticipo": 10, "oltre_5": 55, "oltre_10": 35, "oltre_15": 18, "oltre_30": 8, "oltre_60": 2, "minuti_ritardo_tot": 5100, "minuti_anticipo_tot": 150, "minuti_netti_tot": 4950},
        ]
    )
    st.to_csv(os.path.join("data", "gold", "stazioni_mese_categoria_nodo.csv"), index=False)

    st2 = st.copy()
    st2["ruolo"] = "arrivo"
    st2.to_csv(os.path.join("data", "gold", "stazioni_mese_categoria_ruolo.csv"), index=False)

    od = pd.DataFrame(
        [
            {"mese": "2026-02", "categoria": "FR", "cod_partenza": "S11119", "cod_arrivo": "S01700", "corse_osservate": 40, "effettuate": 38, "cancellate": 0, "soppresse": 1, "parzialmente_cancellate": 1, "info_mancante": 0, "in_orario": 20, "in_ritardo": 16, "in_anticipo": 2, "oltre_5": 14, "oltre_10": 9, "oltre_15": 5, "oltre_30": 2, "oltre_60": 0, "minuti_ritardo_tot": 780, "minuti_anticipo_tot": 20, "minuti_netti_tot": 760, "ritardo_medio": 7.0, "ritardo_mediano": 4.0, "p90": 20.0, "p95": 28.0, "nome_partenza": "BARI CENTRALE", "nome_arrivo": "MILANO CENTRALE"},
        ]
    )
    od.to_csv(os.path.join("data", "gold", "od_mese_categoria.csv"), index=False)

