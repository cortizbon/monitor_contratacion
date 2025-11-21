# src/secop_api.py

import datetime
import pandas as pd


def six_months_ago(today: datetime.date | None = None) -> datetime.date:
    """Devuelve la fecha aproximada de hace 6 meses (180 días)."""
    if today is None:
        today = datetime.date.today()
    return today - datetime.timedelta(days=180)


def fetch_secop1(last_n_days: int = 180) -> pd.DataFrame:
    """
    Descarga contratos de SECOP 1 desde hace last_n_days días hasta hoy.
    Endpoint: f789-7hwg
    """
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=last_n_days)
    start_str = start_date.strftime("%Y-%m-%d")

    url = (
        "https://www.datos.gov.co/resource/f789-7hwg.json"
        f"?$limit=300000&$where=fecha_de_cargue_en_el_secop>='{start_str}'"
    )

    df = pd.read_json(url)

    if df.shape[0] == 0:
        return df  # vacío

    # Nos quedamos con campos clave
    cols = [
        "nombre_entidad",
        "detalle_del_objeto_a_contratar",
        "estado_del_proceso",
        "cuantia_contrato",
        "plazo_de_ejec_del_contrato",
        "rango_de_ejec_del_contrato",
        "fecha_de_cargue_en_el_secop",
        "ruta_proceso_en_secop_i",
    ]
    df = df[[c for c in cols if c in df.columns]].copy()

    # Valor numérico en millones
    df["cuantia_contrato"] = pd.to_numeric(df["cuantia_contrato"], errors="coerce") / 1e6

    # Fecha como datetime.date
    df["fecha_de_cargue_en_el_secop"] = pd.to_datetime(
        df["fecha_de_cargue_en_el_secop"], errors="coerce"
    ).dt.date

    # Limpieza de URLs si vienen como dict
    if "ruta_proceso_en_secop_i" in df.columns:
        def extract_url_secop1(x):
            if isinstance(x, dict) and "url" in x:
                return x["url"]
            return x

        df["ruta_proceso_en_secop_i"] = df["ruta_proceso_en_secop_i"].map(extract_url_secop1)

    # Eliminamos filas sin fecha o sin valor de contrato
    df = df.dropna(subset=["fecha_de_cargue_en_el_secop", "cuantia_contrato"])

    return df


def fetch_secop2(last_n_days: int = 180) -> pd.DataFrame:
    """
    Descarga contratos de SECOP 2 desde hace last_n_days días hasta hoy.
    Endpoint: jbjy-vk9h
    """
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=last_n_days)
    start_str = start_date.strftime("%Y-%m-%d")

    url = (
        "https://www.datos.gov.co/resource/jbjy-vk9h.json"
        f"?$limit=300000&$where=fecha_de_firma>='{start_str}'"
    )

    df = pd.read_json(url)

    if df.shape[0] == 0:
        return df  # vacío

    cols = [
        "nombre_entidad",
        "descripcion_del_proceso",
        "tipo_de_contrato",
        "valor_del_contrato",
        "duraci_n_del_contrato",
        "fecha_de_firma",
        "urlproceso",
    ]
    df = df[[c for c in cols if c in df.columns]].copy()

    # Valor numérico en millones
    df["valor_del_contrato"] = pd.to_numeric(df["valor_del_contrato"], errors="coerce") / 1e6

    # Fecha como datetime.date
    df["fecha_de_firma"] = pd.to_datetime(df["fecha_de_firma"], errors="coerce").dt.date

    # Limpieza de URLs si vienen como dict
    if "urlproceso" in df.columns:
        def extract_url_secop2(x):
            if isinstance(x, dict) and "url" in x:
                return x["url"]
            return x

        df["urlproceso"] = df["urlproceso"].map(extract_url_secop2)

    # Eliminamos filas sin fecha o sin valor de contrato
    df = df.dropna(subset=["fecha_de_firma", "valor_del_contrato"])

    return df
