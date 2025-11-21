# etl/ingest_secop_daily.py

import datetime
from pathlib import Path

import pandas as pd


# Carpeta de datos (relativa a la raíz del repo)
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)


def fetch_secop1_since(start_date: datetime.date) -> pd.DataFrame:
    """
    Descarga contratos de SECOP 1 desde start_date hasta hoy.
    Endpoint: f789-7hwg
    """
    start_str = start_date.strftime("%Y-%m-%d")
    url = (
        "https://www.datos.gov.co/resource/f789-7hwg.json"
        f"?$limit=300000&$where=fecha_de_cargue_en_el_secop>='{start_str}'"
    )

    df = pd.read_json(url)

    if df.shape[0] == 0:
        return df

    cols = [
        "nombre_entidad",
        "detalle_del_objeto_a_contratar",
        "estado_del_proceso",
        "cuantia_contrato",
        "plazo_de_ejec_del_contrato",
        "rango_de_ejec_del_contrato",
        "fecha_de_cargue_en_el_secop",
        "ruta_proceso_en_secop_i",
        # idealmente aquí deberías incluir un ID único de proceso si existe
        # "codigo_proceso", etc.
    ]
    df = df[[c for c in cols if c in df.columns]].copy()

    # Valor en millones
    df["cuantia_contrato"] = pd.to_numeric(df["cuantia_contrato"], errors="coerce") / 1e6
    df["fecha_de_cargue_en_el_secop"] = pd.to_datetime(
        df["fecha_de_cargue_en_el_secop"], errors="coerce"
    )

    # Limpiar URL si viene como dict
    if "ruta_proceso_en_secop_i" in df.columns:
        def extract_url_secop1(x):
            if isinstance(x, dict) and "url" in x:
                return x["url"]
            return x

        df["ruta_proceso_en_secop_i"] = df["ruta_proceso_en_secop_i"].map(extract_url_secop1)

    df = df.dropna(subset=["fecha_de_cargue_en_el_secop", "cuantia_contrato"])

    return df


def fetch_secop2_since(start_date: datetime.date) -> pd.DataFrame:
    """
    Descarga contratos de SECOP 2 desde start_date hasta hoy.
    Endpoint: jbjy-vk9h
    """
    start_str = start_date.strftime("%Y-%m-%d")
    url = (
        "https://www.datos.gov.co/resource/jbjy-vk9h.json"
        f"?$limit=300000&$where=fecha_de_firma>='{start_str}'"
    )

    df = pd.read_json(url)

    if df.shape[0] == 0:
        return df

    cols = [
        "nombre_entidad",
        "descripcion_del_proceso",
        "tipo_de_contrato",
        "valor_del_contrato",
        "duraci_n_del_contrato",
        "fecha_de_firma",
        "urlproceso",
        # idem, aquí idealmente un ID único
    ]
    df = df[[c for c in cols if c in df.columns]].copy()

    df["valor_del_contrato"] = pd.to_numeric(df["valor_del_contrato"], errors="coerce") / 1e6
    df["fecha_de_firma"] = pd.to_datetime(df["fecha_de_firma"], errors="coerce")

    if "urlproceso" in df.columns:
        def extract_url_secop2(x):
            if isinstance(x, dict) and "url" in x:
                return x["url"]
            return x

        df["urlproceso"] = df["urlproceso"].map(extract_url_secop2)

    df = df.dropna(subset=["fecha_de_firma", "valor_del_contrato"])

    return df


def get_last_date_from_parquet(path: Path, fecha_col: str, default_days_back: int = 365):
    """
    Si el parquet existe, devuelve la última fecha registrada.
    Si no existe, devuelve hoy - default_days_back (para inicializar histórico).
    """
    if not path.exists():
        return datetime.date.today() - datetime.timedelta(days=default_days_back)

    df = pd.read_parquet(path, columns=[fecha_col])
    if df.empty:
        return datetime.date.today() - datetime.timedelta(days=default_days_back)

    max_date = df[fecha_col].max()
    return max_date.date() if hasattr(max_date, "date") else max_date


def append_to_parquet(df_new: pd.DataFrame, path: Path, subset_cols: list | None = None):
    """
    Añade df_new al parquet, evitando duplicados.
    subset_cols: columnas que definen un registro único (si las tienes).
    """
    if df_new.empty:
        return

    if path.exists():
        df_old = pd.read_parquet(path)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
        if subset_cols is not None:
            df_all = df_all.drop_duplicates(subset=subset_cols)
        else:
            df_all = df_all.drop_duplicates()
    else:
        df_all = df_new.copy()

    df_all.to_parquet(path, index=False)


def main():
    today = datetime.date.today()

    # ---------- SECOP 1 ----------
    secop1_path = DATA_DIR / "secop1.parquet"
    last_date_1 = get_last_date_from_parquet(secop1_path, "fecha_de_cargue_en_el_secop")

    start_1 = last_date_1 + datetime.timedelta(days=1)
    if start_1 <= today:
        print(f"[SECOP 1] Descargando desde {start_1} hasta hoy...")
        df1_new = fetch_secop1_since(start_1)
        print(f"[SECOP 1] Nuevos registros: {df1_new.shape[0]}")
        append_to_parquet(df1_new, secop1_path)
    else:
        print("[SECOP 1] No hay días nuevos que consultar.")

    # ---------- SECOP 2 ----------
    secop2_path = DATA_DIR / "secop2.parquet"
    last_date_2 = get_last_date_from_parquet(secop2_path, "fecha_de_firma")

    start_2 = last_date_2 + datetime.timedelta(days=1)
    if start_2 <= today:
        print(f"[SECOP 2] Descargando desde {start_2} hasta hoy...")
        df2_new = fetch_secop2_since(start_2)
        print(f"[SECOP 2] Nuevos registros: {df2_new.shape[0]}")
        append_to_parquet(df2_new, secop2_path)
    else:
        print("[SECOP 2] No hay días nuevos que consultar.")


if __name__ == "__main__":
    main()