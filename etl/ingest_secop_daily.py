# etl/ingest_secop_daily.py

import calendar
import datetime
from pathlib import Path

import pandas as pd
import requests
import time
from requests.exceptions import ConnectionError, HTTPError, Timeout

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

BASE_URL = "https://www.datos.gov.co/resource/{dataset_id}.json"

SECOP1_DATASET = "f789-7hwg"
SECOP2_DATASET = "jbjy-vk9h"

PAGE_SIZE = 10_000
MONTHS_BACK = 6
MAX_RETRIES = 5
REQUEST_TIMEOUT = (20, 180)  # 20 segundos conexión, 180 lectura


SECOP1_COLUMNS = [
    "nombre_entidad",
    "detalle_del_objeto_a_contratar",
    "cuantia_contrato",
    "fecha_de_cargue_en_el_secop",
    "ruta_proceso_en_secop_i",
    "estado_del_proceso",
    "tipo_de_contrato",
    "modalidad_de_contratacion",
    "uid",
    "orden_entidad",
]

SECOP2_COLUMNS = [
    "nombre_entidad",
    "departamento",
    "descripcion_del_proceso",
    "valor_del_contrato",
    "fecha_de_firma",
    "urlproceso",
    "estado_contrato",
    "modalidad_de_contratacion",
    "sector",
    "id_contrato",
    "orden",
    "tipo_de_contrato",
    "duraci_n_del_contrato",
    "proveedor_adjudicado"
]


def subtract_months(date_value: datetime.date, months: int) -> datetime.date:
    """
    Resta meses calendario conservando el día cuando sea posible.
    Ejemplo: 2026-05-31 menos 6 meses -> 2025-11-30.
    """
    month = date_value.month - months
    year = date_value.year + (month - 1) // 12
    month = (month - 1) % 12 + 1

    last_day = calendar.monthrange(year, month)[1]
    day = min(date_value.day, last_day)

    return datetime.date(year, month, day)


def fetch_socrata_snapshot(
    dataset_id: str,
    columns: list[str],
    date_column: str,
    id_column: str,
    start_date: datetime.date,
    end_exclusive: datetime.date,
) -> pd.DataFrame:
    """
    Descarga una ventana completa desde datos.gov.co usando paginación.
    No transforma valores monetarios.

    Versión robusta:
    - lotes más pequeños,
    - reintentos ante timeouts,
    - espera progresiva entre intentos.
    """
    url = BASE_URL.format(dataset_id=dataset_id)

    where_clause = (
        f"{date_column} >= '{start_date.isoformat()}' "
        f"AND {date_column} < '{end_exclusive.isoformat()}'"
    )

    all_rows = []
    offset = 0

    session = requests.Session()

    while True:
        params = {
            "$select": ",".join(columns),
            "$where": where_clause,
            "$order": f"{date_column} ASC, {id_column} ASC",
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }

        batch = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = session.get(
                    url,
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                batch = response.json()
                break

            except (Timeout, ConnectionError, HTTPError) as exc:
                wait_seconds = min(60, 2 ** attempt)

                print(
                    f"[{dataset_id}] Error en offset {offset:,}. "
                    f"Intento {attempt}/{MAX_RETRIES}. "
                    f"Esperando {wait_seconds}s. Error: {exc}"
                )

                if attempt == MAX_RETRIES:
                    raise

                time.sleep(wait_seconds)

        if not batch:
            break

        all_rows.extend(batch)

        print(
            f"[{dataset_id}] Descargados {len(all_rows):,} registros "
            f"(último lote: {len(batch):,}, offset: {offset:,})"
        )

        if len(batch) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    return pd.DataFrame(all_rows, columns=columns)


def normalize_url_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Algunos campos URL pueden venir como diccionario {'url': ...}.
    Esto no modifica valores contractuales; solo evita problemas al guardar parquet.
    """
    if column in df.columns:
        df[column] = df[column].map(
            lambda x: x.get("url") if isinstance(x, dict) and "url" in x else x
        )
    return df


def write_snapshot_temp(df: pd.DataFrame, path: Path) -> Path:
    """
    Escribe un archivo temporal. No toca todavía el parquet definitivo.
    """
    temp_path = path.with_suffix(".tmp.parquet")
    df.to_parquet(temp_path, index=False)

    print(
        f"[OK] Temporal escrito {temp_path} "
        f"con {df.shape[0]:,} filas y {df.shape[1]:,} columnas."
    )

    return temp_path


def replace_snapshot(temp_path: Path, final_path: Path) -> None:
    """
    Reemplaza el parquet definitivo solo cuando la descarga completa fue exitosa.
    """
    if final_path.exists():
        final_path.unlink()

    temp_path.rename(final_path)

    print(f"[OK] Reemplazado {final_path}")

def main() -> None:
    today = datetime.date.today()

    # Ventana móvil de seis meses hasta hoy.
    # end_exclusive incluye todo el día de hoy.
    start_date = subtract_months(today, MONTHS_BACK)
    end_exclusive = today + datetime.timedelta(days=1)

    print(f"Ventana de descarga: {start_date} <= fecha < {end_exclusive}")

    # SECOP I
    print("[SECOP I] Descargando snapshot...")
    df_secop1 = fetch_socrata_snapshot(
        dataset_id=SECOP1_DATASET,
        columns=SECOP1_COLUMNS,
        date_column="fecha_de_cargue_en_el_secop",
        id_column="uid",
        start_date=start_date,
        end_exclusive=end_exclusive,
    )
    secop1_path = DATA_DIR / "secop1.parquet"
    secop2_path = DATA_DIR / "secop2.parquet"

    # SECOP I
    print("[SECOP I] Descargando snapshot...")
    df_secop1 = fetch_socrata_snapshot(
        dataset_id=SECOP1_DATASET,
        columns=SECOP1_COLUMNS,
        date_column="fecha_de_cargue_en_el_secop",
        id_column="uid",
        start_date=start_date,
        end_exclusive=end_exclusive,
    )
    df_secop1 = normalize_url_column(df_secop1, "ruta_proceso_en_secop_i")
    temp_secop1 = write_snapshot_temp(df_secop1, secop1_path)

    # SECOP II
    print("[SECOP II] Descargando snapshot...")
    df_secop2 = fetch_socrata_snapshot(
        dataset_id=SECOP2_DATASET,
        columns=SECOP2_COLUMNS,
        date_column="fecha_de_firma",
        id_column="id_contrato",
        start_date=start_date,
        end_exclusive=end_exclusive,
    )
    df_secop2 = normalize_url_column(df_secop2, "urlproceso")
    temp_secop2 = write_snapshot_temp(df_secop2, secop2_path)

    # Solo si ambas descargas terminaron bien, reemplazamos los archivos definitivos.
    replace_snapshot(temp_secop1, secop1_path)
    replace_snapshot(temp_secop2, secop2_path)


if __name__ == "__main__":
    main()