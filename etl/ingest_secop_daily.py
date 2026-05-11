# etl/ingest_secop_daily.py

import calendar
import datetime
from pathlib import Path

import pandas as pd
import requests


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

BASE_URL = "https://www.datos.gov.co/resource/{dataset_id}.json"

SECOP1_DATASET = "f789-7hwg"
SECOP2_DATASET = "jbjy-vk9h"

PAGE_SIZE = 50_000
MONTHS_BACK = 6


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
    """
    url = BASE_URL.format(dataset_id=dataset_id)

    where_clause = (
        f"{date_column} >= '{start_date.isoformat()}' "
        f"AND {date_column} < '{end_exclusive.isoformat()}'"
    )

    all_rows = []
    offset = 0

    while True:
        params = {
            "$select": ",".join(columns),
            "$where": where_clause,
            "$order": f"{date_column} ASC, {id_column} ASC",
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }

        response = requests.get(url, params=params, timeout=90)
        response.raise_for_status()

        batch = response.json()

        if not batch:
            break

        all_rows.extend(batch)
        print(
            f"[{dataset_id}] Descargados {len(all_rows):,} registros "
            f"(último lote: {len(batch):,})"
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


def write_snapshot(df: pd.DataFrame, path: Path) -> None:
    """
    Elimina el dataset anterior y escribe el nuevo snapshot.
    """
    if path.exists():
        path.unlink()

    df.to_parquet(path, index=False)
    print(f"[OK] Escrito {path} con {df.shape[0]:,} filas y {df.shape[1]:,} columnas.")


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
    df_secop1 = normalize_url_column(df_secop1, "ruta_proceso_en_secop_i")
    write_snapshot(df_secop1, DATA_DIR / "secop1.parquet")

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
    write_snapshot(df_secop2, DATA_DIR / "secop2.parquet")


if __name__ == "__main__":
    main()