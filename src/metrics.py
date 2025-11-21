import pandas as pd


def build_daily_metrics(
    df: pd.DataFrame,
    fecha_col: str,
    valor_col: str,
    fuente: str,
) -> pd.DataFrame:
    """
    Construye métricas diarias:
      - n_contratos: número de contratos por día
      - suma_millones: suma diaria de valor del contrato (en millones)
      - promedio_millones: valor promedio del contrato (en millones)
    Añade una columna 'fuente' (SECOP 1 o SECOP 2).
    """
    if df.empty:
        return pd.DataFrame(
            columns=["fecha", "n_contratos", "suma_millones", "promedio_millones", "fuente"]
        )

    df_work = df.copy()

    # Por seguridad convertimos a tipo adecuado
    df_work[valor_col] = pd.to_numeric(df_work[valor_col], errors="coerce")
    df_work = df_work.dropna(subset=[fecha_col, valor_col])

    group = (
        df_work
        .groupby(fecha_col, as_index=False)
        .agg(
            n_contratos=(valor_col, "count"),
            suma_millones=(valor_col, "sum"),
        )
    )

    group["promedio_millones"] = group["suma_millones"] / group["n_contratos"]
    group = group.rename(columns={fecha_col: "fecha"})
    group["fuente"] = fuente

    group = group.sort_values("fecha")

    return group
