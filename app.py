# app.py

import datetime
from io import BytesIO
from pathlib import Path

import openpyxl  # para ExcelWriter
import pandas as pd
import streamlit as st

from src.metrics import build_daily_metrics
from src.plots import (
    chart_n_contratos,
    chart_suma_millones,
    chart_promedio_millones,
)


DATA_DIR = Path("data")


st.set_page_config(
    page_title="Monitor de contratación",
    page_icon=r"imagenes/logo_sapo.svg",
    layout="wide",
)

st.title("Monitor de contratación")
st.caption("Contratación pública (SECOP 1 y SECOP 2) usando datos preprocesados")


today = datetime.date.today()
st.sidebar.header("Parámetros")

last_n_days = st.sidebar.slider(
    "Ventana de días hacia atrás",
    min_value=30,
    max_value=365,
    value=180,
    step=15,
    help="Número de días hacia atrás para consultar contratos.",
)

mostrar_tablas_detalle = st.sidebar.checkbox(
    "Mostrar tablas de métricas diarias",
    value=False,
)

st.write(
    f"Mostrando contratos de los últimos **{last_n_days}** días "
    f"(desde el { (today - datetime.timedelta(days=last_n_days)).strftime('%Y-%m-%d') } hasta hoy), "
    "a partir de los archivos Parquet generados diariamente."
)

# CARGAR PARQUET 
secop1_path = DATA_DIR / "secop1.parquet"
secop2_path = DATA_DIR / "secop2.parquet"

if not secop1_path.exists() and not secop2_path.exists():
    st.error("No se encontraron archivos Parquet (secop1.parquet / secop2.parquet). "
             "Ejecuta primero el ETL diario.")
    st.stop()

df_secop1 = pd.read_parquet(secop1_path) if secop1_path.exists() else pd.DataFrame()
df_secop2 = pd.read_parquet(secop2_path) if secop2_path.exists() else pd.DataFrame()

# Filtrar ventana 
start_date = today - datetime.timedelta(days=last_n_days)

if not df_secop1.empty:
    df_secop1["fecha_de_cargue_en_el_secop"] = pd.to_datetime(
        df_secop1["fecha_de_cargue_en_el_secop"]
    )
    df_secop1 = df_secop1[
        df_secop1["fecha_de_cargue_en_el_secop"].dt.date >= start_date
    ]

if not df_secop2.empty:
    df_secop2["fecha_de_firma"] = pd.to_datetime(df_secop2["fecha_de_firma"])
    df_secop2 = df_secop2[
        df_secop2["fecha_de_firma"].dt.date >= start_date
    ]

if df_secop1.empty and df_secop2.empty:
    st.warning("No hay datos en la ventana de días seleccionada.")
    st.stop()

# MÉTRICAS DIARIAS
df_daily_secop1 = build_daily_metrics(
    df=df_secop1,
    fecha_col="fecha_de_cargue_en_el_secop",
    valor_col="cuantia_contrato",
    fuente="SECOP 1",
)

df_daily_secop2 = build_daily_metrics(
    df=df_secop2,
    fecha_col="fecha_de_firma",
    valor_col="valor_del_contrato",
    fuente="SECOP 2",
)

col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

total_n1 = int(df_daily_secop1["n_contratos"].sum()) if not df_daily_secop1.empty else 0
total_n2 = int(df_daily_secop2["n_contratos"].sum()) if not df_daily_secop2.empty else 0

total_m1 = float(df_daily_secop1["suma_millones"].sum()) if not df_daily_secop1.empty else 0.0
total_m2 = float(df_daily_secop2["suma_millones"].sum()) if not df_daily_secop2.empty else 0.0

with col_kpi1:
    st.metric("Contratos (SECOP 1)", f"{total_n1:,}")
with col_kpi2:
    st.metric("Contratos (SECOP 2)", f"{total_n2:,}")
with col_kpi3:
    st.metric("Total (SECOP 1, millones)", f"{total_m1:,.2f}")
with col_kpi4:
    st.metric("Total (SECOP 2, millones)", f"{total_m2:,.2f}")

# TABS
tab1, tab2 = st.tabs(["SECOP 1", "SECOP 2"])

with tab1:
    st.subheader("SECOP 1 - Contratación diaria")
    if df_daily_secop1.empty:
        st.warning("No hay datos de SECOP 1 para esta ventana.")
    else:
        fig1 = chart_n_contratos(df_daily_secop1, "Número de contratos diarios (SECOP 1)")
        fig2 = chart_suma_millones(df_daily_secop1, "Suma diaria de contratos (SECOP 1, millones)")
        fig3 = chart_promedio_millones(
            df_daily_secop1, "Valor promedio por contrato (SECOP 1, millones)"
        )
        st.plotly_chart(fig1, use_container_width=True)
        st.plotly_chart(fig2, use_container_width=True)
        st.plotly_chart(fig3, use_container_width=True)

        if mostrar_tablas_detalle:
            st.markdown("### Tabla diaria de métricas (SECOP 1)")
            st.dataframe(df_daily_secop1)

        buffer1 = BytesIO()
        with pd.ExcelWriter(buffer1, engine="openpyxl") as writer:
            df_daily_secop1.to_excel(writer, index=False, sheet_name="SECOP1_diario")
        st.download_button(
            label="Descargar métricas SECOP 1 (Excel)",
            data=buffer1.getvalue(),
            file_name="secop1_metricas_diarias.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

with tab2:
    st.subheader("SECOP 2 - Contratación diaria")
    if df_daily_secop2.empty:
        st.warning("No hay datos de SECOP 2 para esta ventana.")
    else:
        fig1 = chart_n_contratos(df_daily_secop2, "Número de contratos diarios (SECOP 2)")
        fig2 = chart_suma_millones(df_daily_secop2, "Suma diaria de contratos (SECOP 2, millones)")
        fig3 = chart_promedio_millones(
            df_daily_secop2, "Valor promedio por contrato (SECOP 2, millones)"
        )
        st.plotly_chart(fig1, use_container_width=True)
        st.plotly_chart(fig2, use_container_width=True)
        st.plotly_chart(fig3, use_container_width=True)

        if mostrar_tablas_detalle:
            st.markdown("### Tabla diaria de métricas (SECOP 2)")
            st.dataframe(df_daily_secop2)

        buffer2 = BytesIO()
        with pd.ExcelWriter(buffer2, engine="openpyxl") as writer:
            df_daily_secop2.to_excel(writer, index=False, sheet_name="SECOP2_diario")
        st.download_button(
            label="Descargar métricas SECOP 2 (Excel)",
            data=buffer2.getvalue(),
            file_name="secop2_metricas_diarias.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
