import datetime
from io import BytesIO
from pathlib import Path

import openpyxl  # para ExcelWriter
import pandas as pd
import streamlit as st
import plotly.express as px

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
    min_value=10,
    max_value=180,
    value=90,
    step=10,
    help="Número de días hacia atrás para consultar contratos.",
)

mostrar_tablas_detalle = st.sidebar.checkbox(
    "Mostrar tablas de métricas diarias",
    value=False,
)

st.write(
    f"Mostrando contratos de los últimos **{last_n_days}** días "
    f"(desde el { (today - datetime.timedelta(days=last_n_days)).strftime('%Y-%m-%d') } hasta hoy)."
)


# CARGAR PARQUET

secop1_path = DATA_DIR / "secop1.parquet"
secop2_path = DATA_DIR / "secop2.parquet"

if not secop1_path.exists() and not secop2_path.exists():
    st.error(
        "No se encontraron archivos Parquet (secop1.parquet / secop2.parquet). "
        "Ejecuta primero el ETL diario."
    )
    st.stop()

df_secop1_raw = pd.read_parquet(secop1_path) if secop1_path.exists() else pd.DataFrame()
df_secop2_raw = pd.read_parquet(secop2_path) if secop2_path.exists() else pd.DataFrame()


# FILTRAR VENTANA DE TIEMPO

start_date = today - datetime.timedelta(days=last_n_days)

if not df_secop1_raw.empty:
    df_secop1_raw["fecha_de_cargue_en_el_secop"] = pd.to_datetime(
        df_secop1_raw["fecha_de_cargue_en_el_secop"]
    )
    df_secop1 = df_secop1_raw[
        df_secop1_raw["fecha_de_cargue_en_el_secop"].dt.date >= start_date
    ].copy()
else:
    df_secop1 = pd.DataFrame()

if not df_secop2_raw.empty:
    df_secop2_raw["fecha_de_firma"] = pd.to_datetime(df_secop2_raw["fecha_de_firma"])
    df_secop2 = df_secop2_raw[
        df_secop2_raw["fecha_de_firma"].dt.date >= start_date
    ].copy()
else:
    df_secop2 = pd.DataFrame()

if df_secop1.empty and df_secop2.empty:
    st.warning("No hay datos en la ventana de días seleccionada.")
    st.stop()


# MÉTRICAS DIARIAS GLOBALES

df_daily_secop1_global = build_daily_metrics(
    df=df_secop1,
    fecha_col="fecha_de_cargue_en_el_secop",
    valor_col="cuantia_contrato",
    fuente="SECOP 1",
    col_id="uid",
)

df_daily_secop2_global = build_daily_metrics(
    df=df_secop2,
    fecha_col="fecha_de_firma",
    valor_col="valor_del_contrato",
    fuente="SECOP 2",
    col_id="id_contrato",
)

col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

total_n1 = int(df_daily_secop1_global["n_contratos"].sum()) if not df_daily_secop1_global.empty else 0
total_n2 = int(df_daily_secop2_global["n_contratos"].sum()) if not df_daily_secop2_global.empty else 0

total_m1 = float(df_daily_secop1_global["suma_millones"].sum()) if not df_daily_secop1_global.empty else 0.0
total_m2 = float(df_daily_secop2_global["suma_millones"].sum()) if not df_daily_secop2_global.empty else 0.0

with col_kpi1:
    st.metric("Contratos (SECOP 1)", f"{total_n1:,}")
with col_kpi2:
    st.metric("Contratos (SECOP 2)", f"{total_n2:,}")
with col_kpi3:
    st.metric("Total (SECOP 1, millones)", f"{total_m1:,.2f}")
with col_kpi4:
    st.metric("Total (SECOP 2, millones)", f"{total_m2:,.2f}")

# -------------------------------------------------
# TABS
# -------------------------------------------------
tab1, tab2 = st.tabs(["SECOP 1", "SECOP 2"])

# TAB SECOP 1 
with tab1:
    st.subheader("SECOP 1 - Contratación diaria")

    if df_secop1.empty:
        st.warning("No hay datos de SECOP 1 para esta ventana.")
    else:
        df1 = df_secop1.copy()

        # Filtros específicos SECOP 1
        st.markdown("### Filtros SECOP 1")

        fcol1, fcol2, fcol3 = st.columns(3)

        with fcol1:
            estados_1 = sorted(df1["estado_del_proceso"].dropna().unique()) \
                if "estado_del_proceso" in df1.columns else []
            opciones_estados_1 = ["Todos"] + estados_1 if estados_1 else ["Todos"]
            estado_sel_1 = st.selectbox(
                "Estado del proceso",
                opciones_estados_1,
                index=0,
            )

        with fcol2:
            tipos_1 = sorted(df1["tipo_de_contrato"].dropna().unique()) \
                if "tipo_de_contrato" in df1.columns else []
            opciones_tipos_1 = ["Todos"] + tipos_1 if tipos_1 else ["Todos"]
            tipo_sel_1 = st.selectbox(
                "Tipo de contrato",
                opciones_tipos_1,
                index=0,
            )

        with fcol3:
            modalidades_1 = sorted(df1["modalidad_de_contratacion"].dropna().unique()) \
                if "modalidad_de_contratacion" in df1.columns else []
            opciones_modalidades_1 = ["Todos"] + modalidades_1 if modalidades_1 else ["Todos"]
            modalidad_sel_1 = st.selectbox(
                "Modalidad de contratación",
                opciones_modalidades_1,
                index=0,
            )

        # Aplicar filtros SOLO a df1
        if estado_sel_1 != "Todos" and "estado_del_proceso" in df1.columns:
            df1 = df1[df1["estado_del_proceso"] == estado_sel_1]

        if tipo_sel_1 != "Todos" and "tipo_de_contrato" in df1.columns:
            df1 = df1[df1["tipo_de_contrato"] == tipo_sel_1]

        if modalidad_sel_1 != "Todos" and "modalidad_de_contratacion" in df1.columns:
            df1 = df1[df1["modalidad_de_contratacion"] == modalidad_sel_1]

        st.write(f"Contratos SECOP 1 en la ventana y filtros: **{df1.shape[0]:,}**")

        if df1.empty:
            st.warning("No hay contratos que cumplan los filtros seleccionados.")
        else:
            # Métricas diarias con filtros
            df_daily_secop1 = build_daily_metrics(
                df=df1,
                fecha_col="fecha_de_cargue_en_el_secop",
                valor_col="cuantia_contrato",
                fuente="SECOP 1",
                col_id="uid",
            )

            # Gráficas de serie de tiempo
            fig1 = chart_n_contratos(df_daily_secop1, "Número de contratos diarios (SECOP 1)")
            fig2 = chart_suma_millones(df_daily_secop1, "Suma diaria de contratos (SECOP 1, millones)")
            fig3 = chart_promedio_millones(
                df_daily_secop1, "Valor promedio por contrato (SECOP 1, millones)"
            )
            st.plotly_chart(fig1, use_container_width=True)
            st.plotly_chart(fig2, use_container_width=True)
            st.plotly_chart(fig3, use_container_width=True)

        # AREAS APILADAS
        st.markdown("### Distribución por estado y modalidad en el tiempo (SECOP 1, % de contratos únicos)")

        df_base1 = df_secop1.copy()  # solo filtrado por tiempo, sin filtros de selectbox

        # Área apilada por estado_del_proceso (%)
        if "estado_del_proceso" in df_base1.columns:
            df_estado_1 = (
                df_base1
                .assign(fecha_dia=df_base1["fecha_de_cargue_en_el_secop"].dt.floor("D"))
                .groupby(["fecha_dia", "estado_del_proceso"], as_index=False)
                .agg(n_contratos=("uid", "nunique"))
            )

            df_estado_1["pct_contratos"] = (
                df_estado_1["n_contratos"]
                / df_estado_1.groupby("fecha_dia")["n_contratos"].transform("sum")
                * 100
            )

            fig_estado_1 = px.area(
                df_estado_1,
                x="fecha_dia",
                y="pct_contratos",
                color="estado_del_proceso",
                title="Porcentaje diario de contratos por estado del proceso",
            )
            fig_estado_1.update_layout(
                xaxis_title="Fecha",
                yaxis_title="% de contratos únicos",
                yaxis=dict(range=[0, 100]),
                height=350,
                margin=dict(l=0, r=0, t=40, b=0),
                legend_title="Estado del proceso",
            )
            st.plotly_chart(fig_estado_1, use_container_width=True)

        # Área apilada por modalidad_de_contratacion (%)
        if "modalidad_de_contratacion" in df_base1.columns:
            df_mod_1 = (
                df_base1
                .assign(fecha_dia=df_base1["fecha_de_cargue_en_el_secop"].dt.floor("D"))
                .groupby(["fecha_dia", "modalidad_de_contratacion"], as_index=False)
                .agg(n_contratos=("uid", "nunique"))
            )

            df_mod_1["pct_contratos"] = (
                df_mod_1["n_contratos"]
                / df_mod_1.groupby("fecha_dia")["n_contratos"].transform("sum")
                * 100
            )

            fig_mod_1 = px.area(
                df_mod_1,
                x="fecha_dia",
                y="pct_contratos",
                color="modalidad_de_contratacion",
                title="Porcentaje diario de contratos por modalidad de contratación",
            )
            fig_mod_1.update_layout(
                xaxis_title="Fecha",
                yaxis_title="% de contratos únicos",
                yaxis=dict(range=[0, 100]),
                height=350,
                margin=dict(l=0, r=0, t=40, b=0),
                legend_title="Modalidad",
            )
            st.plotly_chart(fig_mod_1, use_container_width=True)

        if mostrar_tablas_detalle and not df1.empty:
            st.markdown("### Tabla diaria de métricas (SECOP 1)")
            st.dataframe(df_daily_secop1)

        # Descarga métricas SECOP 1
        buffer1 = BytesIO()
        with pd.ExcelWriter(buffer1, engine="openpyxl") as writer:
            (df_daily_secop1 if not df1.empty else df_daily_secop1_global).to_excel(
                writer, index=False, sheet_name="SECOP1_diario"
            )
        st.download_button(
            label="Descargar métricas SECOP 1 (Excel)",
            data=buffer1.getvalue(),
            file_name="secop1_metricas_diarias.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

# TAB SECOP 2 
with tab2:
    st.subheader("SECOP 2 - Contratación diaria")

    if df_secop2.empty:
        st.warning("No hay datos de SECOP 2 para esta ventana.")
    else:
        df2 = df_secop2.copy()

        # Filtros específicos SECOP 2
        st.markdown("### Filtros SECOP 2")

        fcol1, fcol2, fcol3, fcol4 = st.columns(4)

        with fcol1:
            estados_2 = sorted(df2["estado_contrato"].dropna().unique()) \
                if "estado_contrato" in df2.columns else []
            opciones_estados_2 = ["Todos"] + estados_2 if estados_2 else ["Todos"]
            estado_sel_2 = st.selectbox(
                "Estado del contrato",
                opciones_estados_2,
                index=0,
            )

        with fcol2:
            modalidades_2 = sorted(df2["modalidad_de_contratacion"].dropna().unique()) \
                if "modalidad_de_contratacion" in df2.columns else []
            opciones_modalidades_2 = ["Todos"] + modalidades_2 if modalidades_2 else ["Todos"]
            modalidad_sel_2 = st.selectbox(
                "Modalidad de contratación",
                opciones_modalidades_2,
                index=0,
            )

        with fcol3:
            sectores_2 = sorted(df2["sector"].dropna().unique()) \
                if "sector" in df2.columns else []
            opciones_sectores_2 = ["Todos"] + sectores_2 if sectores_2 else ["Todos"]
            sector_sel_2 = st.selectbox(
                "Sector",
                opciones_sectores_2,
                index=0,
            )

        with fcol4:
            departamentos_2 = sorted(df2["departamento"].dropna().unique()) \
                if "departamento" in df2.columns else []
            opciones_departamentos_2 = ["Todos"] + departamentos_2 if departamentos_2 else ["Todos"]
            departamento_sel_2 = st.selectbox(
                "Departamento",
                opciones_departamentos_2,
                index=0,
            )

        # Aplicar filtros
        if estado_sel_2 != "Todos" and "estado_contrato" in df2.columns:
            df2 = df2[df2["estado_contrato"] == estado_sel_2]

        if modalidad_sel_2 != "Todos" and "modalidad_de_contratacion" in df2.columns:
            df2 = df2[df2["modalidad_de_contratacion"] == modalidad_sel_2]

        if sector_sel_2 != "Todos" and "sector" in df2.columns:
            df2 = df2[df2["sector"] == sector_sel_2]

        if departamento_sel_2 != "Todos" and "departamento" in df2.columns:
            df2 = df2[df2["departamento"] == departamento_sel_2]

        st.write(f"Contratos SECOP 2 en la ventana y filtros: **{df2.shape[0]:,}**")

        if df2.empty:
            st.warning("No hay contratos que cumplan los filtros seleccionados.")
        else:
            # Métricas diarias con filtros
            df_daily_secop2 = build_daily_metrics(
                df=df2,
                fecha_col="fecha_de_firma",
                valor_col="valor_del_contrato",
                fuente="SECOP 2",
                col_id="id_contrato",
            )

            fig1 = chart_n_contratos(df_daily_secop2, "Número de contratos diarios (SECOP 2)")
            fig2 = chart_suma_millones(df_daily_secop2, "Suma diaria de contratos (SECOP 2, millones)")
            fig3 = chart_promedio_millones(
                df_daily_secop2, "Valor promedio por contrato (SECOP 2, millones)"
            )

            st.plotly_chart(fig1, use_container_width=True)
            st.plotly_chart(fig2, use_container_width=True)
            st.plotly_chart(fig3, use_container_width=True)

        # ==============================================================
        # Distribuciones por estado, modalidad, sector y departamento
        # Áreas apiladas — % de contratos únicos (NO AFECTADAS POR FILTROS)
        # ==============================================================

        st.markdown("### Distribución en el tiempo por categorías (SECOP 2, % de contratos únicos)")

        df_base2 = df_secop2.copy()  # solo filtrado por tiempo

        # 1. Área apilada por estado_contrato (%)
        if "estado_contrato" in df_base2.columns:
            df_estado_2 = (
                df_base2
                .assign(fecha_dia=df_base2["fecha_de_firma"].dt.floor("D"))
                .groupby(["fecha_dia", "estado_contrato"], as_index=False)
                .agg(n_contratos=("id_contrato", "nunique"))
            )

            df_estado_2["pct_contratos"] = (
                df_estado_2["n_contratos"]
                / df_estado_2.groupby("fecha_dia")["n_contratos"].transform("sum")
                * 100
            )

            fig_estado_2 = px.area(
                df_estado_2,
                x="fecha_dia",
                y="pct_contratos",
                color="estado_contrato",
                title="Porcentaje diario de contratos por estado",
            )
            fig_estado_2.update_layout(
                xaxis_title="Fecha",
                yaxis_title="% de contratos únicos",
                yaxis=dict(range=[0, 100]),
                height=350,
                margin=dict(l=0, r=0, t=40, b=0),
                legend_title="Estado",
            )
            st.plotly_chart(fig_estado_2, use_container_width=True)

        # 2. Área apilada por modalidad_de_contratacion (%)
        if "modalidad_de_contratacion" in df_base2.columns:
            df_mod_2 = (
                df_base2
                .assign(fecha_dia=df_base2["fecha_de_firma"].dt.floor("D"))
                .groupby(["fecha_dia", "modalidad_de_contratacion"], as_index=False)
                .agg(n_contratos=("id_contrato", "nunique"))
            )

            df_mod_2["pct_contratos"] = (
                df_mod_2["n_contratos"]
                / df_mod_2.groupby("fecha_dia")["n_contratos"].transform("sum")
                * 100
            )

            fig_mod_2 = px.area(
                df_mod_2,
                x="fecha_dia",
                y="pct_contratos",
                color="modalidad_de_contratacion",
                title="Porcentaje diario de contratos por modalidad",
            )
            fig_mod_2.update_layout(
                xaxis_title="Fecha",
                yaxis_title="% de contratos únicos",
                yaxis=dict(range=[0, 100]),
                height=350,
                margin=dict(l=0, r=0, t=40, b=0),
                legend_title="Modalidad",
            )
            st.plotly_chart(fig_mod_2, use_container_width=True)

        # 3. Área apilada por sector (%)
        if "sector" in df_base2.columns:
            df_sector_2 = (
                df_base2
                .assign(fecha_dia=df_base2["fecha_de_firma"].dt.floor("D"))
                .groupby(["fecha_dia", "sector"], as_index=False)
                .agg(n_contratos=("id_contrato", "nunique"))
            )

            df_sector_2["pct_contratos"] = (
                df_sector_2["n_contratos"]
                / df_sector_2.groupby("fecha_dia")["n_contratos"].transform("sum")
                * 100
            )

            fig_sector_2 = px.area(
                df_sector_2,
                x="fecha_dia",
                y="pct_contratos",
                color="sector",
                title="Porcentaje diario de contratos por sector",
            )
            fig_sector_2.update_layout(
                xaxis_title="Fecha",
                yaxis_title="% de contratos únicos",
                yaxis=dict(range=[0, 100]),
                height=350,
                margin=dict(l=0, r=0, t=40, b=0),
                legend_title="Sector",
            )
            st.plotly_chart(fig_sector_2, use_container_width=True)

        # 4. Área apilada por departamento (%)
        if "departamento" in df_base2.columns:
            df_dep_2 = (
                df_base2
                .assign(fecha_dia=df_base2["fecha_de_firma"].dt.floor("D"))
                .groupby(["fecha_dia", "departamento"], as_index=False)
                .agg(n_contratos=("id_contrato", "nunique"))
            )

            df_dep_2["pct_contratos"] = (
                df_dep_2["n_contratos"]
                / df_dep_2.groupby("fecha_dia")["n_contratos"].transform("sum")
                * 100
            )

            fig_dep_2 = px.area(
                df_dep_2,
                x="fecha_dia",
                y="pct_contratos",
                color="departamento",
                title="Porcentaje diario de contratos por departamento",
            )
            fig_dep_2.update_layout(
                xaxis_title="Fecha",
                yaxis_title="% de contratos únicos",
                yaxis=dict(range=[0, 100]),
                height=350,
                margin=dict(l=0, r=0, t=40, b=0),
                legend_title="Departamento",
            )
            st.plotly_chart(fig_dep_2, use_container_width=True)

        if mostrar_tablas_detalle and not df2.empty:
            st.markdown("### Tabla diaria de métricas (SECOP 2)")
            st.dataframe(df_daily_secop2)

        # Descarga métricas SECOP 2
        buffer2 = BytesIO()
        with pd.ExcelWriter(buffer2, engine="openpyxl") as writer:
            (df_daily_secop2 if not df2.empty else df_daily_secop2_global).to_excel(
                writer, index=False, sheet_name="SECOP2_diario"
            )
        st.download_button(
            label="Descargar métricas SECOP 2 (Excel)",
            data=buffer2.getvalue(),
            file_name="secop2_metricas_diarias.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
