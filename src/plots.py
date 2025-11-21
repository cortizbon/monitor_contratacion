# src/plots.py

import pandas as pd
import plotly.express as px


def chart_n_contratos(df_daily: pd.DataFrame, title: str = "Número de contratos diarios"):
    fig = px.line(
        df_daily,
        x="fecha",
        y="n_contratos",
        markers=True,
        title=title,
    )
    fig.update_layout(
        xaxis_title="Fecha",
        yaxis_title="Número de contratos",
        height=350,
        margin=dict(l=0, r=0, t=40, b=0),
    )
    return fig


def chart_suma_millones(
    df_daily: pd.DataFrame,
    title: str = "Suma diaria (millones de pesos)",
):
    fig = px.line(
        df_daily,
        x="fecha",
        y="suma_millones",
        markers=True,
        title=title,
    )
    fig.update_layout(
        xaxis_title="Fecha",
        yaxis_title="Suma de contratos (millones)",
        height=350,
        margin=dict(l=0, r=0, t=40, b=0),
    )
    return fig


def chart_promedio_millones(
    df_daily: pd.DataFrame,
    title: str = "Valor promedio (millones de pesos)",
):
    fig = px.line(
        df_daily,
        x="fecha",
        y="promedio_millones",
        markers=True,
        title=title,
    )
    fig.update_layout(
        xaxis_title="Fecha",
        yaxis_title="Promedio por contrato (millones)",
        height=350,
        margin=dict(l=0, r=0, t=40, b=0),
    )
    return fig
