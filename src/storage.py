import os
import pandas as pd

RUTA_HISTORICO = "data/historico_clientes.xlsx"


def guardar_registro(cliente, mes, data):
    columnas = [
        "cliente",
        "mes",
        "generacion_kwh",
        "consumo_kwh",
        "exportacion_kwh",
        "autoconsumo_kwh",
        "importacion_kwh",
        "ahorro_autoconsumo",
        "ahorro_venta",
        "ahorro_total",
        "saldo",
    ]

    nuevo = pd.DataFrame([{
        "cliente": cliente,
        "mes": mes,
        "generacion_kwh": data["generacion"],
        "consumo_kwh": data["consumo"],
        "exportacion_kwh": data["exportacion"],
        "autoconsumo_kwh": data["autoconsumo"],
        "importacion_kwh": data["importacion"],
        "ahorro_autoconsumo": data["ahorro_autoconsumo"],
        "ahorro_venta": data["ahorro_venta"],
        "ahorro_total": data["ahorro_total"],
        "saldo": data["saldo"],
    }])

    if not os.path.exists(RUTA_HISTORICO):
        nuevo[columnas].to_excel(RUTA_HISTORICO, sheet_name="historico", index=False)
        return

    df = pd.read_excel(RUTA_HISTORICO, sheet_name="historico")

    # elimina duplicado mismo cliente+mes si existiera
    df = df[~((df["cliente"] == cliente) & (df["mes"] == mes))]

    df = pd.concat([df, nuevo], ignore_index=True)
    df = df.sort_values(by=["cliente", "mes"])

    df.to_excel(RUTA_HISTORICO, sheet_name="historico", index=False)


def obtener_acumulado_anual(cliente, anio):
    if not os.path.exists(RUTA_HISTORICO):
        return None

    df = pd.read_excel(RUTA_HISTORICO, sheet_name="historico")
    df = df[df["cliente"] == cliente].copy()

    if df.empty:
        return None

    df["anio"] = df["mes"].astype(str).str[:4].astype(int)
    df_anio = df[df["anio"] == int(anio)]

    if df_anio.empty:
        return None

    return {
        "ahorro_total": round(df_anio["ahorro_total"].sum(), 2),
        "saldo_total": round(df_anio["saldo"].sum(), 2),
        "exportacion_total": round(df_anio["exportacion_kwh"].sum(), 2),
        "importacion_total": round(df_anio["importacion_kwh"].sum(), 2),
    }