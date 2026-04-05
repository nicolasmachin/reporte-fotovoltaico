import os
import pandas as pd

RUTA_HISTORICO = "data/historico_clientes.xlsx"


def _promedio_excluyendo_primer_mes(serie):
    resultado = pd.Series(pd.NA, index=serie.index, dtype="Float64")
    validos = serie.dropna()
    if len(validos) <= 1:
        return resultado

    promedios = validos.iloc[1:].expanding().mean().round(2)
    resultado.loc[validos.index[1:]] = promedios.astype(float).to_numpy()
    return resultado


def _agregar_promedio_historico(df):
    if df.empty:
        df["ahorro_promedio_historico"] = pd.Series(dtype=float)
        return df

    df = df.copy()
    df["mes_orden"] = pd.to_datetime(df["mes"].astype(str), format="%Y-%m", errors="coerce")
    df = df.sort_values(by=["cliente", "mes_orden", "mes"])
    df["ahorro_promedio_historico"] = (
        df.groupby("cliente")["ahorro_total"]
        .transform(_promedio_excluyendo_primer_mes)
    )
    return df.drop(columns=["mes_orden"])


def _normalizar_saldos_historicos(df):
    if df.empty:
        for col in ["saldo_inicial_mes", "saldo_aplicado_mes", "saldo_generado_mes"]:
            if col not in df.columns:
                df[col] = pd.Series(dtype=float)
        return df

    df = df.copy()
    for col in ["saldo_inicial_mes", "saldo_aplicado_mes", "saldo_generado_mes"]:
        if col not in df.columns:
            df[col] = pd.Series(dtype=float)

    if "saldo" not in df.columns:
        df["saldo"] = 0.0

    df["mes_orden"] = pd.to_datetime(df["mes"].astype(str), format="%Y-%m", errors="coerce")
    df = df.sort_values(by=["cliente", "mes_orden", "mes"])

    if df["saldo_generado_mes"].isna().all():
        df["saldo_generado_mes"] = df["saldo"].fillna(0.0)

    df["saldo_aplicado_mes"] = df["saldo_aplicado_mes"].fillna(0.0)

    saldo_iniciales = []
    saldo_finales = []

    for _, grupo in df.groupby("cliente", sort=False):
        saldo_corriente = 0.0
        for _, fila in grupo.iterrows():
            saldo_iniciales.append(round(saldo_corriente, 2))
            saldo_generado = float(fila["saldo_generado_mes"]) if not pd.isna(fila["saldo_generado_mes"]) else 0.0
            saldo_aplicado = float(fila["saldo_aplicado_mes"]) if not pd.isna(fila["saldo_aplicado_mes"]) else 0.0
            saldo_corriente = round(max(saldo_corriente - saldo_aplicado + saldo_generado, 0.0), 2)
            saldo_finales.append(saldo_corriente)

    df["saldo_inicial_mes"] = saldo_iniciales
    df["saldo"] = saldo_finales

    return df.drop(columns=["mes_orden"])


def _asegurar_columnas_historicas(df, tipo_cambio_default=None):
    df = df.copy()

    if "tipo_cambio_usd" not in df.columns:
        df["tipo_cambio_usd"] = pd.Series(dtype=float)

    if "ahorro_total_usd" not in df.columns:
        df["ahorro_total_usd"] = pd.Series(dtype=float)

    if tipo_cambio_default is not None and "tipo_cambio_usd" in df.columns:
        faltantes_tc = df["tipo_cambio_usd"].isna() & df["ahorro_total_usd"].notna()
        df.loc[faltantes_tc, "tipo_cambio_usd"] = tipo_cambio_default

    faltantes_usd = df["ahorro_total_usd"].isna() & df["ahorro_total"].notna() & df["tipo_cambio_usd"].notna() & (df["tipo_cambio_usd"] != 0)
    df.loc[faltantes_usd, "ahorro_total_usd"] = (
        df.loc[faltantes_usd, "ahorro_total"] / df.loc[faltantes_usd, "tipo_cambio_usd"]
    ).round(2)

    return _normalizar_saldos_historicos(df)


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
        "ahorro_total_usd",
        "saldo",
        "saldo_inicial_mes",
        "saldo_aplicado_mes",
        "saldo_generado_mes",
        "tipo_cambio_usd",
        "ahorro_promedio_historico",
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
        "ahorro_total_usd": data["ahorro_total_usd"],
        "saldo": data["saldo_final"],
        "saldo_inicial_mes": data["saldo_inicial"],
        "saldo_aplicado_mes": data["saldo_aplicado"],
        "saldo_generado_mes": data["saldo_generado_mes"],
        "tipo_cambio_usd": data["tipo_cambio_usd"],
        "ahorro_promedio_historico": data["ahorro_total"],
    }])

    if not os.path.exists(RUTA_HISTORICO):
        nuevo = _agregar_promedio_historico(nuevo)
        nuevo[columnas].to_excel(RUTA_HISTORICO, sheet_name="historico", index=False)
        return

    df = pd.read_excel(RUTA_HISTORICO, sheet_name="historico")
    df = _asegurar_columnas_historicas(df, data["tipo_cambio_usd"])

    # elimina duplicado mismo cliente+mes si existiera
    df = df[~((df["cliente"] == cliente) & (df["mes"] == mes))]

    df = pd.concat([df, nuevo], ignore_index=True)
    df = _agregar_promedio_historico(df)

    df[columnas].to_excel(RUTA_HISTORICO, sheet_name="historico", index=False)


def obtener_acumulado_anual(cliente, anio):
    if not os.path.exists(RUTA_HISTORICO):
        return None

    df = pd.read_excel(RUTA_HISTORICO, sheet_name="historico")
    df = _asegurar_columnas_historicas(df)
    if "ahorro_promedio_historico" not in df.columns:
        df = _agregar_promedio_historico(df)
    df = df[df["cliente"] == cliente].copy()

    if df.empty:
        return None

    df["anio"] = df["mes"].astype(str).str[:4].astype(int)
    df_anio = df[df["anio"] == int(anio)]

    if df_anio.empty:
        return None

    serie_ahorro = df["ahorro_total"].dropna().reset_index(drop=True)
    serie_ahorro_usd = df["ahorro_total_usd"].dropna().reset_index(drop=True)

    ahorro_promedio_historico = 0.0
    if len(serie_ahorro) > 1:
        ahorro_promedio_historico = round(serie_ahorro.iloc[1:].mean(), 2)

    meses_con_usd = max(int(serie_ahorro_usd.shape[0]) - 1, 0)
    ahorro_promedio_historico_usd = 0.0
    if len(serie_ahorro_usd) > 1:
        ahorro_promedio_historico_usd = round(serie_ahorro_usd.iloc[1:].mean(), 2)

    return {
        "ahorro_total": round(df["ahorro_total"].sum(), 2),
        "ahorro_total_usd": round(df["ahorro_total_usd"].dropna().sum(), 2),
        "saldo_total": round(df.sort_values(by=["mes"]).iloc[-1]["saldo"], 2),
        "exportacion_total": round(df_anio["exportacion_kwh"].sum(), 2),
        "importacion_total": round(df_anio["importacion_kwh"].sum(), 2),
        "ahorro_promedio_historico": ahorro_promedio_historico,
        "ahorro_promedio_historico_usd": ahorro_promedio_historico_usd,
        "meses_con_datos_usd": meses_con_usd,
    }
