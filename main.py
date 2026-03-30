import shutil
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader

from src.metrics import (
    calcular_autoconsumo,
    calcular_importacion_red,
    calcular_costo_energia_simple,
    calcular_costo_energia_doble,
    calcular_costo_energia_triple,
    calcular_credito_exportacion_simple,
    calcular_credito_exportacion_doble,
    calcular_credito_exportacion_triple,
    calcular_factura,
    calcular_desglose_beneficio,
)
from src.storage import guardar_registro, obtener_acumulado_anual

MESES_ES = {
    1: "enero",
    2: "febrero",
    3: "marzo",
    4: "abril",
    5: "mayo",
    6: "junio",
    7: "julio",
    8: "agosto",
    9: "septiembre",
    10: "octubre",
    11: "noviembre",
    12: "diciembre",
}


def formatear_mes_es(fecha):
    return f"{MESES_ES[fecha.month]} {fecha.year}"


def fmt(num):
    return f"{float(num):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_pct(num):
    return fmt(num).replace(",00", "")


def pct(part, total):
    if float(total) <= 0:
        return 0.0
    return round((float(part) / float(total)) * 100, 1)


# =========================
# Lectura de archivos
# =========================
df_clientes = pd.read_excel("data/datos_clientes.xlsx", sheet_name="datos")
df_parametros = pd.read_excel("data/tarifas.xlsx", sheet_name="parametros")
df_simple = pd.read_excel("data/tarifas.xlsx", sheet_name="precios_simple")
df_doble = pd.read_excel("data/tarifas.xlsx", sheet_name="precios_doble")
df_triple = pd.read_excel("data/tarifas.xlsx", sheet_name="precios_triple")

# Limpieza
df_clientes.columns = df_clientes.columns.str.strip().str.lower()
df_parametros.columns = df_parametros.columns.str.strip().str.lower()
df_simple.columns = df_simple.columns.str.strip().str.lower()
df_doble.columns = df_doble.columns.str.strip().str.lower()
df_triple.columns = df_triple.columns.str.strip().str.lower()

for col in ["tarifa", "concepto"]:
    df_parametros[col] = df_parametros[col].astype(str).str.strip().str.lower()

df_doble["franja"] = df_doble["franja"].astype(str).str.strip().str.lower()
df_triple["franja"] = df_triple["franja"].astype(str).str.strip().str.lower()

# Mes YYYY-MM
df_clientes["mes"] = pd.to_datetime(
    df_clientes["mes"].astype(str) + "-01",
    format="%Y-%m-%d"
)

ultimo_mes = df_clientes["mes"].max()
mes_anterior = ultimo_mes - pd.DateOffset(months=1)

df_mes_actual = df_clientes[df_clientes["mes"] == ultimo_mes].copy()
df_mes_anterior = df_clientes[df_clientes["mes"] == mes_anterior].copy()

tramos_simple = df_simple.to_dict(orient="records")

# Template
env = Environment(loader=FileSystemLoader("templates"))
template = env.get_template("reporte.html")

Path("reports").mkdir(exist_ok=True)
Path("reports/assets").mkdir(exist_ok=True)

logo_origen = Path("templates/assets/logo_voltia_completo.png")
logo_destino = Path("reports/assets/logo_voltia_completo.png")
if logo_origen.exists():
    shutil.copy(logo_origen, logo_destino)


def obtener_parametro(tarifa, concepto):
    fila = df_parametros[
        (df_parametros["tarifa"] == tarifa.lower()) &
        (df_parametros["concepto"] == concepto.lower())
    ]
    if fila.empty:
        raise ValueError(f"No se encontró el parámetro '{concepto}' para la tarifa '{tarifa}'")
    return float(fila.iloc[0]["valor"])


def obtener_precio_franja(df, franja):
    fila = df[df["franja"] == franja.lower()]
    if fila.empty:
        raise ValueError(f"No se encontró la franja '{franja}'")
    return float(fila.iloc[0]["precio_kwh"])


cargo_fijo_simple = obtener_parametro("simple", "cargo_fijo")
cargo_pot_kw_simple = obtener_parametro("simple", "potencia_cont")

cargo_fijo_doble = obtener_parametro("doble", "cargo_fijo")
cargo_pot_kw_doble = obtener_parametro("doble", "potencia_cont")

cargo_fijo_triple = obtener_parametro("triple", "cargo_fijo")
cargo_pot_kw_triple = obtener_parametro("triple", "potencia_cont")

precio_punta_doble = obtener_precio_franja(df_doble, "punta")
precio_fuera_punta = obtener_precio_franja(df_doble, "fuera_punta")

precio_punta_triple = obtener_precio_franja(df_triple, "punta")
precio_llano = obtener_precio_franja(df_triple, "llano")
precio_valle = obtener_precio_franja(df_triple, "valle")

for _, fila in df_mes_actual.iterrows():
    cliente = str(fila["cliente"]).strip()
    mes_dt = fila["mes"]
    mes_calculado = mes_dt.strftime("%Y-%m")
    anio = mes_dt.year

    potencia = float(fila["potencia_contratada_kw"])
    generacion = float(fila["generacion_kwh"])
    consumo = float(fila["consumo_kwh"])
    exportacion = float(fila["exportacion_kwh"])

    pct_punta = float(fila["pct_punta"])
    pct_llano = float(fila["pct_llano"])
    pct_valle = float(fila["pct_valle"])

    if pct_punta > 1 or pct_llano > 1 or pct_valle > 1:
        pct_punta /= 100
        pct_llano /= 100
        pct_valle /= 100

    suma_pct = round(pct_punta + pct_llano + pct_valle, 4)
    if abs(suma_pct - 1.0) > 0.001:
        print(f"ERROR en {cliente}: los porcentajes no suman 1.0")
        continue

    fila_mes_anterior = df_mes_anterior[
        df_mes_anterior["cliente"].astype(str).str.strip() == cliente
    ]

    if fila_mes_anterior.empty:
        print(f"ERROR en {cliente}: no hay registro del mes anterior para calcular crédito.")
        continue

    exportacion_mes_anterior = float(fila_mes_anterior.iloc[0]["exportacion_kwh"])

    autoconsumo = calcular_autoconsumo(generacion, exportacion)
    importacion_red = calcular_importacion_red(consumo, generacion, exportacion)

    pct_consumo_autoconsumo = pct(autoconsumo, consumo)
    pct_consumo_red = pct(importacion_red, consumo)

    pct_generacion_autoconsumo = pct(autoconsumo, generacion)
    pct_generacion_exportada = pct(exportacion, generacion)

    # SIMPLE
    energia_simple_sin = calcular_costo_energia_simple(consumo, tramos_simple)
    energia_simple_con = calcular_costo_energia_simple(importacion_red, tramos_simple)
    credito_simple = calcular_credito_exportacion_simple(exportacion_mes_anterior, tramos_simple)

    factura_simple_sin = calcular_factura(
        cargo_fijo=cargo_fijo_simple,
        cargo_potencia=potencia * cargo_pot_kw_simple,
        cargo_energia=energia_simple_sin,
        credito_exportacion=0,
    )

    factura_simple_con = calcular_factura(
        cargo_fijo=cargo_fijo_simple,
        cargo_potencia=potencia * cargo_pot_kw_simple,
        cargo_energia=energia_simple_con,
        credito_exportacion=credito_simple,
    )

    desglose_simple = calcular_desglose_beneficio(factura_simple_sin, factura_simple_con)

    ahorro_total_simple = round(
        factura_simple_sin["total_neto"] - factura_simple_con["total_neto"] + factura_simple_con["saldo_a_favor"],
        2
    )

    # DOBLE
    energia_doble_sin = calcular_costo_energia_doble(
        consumo, pct_punta, precio_punta_doble, precio_fuera_punta
    )
    energia_doble_con = calcular_costo_energia_doble(
        importacion_red, pct_punta, precio_punta_doble, precio_fuera_punta
    )
    credito_doble = calcular_credito_exportacion_doble(exportacion_mes_anterior, precio_fuera_punta)

    factura_doble_sin = calcular_factura(
        cargo_fijo=cargo_fijo_doble,
        cargo_potencia=potencia * cargo_pot_kw_doble,
        cargo_energia=energia_doble_sin,
        credito_exportacion=0,
    )

    factura_doble_con = calcular_factura(
        cargo_fijo=cargo_fijo_doble,
        cargo_potencia=potencia * cargo_pot_kw_doble,
        cargo_energia=energia_doble_con,
        credito_exportacion=credito_doble,
    )

    desglose_doble = calcular_desglose_beneficio(factura_doble_sin, factura_doble_con)

    ahorro_total_doble = round(
        factura_doble_sin["total_neto"] - factura_doble_con["total_neto"] + factura_doble_con["saldo_a_favor"],
        2
    )

    # TRIPLE
    energia_triple_sin = calcular_costo_energia_triple(
        consumo, pct_punta, pct_llano, pct_valle,
        precio_punta_triple, precio_llano, precio_valle
    )
    energia_triple_con = calcular_costo_energia_triple(
        importacion_red, pct_punta, pct_llano, pct_valle,
        precio_punta_triple, precio_llano, precio_valle
    )
    credito_triple = calcular_credito_exportacion_triple(exportacion_mes_anterior, precio_llano)

    factura_triple_sin = calcular_factura(
        cargo_fijo=cargo_fijo_triple,
        cargo_potencia=potencia * cargo_pot_kw_triple,
        cargo_energia=energia_triple_sin,
        credito_exportacion=0,
    )

    factura_triple_con = calcular_factura(
        cargo_fijo=cargo_fijo_triple,
        cargo_potencia=potencia * cargo_pot_kw_triple,
        cargo_energia=energia_triple_con,
        credito_exportacion=credito_triple,
    )

    desglose_triple = calcular_desglose_beneficio(factura_triple_sin, factura_triple_con)

    ahorro_total_triple = round(
        factura_triple_sin["total_neto"] - factura_triple_con["total_neto"] + factura_triple_con["saldo_a_favor"],
        2
    )

    # Guardar histórico
    guardar_registro(
        cliente,
        mes_calculado,
        {
            "generacion": generacion,
            "consumo": consumo,
            "exportacion": exportacion,
            "autoconsumo": autoconsumo,
            "importacion": importacion_red,
            "ahorro_autoconsumo": desglose_simple["ahorro_autoconsumo"],
            "ahorro_venta": desglose_simple["ahorro_venta"],
            "ahorro_total": desglose_simple["ahorro_total"],
            "saldo": factura_simple_con["saldo_a_favor"],
        }
    )

    acumulado = obtener_acumulado_anual(cliente, anio)

    ahorro_acumulado = 0
    saldo_acumulado = 0
    exportacion_acumulada = 0
    importacion_acumulada = 0
    ratio_ute = 0
    estado_ute = "SIN DATOS"

    if acumulado:
        ahorro_acumulado = acumulado["ahorro_total"]
        saldo_acumulado = acumulado["saldo_total"]
        exportacion_acumulada = acumulado["exportacion_total"]
        importacion_acumulada = acumulado["importacion_total"]

        if importacion_acumulada > 0:
            ratio_ute = round((exportacion_acumulada / importacion_acumulada) * 100, 1)

        if exportacion_acumulada <= importacion_acumulada:
            estado_ute = "OK"
        elif exportacion_acumulada <= importacion_acumulada * 1.1:
            estado_ute = "ALERTA"
        else:
            estado_ute = "EXCEDIDO"

    html = template.render(
        cliente=cliente,
        mes=formatear_mes_es(mes_dt),
        mes_anterior=formatear_mes_es(mes_anterior),
        generacion=fmt(generacion),
        consumo=fmt(consumo),
        exportacion_actual=fmt(exportacion),
        exportacion_anterior=fmt(exportacion_mes_anterior),
        autoconsumo=fmt(autoconsumo),
        autoconsumo_sobre_consumo=fmt_pct(pct_consumo_autoconsumo),
        potencia=fmt_pct(potencia),
        importacion_red=fmt(importacion_red),
        pct_punta=fmt_pct(pct_punta * 100),
        pct_llano=fmt_pct(pct_llano * 100),
        pct_valle=fmt_pct(pct_valle * 100),

        consumo_bar_autoconsumo=round(pct_consumo_autoconsumo, 1),
        consumo_bar_red=round(pct_consumo_red, 1),
        generacion_bar_autoconsumo=round(pct_generacion_autoconsumo, 1),
        generacion_bar_exportada=round(pct_generacion_exportada, 1),

        simple={
            "total_sin": fmt(factura_simple_sin["total_neto"]),
            "total_con": fmt(factura_simple_con["total_neto"]),
            "credito": fmt(factura_simple_con["credito_exportacion"]),
            "saldo": fmt(factura_simple_con["saldo_a_favor"]),
            "ahorro_total": fmt(ahorro_total_simple),
            "ahorro_autoconsumo": fmt(desglose_simple["ahorro_autoconsumo"]),
            "ahorro_venta": fmt(desglose_simple["ahorro_venta"]),
            "ahorro_total_desglose": fmt(desglose_simple["ahorro_total"]),
            "pct_autoconsumo": fmt_pct(desglose_simple["pct_autoconsumo"]),
            "pct_venta": fmt_pct(desglose_simple["pct_venta"]),
        },
        doble={
            "total_sin": fmt(factura_doble_sin["total_neto"]),
            "total_con": fmt(factura_doble_con["total_neto"]),
            "credito": fmt(factura_doble_con["credito_exportacion"]),
            "saldo": fmt(factura_doble_con["saldo_a_favor"]),
            "ahorro_total": fmt(ahorro_total_doble),
            "ahorro_autoconsumo": fmt(desglose_doble["ahorro_autoconsumo"]),
            "ahorro_venta": fmt(desglose_doble["ahorro_venta"]),
            "ahorro_total_desglose": fmt(desglose_doble["ahorro_total"]),
            "pct_autoconsumo": fmt_pct(desglose_doble["pct_autoconsumo"]),
            "pct_venta": fmt_pct(desglose_doble["pct_venta"]),
        },
        triple={
            "total_sin": fmt(factura_triple_sin["total_neto"]),
            "total_con": fmt(factura_triple_con["total_neto"]),
            "credito": fmt(factura_triple_con["credito_exportacion"]),
            "saldo": fmt(factura_triple_con["saldo_a_favor"]),
            "ahorro_total": fmt(ahorro_total_triple),
            "ahorro_autoconsumo": fmt(desglose_triple["ahorro_autoconsumo"]),
            "ahorro_venta": fmt(desglose_triple["ahorro_venta"]),
            "ahorro_total_desglose": fmt(desglose_triple["ahorro_total"]),
            "pct_autoconsumo": fmt_pct(desglose_triple["pct_autoconsumo"]),
            "pct_venta": fmt_pct(desglose_triple["pct_venta"]),
        },

        ahorro_acumulado=fmt(ahorro_acumulado),
        saldo_acumulado=fmt(saldo_acumulado),
        exportacion_acumulada=fmt(exportacion_acumulada),
        importacion_acumulada=fmt(importacion_acumulada),
        ratio_ute=fmt_pct(ratio_ute),
        estado_ute=estado_ute,
    )

    nombre_archivo = f"reporte_{cliente.lower().replace(' ', '_')}_{mes_calculado}.html"
    ruta_salida = Path("reports") / nombre_archivo

    with open(ruta_salida, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Reporte generado: {ruta_salida}")