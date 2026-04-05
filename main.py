import os
import shutil
import subprocess
from pathlib import Path
import unicodedata
import math

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
    aplicar_saldo_cuenta_corriente,
)
from src.storage import RUTA_HISTORICO, guardar_registro, obtener_acumulado_anual

GENERAR_PDF = False
TESTING = False
CLIENTE_TESTING = "BARENOF"

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


def parsear_periodo(valor):
    if pd.isna(valor):
        return None

    texto = str(valor).strip()
    if not texto or texto.lower() == "nan":
        return None

    fecha = pd.to_datetime(texto, format="%Y-%m", errors="coerce")
    if pd.isna(fecha):
        fecha = pd.to_datetime(texto, errors="coerce")

    if pd.isna(fecha):
        return None

    return fecha


def formatear_periodo(valor):
    fecha = parsear_periodo(valor)
    if fecha is None:
        texto = str(valor).strip()
        if not texto or texto.lower() == "nan":
            return "-"
        return texto

    return formatear_mes_es(fecha)


def formatear_duracion_meses(cantidad_meses):
    cantidad_meses = int(cantidad_meses)
    anios = cantidad_meses // 12
    meses = cantidad_meses % 12

    partes = []
    if anios > 0:
        partes.append(f"{anios} año" if anios == 1 else f"{anios} años")
    if meses > 0 or not partes:
        partes.append(f"{meses} mes" if meses == 1 else f"{meses} meses")

    return " y ".join(partes)


def fmt(num):
    return fmt_int(num)


def fmt_int(num):
    if pd.isna(num):
        return "-"
    return f"{int(round(float(num))):,}".replace(",", ".")


def fmt_pct(num):
    return fmt_int(num)


def fmt_decimal(num, decimales=1):
    if pd.isna(num):
        return "-"
    formato = f"{{:,.{decimales}f}}"
    return formato.format(float(num)).replace(",", "X").replace(".", ",").replace("X", ".")


def normalizar_texto(valor):
    texto = str(valor).strip().lower()
    texto = unicodedata.normalize("NFKD", texto)
    return "".join(ch for ch in texto if not unicodedata.combining(ch))


def slug_cliente(valor):
    texto = normalizar_texto(valor)
    return texto.replace(" ", "_")


def texto_normalizado_opcional(valor, default=""):
    if pd.isna(valor):
        return default
    texto = str(valor).strip()
    if not texto or texto.lower() == "nan":
        return default
    return normalizar_texto(texto)


def parsear_flag_reporte(valor):
    if pd.isna(valor):
        return 1
    texto = str(valor).strip().lower()
    if texto in {"0", "no", "false", "falso"}:
        return 0
    if texto in {"1", "si", "sí", "true", "verdadero"}:
        return 1
    try:
        return 1 if int(float(texto)) != 0 else 0
    except ValueError:
        return 1


def etiqueta_tarifa(valor):
    tarifa = texto_normalizado_opcional(valor)
    etiquetas = {
        "simple": "Tarifa simple",
        "doble": "Tarifa doble horario",
        "triple": "Tarifa triple horario",
        "zafral": "Tarifa zafral",
    }
    return etiquetas.get(tarifa, f"Tarifa {str(valor).strip()}")


def obtener_ruta_chrome():
    candidatos = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        shutil.which("google-chrome"),
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
    ]

    for candidato in candidatos:
        if candidato and Path(candidato).exists():
            return candidato

    return None


def generar_pdf_desde_html(ruta_html, ruta_pdf):
    chrome_path = obtener_ruta_chrome()
    if not chrome_path:
        print(f"AVISO: no se encontró Chrome/Chromium para generar PDF: {ruta_pdf}")
        return False

    ruta_html = Path(ruta_html).resolve()
    ruta_pdf = Path(ruta_pdf).resolve()

    comando = [
        chrome_path,
        "--headless=new",
        "--disable-gpu",
        "--allow-file-access-from-files",
        "--no-pdf-header-footer",
        f"--print-to-pdf={ruta_pdf}",
        str(ruta_html),
    ]

    try:
        subprocess.run(comando, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        print(f"AVISO: no se pudo generar PDF para {ruta_html.name}")
        return False


def pct(part, total):
    if float(total) <= 0:
        return 0.0
    return round((float(part) / float(total)) * 100, 1)


# =========================
# Lectura de archivos
# =========================
df_clientes = pd.read_excel("data/datos_clientes.xlsx", sheet_name="datos")
df_constantes = pd.read_excel("data/datos_clientes.xlsx", sheet_name="constantes")
df_constantes_globales_raw = pd.read_excel("data/datos_clientes.xlsx", sheet_name="constantes_globales", header=None)
df_parametros = pd.read_excel("data/tarifas.xlsx", sheet_name="parametros")
df_simple = pd.read_excel("data/tarifas.xlsx", sheet_name="precios_simple")
df_doble = pd.read_excel("data/tarifas.xlsx", sheet_name="precios_doble")
df_triple = pd.read_excel("data/tarifas.xlsx", sheet_name="precios_triple")
df_zafral = pd.read_excel("data/tarifas.xlsx", sheet_name="zafral")

# Limpieza
df_clientes.columns = df_clientes.columns.str.strip().str.lower()
df_constantes.columns = df_constantes.columns.str.strip().str.lower()
df_parametros.columns = df_parametros.columns.str.strip().str.lower()
df_simple.columns = df_simple.columns.str.strip().str.lower()
df_doble.columns = df_doble.columns.str.strip().str.lower()
df_triple.columns = df_triple.columns.str.strip().str.lower()
df_zafral.columns = df_zafral.columns.str.strip().str.lower()

if "fecha_ins" in df_constantes.columns and "fecha_inst" not in df_constantes.columns:
    df_constantes = df_constantes.rename(columns={"fecha_ins": "fecha_inst"})

df_clientes = df_clientes[df_clientes["cliente"].notna()].copy()
df_constantes = df_constantes[df_constantes["cliente"].notna()].copy()
df_clientes["cliente"] = df_clientes["cliente"].astype(str).str.strip()
df_constantes["cliente"] = df_constantes["cliente"].astype(str).str.strip()

df_clientes = df_clientes.merge(
    df_constantes,
    on="cliente",
    how="left",
    validate="many_to_one",
)

if "reporte" in df_clientes.columns:
    df_clientes["reporte"] = df_clientes["reporte"].apply(parsear_flag_reporte)
else:
    df_clientes["reporte"] = 1

if "mail" in df_clientes.columns:
    df_clientes["mail"] = df_clientes["mail"].fillna("").astype(str).str.strip()
else:
    df_clientes["mail"] = ""

constantes_globales = {}
for _, fila_global in df_constantes_globales_raw.iterrows():
    if fila_global.empty or pd.isna(fila_global.iloc[0]):
        continue
    clave = normalizar_texto(fila_global.iloc[0])
    valor = fila_global.iloc[1] if len(fila_global) > 1 else None
    constantes_globales[clave] = valor

tipo_cambio_usd = constantes_globales.get("dolar")
if tipo_cambio_usd is None or pd.isna(tipo_cambio_usd):
    raise ValueError("No se encontró la constante global 'Dólar' en la pestaña 'constantes_globales'.")
tipo_cambio_usd = float(tipo_cambio_usd)

for col in ["tarifa", "concepto"]:
    df_parametros[col] = df_parametros[col].astype(str).str.strip().str.lower()

df_doble["franja"] = df_doble["franja"].astype(str).str.strip().str.lower()
df_triple["franja"] = df_triple["franja"].astype(str).str.strip().str.lower()
df_zafral["franja"] = df_zafral["franja"].astype(str).str.strip().str.lower()

# Mes YYYY-MM
df_clientes["mes"] = pd.to_datetime(
    df_clientes["mes"].astype(str) + "-01",
    format="%Y-%m-%d",
    errors="coerce",
)

df_clientes = df_clientes[df_clientes["mes"].notna()].copy()

ultimo_mes = df_clientes["mes"].max()
mes_anterior = ultimo_mes - pd.DateOffset(months=1)

df_mes_actual = df_clientes[df_clientes["mes"] == ultimo_mes].copy()
df_mes_anterior = df_clientes[df_clientes["mes"] == mes_anterior].copy()

tramos_simple = df_simple.to_dict(orient="records")

# Template
env = Environment(loader=FileSystemLoader("templates"))
template = env.get_template("reporte.html")

procesar_historico_completo = os.environ.get("PROCESS_ALL_MONTHS") == "1"
reconstruir_historico = os.environ.get("REBUILD_HISTORICO") == "1"

Path("reports").mkdir(exist_ok=True)
Path("reports/assets").mkdir(exist_ok=True)

logo_origen = Path("templates/assets/logo_voltia_completo.png")
logo_destino = Path("reports/assets/logo_voltia_completo.png")
if logo_origen.exists():
    shutil.copy(logo_origen, logo_destino)

if reconstruir_historico:
    ruta_historico = Path(RUTA_HISTORICO)
    if ruta_historico.exists():
        ruta_historico.unlink()


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
cargo_fijo_zafral = obtener_parametro("zafral", "cargo_fijo")
cargo_pot_kw_zafral = obtener_parametro("zafral", "potencia_cont")

precio_punta_doble = obtener_precio_franja(df_doble, "punta")
precio_fuera_punta = obtener_precio_franja(df_doble, "fuera_punta")

precio_punta_triple = obtener_precio_franja(df_triple, "punta")
precio_llano = obtener_precio_franja(df_triple, "llano")
precio_valle = obtener_precio_franja(df_triple, "valle")
precio_punta_zafral = obtener_precio_franja(df_zafral, "punta")
precio_llano_zafral = obtener_precio_franja(df_zafral, "llano")
precio_valle_zafral = obtener_precio_franja(df_zafral, "valle")


def calcular_saldos_cuenta_corriente(df_fuente):
    resultados = {}

    for cliente, df_cliente in df_fuente.sort_values(by=["cliente", "mes"]).groupby("cliente"):
        df_cliente = df_cliente.sort_values(by="mes").copy()
        saldos = {"simple": 0.0, "doble": 0.0, "triple": 0.0, "zafral": 0.0}

        for _, fila_cliente in df_cliente.iterrows():
            mes_dt = fila_cliente["mes"]
            mes_calculado = mes_dt.strftime("%Y-%m")
            fila_mes_anterior = df_cliente[df_cliente["mes"] == (mes_dt - pd.DateOffset(months=1))]

            if pd.isna(fila_cliente["potencia_contratada_kw"]):
                continue

            potencia = float(fila_cliente["potencia_contratada_kw"])
            generacion = float(fila_cliente["generacion_kwh"])
            consumo = float(fila_cliente["consumo_kwh"])
            exportacion = float(fila_cliente["exportacion_kwh"])

            pct_punta = float(fila_cliente["pct_punta"])
            pct_llano = float(fila_cliente["pct_llano"])
            pct_valle = float(fila_cliente["pct_valle"])

            if pct_punta > 1 or pct_llano > 1 or pct_valle > 1:
                pct_punta /= 100
                pct_llano /= 100
                pct_valle /= 100

            autoconsumo = calcular_autoconsumo(generacion, exportacion)
            importacion_red = calcular_importacion_red(consumo, generacion, exportacion)
            exportacion_mes_anterior = 0.0
            if not fila_mes_anterior.empty:
                exportacion_mes_anterior = float(fila_mes_anterior.iloc[0]["exportacion_kwh"])

            factura_simple = calcular_factura(
                cargo_fijo=cargo_fijo_simple,
                cargo_potencia=potencia * cargo_pot_kw_simple,
                cargo_energia=calcular_costo_energia_simple(importacion_red, tramos_simple),
                credito_exportacion=calcular_credito_exportacion_simple(exportacion_mes_anterior, tramos_simple),
            )
            factura_doble = calcular_factura(
                cargo_fijo=cargo_fijo_doble,
                cargo_potencia=potencia * cargo_pot_kw_doble,
                cargo_energia=calcular_costo_energia_doble(importacion_red, pct_punta, precio_punta_doble, precio_fuera_punta),
                credito_exportacion=calcular_credito_exportacion_doble(exportacion_mes_anterior, precio_fuera_punta),
            )
            factura_triple = calcular_factura(
                cargo_fijo=cargo_fijo_triple,
                cargo_potencia=potencia * cargo_pot_kw_triple,
                cargo_energia=calcular_costo_energia_triple(importacion_red, pct_punta, pct_llano, pct_valle, precio_punta_triple, precio_llano, precio_valle),
                credito_exportacion=calcular_credito_exportacion_triple(exportacion_mes_anterior, precio_llano),
            )
            factura_zafral = calcular_factura(
                cargo_fijo=cargo_fijo_zafral,
                cargo_potencia=potencia * cargo_pot_kw_zafral,
                cargo_energia=calcular_costo_energia_triple(importacion_red, pct_punta, pct_llano, pct_valle, precio_punta_zafral, precio_llano_zafral, precio_valle_zafral),
                credito_exportacion=calcular_credito_exportacion_triple(exportacion_mes_anterior, precio_llano_zafral),
            )

            ajustada_simple = aplicar_saldo_cuenta_corriente(factura_simple, saldos["simple"])
            ajustada_doble = aplicar_saldo_cuenta_corriente(factura_doble, saldos["doble"])
            ajustada_triple = aplicar_saldo_cuenta_corriente(factura_triple, saldos["triple"])
            ajustada_zafral = aplicar_saldo_cuenta_corriente(factura_zafral, saldos["zafral"])

            saldos["simple"] = ajustada_simple["saldo_final"]
            saldos["doble"] = ajustada_doble["saldo_final"]
            saldos["triple"] = ajustada_triple["saldo_final"]
            saldos["zafral"] = ajustada_zafral["saldo_final"]

            resultados[(cliente, mes_calculado)] = {
                "simple": ajustada_simple,
                "doble": ajustada_doble,
                "triple": ajustada_triple,
                "zafral": ajustada_zafral,
            }

    return resultados


saldos_cuenta_corriente = calcular_saldos_cuenta_corriente(df_clientes)

df_a_procesar = df_mes_actual.copy()
if procesar_historico_completo:
    df_a_procesar = df_clientes.sort_values(by=["mes", "cliente"]).copy()

df_a_procesar = df_a_procesar[df_a_procesar["reporte"] == 1].copy()

if TESTING:
    cliente_testing_normalizado = normalizar_texto(CLIENTE_TESTING)
    df_a_procesar = df_a_procesar[
        df_a_procesar["cliente"].astype(str).map(normalizar_texto) == cliente_testing_normalizado
    ].copy()

    if df_a_procesar.empty:
        raise ValueError(
            f"No se encontró el cliente de testing '{CLIENTE_TESTING}' en los datos a procesar."
        )

for _, fila in df_a_procesar.iterrows():
    cliente = str(fila["cliente"]).strip()
    mes_dt = fila["mes"]
    mes_calculado = mes_dt.strftime("%Y-%m")
    anio = mes_dt.year
    notas_adicionales = []

    columnas_obligatorias = [
        "potencia_contratada_kw",
        "generacion_kwh",
        "consumo_kwh",
        "exportacion_kwh",
        "pct_punta",
        "pct_llano",
        "pct_valle",
    ]
    faltantes = [col for col in columnas_obligatorias if pd.isna(fila.get(col))]
    if faltantes:
        print(
            f"ERROR en {cliente} ({mes_calculado}): faltan datos para generar el reporte "
            f"({', '.join(faltantes)})."
        )
        continue

    potencia = float(fila["potencia_contratada_kw"])
    generacion = float(fila["generacion_kwh"])
    consumo = float(fila["consumo_kwh"])
    exportacion = float(fila["exportacion_kwh"])
    inversion = float(fila["inversion"]) if not pd.isna(fila.get("inversion")) else 0.0
    pot_inst = float(fila["pot_inst"]) if not pd.isna(fila.get("pot_inst")) else 0.0
    fecha_inst_dt = parsear_periodo(fila.get("fecha_inst"))
    fecha_inst = formatear_periodo(fila.get("fecha_inst"))
    tipo_cliente = texto_normalizado_opcional(fila.get("tipo"), default="residencial")
    tarifa_cliente = texto_normalizado_opcional(fila.get("tarifa"))
    es_empresa = tipo_cliente == "empresa"

    pct_punta = float(fila["pct_punta"])
    pct_llano = float(fila["pct_llano"])
    pct_valle = float(fila["pct_valle"])

    historial_cliente_previo = df_clientes[
        (df_clientes["cliente"].astype(str).str.strip() == cliente) &
        (df_clientes["mes"] < mes_dt)
    ].copy()

    if not historial_cliente_previo.empty:
        consumo_promedio_historico = float(historial_cliente_previo["consumo_kwh"].mean())
        if consumo_promedio_historico > 0 and consumo > (consumo_promedio_historico * 1.30):
            aumento_pct = round(((consumo / consumo_promedio_historico) - 1) * 100, 0)
            notas_adicionales.append({
                "tipo": "alerta",
                "texto": (
                    f"Alerta: se detectó un consumo excesivo en {formatear_mes_es(mes_dt)}. "
                    f"El consumo del mes fue de {fmt(consumo)} kWh, un {fmt_pct(aumento_pct)}% por encima "
                    f"del promedio histórico previo del cliente ({fmt(consumo_promedio_historico)} kWh)."
                ),
            })

    if pct_punta > 1 or pct_llano > 1 or pct_valle > 1:
        pct_punta /= 100
        pct_llano /= 100
        pct_valle /= 100

    suma_pct = round(pct_punta + pct_llano + pct_valle, 4)
    if abs(suma_pct - 1.0) > 0.001:
        print(f"ERROR en {cliente}: los porcentajes no suman 1.0")
        continue

    fila_mes_anterior = df_clientes[
        (df_clientes["cliente"].astype(str).str.strip() == cliente) &
        (df_clientes["mes"] == (mes_dt - pd.DateOffset(months=1)))
    ]

    exportacion_mes_anterior = 0.0
    if not fila_mes_anterior.empty:
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

    energia_zafral_sin = calcular_costo_energia_triple(
        consumo, pct_punta, pct_llano, pct_valle,
        precio_punta_zafral, precio_llano_zafral, precio_valle_zafral
    )
    energia_zafral_con = calcular_costo_energia_triple(
        importacion_red, pct_punta, pct_llano, pct_valle,
        precio_punta_zafral, precio_llano_zafral, precio_valle_zafral
    )
    credito_zafral = calcular_credito_exportacion_triple(exportacion_mes_anterior, precio_llano_zafral)

    factura_zafral_sin = calcular_factura(
        cargo_fijo=cargo_fijo_zafral,
        cargo_potencia=potencia * cargo_pot_kw_zafral,
        cargo_energia=energia_zafral_sin,
        credito_exportacion=0,
    )

    factura_zafral_con = calcular_factura(
        cargo_fijo=cargo_fijo_zafral,
        cargo_potencia=potencia * cargo_pot_kw_zafral,
        cargo_energia=energia_zafral_con,
        credito_exportacion=credito_zafral,
    )

    desglose_zafral = calcular_desglose_beneficio(factura_zafral_sin, factura_zafral_con)

    ahorro_total_zafral = round(
        factura_zafral_sin["total_neto"] - factura_zafral_con["total_neto"] + factura_zafral_con["saldo_a_favor"],
        2
    )

    saldos_mes = saldos_cuenta_corriente.get((cliente, mes_calculado), {})
    factura_simple_con_ajustada = saldos_mes.get("simple", aplicar_saldo_cuenta_corriente(factura_simple_con, 0.0))
    factura_doble_con_ajustada = saldos_mes.get("doble", aplicar_saldo_cuenta_corriente(factura_doble_con, 0.0))
    factura_triple_con_ajustada = saldos_mes.get("triple", aplicar_saldo_cuenta_corriente(factura_triple_con, 0.0))
    factura_zafral_con_ajustada = saldos_mes.get("zafral", aplicar_saldo_cuenta_corriente(factura_zafral_con, 0.0))

    tarifas_datos = {
        "simple": {
            "label": etiqueta_tarifa("simple"),
            "factura_sin": factura_simple_sin,
            "factura_con_ajustada": factura_simple_con_ajustada,
            "desglose": desglose_simple,
            "ahorro_total": ahorro_total_simple,
        },
        "doble": {
            "label": etiqueta_tarifa("doble"),
            "factura_sin": factura_doble_sin,
            "factura_con_ajustada": factura_doble_con_ajustada,
            "desglose": desglose_doble,
            "ahorro_total": ahorro_total_doble,
        },
        "triple": {
            "label": etiqueta_tarifa("triple"),
            "factura_sin": factura_triple_sin,
            "factura_con_ajustada": factura_triple_con_ajustada,
            "desglose": desglose_triple,
            "ahorro_total": ahorro_total_triple,
        },
        "zafral": {
            "label": etiqueta_tarifa("zafral"),
            "factura_sin": factura_zafral_sin,
            "factura_con_ajustada": factura_zafral_con_ajustada,
            "desglose": desglose_zafral,
            "ahorro_total": ahorro_total_zafral,
        },
    }

    if es_empresa:
        if not tarifa_cliente:
            print(f"ERROR en {cliente}: el cliente empresa no tiene tarifa definida.")
            continue
        if tarifa_cliente not in tarifas_datos:
            print(f"ERROR en {cliente}: la tarifa '{fila.get('tarifa')}' no está soportada.")
            continue
        tarifas_mostradas_keys = [tarifa_cliente]
    else:
        tarifas_mostradas_keys = ["simple", "doble", "triple"]

    tarifas_mostradas = []
    for tarifa_key in tarifas_mostradas_keys:
        tarifa_data = tarifas_datos[tarifa_key]
        descuento_venta_factura = max(
            tarifa_data["desglose"]["ahorro_venta"] - tarifa_data["factura_con_ajustada"]["saldo_generado_mes"],
            0.0
        )
        tarifas_mostradas.append({
            "key": tarifa_key,
            "label": tarifa_data["label"],
            "total_sin": fmt_int(tarifa_data["factura_sin"]["total_neto"]),
            "total_con": fmt_int(tarifa_data["factura_con_ajustada"]["total_neto"]),
            "descuento_venta": fmt_int(descuento_venta_factura),
            "ahorro_autoconsumo": fmt_int(tarifa_data["desglose"]["ahorro_autoconsumo"]),
            "saldo_generado_mes": fmt_int(tarifa_data["factura_con_ajustada"]["saldo_generado_mes"]),
            "saldo_inicial": fmt_int(tarifa_data["factura_con_ajustada"]["saldo_inicial"]),
            "saldo_aplicado": fmt_int(tarifa_data["factura_con_ajustada"]["saldo_aplicado"]),
            "saldo_final": fmt_int(tarifa_data["factura_con_ajustada"]["saldo_final"]),
            "ahorro_total": fmt_int(tarifa_data["ahorro_total"]),
            "ahorro_venta": fmt_int(tarifa_data["desglose"]["ahorro_venta"]),
            "pct_autoconsumo": fmt_pct(tarifa_data["desglose"]["pct_autoconsumo"]),
            "pct_venta": fmt_pct(tarifa_data["desglose"]["pct_venta"]),
            "ahorro_total_desglose": fmt_int(tarifa_data["desglose"]["ahorro_total"]),
        })

    tarifa_principal = tarifas_datos[tarifas_mostradas_keys[0]]

    # Guardar histórico
    ahorro_total_principal_usd = round(tarifa_principal["desglose"]["ahorro_total"] / tipo_cambio_usd, 2)
    guardar_registro(
        cliente,
        mes_calculado,
        {
            "generacion": generacion,
            "consumo": consumo,
            "exportacion": exportacion,
            "autoconsumo": autoconsumo,
            "importacion": importacion_red,
            "ahorro_autoconsumo": tarifa_principal["desglose"]["ahorro_autoconsumo"],
            "ahorro_venta": tarifa_principal["desglose"]["ahorro_venta"],
            "ahorro_total": tarifa_principal["desglose"]["ahorro_total"],
            "ahorro_total_usd": ahorro_total_principal_usd,
            "saldo_inicial": tarifa_principal["factura_con_ajustada"]["saldo_inicial"],
            "saldo_aplicado": tarifa_principal["factura_con_ajustada"]["saldo_aplicado"],
            "saldo_generado_mes": tarifa_principal["factura_con_ajustada"]["saldo_generado_mes"],
            "saldo_final": tarifa_principal["factura_con_ajustada"]["saldo_final"],
            "tipo_cambio_usd": tipo_cambio_usd,
        }
    )

    acumulado = obtener_acumulado_anual(cliente, anio)

    ahorro_acumulado = 0
    ahorro_acumulado_usd = 0
    saldo_acumulado = 0
    exportacion_acumulada = 0
    importacion_acumulada = 0
    ahorro_promedio_historico = 0
    ahorro_promedio_historico_usd = 0
    meses_con_datos_usd = 0
    ratio_ute = 0
    estado_ute = "SIN DATOS"

    if acumulado:
        ahorro_acumulado = acumulado["ahorro_total"]
        ahorro_acumulado_usd = acumulado["ahorro_total_usd"]
        saldo_acumulado = acumulado["saldo_total"]
        exportacion_acumulada = acumulado["exportacion_total"]
        importacion_acumulada = acumulado["importacion_total"]
        ahorro_promedio_historico = acumulado["ahorro_promedio_historico"]
        ahorro_promedio_historico_usd = acumulado["ahorro_promedio_historico_usd"]
        meses_con_datos_usd = acumulado["meses_con_datos_usd"]

        if importacion_acumulada > 0:
            ratio_ute = round((exportacion_acumulada / importacion_acumulada) * 100, 1)

        if exportacion_acumulada <= importacion_acumulada:
            estado_ute = "OK"
        elif exportacion_acumulada <= importacion_acumulada * 1.1:
            estado_ute = "ALERTA"
        else:
            if mes_dt.month < 8:
                estado_ute = "EXCEDIDO - SIN RIESGO"
            else:
                estado_ute = "EXCEDIDO - CON RIESGO"

    retorno_inversion_pct = 0.0
    tiempo_restante_retorno = "-"
    tiempo_total_retorno = "-"
    mes_retorno_estimado = "-"
    meses_transcurridos_desde_inicio = 0
    estimacion_retorno_nota = ""

    if fecha_inst_dt is not None:
        meses_transcurridos_desde_inicio = (
            (mes_dt.year - fecha_inst_dt.year) * 12 +
            (mes_dt.month - fecha_inst_dt.month) +
            1
        )
        meses_transcurridos_desde_inicio = max(meses_transcurridos_desde_inicio, 0)

    if inversion > 0:
        retorno_inversion_pct = round((ahorro_acumulado_usd / inversion) * 100, 1)

        if ahorro_acumulado_usd >= inversion:
            tiempo_restante_retorno = "Completado"
            if meses_transcurridos_desde_inicio > 0:
                tiempo_total_retorno = formatear_duracion_meses(meses_transcurridos_desde_inicio)
            elif meses_con_datos_usd > 0:
                tiempo_total_retorno = formatear_duracion_meses(meses_con_datos_usd)
            mes_retorno_estimado = formatear_mes_es(mes_dt)
        elif ahorro_promedio_historico_usd > 0:
            faltante_usd = max(inversion - ahorro_acumulado_usd, 0.0)
            meses_adicionales = int(math.ceil(faltante_usd / ahorro_promedio_historico_usd))
            base_meses = meses_transcurridos_desde_inicio if meses_transcurridos_desde_inicio > 0 else meses_con_datos_usd
            meses_totales_retorno = base_meses + meses_adicionales
            tiempo_restante_retorno = formatear_duracion_meses(meses_adicionales)
            tiempo_total_retorno = formatear_duracion_meses(meses_totales_retorno)
            fecha_retorno = mes_dt + pd.DateOffset(months=meses_adicionales)
            mes_retorno_estimado = formatear_mes_es(fecha_retorno)
        else:
            estimacion_retorno_nota = (
                "No hay base suficiente para estimar el retorno de la inversión."
            )

        if 0 < meses_con_datos_usd < 12:
            estimacion_retorno_nota = (
                f"Aviso: la proyección se calculó con {meses_con_datos_usd} "
                f"{'mes' if meses_con_datos_usd == 1 else 'meses'} de historial útil."
            )

    roi_barra_pct = max(0.0, min(retorno_inversion_pct, 100.0))
    inversion_recuperada_usd = min(ahorro_acumulado_usd, inversion) if inversion > 0 else 0.0
    inversion_faltante_usd = max(inversion - ahorro_acumulado_usd, 0.0) if inversion > 0 else 0.0

    html = template.render(
        cliente=cliente,
        es_empresa=es_empresa,
        tarifa_contratada_label=etiqueta_tarifa(tarifa_cliente) if es_empresa else "",
        tarifas_mostradas=tarifas_mostradas,
        mes=formatear_mes_es(mes_dt),
        mes_anterior=formatear_mes_es(mes_dt - pd.DateOffset(months=1)),
        generacion=fmt(generacion),
        consumo=fmt(consumo),
        exportacion_actual=fmt(exportacion),
        exportacion_anterior=fmt(exportacion_mes_anterior),
        autoconsumo=fmt(autoconsumo),
        autoconsumo_sobre_consumo=fmt_pct(pct_consumo_autoconsumo),
        potencia=fmt(potencia),
        pot_inst=fmt_decimal(pot_inst, 2),
        inversion=fmt(inversion),
        fecha_inst=fecha_inst,
        importacion_red=fmt(importacion_red),
        pct_punta=fmt_pct(pct_punta * 100),
        pct_llano=fmt_pct(pct_llano * 100),
        pct_valle=fmt_pct(pct_valle * 100),

        consumo_bar_autoconsumo=round(pct_consumo_autoconsumo, 1),
        consumo_bar_red=round(pct_consumo_red, 1),
        consumo_bar_autoconsumo_label=fmt_int(pct_consumo_autoconsumo),
        consumo_bar_red_label=fmt_int(pct_consumo_red),
        consumo_total=fmt(consumo),
        consumo_autoconsumo_kwh=fmt(autoconsumo),
        consumo_red_kwh=fmt(importacion_red),
        generacion_bar_autoconsumo=round(pct_generacion_autoconsumo, 1),
        generacion_bar_exportada=round(pct_generacion_exportada, 1),
        generacion_bar_autoconsumo_label=fmt_int(pct_generacion_autoconsumo),
        generacion_bar_exportada_label=fmt_int(pct_generacion_exportada),
        generacion_total=fmt(generacion),
        generacion_autoconsumo_kwh=fmt(autoconsumo),
        generacion_exportada_kwh=fmt(exportacion),

        ahorro_acumulado=fmt(ahorro_acumulado),
        ahorro_acumulado_usd=fmt(ahorro_acumulado_usd),
        ahorro_promedio_historico=fmt(ahorro_promedio_historico),
        retorno_inversion_pct=fmt_pct(retorno_inversion_pct),
        roi_barra_pct=round(roi_barra_pct, 1),
        inversion_recuperada_usd=fmt(inversion_recuperada_usd),
        inversion_faltante_usd=fmt(inversion_faltante_usd),
        tiempo_restante_retorno=tiempo_restante_retorno,
        tiempo_total_retorno=tiempo_total_retorno,
        mes_retorno_estimado=mes_retorno_estimado,
        estimacion_retorno_nota=estimacion_retorno_nota,
        saldo_acumulado=fmt(saldo_acumulado),
        exportacion_acumulada=fmt(exportacion_acumulada),
        importacion_acumulada=fmt(importacion_acumulada),
        ratio_ute=fmt_pct(ratio_ute),
        estado_ute=estado_ute,
        tipo_cambio_usado=fmt_decimal(tipo_cambio_usd, 1),
        notas_adicionales=notas_adicionales,
        logo_path="../../assets/logo_voltia_completo.png",
    )

    carpeta_cliente = Path("reports") / slug_cliente(cliente)
    carpeta_html = carpeta_cliente / "html"
    carpeta_pdf = carpeta_cliente / "pdf"
    carpeta_html.mkdir(parents=True, exist_ok=True)
    carpeta_pdf.mkdir(parents=True, exist_ok=True)
    nombre_archivo = f"reporte_{slug_cliente(cliente)}_{mes_calculado}.html"
    ruta_salida = carpeta_html / nombre_archivo
    ruta_pdf = carpeta_pdf / nombre_archivo.replace(".html", ".pdf")

    with open(ruta_salida, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Reporte generado: {ruta_salida}")
    if GENERAR_PDF and generar_pdf_desde_html(ruta_salida, ruta_pdf):
        print(f"PDF generado: {ruta_pdf}")
