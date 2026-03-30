def calcular_autoconsumo(generacion, exportacion):
    return max(float(generacion) - float(exportacion), 0.0)


def calcular_importacion_red(consumo, generacion, exportacion):
    autoconsumo = calcular_autoconsumo(generacion, exportacion)
    return max(float(consumo) - autoconsumo, 0.0)


def calcular_cobertura(consumo, autoconsumo):
    consumo = float(consumo)
    if consumo <= 0:
        return 0.0
    return round((float(autoconsumo) / consumo) * 100, 1)


def calcular_costo_energia_simple(kwh, tramos):
    restante = float(kwh)
    costo = 0.0

    for tramo in tramos:
        desde = float(tramo["desde_kwh"])
        hasta = float(tramo["hasta_kwh"])
        precio = float(tramo["precio_kwh"])

        capacidad_tramo = hasta - desde + 1

        if restante <= 0:
            break

        consumo_en_tramo = min(restante, capacidad_tramo)
        costo += consumo_en_tramo * precio
        restante -= consumo_en_tramo

    return round(costo, 2)


def calcular_costo_energia_doble(kwh_total, pct_punta, precio_punta, precio_fuera_punta):
    kwh_total = float(kwh_total)
    pct_punta = float(pct_punta)

    kwh_punta = kwh_total * pct_punta
    kwh_fuera_punta = kwh_total - kwh_punta

    costo = (kwh_punta * float(precio_punta)) + (kwh_fuera_punta * float(precio_fuera_punta))
    return round(costo, 2)


def calcular_costo_energia_triple(
    kwh_total,
    pct_punta,
    pct_llano,
    pct_valle,
    precio_punta,
    precio_llano,
    precio_valle,
):
    kwh_total = float(kwh_total)

    kwh_punta = kwh_total * float(pct_punta)
    kwh_llano = kwh_total * float(pct_llano)
    kwh_valle = kwh_total * float(pct_valle)

    costo = (
        (kwh_punta * float(precio_punta)) +
        (kwh_llano * float(precio_llano)) +
        (kwh_valle * float(precio_valle))
    )
    return round(costo, 2)


def calcular_credito_exportacion_simple(kwh_exportados_mes_anterior, tramos):
    return calcular_costo_energia_simple(kwh_exportados_mes_anterior, tramos)


def calcular_credito_exportacion_doble(kwh_exportados_mes_anterior, precio_fuera_punta):
    # Supuesto: toda la exportación ocurre fuera de punta
    return round(float(kwh_exportados_mes_anterior) * float(precio_fuera_punta), 2)


def calcular_credito_exportacion_triple(kwh_exportados_mes_anterior, precio_llano):
    # Supuesto: toda la exportación ocurre en llano
    return round(float(kwh_exportados_mes_anterior) * float(precio_llano), 2)


def calcular_factura(
    cargo_fijo,
    cargo_potencia,
    cargo_energia,
    credito_exportacion=0.0,
    tasa_iva=0.22,
    tasa_irpf=0.12,
):
    cargo_fijo = float(cargo_fijo)
    cargo_potencia = float(cargo_potencia)
    cargo_energia = float(cargo_energia)
    credito_exportacion = float(credito_exportacion)

    # Solo potencia + energía gravan IVA
    base_gravada = cargo_potencia + cargo_energia
    iva = round(base_gravada * tasa_iva, 2)

    # La energía vendida no grava IVA, pero sí IRPF
    irpf = round(credito_exportacion * tasa_irpf, 2)

    total_bruto = cargo_fijo + cargo_potencia + cargo_energia + iva
    total_neto = round(total_bruto + irpf - credito_exportacion, 2)

    saldo_a_favor = 0.0
    if total_neto < 0:
        saldo_a_favor = round(abs(total_neto), 2)
        total_neto = 0.0

    return {
        "cargo_fijo": round(cargo_fijo, 2),
        "cargo_potencia": round(cargo_potencia, 2),
        "cargo_energia": round(cargo_energia, 2),
        "base_gravada": round(base_gravada, 2),
        "iva": iva,
        "credito_exportacion": round(credito_exportacion, 2),
        "irpf": irpf,
        "total_bruto": round(total_bruto, 2),
        "total_neto": total_neto,
        "saldo_a_favor": saldo_a_favor,
    }


def calcular_desglose_beneficio(factura_sin, factura_con):
    """
    Descompone el ahorro total en:
    - ahorro por autoconsumo
    - ahorro por venta a UTE

    ahorro_total = ahorro_autoconsumo + ahorro_venta
    """

    ahorro_autoconsumo = round(
        float(factura_sin["total_neto"]) - float(factura_con["total_bruto"]),
        2
    )

    ahorro_venta = round(
        float(factura_con["credito_exportacion"]) - float(factura_con["irpf"]),
        2
    )

    ahorro_total = round(
        ahorro_autoconsumo + ahorro_venta,
        2
    )

    pct_autoconsumo = 0.0
    pct_venta = 0.0

    if ahorro_total > 0:
        pct_autoconsumo = round((ahorro_autoconsumo / ahorro_total) * 100, 1)
        pct_venta = round((ahorro_venta / ahorro_total) * 100, 1)

    return {
        "ahorro_autoconsumo": ahorro_autoconsumo,
        "ahorro_venta": ahorro_venta,
        "ahorro_total": ahorro_total,
        "pct_autoconsumo": pct_autoconsumo,
        "pct_venta": pct_venta,
    }