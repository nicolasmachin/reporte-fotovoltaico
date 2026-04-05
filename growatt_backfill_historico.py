"""
Backfill histórico de datos Growatt para clientes autorizados.

Uso básico:
    python3 growatt_backfill_historico.py --desde 2024-01

Ejemplos:
    python3 growatt_backfill_historico.py --desde 2023-01 --hasta 2026-03
    python3 growatt_backfill_historico.py --desde 2024-01 --actualizar-datos

El script:
    1. Lee las plantas autorizadas desde data/growatt_allowlist.csv
    2. Trae generación mensual histórica por planta desde Growatt Open API
    3. Intenta calcular consumo/exportación mensual usando smart meter
    4. Genera un Excel auxiliar con todas las filas históricas
    5. Opcionalmente actualiza la hoja "datos" del Excel base
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import growattServer
import requests

from growatt_clientes import (
    DEFAULT_ALLOWLIST_PATH,
    DEFAULT_API_TOKEN,
    DEFAULT_PAUSA_SEG,
    DEFAULT_REINTENTOS,
    DEFAULT_XLSX_DATOS,
    actualizar_hoja_datos,
    a_float,
    cargar_allowlist,
    describir_metricas_faltantes_smart_meter,
    guardar_excel_auxiliar,
    obtener_nombre_salida,
    obtener_dataloggers_planta,
    obtener_metricas_mes_desde_smart_meter,
    obtener_todas_las_plantas_openapi,
    ultimo_dia_mes,
)


@dataclass
class BackfillConfig:
    api_token: str
    desde: str
    hasta: str
    pausa_seg: float
    reintentos: int
    salida: Path | None
    actualizar_datos: bool
    xlsx_datos: Path
    allowlist_path: Path
    chunk_meses: int


def parse_args() -> BackfillConfig:
    parser = argparse.ArgumentParser(
        description="Recaba el histórico mensual de Growatt para todas las plantas autorizadas."
    )
    parser.add_argument(
        "--desde",
        default=os.environ.get("GROWATT_HISTORY_START", "2024-01"),
        help='Mes inicial del backfill en formato "YYYY-MM".',
    )
    parser.add_argument(
        "--hasta",
        default=os.environ.get("GROWATT_HISTORY_END", "anterior"),
        help='Mes final del backfill en formato "YYYY-MM" o "anterior".',
    )
    parser.add_argument(
        "--pausa",
        type=float,
        default=float(os.environ.get("GROWATT_PAUSE_SECONDS", DEFAULT_PAUSA_SEG)),
        help="Pausa entre requests pesadas a Growatt.",
    )
    parser.add_argument(
        "--reintentos",
        type=int,
        default=int(os.environ.get("GROWATT_RETRIES", DEFAULT_REINTENTOS)),
        help="Reintentos por bloque o planta.",
    )
    parser.add_argument(
        "--chunk-meses",
        type=int,
        default=int(os.environ.get("GROWATT_HISTORY_CHUNK_MONTHS", "12")),
        help="Cantidad de meses por request de histórico de generación.",
    )
    parser.add_argument(
        "--salida",
        type=Path,
        default=None,
        help="Ruta del Excel auxiliar histórico.",
    )
    parser.add_argument(
        "--actualizar-datos",
        action="store_true",
        help='Actualiza la hoja "datos" del archivo base con todo el histórico recabado.',
    )
    parser.add_argument(
        "--xlsx-datos",
        type=Path,
        default=DEFAULT_XLSX_DATOS,
        help='Ruta al archivo Excel que contiene la hoja "datos".',
    )
    parser.add_argument(
        "--allowlist",
        type=Path,
        default=DEFAULT_ALLOWLIST_PATH,
        help="CSV con plantas autorizadas. La columna obligatoria es plant_id.",
    )

    args = parser.parse_args()
    api_token = os.environ.get("GROWATT_API_TOKEN", DEFAULT_API_TOKEN).strip()
    if not api_token:
        raise SystemExit("Falta GROWATT_API_TOKEN para correr el backfill histórico.")

    return BackfillConfig(
        api_token=api_token,
        desde=args.desde,
        hasta=args.hasta,
        pausa_seg=args.pausa,
        reintentos=args.reintentos,
        salida=args.salida,
        actualizar_datos=args.actualizar_datos,
        xlsx_datos=args.xlsx_datos,
        allowlist_path=args.allowlist,
        chunk_meses=max(1, args.chunk_meses),
    )


def mes_a_fecha(valor: str) -> date:
    valor = valor.strip().lower()
    if valor == "anterior":
        hoy = date.today()
        anio = hoy.year if hoy.month > 1 else hoy.year - 1
        mes = hoy.month - 1 if hoy.month > 1 else 12
        return date(anio, mes, 1)
    if valor == "actual":
        hoy = date.today()
        return date(hoy.year, hoy.month, 1)
    return datetime.strptime(valor, "%Y-%m").date()


def sumar_meses(fecha: date, cantidad: int) -> date:
    total = (fecha.year * 12 + fecha.month - 1) + cantidad
    year = total // 12
    month = total % 12 + 1
    return date(year, month, 1)


def iterar_meses(desde: date, hasta: date) -> list[date]:
    meses = []
    actual = date(desde.year, desde.month, 1)
    limite = date(hasta.year, hasta.month, 1)
    while actual <= limite:
        meses.append(actual)
        actual = sumar_meses(actual, 1)
    return meses


def obtener_generacion_historica_directa(
    api_token: str,
    plant_id: int,
    desde: date,
    hasta: date,
    chunk_meses: int,
    reintentos: int,
) -> dict[str, float]:
    resultados: dict[str, float] = {}
    inicio = date(desde.year, desde.month, 1)
    fin_total = date(hasta.year, hasta.month, 1)

    while inicio <= fin_total:
        fin_chunk = sumar_meses(inicio, chunk_meses - 1)
        if fin_chunk > fin_total:
            fin_chunk = fin_total

        ultimo_error = None
        for intento in range(1, reintentos + 1):
            try:
                response = requests.get(
                    "https://openapi.growatt.com/v1/plant/energy",
                    headers={"Token": api_token},
                    params={
                        "plant_id": plant_id,
                        "start_date": inicio.strftime("%Y-%m-%d"),
                        "end_date": fin_chunk.strftime("%Y-%m-%d"),
                        "time_unit": "month",
                    },
                    timeout=60,
                )
                response.raise_for_status()
                payload = response.json()
                if payload.get("error_code") != 0:
                    raise RuntimeError(
                        f"Growatt devolvió error_code={payload.get('error_code')} error_msg={payload.get('error_msg')}"
                    )

                for item in payload.get("data", {}).get("energys", []):
                    mes = str(item.get("date", "")).strip()
                    energia = a_float(item.get("energy"))
                    if mes and energia is not None:
                        resultados[mes] = energia

                ultimo_error = None
                break
            except Exception as exc:  # noqa: BLE001
                ultimo_error = exc
                if intento < reintentos:
                    time.sleep(min(1.5 * intento, 6.0))

        if ultimo_error is not None:
            raise ultimo_error

        inicio = sumar_meses(fin_chunk, 1)

    return resultados


def obtener_filas_historicas(config: BackfillConfig) -> list[dict]:
    desde = mes_a_fecha(config.desde)
    hasta = mes_a_fecha(config.hasta)
    if desde > hasta:
        raise ValueError("El mes inicial no puede ser posterior al mes final.")

    print(f"\nConectando a Growatt Open API para histórico {desde.strftime('%Y-%m')} -> {hasta.strftime('%Y-%m')}...")
    api = growattServer.OpenApiV1(config.api_token)
    plantas = obtener_todas_las_plantas_openapi(api)

    allowlist_ids = cargar_allowlist(config.allowlist_path)
    if allowlist_ids is None:
        raise RuntimeError(
            f"No existe la allowlist {config.allowlist_path}. Creala con una columna 'plant_id'."
        )
    if not allowlist_ids:
        raise RuntimeError(f"La allowlist {config.allowlist_path} está vacía.")

    plantas = [planta for planta in plantas if int(planta.get("plant_id")) in allowlist_ids]
    print(f"Plantas autorizadas por allowlist: {len(plantas)}")
    if not plantas:
        return []

    filas: list[dict] = []
    for planta in plantas:
        plant_id = int(planta.get("plant_id"))
        cliente = str(planta.get("name") or "Sin nombre").strip()
        print(f"\nProcesando {cliente} ({plant_id})...")
        dataloggers_planta = None

        try:
            generaciones = obtener_generacion_historica_directa(
                config.api_token,
                plant_id,
                desde,
                hasta,
                config.chunk_meses,
                max(config.reintentos, 5),
            )
        except Exception as exc:  # noqa: BLE001
            print(f"  ERROR al obtener generación histórica: {exc}")
            continue

        meses_disponibles = sorted(generaciones.keys())
        print(f"  Meses con generación: {len(meses_disponibles)}")

        try:
            dataloggers_planta = obtener_dataloggers_planta(api, plant_id)
            if not dataloggers_planta:
                print("  AVISO: planta sin smart meter/datalogger visible en Open API.")
        except Exception as exc:  # noqa: BLE001
            print(f"  AVISO: no se pudo leer device_list una vez para toda la planta: {exc}")

        for mes in meses_disponibles:
            fecha_mes = datetime.strptime(mes, "%Y-%m").date()
            fecha_fin = ultimo_dia_mes(fecha_mes)
            generacion_kwh = generaciones[mes]
            aviso_metricas = None

            try:
                metricas = obtener_metricas_mes_desde_smart_meter(
                    api,
                    plant_id,
                    fecha_mes,
                    fecha_fin,
                    generacion_kwh,
                    dataloggers=dataloggers_planta,
                )
            except Exception as exc:  # noqa: BLE001
                aviso_metricas = f"error al consultar smart meter: {exc}"
                metricas = {
                    "generacion_kwh": generacion_kwh,
                    "consumo_kwh": None,
                    "exportacion_kwh": None,
                }

            if metricas.get("consumo_kwh") is None or metricas.get("exportacion_kwh") is None:
                if aviso_metricas is None:
                    if dataloggers_planta == []:
                        aviso_metricas = "la planta no tiene smart meter/datalogger tipo 3 visible en Open API"
                    elif dataloggers_planta:
                        aviso_metricas = (
                            "smart meter visible, pero sin muestras históricas útiles "
                            f"para esa planta/mes ({len(dataloggers_planta)} datalogger/s)"
                        )
                    else:
                        aviso_metricas = describir_metricas_faltantes_smart_meter(api, plant_id)

            filas.append(
                {
                    "id_planta": plant_id,
                    "cliente": cliente,
                    "mes": mes,
                    "generacion_kwh": metricas.get("generacion_kwh", generacion_kwh),
                    "consumo_kwh": metricas.get("consumo_kwh"),
                    "exportacion_kwh": metricas.get("exportacion_kwh"),
                    "potencia_nominal_kw": a_float(planta.get("peak_power")),
                    "estado": planta.get("status", "N/D"),
                    "ultimo_dato": "",
                }
            )
            print(
                f"  OK {mes}: gen={metricas.get('generacion_kwh', generacion_kwh)} "
                f"cons={metricas.get('consumo_kwh')} exp={metricas.get('exportacion_kwh')}"
            )
            if aviso_metricas is not None:
                print(f"     AVISO {mes}: {aviso_metricas}")
            time.sleep(config.pausa_seg)

    return filas


def obtener_nombre_salida_historica(desde: date, hasta: date) -> Path:
    return Path(f"data/growatt_historico_{desde.strftime('%Y-%m')}_a_{hasta.strftime('%Y-%m')}.xlsx")


def guardar_excels_mensuales_historicos(filas: list[dict]) -> list[Path]:
    filas_por_mes: dict[str, list[dict]] = {}
    for fila in filas:
        mes = str(fila.get("mes") or "").strip()
        if not mes:
            continue
        filas_por_mes.setdefault(mes, []).append(fila)

    archivos_generados: list[Path] = []
    for mes in sorted(filas_por_mes.keys()):
        fecha_mes = datetime.strptime(mes, "%Y-%m").date()
        destino_mes = obtener_nombre_salida(fecha_mes)
        guardar_excel_auxiliar(filas_por_mes[mes], fecha_mes, destino_mes)
        archivos_generados.append(destino_mes)

    return archivos_generados


def main() -> int:
    config = parse_args()
    fecha_desde = mes_a_fecha(config.desde)
    fecha_hasta = mes_a_fecha(config.hasta)
    destino = config.salida or obtener_nombre_salida_historica(fecha_desde, fecha_hasta)

    filas = obtener_filas_historicas(config)
    if not filas:
        print("No hubo datos históricos para exportar.")
        return 0

    guardar_excel_auxiliar(filas, fecha_hasta, destino)
    archivos_mensuales = guardar_excels_mensuales_historicos(filas)
    print(f"Auxiliares mensuales generados: {len(archivos_mensuales)}")

    if config.actualizar_datos:
        actualizados, insertados = actualizar_hoja_datos(config.xlsx_datos, filas)
        print(
            f'Hoja "datos" actualizada en {config.xlsx_datos}: '
            f"{actualizados} registros actualizados, {insertados} registros nuevos."
        )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nProceso cancelado por el usuario.")
        raise SystemExit(130)
    except Exception as exc:  # noqa: BLE001
        print(f"\nError: {exc}", file=sys.stderr)
        raise SystemExit(1)
