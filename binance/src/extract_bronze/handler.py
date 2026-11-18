"""
==============================================================================
Lambda: extract_bronze (Binance BTCUSDT Klines)
==============================================================================
Descripción:
    Función Lambda que extrae datos de velas (klines/candlestick) del par
    BTCUSDT desde la API pública de Binance y los almacena en la capa Bronze
    del Data Lake en formato JSON crudo.

Trigger:
    Amazon EventBridge - Regla programada diaria a las 00:05 UTC.
    Esto asegura que la vela del día anterior ya esté cerrada y disponible.

Destino S3:
    s3://{BUCKET}/bronze/year=YYYY/month=MM/BTCUSDT_YYYYMMDD.json

Idempotencia:
    Si se ejecuta múltiples veces para el mismo día, sobrescribe el mismo
    archivo en S3 (PUT es idempotente). No genera duplicados.

API de Binance:
    GET https://api.binance.com/api/v3/klines
    Parámetros: symbol, interval, limit, startTime, endTime (UTC en ms).
    Respuesta: Array de arrays con formato:
    [[open_time, open, high, low, close, volume, close_time,
      quote_asset_volume, number_of_trades, taker_buy_base_volume,
      taker_buy_quote_volume, ignore]]

Variables de entorno:
    - BUCKET_NAME: Nombre del bucket S3 destino
    - SYMBOL: Par de trading (default: BTCUSDT)
    - INTERVAL: Intervalo de las velas (default: 1d)

Autor: Equipo MarketMatrix - Proyecto de Modernización de Datos
Cliente: MarketMatrix (fondo de trading cuantitativo)
==============================================================================
"""

import json
import os
import logging
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuración del logger para CloudWatch
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Cliente de AWS S3
# ---------------------------------------------------------------------------
s3_client = boto3.client("s3")

# ---------------------------------------------------------------------------
# Constantes y variables de entorno
# ---------------------------------------------------------------------------
BUCKET_NAME = os.environ.get("BUCKET_NAME")
SYMBOL = os.environ.get("SYMBOL", "BTCUSDT")
INTERVAL = os.environ.get("INTERVAL", "1d")
BINANCE_BASE_URL = "https://api.binance.com/api/v3/klines"
API_MAX_ATTEMPTS = 4
API_RETRY_BASE_SECONDS = 1

# Nombres de los campos de una vela de Binance (para documentación y mapeo)
KLINE_FIELDS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]


def _get_yesterday_date():
    """
    Obtiene la fecha de ayer en UTC.

    Retorna:
        datetime: Objeto datetime representando el día anterior en UTC.

    Nota:
        Usamos UTC porque Binance trabaja con timestamps en UTC y la regla
        de EventBridge está configurada a las 00:05 UTC, momento en que
        la vela del día anterior ya cerró.
    """
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    return datetime.combine(yesterday, datetime.min.time(), timezone.utc)


def _build_api_url(symbol, interval, target_date, limit=1):
    """
    Construye la URL de la API de Binance para obtener klines.

    Args:
        symbol (str): Par de trading (ej: BTCUSDT).
        interval (str): Intervalo de la vela (ej: 1d, 1h, 15m).
        target_date (datetime): Fecha UTC de apertura de la vela solicitada.
        limit (int): Número de velas a obtener (default: 1).

    Retorna:
        str: URL completa con parámetros de consulta.

    Ejemplo:
        >>> _build_api_url("BTCUSDT", "1d", datetime(2024, 3, 15, tzinfo=timezone.utc))
        URL limitada al día UTC solicitado con startTime y endTime.
    """
    start_of_day = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        tzinfo=timezone.utc,
    )
    end_of_day = start_of_day + timedelta(days=1) - timedelta(milliseconds=1)
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
        "startTime": int(start_of_day.timestamp() * 1000),
        "endTime": int(end_of_day.timestamp() * 1000),
    }
    return f"{BINANCE_BASE_URL}?{urlencode(params)}"


def _call_binance_api(url):
    """
    Realiza una petición GET a la API pública de Binance.

    Args:
        url (str): URL completa de la API con parámetros.

    Retorna:
        list: Respuesta JSON parseada (array de arrays con datos de klines).

    Raises:
        urllib.error.URLError: Si hay un error de conexión.
        urllib.error.HTTPError: Si la API responde con un código de error.
        json.JSONDecodeError: Si la respuesta no es JSON válido.

    Nota:
        Usamos urllib.request en lugar de 'requests' para evitar
        dependencias externas adicionales en la Lambda. urllib viene
        incluido en la biblioteca estándar de Python.
    """
    logger.info("Llamando a la API pública de Binance")

    for attempt in range(API_MAX_ATTEMPTS):
        req = urllib.request.Request(
            url, headers={"User-Agent": "MarketMatrix-DataPipeline/1.0"}
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                status_code = response.getcode()
                raw_data = response.read().decode("utf-8")
                logger.info(
                    "Respuesta de Binance - Status: %s, Bytes: %d",
                    status_code,
                    len(raw_data),
                )
                return json.loads(raw_data)
        except urllib.error.HTTPError as error:
            retryable = error.code == 429 or error.code >= 500
            if not retryable or attempt == API_MAX_ATTEMPTS - 1:
                logger.error(
                    "Error HTTP al llamar a Binance: %s - %s",
                    error.code,
                    error.reason,
                )
                raise
        except urllib.error.URLError as error:
            if attempt == API_MAX_ATTEMPTS - 1:
                logger.error("Error de conexión con Binance: %s", error.reason)
                raise

        delay = API_RETRY_BASE_SECONDS * (2 ** attempt)
        logger.warning(
            "Reintentando API de Binance (%d/%d) en %d s",
            attempt + 1,
            API_MAX_ATTEMPTS - 1,
            delay,
        )
        time.sleep(delay)

    raise RuntimeError("Se agotaron los intentos para consultar Binance")


def _validate_daily_kline(raw_klines, target_date):
    """Rechaza respuestas que no contienen la vela diaria cerrada solicitada."""
    if not isinstance(raw_klines, list) or len(raw_klines) != 1:
        raise ValueError(
            f"Se esperaba exactamente una vela diaria; se recibieron "
            f"{len(raw_klines) if isinstance(raw_klines, list) else 'datos inválidos'}."
        )

    kline = raw_klines[0]
    if not isinstance(kline, list) or len(kline) != len(KLINE_FIELDS):
        raise ValueError(
            f"La vela debe contener exactamente {len(KLINE_FIELDS)} campos."
        )

    try:
        open_time_ms = int(kline[0])
        close_time_ms = int(kline[6])
    except (TypeError, ValueError) as error:
        raise ValueError("open_time y close_time deben ser timestamps válidos.") from error

    expected_open = int(target_date.timestamp() * 1000)
    expected_close = expected_open + 86_400_000 - 1
    if open_time_ms != expected_open:
        raise ValueError(
            f"Binance devolvió open_time={open_time_ms}; se esperaba "
            f"{expected_open} para {target_date.date()} UTC."
        )
    if close_time_ms != expected_close:
        raise ValueError(
            f"Binance devolvió close_time={close_time_ms}; se esperaba "
            f"{expected_close} para la vela diaria completa."
        )
    if close_time_ms >= int(datetime.now(timezone.utc).timestamp() * 1000):
        raise ValueError(
            f"La vela de {target_date.date()} UTC aún no ha cerrado."
        )


def _build_s3_key(symbol, target_date):
    """
    Construye la clave (key) de S3 con particionamiento por año y mes.

    Args:
        symbol (str): Par de trading (ej: BTCUSDT).
        target_date (datetime): Fecha de los datos.

    Retorna:
        str: Clave S3 con formato:
             bronze/year=YYYY/month=MM/BTCUSDT_YYYYMMDD.json

    Ejemplo:
        >>> _build_s3_key("BTCUSDT", datetime(2024, 3, 15))
        'bronze/year=2024/month=03/BTCUSDT_20240315.json'
    """
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    date_str = target_date.strftime("%Y%m%d")

    key = f"bronze/year={year}/month={month}/{symbol}_{date_str}.json"
    return key


def _add_audit_metadata(raw_klines, symbol, interval, api_url):
    """
    Agrega metadatos de auditoría al registro crudo de Binance.

    Los metadatos de auditoría son esenciales para la trazabilidad del dato:
    - ¿Cuándo se ingirió?
    - ¿De dónde vino?
    - ¿Qué parámetros se usaron?

    Args:
        raw_klines (list): Datos crudos de klines de Binance.
        symbol (str): Par de trading consultado.
        interval (str): Intervalo de velas consultado.
        api_url (str): URL exacta que se consultó.

    Retorna:
        dict: Diccionario con los datos crudos y metadatos de auditoría.
    """
    audit_record = {
        # === Datos crudos sin modificar (preservación del dato original) ===
        "raw_klines": raw_klines,
        # === Metadatos de auditoría ===
        "audit": {
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "source_endpoint": api_url,
            "symbol": symbol,
            "interval": interval,
            "record_count": len(raw_klines),
            "pipeline_version": "1.0.0",
            "lambda_function": "extract_bronze",
        },
    }
    return audit_record


def _save_to_s3(bucket, key, data):
    """
    Guarda los datos en S3 como JSON con formato legible.

    Args:
        bucket (str): Nombre del bucket S3.
        key (str): Clave (ruta) del objeto en S3.
        data (dict): Datos a guardar (serializables a JSON).

    Raises:
        ClientError: Si hay un error al escribir en S3.

    Nota:
        - ensure_ascii=False permite caracteres especiales (acentos, etc.)
        - indent=2 hace el JSON legible para debugging y auditoría
        - PUT en S3 es idempotente: si el archivo ya existe, se sobrescribe
    """
    try:
        json_body = json.dumps(data, ensure_ascii=False, indent=2)

        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_body.encode("utf-8"),
            ContentType="application/json",
            # Metadatos útiles para exploración desde la consola de S3
            Metadata={
                "pipeline": "binance-bronze",
                "symbol": SYMBOL,
                "created_by": "extract_bronze_lambda",
            },
        )

        logger.info(
            f"Datos guardados exitosamente en s3://{bucket}/{key} "
            f"({len(json_body)} bytes)"
        )

    except ClientError as e:
        logger.error(f"Error al guardar en S3: {e.response['Error']['Message']}")
        raise


def lambda_handler(event, context):
    """
    Función principal de la Lambda - Punto de entrada.

    Orquesta el proceso de extracción de datos de Binance:
    1. Determina la fecha objetivo (ayer)
    2. Llama a la API de Binance para obtener la vela diaria
    3. Agrega metadatos de auditoría
    4. Guarda el JSON crudo en la capa Bronze de S3

    Args:
        event (dict): Evento de EventBridge. Contiene metadata del trigger.
                      Puede incluir 'target_date' para reprocesamiento manual:
                      {"target_date": "2024-03-15"}
        context (LambdaContext): Contexto de ejecución de Lambda.
                                 Contiene request_id, function_name, etc.

    Retorna:
        dict: Resultado de la ejecución.

    Ejemplo de evento EventBridge (automático):
        {
            "version": "0",
            "source": "aws.events",
            "detail-type": "Scheduled Event",
            ...
        }

    Ejemplo de evento manual (para reprocesamiento):
        {
            "target_date": "2024-03-15"
        }
    """
    logger.info("=" * 60)
    logger.info("INICIO - Extracción Bronze de Binance")
    logger.info(f"Request ID: {context.aws_request_id}")
    logger.info(f"Función: {context.function_name}")
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # 1. Validar variables de entorno
    # ------------------------------------------------------------------
    if not BUCKET_NAME:
        raise RuntimeError("Variable de entorno BUCKET_NAME no configurada")

    # ------------------------------------------------------------------
    # 2. Determinar la fecha objetivo
    #    - Por defecto: ayer (la vela cerrada más reciente)
    #    - Se puede pasar 'target_date' en el evento para reprocesar
    # ------------------------------------------------------------------
    if "target_date" in event:
        # Modo reprocesamiento: fecha específica pasada en el evento
        try:
            target_date = datetime.strptime(
                event["target_date"], "%Y-%m-%d"
            ).replace(tzinfo=timezone.utc)
        except (TypeError, ValueError) as error:
            raise ValueError(
                "target_date debe tener formato YYYY-MM-DD."
            ) from error
        logger.info(f"Modo reprocesamiento - Fecha objetivo: {target_date.date()}")
    else:
        # Modo normal: obtener datos de ayer
        target_date = _get_yesterday_date()
        logger.info(f"Modo automático - Fecha objetivo (ayer): {target_date.date()}")

    # ------------------------------------------------------------------
    # 3. Llamar a la API de Binance
    # ------------------------------------------------------------------
    api_url = _build_api_url(SYMBOL, INTERVAL, target_date, limit=1)
    logger.info(f"Símbolo: {SYMBOL}, Intervalo: {INTERVAL}")

    raw_klines = _call_binance_api(api_url)
    _validate_daily_kline(raw_klines, target_date)

    logger.info(
        f"Velas recibidas: {len(raw_klines)} | "
        f"Campos por vela: {len(raw_klines[0])}"
    )

    # ------------------------------------------------------------------
    # 4. Agregar metadatos de auditoría
    # ------------------------------------------------------------------
    record = _add_audit_metadata(raw_klines, SYMBOL, INTERVAL, api_url)

    # ------------------------------------------------------------------
    # 5. Guardar en S3 (capa Bronze)
    # ------------------------------------------------------------------
    s3_key = _build_s3_key(SYMBOL, target_date)
    logger.info(f"Destino S3: s3://{BUCKET_NAME}/{s3_key}")

    _save_to_s3(BUCKET_NAME, s3_key, record)

    # ------------------------------------------------------------------
    # 6. Respuesta exitosa
    # ------------------------------------------------------------------
    response = {
        "status": "success",
        "message": "Extracción Bronze completada exitosamente",
        "symbol": SYMBOL,
        "target_date": str(target_date.date()),
        "s3_key": s3_key,
        "records_extracted": len(raw_klines),
        "bucket": BUCKET_NAME,
    }

    logger.info(f"ÉXITO - Datos guardados en s3://{BUCKET_NAME}/{s3_key}")
    logger.info("=" * 60)
    logger.info("FIN - Extracción Bronze de Binance")
    logger.info("=" * 60)

    return response
