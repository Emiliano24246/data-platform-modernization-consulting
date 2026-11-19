import io
"""
==============================================================================
Lambda: transform_silver (Binance BTCUSDT - Indicadores Técnicos)
==============================================================================
Descripción:
    Función Lambda que transforma los datos crudos de la capa Bronze a la
    capa Silver, aplicando validación de esquema y calculando indicadores
    técnicos de análisis cuantitativo:
      - RSI (Relative Strength Index) de 14 períodos
      - Bandas de Bollinger (20 períodos, 2 desviaciones estándar)
      - MACD (Moving Average Convergence Divergence) (12, 26, 9)

Trigger:
    Evento S3 cuando se crea un objeto con prefijo 'bronze/'.

Flujo de datos:
    1. Lee los JSON crudos de Bronze en orden cronológico
    2. Valida las velas antes de calcular indicadores
    3. Si una vela es inválida → la guarda en cuarentena y aborta
    4. Recalcula la serie desde Bronze para que el resultado no dependa
       del orden en que lleguen las notificaciones S3
    5. En una ejecución normal guarda la fecha solicitada; en backfill
       publica todas las fechas y emite una sola señal de actualización Gold

Destino S3:
    - Válidos:  s3://{BUCKET}/silver/year=YYYY/month=MM/BTCUSDT_YYYYMMDD.parquet
    - Inválidos: s3://{BUCKET}/cuarentena/year=YYYY/month=MM/BTCUSDT_YYYYMMDD_quarantine.json

Idempotencia:
    Sobrescribe el mismo archivo parquet si se ejecuta múltiples veces
    para la misma fecha. No genera duplicados.

Variables de entorno:
    - BUCKET_NAME: Nombre del bucket S3

Dependencias:
    - pandas (incluido en Lambda Layer)
    - boto3 (incluido en el runtime de Lambda)

Autor: Equipo MarketMatrix - Proyecto de Modernización de Datos
Cliente: MarketMatrix (fondo de trading cuantitativo)
==============================================================================
"""

import json
import os
import math
import re
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import pandas as pd

# ---------------------------------------------------------------------------
# Configuración del logger para CloudWatch
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Clientes de AWS
# ---------------------------------------------------------------------------
s3_client = boto3.client("s3")

# ---------------------------------------------------------------------------
# Variables de entorno y constantes
# ---------------------------------------------------------------------------
BUCKET_NAME = os.environ.get("BUCKET_NAME")

# Nombres de los campos de una vela de Binance en orden
KLINE_FIELDS = [
    "open_time",    # 0:  Timestamp de apertura (ms)
    "open",         # 1:  Precio de apertura
    "high",         # 2:  Precio máximo
    "low",          # 3:  Precio mínimo
    "close",        # 4:  Precio de cierre
    "volume",       # 5:  Volumen en moneda base
    "close_time",   # 6:  Timestamp de cierre (ms)
    "quote_asset_volume", # 7:  Volumen en moneda cotizada
    "number_of_trades",  # 8:  Número de operaciones
    "taker_buy_base_volume",   # 9:  Volumen de compra del taker (base)
    "taker_buy_quote_volume",  # 10: Volumen de compra del taker (cotizada)
    "ignore",       # 11: Campo ignorado por Binance
]

# Campos OHLCV que deben ser numéricos y positivos
REQUIRED_OHLCV = ["open", "high", "low", "close", "volume"]

# Parámetros de indicadores técnicos
RSI_PERIOD = 14
BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
RSI_SEED_METHOD = "SMA de los primeros 14 cambios; después suavizado de Wilder"
BOLLINGER_DDOF = 0
MACD_EMA_SEED_METHOD = "SMA de cada periodo EMA; después suavizado recursivo"
DEFAULT_SYMBOL = os.environ.get("SYMBOL", "BTCUSDT")
GOLD_READY_PREFIX = "control/gold-ready/"


# ===========================================================================
# FUNCIONES DE LECTURA Y PARSEO
# ===========================================================================

def _extract_s3_info(event):
    """
    Extrae el bucket y la clave del objeto del evento S3.

    Args:
        event (dict): Evento S3 que contiene la información del objeto creado.

    Retorna:
        tuple: (bucket_name, object_key) con los datos del objeto S3.

    Nota:
        La clave del objeto viene URL-encoded, por lo que necesitamos
        decodificarla con unquote_plus para manejar caracteres especiales.
    """
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key = unquote_plus(record["object"]["key"])
    return bucket, key


def _read_bronze_json(bucket, key):
    """
    Lee el archivo JSON crudo de la capa Bronze desde S3.

    Args:
        bucket (str): Nombre del bucket S3.
        key (str): Clave del objeto JSON en S3.

    Retorna:
        dict: Contenido del JSON parseado.

    Raises:
        ClientError: Si el objeto no existe o no se puede leer.
        json.JSONDecodeError: Si el contenido no es JSON válido.
    """
    logger.info(f"Leyendo archivo Bronze: s3://{bucket}/{key}")

    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    data = json.loads(content)

    logger.info(f"Archivo Bronze leído exitosamente ({len(content)} bytes)")
    return data


def _parse_kline_to_dict(kline_array):
    """
    Convierte un array de kline de Binance a un diccionario con nombres.

    La API de Binance retorna las velas como arrays posicionales:
    [open_time, open, high, low, close, volume, ...]

    Esta función los convierte a un diccionario con claves descriptivas
    para facilitar la validación y transformación.

    Args:
        kline_array (list): Array de 12 elementos con datos de una vela.

    Retorna:
        dict: Diccionario con claves nombradas para cada campo.
    """
    if not isinstance(kline_array, (list, tuple)) or len(kline_array) != len(KLINE_FIELDS):
        raise ValueError(
            f"La vela debe contener exactamente {len(KLINE_FIELDS)} campos."
        )
    return dict(zip(KLINE_FIELDS, kline_array))


def _extract_date_from_key(key):
    """
    Extrae la fecha del nombre del archivo Bronze.

    Args:
        key (str): Clave S3 (ej: bronze/year=2024/month=03/BTCUSDT_20240315.json)

    Retorna:
        str: Fecha en formato YYYYMMDD (ej: '20240315')
    """
    filename = key.rsplit("/", 1)[-1]
    match = re.fullmatch(r"[A-Z0-9]+_(\d{8})\.json", filename)
    if not match:
        raise ValueError(f"Nombre de archivo Bronze inválido: {filename}")
    date_str = match.group(1)
    datetime.strptime(date_str, "%Y%m%d")
    return date_str


def _extract_symbol_from_key(key):
    """
    Extrae el símbolo del nombre del archivo Bronze.

    Args:
        key (str): Clave S3 (ej: bronze/year=2024/month=03/BTCUSDT_20240315.json)

    Retorna:
        str: Símbolo de trading (ej: 'BTCUSDT')
    """
    filename = key.rsplit("/", 1)[-1]
    if not filename.endswith(".json") or "_" not in filename:
        raise ValueError(f"Nombre de archivo Bronze inválido: {filename}")
    symbol, _ = filename.rsplit("_", 1)
    if not re.fullmatch(r"[A-Z0-9]+", symbol):
        raise ValueError(f"Símbolo inválido en el archivo Bronze: {filename}")
    return symbol


# ===========================================================================
# FUNCIONES DE VALIDACIÓN
# ===========================================================================

def _validate_kline(kline_dict, expected_date=None):
    """
    Valida que los campos OHLCV de la vela sean numéricos y positivos.

    Reglas de validación:
    - Todos los campos OHLCV deben estar presentes
    - Todos deben ser convertibles a float
    - Todos deben ser mayores que 0

    Args:
        kline_dict (dict): Diccionario con los datos de la vela.

    Retorna:
        tuple: (es_valido: bool, errores: list[str])

    Ejemplo:
        >>> kline = {"open": "50000.0", "high": "51000.0", ...}
        >>> valido, errores = _validate_kline(kline)
        >>> print(valido)  # True
    """
    errors = []

    numeric_values = {}
    for field in REQUIRED_OHLCV:
        # Verificar que el campo existe
        if field not in kline_dict:
            errors.append(f"Campo '{field}' faltante en los datos de la vela")
            continue

        value = kline_dict[field]

        # Verificar que sea convertible a número
        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
            errors.append(
                f"Campo '{field}' no es numérico: '{value}' "
                f"(tipo: {type(value).__name__})"
            )
            continue

        if not math.isfinite(numeric_value):
            errors.append(f"Campo '{field}' debe ser finito.")
        elif numeric_value <= 0:
            errors.append(
                f"Campo '{field}' debe ser > 0, pero es: {numeric_value}"
            )
        numeric_values[field] = numeric_value

    timestamps = {}
    for field in ("open_time", "close_time"):
        try:
            timestamp = float(kline_dict[field])
            if not math.isfinite(timestamp) or not timestamp.is_integer():
                raise ValueError
            timestamps[field] = int(timestamp)
        except (KeyError, TypeError, ValueError):
            errors.append(f"Campo '{field}' debe ser un timestamp entero válido.")

    if all(field in numeric_values for field in ("open", "high", "low", "close")):
        values = numeric_values
        if values["high"] < max(values["open"], values["low"], values["close"]):
            errors.append("high debe ser mayor o igual que open, low y close.")
        if values["low"] > min(values["open"], values["high"], values["close"]):
            errors.append("low debe ser menor o igual que open, high y close.")

    if "open_time" in timestamps and "close_time" in timestamps:
        if timestamps["close_time"] < timestamps["open_time"]:
            errors.append("close_time no puede preceder a open_time.")
        if timestamps["close_time"] >= int(
            datetime.now(timezone.utc).timestamp() * 1000
        ):
            errors.append("La vela todavía no ha cerrado.")
        if expected_date is not None:
            expected_open = int(
                datetime.strptime(expected_date, "%Y%m%d")
                .replace(tzinfo=timezone.utc)
                .timestamp() * 1000
            )
            expected_close = expected_open + 86_400_000 - 1
            if timestamps["open_time"] != expected_open:
                errors.append(
                    f"open_time no corresponde a la fecha {expected_date} UTC."
                )
            if timestamps["close_time"] != expected_close:
                errors.append(
                    f"close_time no corresponde a la vela diaria {expected_date} UTC."
                )

    is_valid = len(errors) == 0

    if is_valid:
        logger.info("Validación OHLCV: PASÓ - Todos los campos son válidos")
    else:
        logger.warning(
            f"Validación OHLCV: FALLÓ - {len(errors)} error(es) encontrado(s)"
        )
        for err in errors:
            logger.warning(f"  → {err}")

    return is_valid, errors


def _save_to_quarantine(bucket, date_str, symbol, raw_data, errors):
    """
    Guarda un registro inválido en la zona de cuarentena.

    Los registros en cuarentena incluyen:
    - Los datos originales crudos
    - Los errores de validación encontrados
    - Metadatos de auditoría del proceso de cuarentena

    Args:
        bucket (str): Nombre del bucket S3.
        date_str (str): Fecha en formato YYYYMMDD.
        symbol (str): Par de trading.
        raw_data (dict): Datos crudos originales.
        errors (list): Lista de errores de validación.
    """
    # Construir la fecha para particionamiento
    dt = datetime.strptime(date_str, "%Y%m%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")

    quarantine_key = (
        f"cuarentena/year={year}/month={month}/"
        f"{symbol}_{date_str}_quarantine.json"
    )

    quarantine_record = {
        "raw_data": raw_data,
        "validation_errors": errors,
        "quarantine_metadata": {
            "quarantine_timestamp": datetime.now(timezone.utc).isoformat(),
            "source_symbol": symbol,
            "source_date": date_str,
            "lambda_function": "transform_silver",
        },
    }

    body = json.dumps(quarantine_record, ensure_ascii=False, indent=2)
    s3_client.put_object(
        Bucket=bucket,
        Key=quarantine_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        f"Registro guardado en cuarentena: s3://{bucket}/{quarantine_key}"
    )


# ===========================================================================
# FUNCIONES DE HISTORIAL (LECTURA DE BRONZE)
# ===========================================================================

def _list_bronze_json(bucket, symbol, through_date=None):
    """Lista archivos Bronze del símbolo en orden cronológico."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="bronze/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            try:
                if _extract_symbol_from_key(key) != symbol:
                    continue
                date_str = _extract_date_from_key(key)
            except ValueError:
                continue
            if through_date is None or date_str <= through_date:
                keys.append((date_str, key))
    return sorted(keys)


def _load_bronze_history(bucket, symbol, through_date=None):
    """Read and validate the Bronze source of truth for indicator history."""
    bronze_keys = _list_bronze_json(bucket, symbol, through_date)
    dates = [date_str for date_str, _ in bronze_keys]
    if len(dates) != len(set(dates)):
        raise ValueError(f"Hay más de un objeto Bronze para {symbol} en una fecha.")

    rows = []
    for date_str, key in bronze_keys:
        bronze_data = _read_bronze_json(bucket, key)
        raw_klines = bronze_data.get("raw_klines") if isinstance(bronze_data, dict) else None
        if not isinstance(raw_klines, list) or len(raw_klines) != 1:
            errors = ["El JSON Bronze debe contener exactamente una vela."]
            _save_to_quarantine(bucket, date_str, symbol, bronze_data, errors)
            raise ValueError(f"Bronze inválido en {key}: {errors[0]}")

        try:
            kline = _parse_kline_to_dict(raw_klines[0])
        except ValueError as error:
            _save_to_quarantine(bucket, date_str, symbol, bronze_data, [str(error)])
            raise

        valid, errors = _validate_kline(kline, expected_date=date_str)
        if not valid:
            _save_to_quarantine(bucket, date_str, symbol, bronze_data, errors)
            raise ValueError(f"Bronze inválido en {key}: {'; '.join(errors)}")

        date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        rows.append({
            "date": date,
            "symbol": symbol,
            "open": float(kline["open"]),
            "high": float(kline["high"]),
            "low": float(kline["low"]),
            "close": float(kline["close"]),
            "volume": float(kline["volume"]),
        })

    if len(rows) > 1:
        dates = pd.to_datetime([row["date"] for row in rows]).sort_values()
        missing_days = pd.date_range(dates[0], dates[-1], freq="D").difference(dates)
        if not missing_days.empty:
            raise ValueError(
                "Faltan velas Bronze entre las fechas "
                f"{dates[0].date()} y {dates[-1].date()}: "
                f"{[day.strftime('%Y-%m-%d') for day in missing_days[:5]]}"
            )
    return pd.DataFrame(rows)


def _write_gold_ready_marker(bucket, reason):
    """Notify Gold only after the Silver output set is complete."""
    marker_key = f"{GOLD_READY_PREFIX}{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    marker = {
        "reason": reason,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=marker_key,
        Body=json.dumps(marker).encode("utf-8"),
        ContentType="application/json",
    )
    return marker_key


# ===========================================================================
# FUNCIONES DE INDICADORES TÉCNICOS
# ===========================================================================

def _calculate_rsi(series, period=RSI_PERIOD):
    """
    Calcula el RSI (Relative Strength Index) de Wilder.

    El RSI mide la velocidad y magnitud de los cambios de precio.
    Valores < 30 indican sobreventa; > 70 indican sobrecompra.

    Fórmula:
        RSI = 100 - (100 / (1 + RS))
        RS = Promedio de ganancias / Promedio de pérdidas (últimos N períodos)

    Args:
        series (pd.Series): Serie de precios de cierre.
        period (int): Número de períodos para el cálculo (default: 14).

    Retorna:
        pd.Series: Serie con valores de RSI.

    Nota:
        Si no hay suficientes datos (< period + 1), los valores serán NaN.
        La semilla es el promedio simple de los primeros cambios; luego se
        aplica el suavizado recursivo de Wilder.
    """
    result = pd.Series(float("nan"), index=series.index, dtype="float64")
    if len(series) <= period:
        return result

    delta = series.astype("float64").diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = float(gains.iloc[1:period + 1].mean())
    avg_loss = float(losses.iloc[1:period + 1].mean())

    def rsi_value(gain, loss):
        if loss == 0:
            return 100.0 if gain > 0 else 0.0
        return 100.0 - (100.0 / (1.0 + gain / loss))

    result.iloc[period] = rsi_value(avg_gain, avg_loss)
    for index in range(period + 1, len(series)):
        avg_gain = ((period - 1) * avg_gain + float(gains.iloc[index])) / period
        avg_loss = ((period - 1) * avg_loss + float(losses.iloc[index])) / period
        result.iloc[index] = rsi_value(avg_gain, avg_loss)
    return result


def _calculate_bollinger_bands(series, period=BOLLINGER_PERIOD, std_dev=BOLLINGER_STD):
    """
    Calcula las Bandas de Bollinger.

    Las Bandas de Bollinger miden la volatilidad del precio.
    - Banda superior: SMA + (2 * desviación estándar)
    - Banda media: SMA (Media Móvil Simple)
    - Banda inferior: SMA - (2 * desviación estándar)

    Cuando el precio toca la banda inferior, puede indicar sobreventa.
    Cuando toca la superior, puede indicar sobrecompra.

    Args:
        series (pd.Series): Serie de precios de cierre.
        period (int): Períodos para la media móvil (default: 20).
        std_dev (int): Número de desviaciones estándar (default: 2).

    Retorna:
        tuple: (upper_band, middle_band, lower_band) como pd.Series.
    """
    # Media Móvil Simple (SMA)
    middle = series.rolling(window=period, min_periods=period).mean()

    # Desviación estándar poblacional, según la convención documentada.
    std = series.rolling(
        window=period, min_periods=period
    ).std(ddof=BOLLINGER_DDOF)

    # Bandas superior e inferior
    upper = middle + (std_dev * std)
    lower = middle - (std_dev * std)

    return upper, middle, lower


def _calculate_macd(series, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL):
    """
    Calcula el MACD (Moving Average Convergence Divergence).

    El MACD es un indicador de tendencia que muestra la relación entre
    dos medias móviles exponenciales del precio.

    Componentes:
    - Línea MACD: EMA rápida (12) - EMA lenta (26)
    - Línea de señal: EMA(9) de la línea MACD
    - Histograma: MACD - Señal (fuerza de la tendencia)
    Las EMAs y la línea de señal se inicializan con la SMA de sus periodos.

    Señales:
    - Cruce MACD arriba de señal → señal alcista (compra)
    - Cruce MACD debajo de señal → señal bajista (venta)

    Args:
        series (pd.Series): Serie de precios de cierre.
        fast (int): Períodos para la EMA rápida (default: 12).
        slow (int): Períodos para la EMA lenta (default: 26).
        signal (int): Períodos para la línea de señal (default: 9).

    Retorna:
        tuple: (macd_line, signal_line, histogram) como pd.Series.
    """
    values = series.astype("float64")

    def seeded_ema(period):
        ema = pd.Series(float("nan"), index=values.index, dtype="float64")
        if len(values) < period:
            return ema
        ema.iloc[period - 1] = values.iloc[:period].mean()
        alpha = 2.0 / (period + 1)
        for index in range(period, len(values)):
            ema.iloc[index] = (
                alpha * values.iloc[index]
                + (1.0 - alpha) * ema.iloc[index - 1]
            )
        return ema

    ema_fast = seeded_ema(fast)
    ema_slow = seeded_ema(slow)
    macd_line = ema_fast - ema_slow
    signal_line = pd.Series(float("nan"), index=values.index, dtype="float64")
    first_macd = slow - 1
    first_signal = first_macd + signal - 1
    if len(values) > first_signal:
        signal_line.iloc[first_signal] = macd_line.iloc[
            first_macd:first_signal + 1
        ].mean()
        alpha = 2.0 / (signal + 1)
        for index in range(first_signal + 1, len(values)):
            signal_line.iloc[index] = (
                alpha * macd_line.iloc[index]
                + (1.0 - alpha) * signal_line.iloc[index - 1]
            )
    return macd_line, signal_line, macd_line - signal_line


def _calculate_all_indicators(df):
    """
    Calcula todos los indicadores técnicos sobre el DataFrame completo.

    Orquesta el cálculo de RSI, Bandas de Bollinger y MACD sobre la
    serie histórica de precios de cierre.

    Args:
        df (pd.DataFrame): DataFrame con columna 'close' y datos históricos.

    Retorna:
        pd.DataFrame: DataFrame con columnas adicionales de indicadores.

    Nota:
        Si no hay suficiente historial para un indicador, los valores
        serán NaN/None. Esto es esperado y se maneja correctamente
        en la capa Gold.
    """
    close = df["close"].astype(float)

    # --- RSI (14 períodos) ---
    df["rsi_14"] = _calculate_rsi(close, RSI_PERIOD)
    logger.info(f"RSI calculado (período: {RSI_PERIOD})")

    # --- Bandas de Bollinger (20, 2σ) ---
    upper, middle, lower = _calculate_bollinger_bands(
        close, BOLLINGER_PERIOD, BOLLINGER_STD
    )
    df["bollinger_upper"] = upper
    df["bollinger_middle"] = middle
    df["bollinger_lower"] = lower
    logger.info(
        f"Bollinger Bands calculadas (período: {BOLLINGER_PERIOD}, "
        f"σ: {BOLLINGER_STD})"
    )

    # --- MACD (12, 26, 9) ---
    macd_line, signal_line, histogram = _calculate_macd(
        close, MACD_FAST, MACD_SLOW, MACD_SIGNAL
    )
    df["macd_line"] = macd_line
    df["macd_signal"] = signal_line
    df["macd_histogram"] = histogram
    logger.info(
        f"MACD calculado (rápido: {MACD_FAST}, lento: {MACD_SLOW}, "
        f"señal: {MACD_SIGNAL})"
    )

    return df


# ===========================================================================
# FUNCIÓN DE GUARDADO SILVER
# ===========================================================================

def _save_silver_parquet(bucket, symbol, date_str, row_df):
    """
    Guarda la fila procesada como archivo Parquet en la capa Silver.

    Solo guarda la fila correspondiente al día procesado (no todo el
    historial), con todos los indicadores técnicos calculados.

    Args:
        bucket (str): Nombre del bucket S3.
        symbol (str): Par de trading.
        date_str (str): Fecha en formato YYYYMMDD.
        row_df (pd.DataFrame): DataFrame de una fila con indicadores.

    Columnas del parquet:
        date, symbol, open, high, low, close, volume,
        rsi_14, bollinger_upper, bollinger_middle, bollinger_lower,
        macd_line, macd_signal, macd_histogram
    """
    dt = datetime.strptime(date_str, "%Y%m%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")

    silver_key = f"silver/year={year}/month={month}/{symbol}_{date_str}.parquet"

    # Seleccionar y ordenar columnas finales
    output_columns = [
        "date", "symbol", "open", "high", "low", "close", "volume",
        "rsi_14", "bollinger_upper", "bollinger_middle", "bollinger_lower",
        "macd_line", "macd_signal", "macd_histogram",
    ]

    # Asegurar que todas las columnas existan
    for col in output_columns:
        if col not in row_df.columns:
            row_df[col] = None

    output_df = row_df[output_columns].copy()

    # Convertir a Parquet en memoria
    buffer = io.BytesIO()
    output_df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Subir a S3
    s3_client.put_object(
        Bucket=bucket,
        Key=silver_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
        Metadata={
            "pipeline": "binance-silver",
            "symbol": symbol,
            "created_by": "transform_silver_lambda",
        },
    )

    logger.info(
        f"Parquet Silver guardado: s3://{bucket}/{silver_key} "
        f"({buffer.getbuffer().nbytes} bytes)"
    )
    return silver_key


# ===========================================================================
# HANDLER PRINCIPAL
# ===========================================================================

def lambda_handler(event, context):
    """
    Función principal de la Lambda - Punto de entrada.

    Orquesta la transformación de Bronze a Silver. El evento manual
    {"mode": "backfill"} recalcula y publica todos los datos Bronze.

    Args:
        event (dict): Evento S3 de Bronze o {"mode": "backfill"}.
        context (LambdaContext): Contexto de ejecución de Lambda.

    Retorna:
        dict: Resultado de la ejecución.

    Ejemplo de evento S3:
        {
            "Records": [{
                "s3": {
                    "bucket": {"name": "mi-bucket"},
                    "object": {"key": "bronze/year=2024/month=03/BTCUSDT_20240315.json"}
                }
            }]
        }
    """
    logger.info("=" * 60)
    logger.info("INICIO - Transformación Silver de Binance")
    request_id = getattr(context, "aws_request_id", "manual")
    logger.info(f"Request ID: {request_id}")
    logger.info("=" * 60)

    backfill = event.get("mode") == "backfill"
    if backfill:
        if not BUCKET_NAME:
            raise RuntimeError("BUCKET_NAME es obligatorio para el backfill.")
        bucket = BUCKET_NAME
        symbol = event.get("symbol", DEFAULT_SYMBOL)
        target_date = None
    else:
        source_bucket, source_key = _extract_s3_info(event)
        if not source_key.startswith("bronze/"):
            raise ValueError(f"Se esperaba un objeto Bronze, recibido: {source_key}")
        if BUCKET_NAME and source_bucket != BUCKET_NAME:
            raise ValueError(
                f"El evento apunta a {source_bucket}, no al bucket configurado {BUCKET_NAME}."
            )
        bucket = BUCKET_NAME or source_bucket
        symbol = _extract_symbol_from_key(source_key)
        target_date = _extract_date_from_key(source_key)

    bronze_keys = _list_bronze_json(bucket, symbol, target_date)
    if not bronze_keys:
        raise FileNotFoundError(
            f"No hay archivos Bronze para {symbol}"
            + (f" hasta {target_date}." if target_date else ".")
        )
    if not backfill and not any(key == source_key for _, key in bronze_keys):
        raise FileNotFoundError(f"No se encontró el archivo Bronze: {source_key}")

    full_df = _load_bronze_history(bucket, symbol, target_date)
    if full_df.empty:
        raise ValueError(f"No se pudo construir historial Bronze para {symbol}.")
    full_df = full_df.sort_values("date").reset_index(drop=True)
    full_df = _calculate_all_indicators(full_df)

    if backfill:
        rows_to_write = full_df
    else:
        rows_to_write = full_df[full_df["date"] == datetime.strptime(
            target_date, "%Y%m%d"
        ).strftime("%Y-%m-%d")]
        if rows_to_write.empty:
            raise ValueError(f"No existe la fecha objetivo {target_date} en Bronze.")

    written_keys = []
    for _, row in rows_to_write.iterrows():
        date_str = row["date"].replace("-", "")
        written_keys.append(
            _save_silver_parquet(bucket, symbol, date_str, row.to_frame().T)
        )

    marker_key = _write_gold_ready_marker(
        bucket, "backfill" if backfill else f"daily:{target_date}"
    )
    latest = rows_to_write.iloc[-1]
    logger.info(
        "Silver escrito para %d fecha(s); Gold actualizado por %s",
        len(written_keys),
        marker_key,
    )
    response = {
        "message": "Backfill Silver completado" if backfill else "Transformación Silver completada",
        "symbol": symbol,
        "dates_written": len(written_keys),
        "silver_keys": written_keys,
        "history_records": len(full_df),
        "gold_ready_marker": marker_key,
        "close_price": float(latest["close"]),
        "rsi_14": float(latest["rsi_14"]) if pd.notna(latest["rsi_14"]) else None,
    }

    logger.info(f"ÉXITO - {len(written_keys)} archivo(s) Silver publicados")
    logger.info("=" * 60)
    logger.info("FIN - Transformación Silver de Binance")
    logger.info("=" * 60)

    return response
