"""
==============================================================================
Lambda: signals_gold (Binance BTCUSDT - Señales de Trading)
==============================================================================
Descripción:
    Función Lambda que genera señales de trading en la capa Gold del Data Lake.
    Utiliza una estrategia de TRIPLE CONFIRMACIÓN basada en tres indicadores
    técnicos: RSI, Bandas de Bollinger y MACD.

    Señales generadas:
    - COMPRA FUERTE: RSI < 30 AND close <= Bollinger inferior AND MACD > Señal
    - VENTA: RSI > 70 AND close >= Bollinger superior AND MACD < Señal
    - MANTENER: Todo lo demás

Trigger:
    Evento S3 cuando Silver publica un marcador en 'control/gold-ready/'.

Mecanismo:
    Ejecuta una consulta CTAS (CREATE TABLE AS SELECT) en Athena que:
    1. Lee todos los datos de la tabla Silver
    2. Aplica la lógica de señales con CASE WHEN
    3. Materializa los resultados como tabla Gold en S3

Destino S3:
    s3://{BUCKET}/gold/signals/

Variables de entorno:
    - BUCKET_NAME: Nombre del bucket S3
    - ATHENA_DATABASE: Base de datos en AWS Glue Catalog
    - ATHENA_OUTPUT_LOCATION: Ubicación S3 para resultados temporales de Athena
    - SILVER_TABLE: Tabla Glue de Silver (default: binance_silver)
    - GOLD_TABLE: Tabla Glue de Gold (default: binance_gold_signals)

Idempotencia:
    Elimina la tabla Gold anterior antes de recrearla (DROP + CTAS).
    El resultado es siempre consistente con los datos Silver actuales.

Autor: Equipo MarketMatrix - Proyecto de Modernización de Datos
Cliente: MarketMatrix (fondo de trading cuantitativo)
==============================================================================
"""

import os
import re
import time
import logging
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuración del logger para CloudWatch
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Clientes de AWS
# ---------------------------------------------------------------------------
athena_client = boto3.client("athena")
s3_client = boto3.client("s3")

# ---------------------------------------------------------------------------
# Variables de entorno
# ---------------------------------------------------------------------------
BUCKET_NAME = os.environ.get("BUCKET_NAME")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "marketmatrix_db")
ATHENA_OUTPUT_LOCATION = os.environ.get(
    "ATHENA_OUTPUT_LOCATION",
    f"s3://{BUCKET_NAME}/athena-results/" if BUCKET_NAME else ""
)
SILVER_TABLE = os.environ.get("SILVER_TABLE", "binance_silver")
GOLD_TABLE = os.environ.get("GOLD_TABLE", "binance_gold_signals")

# ---------------------------------------------------------------------------
# Constantes de la estrategia de triple confirmación
# ---------------------------------------------------------------------------
RSI_OVERSOLD = 30       # RSI por debajo de 30 → sobreventa
RSI_OVERBOUGHT = 70     # RSI por encima de 70 → sobrecompra

# Tiempo máximo de espera para consultas Athena (en segundos)
ATHENA_MAX_WAIT_SECONDS = 120
ATHENA_POLL_INTERVAL_SECONDS = 3


def _extract_s3_info(event):
    """
    Extrae el bucket y la clave del objeto del evento S3.

    Args:
        event (dict): Evento S3 del marcador de finalización de Silver.

    Retorna:
        tuple: (bucket_name, object_key) con los datos del objeto S3.
    """
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key = unquote_plus(record["object"]["key"])
    return bucket, key


def _drop_gold_table_if_exists(database, table_name):
    """
    Elimina la tabla Gold en Athena si ya existe.

    Esto es necesario antes de ejecutar CTAS porque Athena no permite
    crear una tabla que ya existe. Esta operación garantiza idempotencia.

    Args:
        database (str): Nombre de la base de datos en Glue Catalog.
        table_name (str): Nombre de la tabla a eliminar.

    Retorna:
        str: ID de la consulta de Athena ejecutada.
    """
    drop_query = f"DROP TABLE IF EXISTS {database}.{table_name}"

    logger.info(f"Eliminando tabla Gold existente: {database}.{table_name}")

    response = athena_client.start_query_execution(
        QueryString=drop_query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )

    query_id = response["QueryExecutionId"]
    logger.info(f"DROP TABLE ejecutado - Query ID: {query_id}")

    # Esperar a que termine el DROP
    _wait_for_query(query_id, "DROP TABLE")

    return query_id


def _delete_gold_s3_data(bucket, gold_prefix):
    """
    Elimina los archivos de datos Gold anteriores en S3.

    CTAS no sobrescribe archivos existentes, por lo que necesitamos
    limpiar el directorio Gold antes de recrear la tabla.

    Args:
        bucket (str): Nombre del bucket S3.
        gold_prefix (str): Prefijo S3 de los datos Gold (ej: 'gold/signals/').
    """
    logger.info(f"Limpiando datos Gold anteriores en s3://{bucket}/{gold_prefix}")

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=gold_prefix)
    objects_to_delete = [
        {"Key": obj["Key"]}
        for page in pages
        for obj in page.get("Contents", [])
    ]
    if not objects_to_delete:
        logger.info("No hay datos Gold anteriores para eliminar")
        return

    for index in range(0, len(objects_to_delete), 1000):
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": objects_to_delete[index:index + 1000], "Quiet": True},
        )
        errors = response.get("Errors", [])
        if errors:
            raise RuntimeError(f"S3 no pudo eliminar datos Gold: {errors}")
    logger.info("Eliminados %d archivos Gold anteriores", len(objects_to_delete))


def _build_ctas_query(database, gold_table, silver_table, bucket):
    """
    Construye la consulta CTAS (CREATE TABLE AS SELECT) para generar
    la tabla Gold con señales de trading.

    La estrategia de TRIPLE CONFIRMACIÓN requiere que los tres indicadores
    coincidan para generar una señal de COMPRA FUERTE o VENTA:

    COMPRA FUERTE (máxima convicción alcista):
        1. RSI < 30 → El activo está sobrevendido
        2. Precio ≤ Banda Bollinger inferior → Precio en zona baja
        3. MACD > Señal → Momentum cambiando a alcista

    VENTA (máxima convicción bajista):
        1. RSI > 70 → El activo está sobrecomprado
        2. Precio ≥ Banda Bollinger superior → Precio en zona alta
        3. MACD < Señal → Momentum cambiando a bajista

    MANTENER: Cuando no hay triple confirmación en ninguna dirección.

    Args:
        database (str): Nombre de la base de datos.
        gold_table (str): Nombre de la tabla Gold a crear.
        silver_table (str): Nombre de la tabla Silver fuente.
        bucket (str): Bucket S3 para almacenar los datos Gold.

    Retorna:
        str: Consulta SQL CTAS completa.
    """
    for identifier in (database, gold_table, silver_table):
        if not re.fullmatch(r"[A-Za-z0-9_]+", identifier):
            raise ValueError(f"Identificador Glue inválido: {identifier!r}")

    gold_s3_path = f"s3://{bucket}/gold/signals/"

    ctas_query = f"""
    CREATE TABLE {database}.{gold_table}
    WITH (
        format = 'PARQUET',
        external_location = '{gold_s3_path}',
        write_compression = 'SNAPPY'
    )
    AS
    SELECT
        "date",
        symbol,
        close,
        rsi_14,
        bollinger_upper,
        bollinger_lower,
        macd_line,
        macd_signal,
        CASE
            -- ============================================================
            -- COMPRA FUERTE: Triple confirmación alcista
            -- RSI sobrevendido + Precio en banda baja + MACD cruzando arriba
            -- ============================================================
            WHEN rsi_14 IS NOT NULL
                 AND bollinger_lower IS NOT NULL
                 AND macd_line IS NOT NULL
                 AND macd_signal IS NOT NULL
                 AND rsi_14 < {RSI_OVERSOLD}
                 AND close <= bollinger_lower
                 AND macd_line > macd_signal
            THEN 'COMPRA FUERTE'

            -- ============================================================
            -- VENTA: Triple confirmación bajista
            -- RSI sobrecomprado + Precio en banda alta + MACD cruzando abajo
            -- ============================================================
            WHEN rsi_14 IS NOT NULL
                 AND bollinger_upper IS NOT NULL
                 AND macd_line IS NOT NULL
                 AND macd_signal IS NOT NULL
                 AND rsi_14 > {RSI_OVERBOUGHT}
                 AND close >= bollinger_upper
                 AND macd_line < macd_signal
            THEN 'VENTA'

            -- ============================================================
            -- MANTENER: Sin triple confirmación en ninguna dirección
            -- ============================================================
            ELSE 'MANTENER'
        END AS signal
    FROM {database}.{silver_table}
    WHERE "date" IS NOT NULL
    """

    return ctas_query


def _wait_for_query(query_id, query_description=""):
    """
    Espera a que una consulta de Athena termine su ejecución.

    Implementa un bucle de polling con intervalo configurable y
    tiempo máximo de espera para evitar timeouts de la Lambda.

    Args:
        query_id (str): ID de la consulta de Athena.
        query_description (str): Descripción para los logs.

    Retorna:
        dict: Resultado de la ejecución de la consulta.

    Raises:
        RuntimeError: Si la consulta falla o se cancela.
        TimeoutError: Si se excede el tiempo máximo de espera.
    """
    logger.info(
        f"Esperando consulta Athena [{query_description}]: {query_id}"
    )

    elapsed = 0

    while elapsed < ATHENA_MAX_WAIT_SECONDS:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_id
        )
        state = response["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            logger.info(
                f"Consulta [{query_description}] completada exitosamente "
                f"en ~{elapsed}s"
            )
            return response

        elif state in ("FAILED", "CANCELLED"):
            reason = response["QueryExecution"]["Status"].get(
                "StateChangeReason", "Sin razón proporcionada"
            )
            error_msg = (
                f"Consulta [{query_description}] falló con estado '{state}': "
                f"{reason}"
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        # Estado QUEUED o RUNNING → seguir esperando
        time.sleep(ATHENA_POLL_INTERVAL_SECONDS)
        elapsed += ATHENA_POLL_INTERVAL_SECONDS

    # Timeout alcanzado
    raise TimeoutError(
        f"Consulta [{query_description}] excedió el tiempo máximo de "
        f"{ATHENA_MAX_WAIT_SECONDS}s. Query ID: {query_id}"
    )


def _execute_ctas_query(ctas_query, database):
    """
    Ejecuta la consulta CTAS en Athena y espera su finalización.

    Args:
        ctas_query (str): Consulta SQL CTAS a ejecutar.
        database (str): Nombre de la base de datos.

    Retorna:
        str: ID de la consulta ejecutada.

    Raises:
        RuntimeError: Si la consulta falla.
        ClientError: Si hay un error de Athena.
    """
    logger.info("Ejecutando consulta CTAS para generar tabla Gold")
    logger.info(f"Base de datos: {database}")

    try:
        response = athena_client.start_query_execution(
            QueryString=ctas_query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
        )

        query_id = response["QueryExecutionId"]
        logger.info(f"CTAS iniciada - Query ID: {query_id}")

        # Esperar a que termine
        result = _wait_for_query(query_id, "CTAS Gold Signals")

        # Obtener estadísticas si están disponibles
        stats = result.get("QueryExecution", {}).get("Statistics", {})
        if stats:
            logger.info(
                f"Estadísticas CTAS - "
                f"Datos escaneados: {stats.get('DataScannedInBytes', 0)} bytes, "
                f"Tiempo de ejecución: {stats.get('TotalExecutionTimeInMillis', 0)} ms"
            )

        return query_id

    except ClientError as e:
        error_msg = f"Error al ejecutar CTAS en Athena: {e.response['Error']['Message']}"
        logger.error(error_msg)
        raise


def lambda_handler(event, context):
    """
    Función principal de la Lambda - Punto de entrada.

    Orquesta la generación de señales de trading en la capa Gold:
    1. Recibe evento S3 de nueva data en Silver
    2. Elimina la tabla Gold anterior (idempotencia)
    3. Limpia datos Gold anteriores en S3
    4. Ejecuta CTAS para reconstruir la tabla Gold con señales
    5. Retorna resultado de la ejecución

    La tabla Gold contiene señales de trading basadas en la estrategia
    de triple confirmación (RSI + Bollinger + MACD) aplicada a todos
    los datos históricos disponibles en Silver.

    Args:
        event (dict): Evento S3 con información del objeto creado en Silver.
        context (LambdaContext): Contexto de ejecución de Lambda.

    Retorna:
        dict: Resultado de la ejecución.

    Ejemplo de evento S3:
        {
            "Records": [{
                "s3": {
                    "bucket": {"name": "mi-bucket"},
                    "object": {"key": "control/gold-ready/20240315.json"}
                }
            }]
        }
    """
    logger.info("=" * 60)
    logger.info("INICIO - Generación de Señales Gold (Binance)")
    logger.info("Request ID: %s", getattr(context, "aws_request_id", "manual"))
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # 1. Validar variables de entorno
    # ------------------------------------------------------------------
    if not BUCKET_NAME:
        raise RuntimeError("Variable de entorno BUCKET_NAME no configurada")
    if not ATHENA_OUTPUT_LOCATION:
        raise RuntimeError("Variable de entorno ATHENA_OUTPUT_LOCATION no configurada")

    source_bucket, source_key = _extract_s3_info(event)
    if source_bucket != BUCKET_NAME or not source_key.startswith("control/gold-ready/"):
        raise ValueError(
            "Gold solo procesa marcadores control/gold-ready/ del bucket configurado."
        )
    logger.info("Marcador Silver detectado: s3://%s/%s", source_bucket, source_key)

    gold_prefix = "gold/signals/"

    logger.info(f"Base de datos Athena: {ATHENA_DATABASE}")
    logger.info(f"Tabla Silver (fuente): {SILVER_TABLE}")
    logger.info(f"Tabla Gold (destino): {GOLD_TABLE}")

    # ------------------------------------------------------------------
    # 4. Eliminar tabla Gold anterior (idempotencia)
    # ------------------------------------------------------------------
    _drop_gold_table_if_exists(ATHENA_DATABASE, GOLD_TABLE)

    # ------------------------------------------------------------------
    # 5. Limpiar datos Gold anteriores en S3
    # ------------------------------------------------------------------
    _delete_gold_s3_data(BUCKET_NAME, gold_prefix)

    # ------------------------------------------------------------------
    # 6. Construir y ejecutar CTAS
    # ------------------------------------------------------------------
    ctas_query = _build_ctas_query(
        ATHENA_DATABASE, GOLD_TABLE, SILVER_TABLE, BUCKET_NAME
    )
    logger.info("Consulta CTAS construida:")
    logger.info(ctas_query)

    query_id = _execute_ctas_query(ctas_query, ATHENA_DATABASE)

    # ------------------------------------------------------------------
    # 7. Respuesta exitosa
    # ------------------------------------------------------------------
    response = {
        "message": "Señales Gold generadas exitosamente",
        "athena_query_id": query_id,
        "gold_table": f"{ATHENA_DATABASE}.{GOLD_TABLE}",
        "gold_s3_path": f"s3://{BUCKET_NAME}/{gold_prefix}",
        "strategy": "Triple Confirmación (RSI + Bollinger + MACD)",
        "signals_defined": {
            "COMPRA_FUERTE": f"RSI < {RSI_OVERSOLD} AND close <= bollinger_lower AND MACD > signal",
            "VENTA": f"RSI > {RSI_OVERBOUGHT} AND close >= bollinger_upper AND MACD < signal",
            "MANTENER": "Sin triple confirmación",
        },
    }

    logger.info(f"ÉXITO - Tabla Gold creada: {ATHENA_DATABASE}.{GOLD_TABLE}")
    logger.info(f"Datos en: s3://{BUCKET_NAME}/{gold_prefix}")
    logger.info("=" * 60)
    logger.info("FIN - Generación de Señales Gold (Binance)")
    logger.info("=" * 60)

    return response
