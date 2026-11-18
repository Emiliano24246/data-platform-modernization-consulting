"""
===============================================================================
 extract_bronze/handler.py — Ingesta de NEOs desde la API NeoWs de la NASA
===============================================================================

Función AWS Lambda que se ejecuta diariamente mediante Amazon EventBridge.
Consulta la API NeoWs (Near Earth Objects Web Service) de la NASA para obtener
los asteroides cercanos a la Tierra del día actual y almacena la respuesta
JSON cruda e intacta en la capa Bronze del Data Lake en Amazon S3.

Capa:       Bronze (dato crudo)
Disparador: Amazon EventBridge (programación diaria a las 01:00 UTC)
Destino:    s3://<bucket>/bronze/year=YYYY/month=MM/neo_YYYYMMDD.json

Principios de diseño:
    - Dato crudo intacto: se guarda el JSON tal cual lo devuelve la NASA.
    - Idempotencia: re-ejecutar el mismo día sobrescribe la misma ruta en S3.
    - Metadatos de auditoría: se añade un bloque _metadata con la marca de
      tiempo de ingesta, el endpoint consultado y el identificador de Lambda.
    - Sin dependencias externas: solo usa boto3 (incluido en Lambda) y urllib.

Variables de entorno requeridas:
    BUCKET_NAME         Nombre del bucket S3 del Data Lake.
    SSM_API_KEY_NAME    Ruta del parámetro en SSM Parameter Store con la API key
                        de la NASA (por defecto: /asteroid/nasa_api_key).
===============================================================================
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import boto3

# ---------------------------------------------------------------------------
# Configuración del logger para CloudWatch
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Clientes de AWS (se reutilizan entre invocaciones por warm start)
# ---------------------------------------------------------------------------
s3_client = boto3.client("s3")
ssm_client = boto3.client("ssm")

# ---------------------------------------------------------------------------
# Constantes y variables de entorno
# ---------------------------------------------------------------------------
BUCKET_NAME = os.environ.get("BUCKET_NAME", "asteroid-data-lake")
SSM_API_KEY_NAME = os.environ.get("SSM_API_KEY_NAME", "/asteroid/nasa_api_key")
NASA_NEOWS_BASE_URL = "https://api.nasa.gov/neo/rest/v1/feed"


def _obtener_api_key() -> str:
    """
    Obtiene la API key de la NASA desde AWS Systems Manager Parameter Store.

    Returns:
        str: La API key almacenada de forma segura en Parameter Store.

    Raises:
        RuntimeError: Si no se puede obtener el parámetro de SSM.
    """
    try:
        respuesta = ssm_client.get_parameter(
            Name=SSM_API_KEY_NAME,
            WithDecryption=True
        )
        api_key = respuesta["Parameter"]["Value"]
        logger.info("API key de la NASA obtenida exitosamente desde SSM: %s", SSM_API_KEY_NAME)
        return api_key
    except Exception as error:
        mensaje = f"Error al obtener la API key desde SSM ({SSM_API_KEY_NAME}): {error}"
        logger.error(mensaje)
        raise RuntimeError(mensaje) from error


def _consultar_neows(api_key: str, fecha: str) -> dict:
    """
    Consulta la API NeoWs de la NASA para obtener los NEOs de una fecha dada.

    Args:
        api_key: API key válida para la NASA.
        fecha:   Fecha en formato YYYY-MM-DD.

    Returns:
        dict: Respuesta JSON completa de la API de la NASA.

    Raises:
        RuntimeError: Si la llamada a la API falla.
    """
    url = f"{NASA_NEOWS_BASE_URL}?start_date={fecha}&end_date={fecha}&api_key={api_key}"
    logger.info("Consultando API NeoWs de la NASA para la fecha: %s", fecha)

    request = Request(url, method="GET")
    request.add_header("Accept", "application/json")
    request.add_header("User-Agent", "asteroid-data-pipeline/1.0")

    try:
        with urlopen(request, timeout=30) as respuesta:
            codigo_estado = respuesta.getcode()
            cuerpo = respuesta.read().decode("utf-8")

            if codigo_estado != 200:
                raise RuntimeError(
                    f"La API de la NASA respondió con código {codigo_estado}"
                )

            datos = json.loads(cuerpo)
            cantidad_neos = datos.get("element_count", 0)
            logger.info(
                "Respuesta exitosa de la NASA. NEOs encontrados: %d",
                cantidad_neos
            )
            return datos

    except HTTPError as error:
        mensaje = f"Error HTTP al consultar NeoWs: {error.code} - {error.reason}"
        logger.error(mensaje)
        raise RuntimeError(mensaje) from error
    except URLError as error:
        mensaje = f"Error de conexión con la API de la NASA: {error.reason}"
        logger.error(mensaje)
        raise RuntimeError(mensaje) from error
    except json.JSONDecodeError as error:
        mensaje = f"Error al decodificar la respuesta JSON de la NASA: {error}"
        logger.error(mensaje)
        raise RuntimeError(mensaje) from error


def _construir_ruta_s3(fecha_dt: datetime) -> str:
    """
    Construye la ruta determinista en S3 para la capa Bronze.

    La ruta sigue el patrón: bronze/year=YYYY/month=MM/neo_YYYYMMDD.json
    Al ser determinista, re-ejecutar el mismo día sobrescribe el mismo archivo
    (idempotencia).

    Args:
        fecha_dt: Objeto datetime con la fecha de consulta.

    Returns:
        str: Ruta (key) del objeto en S3.
    """
    year = fecha_dt.strftime("%Y")
    month = fecha_dt.strftime("%m")
    fecha_str = fecha_dt.strftime("%Y%m%d")
    return f"bronze/year={year}/month={month}/neo_{fecha_str}.json"


def _guardar_en_s3(datos: dict, ruta_s3: str, metadatos_auditoria: dict) -> None:
    """
    Guarda el JSON crudo con metadatos de auditoría en la capa Bronze de S3.

    Se envuelve la respuesta original de la NASA junto con un bloque
    '_metadata' que permite rastrear cuándo y cómo se ingirió el dato.

    Args:
        datos:                Respuesta JSON original de la API de la NASA.
        ruta_s3:              Ruta (key) del objeto en S3.
        metadatos_auditoria:  Diccionario con información de auditoría.
    """
    documento = {
        "_metadata": metadatos_auditoria,
        "payload": datos
    }

    cuerpo_json = json.dumps(documento, ensure_ascii=False, indent=2)

    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=ruta_s3,
            Body=cuerpo_json.encode("utf-8"),
            ContentType="application/json"
        )
        tamano_kb = len(cuerpo_json.encode("utf-8")) / 1024
        logger.info(
            "Archivo guardado exitosamente en s3://%s/%s (%.2f KB)",
            BUCKET_NAME,
            ruta_s3,
            tamano_kb
        )
    except Exception as error:
        mensaje = f"Error al guardar en S3 ({BUCKET_NAME}/{ruta_s3}): {error}"
        logger.error(mensaje)
        raise RuntimeError(mensaje) from error


def lambda_handler(event: dict, context) -> dict:
    """
    Punto de entrada de la función Lambda de ingesta Bronze.

    Flujo de ejecución:
        1. Determina la fecha de consulta (hoy por defecto, o la especificada
           en el evento para reprocesamiento manual).
        2. Obtiene la API key de la NASA desde SSM Parameter Store.
        3. Consulta la API NeoWs de la NASA para los NEOs de esa fecha.
        4. Guarda la respuesta JSON cruda con metadatos de auditoría en S3.

    Args:
        event:   Evento de EventBridge o evento manual con campo opcional
                 'target_date' (formato YYYY-MM-DD) para reprocesamiento.
        context: Objeto de contexto de AWS Lambda.

    Returns:
        dict: Resultado de la ejecución con estado, ruta del archivo y
              cantidad de NEOs procesados.
    """
    logger.info("=== INICIO: Ingesta Bronze de Asteroides (NEOs) ===")
    logger.info("Evento recibido: %s", json.dumps(event, default=str))

    # -----------------------------------------------------------------
    # 1. Determinar la fecha de consulta
    # -----------------------------------------------------------------
    # Por defecto se consulta el día de hoy (UTC).
    # Si el evento contiene 'target_date', se usa esa fecha (útil para
    # reprocesamiento manual o backfill).
    fecha_objetivo = event.get("target_date")

    if fecha_objetivo:
        try:
            fecha_dt = datetime.strptime(fecha_objetivo, "%Y-%m-%d")
            logger.info("Usando fecha objetivo del evento: %s", fecha_objetivo)
        except ValueError:
            mensaje = f"Formato de fecha inválido: '{fecha_objetivo}'. Use YYYY-MM-DD."
            logger.error(mensaje)
            raise ValueError(mensaje)
    else:
        fecha_dt = datetime.now(timezone.utc)
        fecha_objetivo = fecha_dt.strftime("%Y-%m-%d")
        logger.info("Usando fecha actual (UTC): %s", fecha_objetivo)

    # -----------------------------------------------------------------
    # 2. Obtener la API key desde SSM Parameter Store
    # -----------------------------------------------------------------
    api_key = _obtener_api_key()

    # -----------------------------------------------------------------
    # 3. Consultar la API NeoWs de la NASA
    # -----------------------------------------------------------------
    datos_nasa = _consultar_neows(api_key, fecha_objetivo)
    cantidad_neos = datos_nasa.get("element_count", 0)

    # -----------------------------------------------------------------
    # 4. Preparar metadatos de auditoría
    # -----------------------------------------------------------------
    metadatos_auditoria = {
        "timestamp_ingesta": datetime.now(timezone.utc).isoformat(),
        "fuente_endpoint": f"{NASA_NEOWS_BASE_URL}?start_date={fecha_objetivo}&end_date={fecha_objetivo}",
        "fecha_consulta": fecha_objetivo,
        "cantidad_neos": cantidad_neos,
        "lambda_request_id": getattr(context, "aws_request_id", "local-test"),
        "lambda_function_name": getattr(context, "function_name", "extract_bronze"),
        "version": "1.0.0"
    }

    # -----------------------------------------------------------------
    # 5. Guardar en S3 (capa Bronze)
    # -----------------------------------------------------------------
    ruta_s3 = _construir_ruta_s3(fecha_dt)
    _guardar_en_s3(datos_nasa, ruta_s3, metadatos_auditoria)

    # -----------------------------------------------------------------
    # 6. Resultado exitoso
    # -----------------------------------------------------------------
    resultado = {
        "statusCode": 200,
        "capa": "bronze",
        "bucket": BUCKET_NAME,
        "ruta_s3": ruta_s3,
        "fecha_consulta": fecha_objetivo,
        "cantidad_neos": cantidad_neos,
        "mensaje": f"Ingesta Bronze completada: {cantidad_neos} NEOs guardados en s3://{BUCKET_NAME}/{ruta_s3}"
    }

    logger.info("=== FIN: %s ===", resultado["mensaje"])
    return resultado