"""
Lambda: extract_bronze — Extracción de datos crudos de API-Football hacia la capa Bronze.

Descripción:
    Esta función Lambda se ejecuta diariamente mediante un evento programado de
    Amazon EventBridge (cron a las 12:00 UTC). Consulta la API de API-Football
    para obtener los fixtures del día anterior ya disputado y almacena la respuesta
    JSON cruda en un bucket S3, organizada con particionamiento Hive-style
    (year=YYYY/month=MM).

Flujo:
    EventBridge (12:00 UTC) → Lambda extract_bronze → S3 bronze/year=YYYY/month=MM/fixtures_YYYYMMDD.json

Idempotencia:
    Si la función se ejecuta más de una vez para el mismo día, sobrescribe
    determinísticamente el mismo archivo en S3, garantizando que no se generen
    duplicados.

Variables de entorno requeridas:
    - BUCKET_NAME: Nombre del bucket S3 destino.
    - API_HOST: Host de la API (default: v3.football.api-sports.io).
    - LEAGUE_ID: ID de la liga a consultar (default: 1 = World Cup).
    - SEASON: Temporada a consultar (default: 2026).

Parámetros SSM:
    - /football/api_key: Clave de API para autenticación con API-Football.

Autor: Equipo de Ingeniería de Datos — Dirección de Selecciones Nacionales
"""

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

import boto3
import urllib.request
import urllib.error

# ---------------------------------------------------------------------------
# Configuración del logger para CloudWatch
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Clientes de AWS (se inicializan fuera del handler para reutilización)
# ---------------------------------------------------------------------------
s3_client = boto3.client("s3")
ssm_client = boto3.client("ssm")
API_MAX_ATTEMPTS = 4
API_RETRY_BASE_SECONDS = 1


def _obtener_api_key() -> str:
    """
    Obtiene la clave de API desde AWS Systems Manager Parameter Store.

    La clave se almacena como SecureString en el parámetro '/football/api_key'.

    Returns:
        str: La clave de API desencriptada.

    Raises:
        Exception: Si no se puede recuperar el parámetro de SSM.
    """
    try:
        respuesta = ssm_client.get_parameter(
            Name=os.environ.get("SSM_API_KEY_NAME", "/football/api_key"),
            WithDecryption=True,
        )
        logger.info("Clave de API obtenida exitosamente desde SSM Parameter Store.")
        return respuesta["Parameter"]["Value"]
    except Exception as error:
        logger.error("Error al obtener la clave de API desde SSM: %s", str(error))
        raise


def _llamar_api_football(
    api_key: str, host: str, liga_id: str, temporada: str, fecha: str
) -> dict:
    """
    Realiza una petición HTTP GET al endpoint de fixtures de API-Football.

    Args:
        api_key (str): Clave de autenticación para la API.
        host (str): Host de la API (ej. v3.football.api-sports.io).
        liga_id (str): Identificador de la liga (ej. '1' para World Cup).
        temporada (str): Año de la temporada (ej. '2026').
        fecha (str): Fecha en formato YYYY-MM-DD para filtrar fixtures.

    Returns:
        dict: Respuesta JSON completa de la API, incluyendo metadatos y fixtures.

    Raises:
        urllib.error.HTTPError: Si la API responde con un código de error HTTP.
        Exception: Para cualquier otro error de conexión o parsing.
    """
    query = urlencode({
        "date": fecha,
        "timezone": "UTC",
    })
    url = f"https://{host}/fixtures?{query}"

    # Construir la petición con los headers de autenticación requeridos por API-Football
    peticion = urllib.request.Request(url)
    peticion.add_header("x-apisports-key", api_key)
    peticion.add_header("x-apisports-host", host)

    logger.info("Llamando a API-Football: %s", url)

    for intento in range(API_MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(peticion, timeout=30) as respuesta:
                cuerpo = respuesta.read().decode("utf-8")
            datos = json.loads(cuerpo)
            if not isinstance(datos, dict):
                raise ValueError("La API-Football devolvió un objeto JSON inválido.")
            errores_api = datos.get("errors")
            if errores_api:
                raise RuntimeError(f"API-Football reportó errores: {errores_api}")
            fixtures = datos.get("response")
            if not isinstance(fixtures, list):
                raise ValueError("La respuesta de API-Football no contiene response[].")
            logger.info("Respuesta recibida exitosamente. Fixtures obtenidos: %d", len(fixtures))
            return datos
        except urllib.error.HTTPError as error:
            reintentable = error.code == 429 or error.code >= 500
            if not reintentable or intento == API_MAX_ATTEMPTS - 1:
                logger.error("Error HTTP al llamar a la API: %s — %s", error.code, error.reason)
                raise
        except (urllib.error.URLError, TimeoutError) as error:
            if intento == API_MAX_ATTEMPTS - 1:
                logger.error("Error de conexión con API-Football: %s", error)
                raise

        espera = API_RETRY_BASE_SECONDS * (2 ** intento)
        logger.warning(
            "Reintentando API-Football (%d/%d) en %d s",
            intento + 1,
            API_MAX_ATTEMPTS - 1,
            espera,
        )
        time.sleep(espera)

    raise RuntimeError("Se agotaron los intentos para consultar API-Football.")


def _guardar_en_s3(bucket: str, key: str, datos: dict) -> None:
    """
    Guarda un diccionario Python como archivo JSON en Amazon S3.

    La operación PutObject es inherentemente idempotente: si el archivo ya
    existe, se sobrescribe con el mismo contenido, lo que garantiza que no
    se generen duplicados al re-ejecutar la función.

    Args:
        bucket (str): Nombre del bucket S3 destino.
        key (str): Ruta completa (key) del objeto en S3.
        datos (dict): Diccionario con los datos a serializar como JSON.

    Raises:
        Exception: Si ocurre un error al escribir en S3.
    """
    try:
        cuerpo_json = json.dumps(datos, ensure_ascii=False, indent=2)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=cuerpo_json.encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("Archivo guardado exitosamente en s3://%s/%s", bucket, key)
    except Exception as error:
        logger.error("Error al guardar archivo en S3: %s", str(error))
        raise


def lambda_handler(event: dict, context) -> dict:
    """
    Punto de entrada de la función Lambda — Extracción Bronze.

    Este handler es invocado por Amazon EventBridge a las 12:00 UTC. Extrae
    los fixtures del día anterior para dar tiempo a que finalicen los partidos.
    Un evento manual puede indicar {"target_date": "YYYY-MM-DD"}.

    Estructura del archivo destino:
        bronze/year=YYYY/month=MM/fixtures_YYYYMMDD.json

    El archivo incluye metadatos de auditoría:
        - timestamp_ingesta: Momento exacto de la extracción (ISO 8601).
        - fuente_endpoint: URL completa del endpoint consultado.
        - lambda_request_id: ID de la solicitud Lambda para trazabilidad.

    Args:
        event (dict): Evento de EventBridge (no se utiliza directamente).
        context: Objeto de contexto de Lambda con metadata de la ejecución.

    Returns:
        dict: Respuesta con statusCode 200 y resumen de la operación.
    """
    logger.info("=== INICIO: Extracción Bronze de Football ===")
    logger.info("Evento recibido: %s", json.dumps(event, default=str))

    # -----------------------------------------------------------------
    # 1. Leer variables de entorno con valores por defecto
    # -----------------------------------------------------------------
    bucket = os.environ["BUCKET_NAME"]
    api_host = os.environ.get("API_HOST", "v3.football.api-sports.io")
    liga_id = os.environ.get("LEAGUE_ID", "1")
    temporada = os.environ.get("SEASON", "2026")

    logger.info(
        "Configuración — Bucket: %s | Host: %s | Liga: %s | Temporada: %s",
        bucket,
        api_host,
        liga_id,
        temporada,
    )

    # -----------------------------------------------------------------
    # 2. Determinar la fecha de consulta (ayer en UTC, salvo reproceso manual)
    # -----------------------------------------------------------------
    ahora_utc = datetime.now(timezone.utc)
    target_date = event.get("target_date") if isinstance(event, dict) else None
    if target_date:
        try:
            fecha_objetivo = datetime.strptime(target_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except (TypeError, ValueError) as error:
            raise ValueError("target_date debe usar el formato YYYY-MM-DD.") from error
    else:
        fecha_objetivo = datetime.combine(
            ahora_utc.date() - timedelta(days=1),
            datetime.min.time(),
            timezone.utc,
        )
    fecha_str = fecha_objetivo.strftime("%Y-%m-%d")
    fecha_compacta = fecha_objetivo.strftime("%Y%m%d")
    anio = fecha_objetivo.strftime("%Y")
    mes = fecha_objetivo.strftime("%m")

    logger.info("Fecha de procesamiento (UTC): %s", fecha_str)

    # -----------------------------------------------------------------
    # 3. Obtener la clave de API desde SSM Parameter Store
    # -----------------------------------------------------------------
    api_key = _obtener_api_key()

    # -----------------------------------------------------------------
    # 4. Llamar al endpoint de fixtures de API-Football
    # -----------------------------------------------------------------
    datos_api = _llamar_api_football(api_key, api_host, liga_id, temporada, fecha_str)

    # -----------------------------------------------------------------
    # 5. Enriquecer con metadatos de auditoría
    #    Estos metadatos permiten rastrear cuándo y cómo se extrajeron
    #    los datos, sin modificar la respuesta original de la API.
    # -----------------------------------------------------------------
    endpoint_fuente = (
        f"https://{api_host}/fixtures?league={liga_id}"
        f"&season={temporada}&date={fecha_str}&timezone=UTC"
    )

    documento_bronze = {
        "_metadata": {
            "timestamp_ingesta": ahora_utc.isoformat(),
            "fuente_endpoint": endpoint_fuente,
            "lambda_request_id": getattr(context, "aws_request_id", "local-test"),
            "liga_id": liga_id,
            "temporada": temporada,
            "fecha_consulta": fecha_str,
        },
        "respuesta_api": datos_api,
    }

    # -----------------------------------------------------------------
    # 6. Guardar JSON crudo en S3 con particionamiento Hive-style
    #    Ruta: bronze/year=YYYY/month=MM/fixtures_YYYYMMDD.json
    # -----------------------------------------------------------------
    s3_key = f"bronze/year={anio}/month={mes}/fixtures_{fecha_compacta}.json"
    _guardar_en_s3(bucket, s3_key, documento_bronze)

    # -----------------------------------------------------------------
    # 7. Respuesta exitosa
    # -----------------------------------------------------------------
    cantidad_fixtures = len(datos_api.get("response", []))
    mensaje = (
        f"Extracción Bronze completada. {cantidad_fixtures} fixtures guardados "
        f"en s3://{bucket}/{s3_key}"
    )
    logger.info(mensaje)
    logger.info("=== FIN: Extracción Bronze de Football ===")

    return {
        "statusCode": 200,
        "body": {
            "mensaje": mensaje,
            "bucket": bucket,
            "key": s3_key,
            "fixtures_extraidos": cantidad_fixtures,
            "fecha": fecha_str,
        },
    }
