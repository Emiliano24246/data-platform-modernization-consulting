"""
===============================================================================
 Lambda: transform_silver — Transformación y validación de datos de asteroides
===============================================================================

Descripción:
    Esta función Lambda toma los datos crudos (JSON) de la capa Bronze,
    los transforma aplanando la estructura anidada de la API NeoWs,
    valida los campos requeridos y produce archivos Parquet limpios
    en la capa Silver del Data Lake.

Arquitectura Medallion — Capa Silver:
    La capa Silver contiene datos limpiados, validados y estructurados.
    Se aplanan estructuras anidadas, se validan campos obligatorios y
    los registros que no pasan la validación se envían a cuarentena
    para revisión posterior.

Cliente:
    Dirección General de Exposiciones (museos y planetarios)

Trigger:
    Evento S3 — Creación de objeto en prefijo 'bronze/'

Destino S3:
    - Válidos:    s3://<BUCKET>/silver/year=YYYY/month=MM/neo_YYYYMMDD.parquet
    - Inválidos:  s3://<BUCKET>/cuarentena/year=YYYY/month=MM/neo_YYYYMMDD_quarantine.json

Variables de entorno requeridas:
    - BUCKET_NAME: Nombre del bucket S3

Idempotencia:
    Las rutas de salida son deterministas basadas en la fecha del archivo
    Bronze. Re-ejecutar para el mismo archivo simplemente sobreescribe
    los resultados anteriores.

Autor: Equipo de Ingeniería de Datos
Proyecto: Data Platform Modernization — Asteroid Pipeline
===============================================================================
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from io import BytesIO
from typing import Tuple

import boto3
import pandas as pd
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuración del logger
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Cliente de AWS S3 (reutilizable entre invocaciones)
# ---------------------------------------------------------------------------
s3_client = boto3.client("s3")

# ---------------------------------------------------------------------------
# Constantes de validación
# ---------------------------------------------------------------------------
# Estos son los campos que DEBEN estar presentes y no ser nulos para que
# un registro NEO sea considerado válido. Si alguno falta, el registro
# se envía a cuarentena.
# ---------------------------------------------------------------------------
CAMPOS_REQUERIDOS = [
    "neo_id",
    "name",
    "estimated_diameter_min_km",
    "estimated_diameter_max_km",
    "relative_velocity_km_h",
    "miss_distance_km",
]


def leer_json_desde_s3(bucket: str, key: str) -> dict:
    """
    Lee y deserializa un archivo JSON desde Amazon S3.

    Args:
        bucket: Nombre del bucket S3.
        key: Ruta (key) del archivo dentro del bucket.

    Returns:
        Diccionario con el contenido JSON del archivo.

    Raises:
        ClientError: Si el archivo no existe o no se tienen permisos.
        json.JSONDecodeError: Si el contenido no es JSON válido.
    """
    logger.info("Leyendo archivo Bronze desde s3://%s/%s", bucket, key)

    try:
        respuesta = s3_client.get_object(Bucket=bucket, Key=key)
        contenido = respuesta["Body"].read().decode("utf-8")
        datos = json.loads(contenido)

        logger.info(
            "Archivo leído exitosamente — Tamaño: %d bytes",
            len(contenido.encode("utf-8"))
        )
        return datos

    except ClientError as error:
        logger.error(
            "Error al leer desde S3 (s3://%s/%s): %s",
            bucket, key, error
        )
        raise


def extraer_fecha_de_key(key: str) -> str:
    """
    Extrae la fecha del nombre del archivo Bronze.

    La convención de nombrado es: bronze/year=YYYY/month=MM/neo_YYYYMMDD.json
    Necesitamos extraer 'YYYYMMDD' para construir las rutas de salida.

    Ejemplo:
        'bronze/year=2024/month=01/neo_20240115.json' → '20240115'

    Args:
        key: Ruta (key) del archivo en S3.

    Returns:
        Fecha en formato compacto 'YYYYMMDD'.

    Raises:
        ValueError: Si no se puede extraer la fecha del nombre del archivo.
    """
    # Buscar el patrón neo_YYYYMMDD en el nombre del archivo
    patron = r"neo_(\d{8})"
    coincidencia = re.search(patron, key)

    if not coincidencia:
        raise ValueError(
            f"No se pudo extraer la fecha del key de S3: {key}. "
            f"Se esperaba el patrón 'neo_YYYYMMDD'."
        )

    fecha_compacta = coincidencia.group(1)
    logger.info("Fecha extraída del nombre del archivo: %s", fecha_compacta)
    return fecha_compacta


def aplanar_neo(neo: dict, fecha_aproximacion: str) -> dict:
    """
    Aplana un objeto NEO individual de la estructura anidada de la API.

    La API NeoWs retorna datos con estructuras muy anidadas. Por ejemplo,
    el diámetro estimado está en:
        neo['estimated_diameter']['kilometers']['estimated_diameter_min']

    Esta función extrae y aplana todos los campos relevantes en un
    diccionario plano, que es más fácil de trabajar con pandas y Athena.

    Ejemplo de estructura de entrada (simplificada):
    {
        "id": "3542519",
        "name": "(2010 PK9)",
        "absolute_magnitude_h": 21.8,
        "is_potentially_hazardous_asteroid": false,
        "estimated_diameter": {
            "kilometers": {
                "estimated_diameter_min": 0.1,
                "estimated_diameter_max": 0.2
            }
        },
        "close_approach_data": [{
            "close_approach_date": "2024-01-15",
            "relative_velocity": {"kilometers_per_hour": "55000.123"},
            "miss_distance": {"kilometers": "7500000.456"},
            "orbiting_body": "Earth"
        }]
    }

    Args:
        neo: Diccionario con los datos de un NEO individual.
        fecha_aproximacion: Fecha clave del diccionario near_earth_objects.

    Returns:
        Diccionario con los campos aplanados del NEO.
    """
    # -------------------------------------------------------------------
    # Extraer diámetro estimado (estructura anidada)
    # -------------------------------------------------------------------
    # Usamos .get() encadenados con diccionarios vacíos como default
    # para evitar KeyError si falta algún nivel de la estructura.
    # -------------------------------------------------------------------
    diametro = neo.get("estimated_diameter", {})
    diametro_km = diametro.get("kilometers", {})

    # -------------------------------------------------------------------
    # Extraer datos de aproximación cercana
    # -------------------------------------------------------------------
    # close_approach_data es una lista; tomamos el primer elemento [0]
    # que corresponde a la aproximación más cercana.
    # -------------------------------------------------------------------
    datos_aproximacion = neo.get("close_approach_data", [{}])
    primera_aproximacion = datos_aproximacion[0] if datos_aproximacion else {}

    velocidad = primera_aproximacion.get("relative_velocity", {})
    distancia = primera_aproximacion.get("miss_distance", {})

    # -------------------------------------------------------------------
    # Construir el registro aplanado
    # -------------------------------------------------------------------
    # Convertimos valores numéricos que la API retorna como strings
    # (velocidad y distancia) a float. Usamos una función auxiliar
    # para manejar valores None de forma segura.
    # -------------------------------------------------------------------
    return {
        "neo_id": str(neo.get("id")) if neo.get("id") is not None else None,
        "name": neo.get("name"),
        "absolute_magnitude_h": _a_float(neo.get("absolute_magnitude_h")),
        "is_potentially_hazardous": neo.get("is_potentially_hazardous_asteroid"),
        "estimated_diameter_min_km": _a_float(
            diametro_km.get("estimated_diameter_min")
        ),
        "estimated_diameter_max_km": _a_float(
            diametro_km.get("estimated_diameter_max")
        ),
        "close_approach_date": primera_aproximacion.get(
            "close_approach_date", fecha_aproximacion
        ),
        "relative_velocity_km_h": _a_float(
            velocidad.get("kilometers_per_hour")
        ),
        "miss_distance_km": _a_float(
            distancia.get("kilometers")
        ),
        "orbiting_body": primera_aproximacion.get("orbiting_body"),
    }


def _a_float(valor) -> float:
    """
    Convierte un valor a float de forma segura.

    La API de NASA a veces retorna valores numéricos como cadenas de texto
    (por ejemplo, "55000.123"). Esta función maneja ambos casos.

    Args:
        valor: Valor a convertir (puede ser str, int, float o None).

    Returns:
        Valor como float, o None si la conversión no es posible.
    """
    if valor is None:
        return None
    try:
        return float(valor)
    except (ValueError, TypeError):
        logger.warning("No se pudo convertir a float: %s", valor)
        return None


def aplanar_respuesta_neows(datos: dict) -> list:
    """
    Procesa la respuesta de NeoWs, con o sin la envoltura de auditoría de Bronze,
    y aplana todos los NEOs.

    La estructura de la API NeoWs organiza los NEOs por fecha dentro
    del campo 'near_earth_objects':
    {
        "near_earth_objects": {
            "2024-01-15": [neo1, neo2, ...],
            "2024-01-16": [neo3, neo4, ...]
        }
    }

    Esta función itera sobre todas las fechas y todos los NEOs,
    aplanando cada uno.

    Args:
        datos: Respuesta completa de la API NeoWs (JSON deserializado).

    Returns:
        Lista de diccionarios, donde cada diccionario es un NEO aplanado.
    """
    if "payload" in datos:
        payload = datos["payload"]
        if not isinstance(payload, dict):
            raise ValueError(
                "El campo 'payload' del archivo Bronze debe ser un objeto JSON."
            )
        datos = payload

    neos_aplanados = []

    # Obtener el diccionario de NEOs agrupados por fecha
    neos_por_fecha = datos.get("near_earth_objects", {})

    if not neos_por_fecha:
        logger.warning("No se encontraron datos en 'near_earth_objects'")
        return neos_aplanados

    # Iterar sobre cada fecha y sus NEOs correspondientes
    for fecha, lista_neos in neos_por_fecha.items():
        logger.info(
            "Procesando fecha %s — %d NEOs encontrados",
            fecha, len(lista_neos)
        )

        for neo in lista_neos:
            neo_aplanado = aplanar_neo(neo, fecha)
            neos_aplanados.append(neo_aplanado)

    logger.info("Total de NEOs aplanados: %d", len(neos_aplanados))
    return neos_aplanados


def validar_registros(registros: list) -> Tuple[list, list]:
    """
    Separa los registros en válidos e inválidos según los campos requeridos.

    Un registro es inválido si alguno de los campos requeridos es None o
    está ausente. Los registros inválidos se envían a cuarentena para
    investigación posterior, mientras que los válidos continúan al
    procesamiento Silver.

    Principio de cuarentena:
        No descartamos datos — los registros con problemas se almacenan
        por separado para que puedan ser revisados y potencialmente
        corregidos en el futuro.

    Args:
        registros: Lista de diccionarios con NEOs aplanados.

    Returns:
        Tupla (válidos, inválidos):
            - válidos: Lista de registros que pasaron la validación.
            - inválidos: Lista de registros con campos faltantes,
              cada uno enriquecido con información del motivo de rechazo.
    """
    validos = []
    invalidos = []

    for registro in registros:
        # Verificar cada campo requerido
        campos_faltantes = [
            campo for campo in CAMPOS_REQUERIDOS
            if registro.get(campo) is None
        ]

        if campos_faltantes:
            # Registrar el motivo de la invalidez para auditoría
            registro["_quarantine_reason"] = (
                f"Campos requeridos faltantes o nulos: {campos_faltantes}"
            )
            registro["_quarantine_timestamp"] = datetime.now(
                timezone.utc
            ).isoformat()
            invalidos.append(registro)

            logger.warning(
                "NEO %s enviado a cuarentena — Campos faltantes: %s",
                registro.get("neo_id", "DESCONOCIDO"),
                campos_faltantes
            )
        else:
            validos.append(registro)

    logger.info(
        "Resultado de validación — Válidos: %d, Inválidos: %d",
        len(validos), len(invalidos)
    )

    return validos, invalidos


def guardar_parquet_en_s3(
    bucket: str,
    registros: list,
    fecha_compacta: str
) -> str:
    """
    Convierte los registros válidos a formato Parquet y los guarda en S3.

    ¿Por qué Parquet?
        - Formato columnar: consultas analíticas son mucho más rápidas
        - Compresión eficiente: ocupa menos espacio que JSON o CSV
        - Tipos de datos nativos: preserva tipos (float, bool, etc.)
        - Compatibilidad con Athena, Spark, Glue y otros servicios

    Args:
        bucket: Nombre del bucket S3.
        registros: Lista de diccionarios con NEOs válidos.
        fecha_compacta: Fecha en formato 'YYYYMMDD'.

    Returns:
        Ruta S3 donde se guardó el archivo.
    """
    # Parsear la fecha para construir la ruta particionada
    fecha_obj = datetime.strptime(fecha_compacta, "%Y%m%d")

    ruta_s3 = (
        f"silver/"
        f"year={fecha_obj.year}/"
        f"month={fecha_obj.month:02d}/"
        f"neo_{fecha_compacta}.parquet"
    )

    logger.info(
        "Convirtiendo %d registros a Parquet → s3://%s/%s",
        len(registros), bucket, ruta_s3
    )

    # Crear DataFrame de pandas a partir de los registros
    df = pd.DataFrame(registros)

    # -------------------------------------------------------------------
    # Asegurar tipos de datos correctos para el esquema Parquet
    # -------------------------------------------------------------------
    # Definimos explícitamente los tipos para garantizar consistencia
    # en el esquema, independientemente de los datos de cada ejecución.
    # -------------------------------------------------------------------
    esquema_tipos = {
        "neo_id": "str",
        "name": "str",
        "absolute_magnitude_h": "float64",
        "is_potentially_hazardous": "bool",
        "estimated_diameter_min_km": "float64",
        "estimated_diameter_max_km": "float64",
        "close_approach_date": "str",
        "relative_velocity_km_h": "float64",
        "miss_distance_km": "float64",
        "orbiting_body": "str",
    }

    for columna, tipo in esquema_tipos.items():
        if columna in df.columns:
            try:
                df[columna] = df[columna].astype(tipo)
            except (ValueError, TypeError) as e:
                logger.warning(
                    "No se pudo convertir columna '%s' a tipo '%s': %s",
                    columna, tipo, e
                )

    # Escribir Parquet a un buffer en memoria
    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    # Subir a S3
    s3_client.put_object(
        Bucket=bucket,
        Key=ruta_s3,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    logger.info(
        "Parquet guardado exitosamente — %d registros, %d bytes",
        len(df), len(buffer.getvalue())
    )

    return ruta_s3


def guardar_cuarentena_en_s3(
    bucket: str,
    registros_invalidos: list,
    fecha_compacta: str
) -> str:
    """
    Guarda los registros inválidos en la carpeta de cuarentena.

    Los registros en cuarentena se guardan en formato JSON para facilitar
    la inspección manual. Cada registro incluye los metadatos del motivo
    de rechazo (_quarantine_reason).

    Args:
        bucket: Nombre del bucket S3.
        registros_invalidos: Lista de registros que no pasaron la validación.
        fecha_compacta: Fecha en formato 'YYYYMMDD'.

    Returns:
        Ruta S3 donde se guardó el archivo de cuarentena.
    """
    fecha_obj = datetime.strptime(fecha_compacta, "%Y%m%d")

    ruta_s3 = (
        f"cuarentena/"
        f"year={fecha_obj.year}/"
        f"month={fecha_obj.month:02d}/"
        f"neo_{fecha_compacta}_quarantine.json"
    )

    logger.info(
        "Guardando %d registros en cuarentena → s3://%s/%s",
        len(registros_invalidos), bucket, ruta_s3
    )

    contenido = json.dumps(
        registros_invalidos,
        ensure_ascii=False,
        indent=2
    )

    s3_client.put_object(
        Bucket=bucket,
        Key=ruta_s3,
        Body=contenido.encode("utf-8"),
        ContentType="application/json"
    )

    logger.info("Archivo de cuarentena guardado exitosamente")
    return ruta_s3


def lambda_handler(event: dict, context) -> dict:
    """
    Punto de entrada principal de la función Lambda.

    Orquesta el flujo completo de transformación Silver:
        1. Extraer bucket y key del evento S3
        2. Leer el JSON Bronze desde S3
        3. Aplanar la estructura anidada de NeoWs
        4. Validar campos requeridos
        5. Guardar válidos como Parquet en Silver
        6. Guardar inválidos como JSON en Cuarentena

    Flujo de idempotencia:
        Las rutas de salida son deterministas (basadas en la fecha
        del archivo de entrada). Re-ejecutar para el mismo archivo
        Bronze sobreescribe los archivos Silver y Cuarentena
        correspondientes.

    Args:
        event: Evento S3 con la información del archivo creado en Bronze.
               Estructura esperada:
               {
                   "Records": [{
                       "s3": {
                           "bucket": {"name": "mi-bucket"},
                           "object": {"key": "bronze/year=.../neo_YYYYMMDD.json"}
                       }
                   }]
               }
        context: Objeto de contexto de Lambda (proporcionado por AWS).

    Returns:
        Diccionario con el resultado de la ejecución.
    """
    logger.info("=" * 60)
    logger.info("INICIO — Lambda transform_silver (Asteroid Pipeline)")
    logger.info("Evento recibido: %s", json.dumps(event))
    logger.info("=" * 60)

    try:
        # ---------------------------------------------------------------
        # Paso 1: Extraer información del evento S3
        # ---------------------------------------------------------------
        # El evento S3 puede contener múltiples registros, pero en la
        # práctica cada evento de creación de objeto genera un solo
        # registro. Procesamos solo el primero.
        # ---------------------------------------------------------------
        registro_s3 = event["Records"][0]["s3"]
        bucket_origen = registro_s3["bucket"]["name"]
        key_origen = registro_s3["object"]["key"]

        # Usar el bucket de la variable de entorno si está disponible;
        # de lo contrario, usar el bucket del evento
        bucket_destino = os.environ.get("BUCKET_NAME", bucket_origen)

        logger.info("Archivo Bronze detectado: s3://%s/%s", bucket_origen, key_origen)

        # ---------------------------------------------------------------
        # Paso 2: Extraer la fecha del nombre del archivo
        # ---------------------------------------------------------------
        fecha_compacta = extraer_fecha_de_key(key_origen)

        # ---------------------------------------------------------------
        # Paso 3: Leer el JSON Bronze desde S3
        # ---------------------------------------------------------------
        datos_bronze = leer_json_desde_s3(bucket_origen, key_origen)

        # ---------------------------------------------------------------
        # Paso 4: Aplanar la estructura anidada de NeoWs
        # ---------------------------------------------------------------
        registros_aplanados = aplanar_respuesta_neows(datos_bronze)

        if not registros_aplanados:
            logger.warning("No se encontraron registros NEO para procesar")
            return {
                "statusCode": 200,
                "body": {
                    "message": "Sin registros NEO para procesar",
                    "source_key": key_origen
                }
            }

        # ---------------------------------------------------------------
        # Paso 5: Validar campos requeridos
        # ---------------------------------------------------------------
        validos, invalidos = validar_registros(registros_aplanados)

        # ---------------------------------------------------------------
        # Paso 6: Guardar resultados
        # ---------------------------------------------------------------
        ruta_silver = None
        ruta_cuarentena = None

        # Guardar registros válidos como Parquet en Silver
        if validos:
            ruta_silver = guardar_parquet_en_s3(
                bucket_destino, validos, fecha_compacta
            )

        # Guardar registros inválidos en cuarentena
        if invalidos:
            ruta_cuarentena = guardar_cuarentena_en_s3(
                bucket_destino, invalidos, fecha_compacta
            )

        # ---------------------------------------------------------------
        # Resultado exitoso
        # ---------------------------------------------------------------
        resultado = {
            "statusCode": 200,
            "body": {
                "message": "Transformación Silver completada exitosamente",
                "source_key": key_origen,
                "total_records": len(registros_aplanados),
                "valid_records": len(validos),
                "quarantined_records": len(invalidos),
                "silver_path": (
                    f"s3://{bucket_destino}/{ruta_silver}" if ruta_silver else None
                ),
                "quarantine_path": (
                    f"s3://{bucket_destino}/{ruta_cuarentena}"
                    if ruta_cuarentena
                    else None
                ),
                "processing_timestamp": datetime.now(timezone.utc).isoformat()
            }
        }

        logger.info("=" * 60)
        logger.info(
            "FIN EXITOSO — %d válidos, %d en cuarentena",
            len(validos), len(invalidos)
        )
        logger.info("=" * 60)

        return resultado

    except KeyError as error:
        mensaje_error = (
            f"Estructura de evento inesperada — campo faltante: {error}"
        )
        logger.error(mensaje_error)
        raise ValueError(mensaje_error) from error

    except ClientError as error:
        mensaje_error = f"Error en servicio AWS S3: {error}"
        logger.error(mensaje_error)
        raise RuntimeError(mensaje_error) from error

    except Exception as error:
        mensaje_error = f"Error inesperado en transformación Silver: {error}"
        logger.exception(mensaje_error)
        raise