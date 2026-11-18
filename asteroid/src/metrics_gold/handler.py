"""
===============================================================================
 Lambda: metrics_gold — Cálculo de métricas de riesgo de asteroides
===============================================================================

Descripción:
    Esta función Lambda ejecuta una consulta CTAS (CREATE TABLE AS SELECT)
    en Amazon Athena para calcular métricas avanzadas de riesgo de impacto
    para cada objeto cercano a la Tierra (NEO). Los resultados se almacenan
    en la capa Gold del Data Lake.

Arquitectura Medallion — Capa Gold:
    La capa Gold contiene datos agregados y métricas listas para consumo
    por aplicaciones de negocio. En este caso, calculamos métricas de
    riesgo que serán utilizadas en las exhibiciones de los museos y
    planetarios de la Dirección General de Exposiciones.

Métricas calculadas:
    1. estimated_mass_kg — Masa estimada usando densidad promedio de asteroides
    2. impact_energy_megatons — Energía cinética de impacto en megatones de TNT
    3. risk_score — Puntuación de riesgo compuesta (escala 0-10)

Fórmulas (proporcionadas por el cliente/profesor):
    - Masa: V = (4/3) × π × (d_avg/2)³, masa = V × 2600 kg/m³
    - Energía: E = ½mv², convertida a megatones de TNT (÷ 4.184×10¹⁵ J)
    - Riesgo: Para NEOs peligrosos: min(10, (E/d_miss) × 10⁶)

Cliente:
    Dirección General de Exposiciones (museos y planetarios)

Trigger:
    Evento S3 — Creación de objeto en prefijo 'silver/'

Variables de entorno requeridas:
    - BUCKET_NAME: Nombre del bucket S3
    - ATHENA_DATABASE: Nombre de la base de datos en Athena/Glue Catalog
    - ATHENA_OUTPUT_LOCATION: s3:// path para resultados de Athena. Debe ser
      un prefijo que NO dispare triggers (por ejemplo s3://<bucket>/athena-results/),
      nunca bronze/ ni silver/.

Prerrequisito:
    La tabla externa <ATHENA_DATABASE>.silver_asteroids (apuntando a
    s3://<bucket>/silver/) debe existir en el Glue Data Catalog. La crea
    infra/template.yaml (recurso SilverAsteroidsTable); si no existe, el CTAS
    falla. La tabla Gold (asteroid_risk_metrics) la crea esta misma Lambda.

Autor: Equipo de Ingeniería de Datos
Proyecto: Data Platform Modernization — Asteroid Pipeline
===============================================================================
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Tuple

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuración del logger
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Cliente de AWS Athena (reutilizable entre invocaciones)
# ---------------------------------------------------------------------------
athena_client = boto3.client("athena")

# ---------------------------------------------------------------------------
# Constantes físicas para los cálculos de riesgo
# ---------------------------------------------------------------------------
# Densidad promedio de asteroides tipo S (rocosos): ~2600 kg/m³
# Referencia: Carry, B. (2012) "Density of asteroids"
DENSIDAD_ASTEROIDE_KG_M3 = 2600

# 1 megatón de TNT = 4.184 × 10¹⁵ joules
JOULES_POR_MEGATON = 4.184e15

# Valor de π con suficiente precisión
PI = 3.141592653589793

# ---------------------------------------------------------------------------
# Configuración de polling para Athena
# ---------------------------------------------------------------------------
# Athena ejecuta queries de forma asíncrona. Necesitamos hacer polling
# para saber cuándo termina.
# ---------------------------------------------------------------------------
ATHENA_MAX_INTENTOS = 60  # Máximo número de intentos de polling
ATHENA_INTERVALO_SEGUNDOS = 2  # Segundos entre cada intento


def construir_query_ctas(
    database: str,
    bucket: str,
    tabla_gold: str = "asteroid_risk_metrics"
) -> Tuple[str, str]:
    """
    Construye la consulta SQL CTAS para calcular las métricas Gold.

    CTAS (CREATE TABLE AS SELECT) permite crear una tabla nueva con los
    resultados de un SELECT, almacenando los datos en formato Parquet
    en S3. Esto es más eficiente que INSERT INTO porque Athena puede
    optimizar la escritura.

    Usamos DROP TABLE + CTAS en lugar de INSERT para garantizar
    idempotencia — cada ejecución reemplaza completamente la tabla.

    Fórmulas implementadas en SQL:
    ─────────────────────────────────────────────────────────────────
    1. Diámetro promedio (metros):
       avg_diameter_m = (diameter_min_km + diameter_max_km) / 2 * 1000

    2. Volumen (m³):
       volume = (4/3) × π × (avg_diameter_m / 2)³

    3. Masa estimada (kg):
       mass = volume × 2600

    4. Velocidad (m/s):
       velocity_m_s = velocity_km_h × 1000 / 3600

    5. Energía cinética (joules):
       energy = 0.5 × mass × velocity²

    6. Energía de impacto (megatones TNT):
       impact_energy = energy / 4.184e15

    7. Puntuación de riesgo (0-10):
       Si es potencialmente peligroso:
           risk = min(10, (impact_energy / miss_distance_km) × 1e6)
       Si no: risk = 0
    ─────────────────────────────────────────────────────────────────

    Args:
        database: Nombre de la base de datos en Athena.
        bucket: Nombre del bucket S3 para los resultados.
        tabla_gold: Nombre de la tabla Gold (default: 'asteroid_risk_metrics').

    Returns:
        Tupla (query_drop, query_ctas) con las dos sentencias SQL que deben
        ejecutarse por separado y en ese orden.
    """
    # Ruta en S3 donde se almacenarán los datos de la tabla Gold
    ruta_gold = f"s3://{bucket}/gold/metrics/"

    # -------------------------------------------------------------------
    # DROP TABLE: una sola sentencia, sin comentarios ni ';'
    # -------------------------------------------------------------------
    # Athena ejecuta los DDL (DROP/CREATE sin AS SELECT) con el parser de
    # Hive, que puede fallar si la sentencia empieza con comentarios o trae
    # varias sentencias. Por eso va limpia. El DROP de una tabla externa NO
    # borra los datos en S3; eso lo hace limpiar_ubicacion_gold().
    # -------------------------------------------------------------------
    query_drop = f"DROP TABLE IF EXISTS {database}.{tabla_gold}"

    # -------------------------------------------------------------------
    # Consulta SQL con CTEs para claridad y mantenibilidad
    # -------------------------------------------------------------------
    query_ctas = f"""
    CREATE TABLE {database}.{tabla_gold}
    WITH (
        format = 'PARQUET',
        external_location = '{ruta_gold}',
        write_compression = 'SNAPPY'
    )
    AS
    WITH calculos_base AS (
        -- ─────────────────────────────────────────────────────────────
        -- CTE 1: Cálculos intermedios (diámetro, volumen, masa, velocidad)
        -- ─────────────────────────────────────────────────────────────
        SELECT
            neo_id,
            name,
            close_approach_date,
            is_potentially_hazardous,

            -- Diámetro promedio en kilómetros (para la columna de salida)
            (estimated_diameter_min_km + estimated_diameter_max_km) / 2.0
                AS estimated_diameter_avg_km,

            -- Diámetro promedio en metros (para cálculos de masa)
            ((estimated_diameter_min_km + estimated_diameter_max_km) / 2.0) * 1000.0
                AS avg_diameter_m,

            -- Velocidad relativa original (para la columna de salida)
            relative_velocity_km_h,

            -- Velocidad en metros por segundo (para cálculos de energía)
            (relative_velocity_km_h * 1000.0 / 3600.0)
                AS velocity_m_s,

            -- Distancia de fallo (para la columna de salida y riesgo)
            miss_distance_km

        FROM {database}.silver_asteroids
    ),

    calculos_masa_energia AS (
        -- ─────────────────────────────────────────────────────────────
        -- CTE 2: Masa estimada y energía de impacto
        -- ─────────────────────────────────────────────────────────────
        SELECT
            cb.*,

            -- Volumen de una esfera: V = (4/3) × π × r³
            -- donde r = avg_diameter_m / 2
            ({4.0/3.0} * {PI} * POWER(avg_diameter_m / 2.0, 3))
                AS volume_m3,

            -- Masa estimada: masa = volumen × densidad
            ({4.0/3.0} * {PI} * POWER(avg_diameter_m / 2.0, 3)) * {DENSIDAD_ASTEROIDE_KG_M3}
                AS estimated_mass_kg,

            -- Energía cinética: E = ½mv²
            0.5 * ({4.0/3.0} * {PI} * POWER(avg_diameter_m / 2.0, 3)) * {DENSIDAD_ASTEROIDE_KG_M3}
                * POWER(velocity_m_s, 2)
                AS kinetic_energy_joules

        FROM calculos_base cb
    )

    -- ─────────────────────────────────────────────────────────────────
    -- SELECT final: Métricas Gold con puntuación de riesgo
    -- ─────────────────────────────────────────────────────────────────
    SELECT
        neo_id,
        name,
        close_approach_date,
        is_potentially_hazardous,
        estimated_diameter_avg_km,
        estimated_mass_kg,
        relative_velocity_km_h,
        miss_distance_km,

        -- Energía de impacto en megatones de TNT
        (kinetic_energy_joules / {JOULES_POR_MEGATON})
            AS impact_energy_megatons,

        -- Puntuación de riesgo (escala 0-10)
        -- Solo los NEOs potencialmente peligrosos reciben puntuación > 0
        CASE
            WHEN is_potentially_hazardous = true THEN
                LEAST(
                    10.0,
                    ((kinetic_energy_joules / {JOULES_POR_MEGATON}) / miss_distance_km) * 1e6
                )
            ELSE 0.0
        END AS risk_score

    FROM calculos_masa_energia
    """

    return query_drop, query_ctas


def ejecutar_query_athena(
    query: str,
    database: str,
    output_location: str
) -> str:
    """
    Ejecuta una consulta SQL en Amazon Athena y espera su finalización.

    Athena es un servicio serverless de consultas SQL que trabaja
    directamente sobre datos en S3. Las consultas se ejecutan de forma
    asíncrona, por lo que necesitamos hacer polling del estado.

    Args:
        query: Consulta SQL a ejecutar.
        database: Base de datos de Athena (Glue Catalog).
        output_location: Ruta S3 para los resultados intermedios de Athena.

    Returns:
        ID de ejecución de la consulta (QueryExecutionId).

    Raises:
        RuntimeError: Si la consulta falla o excede el tiempo de espera.
    """
    logger.info("Ejecutando query en Athena (database: %s)", database)
    logger.info("Query SQL:\n%s", query[:500] + "..." if len(query) > 500 else query)

    try:
        # Iniciar la ejecución de la consulta
        respuesta = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location}
        )

        query_execution_id = respuesta["QueryExecutionId"]
        logger.info("Query iniciada — ID: %s", query_execution_id)

        # -------------------------------------------------------------------
        # Polling: esperar a que la consulta termine
        # -------------------------------------------------------------------
        # Athena ejecuta queries de forma asíncrona. Verificamos el estado
        # periódicamente hasta que termine o se agote el tiempo.
        # -------------------------------------------------------------------
        for intento in range(ATHENA_MAX_INTENTOS):
            estado_respuesta = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )

            estado = estado_respuesta["QueryExecution"]["Status"]["State"]
            logger.info(
                "Estado de query (intento %d/%d): %s",
                intento + 1, ATHENA_MAX_INTENTOS, estado
            )

            if estado == "SUCCEEDED":
                logger.info("Query completada exitosamente")
                return query_execution_id

            elif estado in ("FAILED", "CANCELLED"):
                # Obtener el mensaje de error para diagnóstico
                razon = estado_respuesta["QueryExecution"]["Status"].get(
                    "StateChangeReason", "Sin razón proporcionada"
                )
                mensaje_error = (
                    f"Query de Athena falló con estado '{estado}': {razon}"
                )
                logger.error(mensaje_error)
                raise RuntimeError(mensaje_error)

            # Esperar antes del siguiente intento
            time.sleep(ATHENA_INTERVALO_SEGUNDOS)

        # Si llegamos aquí, se agotó el tiempo de espera
        raise RuntimeError(
            f"Query de Athena excedió el tiempo máximo de espera "
            f"({ATHENA_MAX_INTENTOS * ATHENA_INTERVALO_SEGUNDOS} segundos). "
            f"ID: {query_execution_id}"
        )

    except ClientError as error:
        logger.error("Error al ejecutar query en Athena: %s", error)
        raise


def limpiar_ubicacion_gold(bucket: str, prefijo: str = "gold/metrics/") -> None:
    """
    Elimina los archivos existentes en la ubicación Gold antes de escribir.

    Athena CTAS requiere que la ubicación external_location esté vacía.
    Esta función elimina todos los objetos existentes en el prefijo
    especificado para garantizar que la operación CTAS pueda completarse.

    Nota sobre idempotencia:
        Esta limpieza es parte del mecanismo de idempotencia. Al eliminar
        y recrear los datos, garantizamos que el resultado siempre refleja
        la ejecución más reciente.

    Args:
        bucket: Nombre del bucket S3.
        prefijo: Prefijo de S3 a limpiar (default: 'gold/metrics/').
    """
    logger.info(
        "Limpiando ubicación Gold: s3://%s/%s",
        bucket, prefijo
    )

    s3_client = boto3.client("s3")

    try:
        # Listar todos los objetos en el prefijo
        respuesta = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefijo
        )

        # Si no hay objetos, no hay nada que eliminar
        if "Contents" not in respuesta:
            logger.info("Ubicación Gold ya está vacía")
            return

        # Preparar la lista de objetos a eliminar
        objetos_a_eliminar = [
            {"Key": obj["Key"]}
            for obj in respuesta["Contents"]
        ]

        # Eliminar en lote (máximo 1000 por llamada)
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": objetos_a_eliminar}
        )

        logger.info(
            "Se eliminaron %d archivos de la ubicación Gold",
            len(objetos_a_eliminar)
        )

        # Si hay más de 1000 objetos, continuar eliminando
        while respuesta.get("IsTruncated", False):
            respuesta = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefijo,
                ContinuationToken=respuesta["NextContinuationToken"]
            )

            if "Contents" in respuesta:
                objetos_a_eliminar = [
                    {"Key": obj["Key"]}
                    for obj in respuesta["Contents"]
                ]
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": objetos_a_eliminar}
                )
                logger.info(
                    "Se eliminaron %d archivos adicionales",
                    len(objetos_a_eliminar)
                )

    except ClientError as error:
        logger.error("Error al limpiar ubicación Gold: %s", error)
        raise


def lambda_handler(event: dict, context) -> dict:
    """
    Punto de entrada principal de la función Lambda.

    Orquesta el flujo completo de generación de métricas Gold:
        1. Leer variables de entorno
        2. Limpiar la ubicación Gold anterior (para idempotencia)
        3. Ejecutar DROP TABLE para eliminar la tabla anterior
        4. Ejecutar CTAS para crear la tabla Gold con métricas calculadas
        5. Retornar resultado

    Métricas calculadas:
        - estimated_mass_kg: Masa estimada del asteroide (densidad 2600 kg/m³)
        - impact_energy_megatons: Energía cinética de impacto en megatones TNT
        - risk_score: Puntuación de riesgo compuesta (0-10)

    Flujo de idempotencia:
        Se elimina la tabla Gold existente y se recrea desde cero usando
        todos los datos de la capa Silver. Esto garantiza que las métricas
        siempre estén actualizadas y no haya duplicados.

    Args:
        event: Evento S3 con la información del archivo creado en Silver.
               Estructura esperada:
               {
                   "Records": [{
                       "s3": {
                           "bucket": {"name": "mi-bucket"},
                           "object": {"key": "silver/year=.../neo_YYYYMMDD.parquet"}
                       }
                   }]
               }
        context: Objeto de contexto de Lambda (proporcionado por AWS).

    Returns:
        Diccionario con el resultado de la ejecución.
    """
    logger.info("=" * 60)
    logger.info("INICIO — Lambda metrics_gold (Asteroid Pipeline)")
    logger.info("Evento recibido: %s", json.dumps(event))
    logger.info("=" * 60)

    try:
        # ---------------------------------------------------------------
        # Paso 1: Leer variables de entorno
        # ---------------------------------------------------------------
        bucket_name = os.environ["BUCKET_NAME"]
        athena_database = os.environ["ATHENA_DATABASE"]
        athena_output = os.environ["ATHENA_OUTPUT_LOCATION"]

        logger.info("Bucket: %s", bucket_name)
        logger.info("Base de datos Athena: %s", athena_database)
        logger.info("Ubicación de salida Athena: %s", athena_output)

        # ---------------------------------------------------------------
        # Paso 2: Limpiar la ubicación Gold en S3
        # ---------------------------------------------------------------
        # Athena CTAS requiere que la ubicación external_location esté
        # vacía. Eliminamos los archivos existentes antes de recrear.
        # ---------------------------------------------------------------
        limpiar_ubicacion_gold(bucket_name)

        # ---------------------------------------------------------------
        # Paso 3: Construir las consultas SQL
        # ---------------------------------------------------------------
        query_drop, query_ctas = construir_query_ctas(
            athena_database, bucket_name
        )

        # ---------------------------------------------------------------
        # Paso 4: Ejecutar DROP TABLE (eliminar tabla anterior)
        # ---------------------------------------------------------------
        logger.info("Ejecutando DROP TABLE para tabla Gold anterior...")
        ejecutar_query_athena(query_drop, athena_database, athena_output)

        # ---------------------------------------------------------------
        # Paso 5: Ejecutar CTAS (crear tabla Gold con métricas)
        # ---------------------------------------------------------------
        logger.info("Ejecutando CTAS para crear tabla Gold con métricas...")
        query_id = ejecutar_query_athena(
            query_ctas, athena_database, athena_output
        )

        # ---------------------------------------------------------------
        # Resultado exitoso
        # ---------------------------------------------------------------
        resultado = {
            "statusCode": 200,
            "body": {
                "message": "Métricas Gold calculadas exitosamente",
                "athena_query_id": query_id,
                "athena_database": athena_database,
                "gold_table": "asteroid_risk_metrics",
                "gold_location": f"s3://{bucket_name}/gold/metrics/",
                "metrics_calculated": [
                    "estimated_mass_kg",
                    "impact_energy_megatons",
                    "risk_score"
                ],
                "processing_timestamp": datetime.now(timezone.utc).isoformat()
            }
        }

        logger.info("=" * 60)
        logger.info("FIN EXITOSO — Tabla Gold creada/actualizada")
        logger.info("=" * 60)

        return resultado

    except KeyError as error:
        mensaje_error = (
            f"Variable de entorno requerida no configurada: {error}"
        )
        logger.error(mensaje_error)
        raise RuntimeError(mensaje_error) from error

    except (RuntimeError, ClientError) as error:
        # Error en Athena o en un servicio AWS. Se relanza (en lugar de
        # devolver un dict con statusCode 500) para que Lambda marque la
        # invocación como fallida y se dispare la alarma de CloudWatch.
        logger.error("Error en métricas Gold: %s", error)
        raise

    except Exception as error:
        logger.exception("Error inesperado en métricas Gold: %s", error)
        raise