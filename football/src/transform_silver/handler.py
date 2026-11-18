"""
Lambda: transform_silver — Transformación y validación de datos Bronze a Silver.

Descripción:
    Esta función Lambda se activa automáticamente cuando se crea un nuevo objeto
    en el prefijo 'bronze/' del bucket S3 (evento s3:ObjectCreated). Lee el JSON
    crudo de la capa Bronze, valida los campos requeridos de cada fixture,
    separa los registros inválidos a una carpeta de cuarentena, y transforma
    los registros válidos a formato Parquet tabular en la capa Silver.

Flujo:
    S3 event (bronze/) → Lambda transform_silver → S3 silver/season=YYYY/fixtures_YYYYMMDD.parquet
                                                  → S3 cuarentena/year=YYYY/month=MM/fixtures_YYYYMMDD_quarantine.json

Validación de campos requeridos:
    - fixture.id, fixture.date
    - league.id
    - teams.home.id, teams.away.id
    - goals.home, goals.away

Idempotencia:
    Al re-ejecutarse para el mismo archivo Bronze, sobrescribe los archivos
    Silver y de cuarentena correspondientes sin generar duplicados.

Variables de entorno requeridas:
    - BUCKET_NAME: Nombre del bucket S3 (lectura y escritura).

Autor: Equipo de Ingeniería de Datos — Dirección de Selecciones Nacionales
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from io import BytesIO
from typing import Any
from urllib.parse import unquote_plus

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Configuración del logger para CloudWatch
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)
GOLD_READY_PREFIX = "control/gold-ready/"
FINAL_STATUSES = {"FT", "AET", "PEN"}

# ---------------------------------------------------------------------------
# Cliente de AWS S3 (reutilizado entre invocaciones)
# ---------------------------------------------------------------------------
s3_client = boto3.client("s3")

# ---------------------------------------------------------------------------
# Campos requeridos para que un fixture sea considerado válido.
# Cada tupla representa la ruta de acceso anidada dentro del JSON del fixture.
# ---------------------------------------------------------------------------
CAMPOS_REQUERIDOS = [
    ("fixture", "id"),
    ("fixture", "date"),
    ("fixture", "status", "short"),
    ("league", "id"),
    ("league", "season"),
    ("teams", "home", "id"),
    ("teams", "away", "id"),
]


def _obtener_valor_anidado(diccionario: dict, *claves) -> Any:
    """
    Accede a un valor en un diccionario anidado de forma segura.

    Recorre las claves proporcionadas una por una. Si en algún punto
    el valor intermedio no es un diccionario o la clave no existe,
    retorna None en lugar de lanzar una excepción.

    Args:
        diccionario (dict): Diccionario raíz desde donde buscar.
        *claves: Secuencia de claves para navegar la estructura anidada.

    Returns:
        Any: El valor encontrado, o None si la ruta no existe.

    Ejemplo:
        >>> datos = {"fixture": {"id": 123}}
        >>> _obtener_valor_anidado(datos, "fixture", "id")
        123
    """
    actual = diccionario
    for clave in claves:
        if isinstance(actual, dict):
            actual = actual.get(clave)
        else:
            return None
    return actual


def _entero_valido(value: Any, minimum: int) -> bool:
    if isinstance(value, bool) or not re.fullmatch(r"\d+", str(value)):
        return False
    return int(value) >= minimum


def _validar_fixture(fixture: dict) -> bool:
    """
    Valida que un fixture contenga todos los campos requeridos.

    Recorre la lista CAMPOS_REQUERIDOS y verifica que cada ruta anidada
    exista y no sea None. Los goles pueden ser 0 (valor válido), por lo
    que se verifica explícitamente contra None.

    Args:
        fixture (dict): Diccionario con los datos de un fixture individual.

    Returns:
        bool: True si todos los campos requeridos están presentes, False si no.
    """
    if not isinstance(fixture, dict):
        logger.warning("Fixture inválido: se esperaba un objeto JSON.")
        return False

    for ruta_campo in CAMPOS_REQUERIDOS:
        valor = _obtener_valor_anidado(fixture, *ruta_campo)
        if valor is None:
            nombre_campo = ".".join(ruta_campo)
            logger.warning(
                "Fixture inválido — campo faltante: '%s' | datos: %s",
                nombre_campo,
                json.dumps(fixture, default=str)[:200],
            )
            return False

    for path in (
        ("fixture", "id"),
        ("league", "id"),
        ("league", "season"),
        ("teams", "home", "id"),
        ("teams", "away", "id"),
    ):
        value = _obtener_valor_anidado(fixture, *path)
        if not _entero_valido(value, 1):
            logger.warning("Fixture inválido — '%s' debe ser entero positivo.", ".".join(path))
            return False

    fixture_date = _obtener_valor_anidado(fixture, "fixture", "date")
    try:
        datetime.fromisoformat(str(fixture_date).replace("Z", "+00:00"))
    except ValueError:
        logger.warning("Fixture inválido — fixture.date no es una fecha ISO 8601.")
        return False

    status = _obtener_valor_anidado(fixture, "fixture", "status", "short")
    if not isinstance(status, str) or not status.strip():
        logger.warning("Fixture inválido — fixture.status.short está vacío.")
        return False

    goals = fixture.get("goals")
    if not isinstance(goals, dict) or not {"home", "away"}.issubset(goals):
        logger.warning("Fixture inválido — faltan las claves goals.home/goals.away.")
        return False
    home_goals = goals["home"]
    away_goals = goals["away"]
    if (home_goals is None) != (away_goals is None):
        logger.warning("Fixture inválido — falta el marcador de uno de los equipos.")
        return False
    for value in (home_goals, away_goals):
        if value is not None:
            if not _entero_valido(value, 0):
                logger.warning("Fixture inválido — los goles deben ser enteros no negativos.")
                return False
    if status in FINAL_STATUSES and home_goals is None:
        logger.warning("Fixture inválido — un partido finalizado no tiene marcador.")
        return False

    return True


def _aplanar_fixture(fixture: dict) -> dict:
    """
    Transforma un fixture anidado de API-Football a un registro tabular plano.

    Extrae y renombra los campos relevantes para crear una estructura
    adecuada para almacenamiento columnar (Parquet).

    Args:
        fixture (dict): Diccionario con la estructura anidada de un fixture.

    Returns:
        dict: Registro plano con las siguientes columnas:
            - fixture_id (int): Identificador único del partido.
            - league_id (int): Identificador de la liga.
            - league_name (str): Nombre de la liga.
            - season (int): Año de la temporada.
            - date (str): Fecha y hora del partido (ISO 8601).
            - home_team_id (int): ID del equipo local.
            - home_team_name (str): Nombre del equipo local.
            - away_team_id (int): ID del equipo visitante.
            - away_team_name (str): Nombre del equipo visitante.
            - goals_home (int): Goles del equipo local.
            - goals_away (int): Goles del equipo visitante.
            - venue_name (str): Nombre del estadio.
            - referee (str): Nombre del árbitro.
            - status_short (str): Estado abreviado del partido (ej. 'FT').
            - status_long (str): Estado descriptivo del partido.
    """
    return {
        "fixture_id": int(_obtener_valor_anidado(fixture, "fixture", "id")),
        "league_id": int(_obtener_valor_anidado(fixture, "league", "id")),
        "league_name": _obtener_valor_anidado(fixture, "league", "name") or "",
        "season": int(_obtener_valor_anidado(fixture, "league", "season")),
        "date": _obtener_valor_anidado(fixture, "fixture", "date") or "",
        "home_team_id": int(_obtener_valor_anidado(fixture, "teams", "home", "id")),
        "home_team_name": _obtener_valor_anidado(fixture, "teams", "home", "name") or "",
        "away_team_id": int(_obtener_valor_anidado(fixture, "teams", "away", "id")),
        "away_team_name": _obtener_valor_anidado(fixture, "teams", "away", "name") or "",
        "goals_home": (
            int(_obtener_valor_anidado(fixture, "goals", "home"))
            if _obtener_valor_anidado(fixture, "goals", "home") is not None
            else None
        ),
        "goals_away": (
            int(_obtener_valor_anidado(fixture, "goals", "away"))
            if _obtener_valor_anidado(fixture, "goals", "away") is not None
            else None
        ),
        "status_short": _obtener_valor_anidado(fixture, "fixture", "status", "short") or "",
        "status_long": _obtener_valor_anidado(fixture, "fixture", "status", "long") or "",
        "venue_name": _obtener_valor_anidado(fixture, "fixture", "venue", "name") or "",
        "referee": _obtener_valor_anidado(fixture, "fixture", "referee") or "",
        "status_short": _obtener_valor_anidado(fixture, "fixture", "status", "short") or "",
        "status_long": _obtener_valor_anidado(fixture, "fixture", "status", "long") or "",
    }


def _construir_tabla_parquet(registros: list[dict]) -> pa.Table:
    """
    Convierte una lista de registros planos en una tabla PyArrow con esquema explícito.

    Define un esquema tipado para garantizar consistencia en los archivos
    Parquet generados, independientemente de los valores de los datos.

    Args:
        registros (list[dict]): Lista de diccionarios con los fixtures aplanados.

    Returns:
        pa.Table: Tabla PyArrow lista para serializar como Parquet.
    """
    # Definir el esquema explícito para garantizar tipos consistentes
    esquema = pa.schema([
        pa.field("fixture_id", pa.int64()),
        pa.field("league_id", pa.int64()),
        pa.field("league_name", pa.string()),
        pa.field("date", pa.string()),
        pa.field("home_team_id", pa.int64()),
        pa.field("home_team_name", pa.string()),
        pa.field("away_team_id", pa.int64()),
        pa.field("away_team_name", pa.string()),
        pa.field("goals_home", pa.int64()),
        pa.field("goals_away", pa.int64()),
        pa.field("status_short", pa.string()),
        pa.field("status_long", pa.string()),
        pa.field("venue_name", pa.string()),
        pa.field("referee", pa.string()),
        pa.field("status_short", pa.string()),
        pa.field("status_long", pa.string()),
    ])

    # Convertir lista de diccionarios a columnas individuales
    columnas = {campo.name: [r[campo.name] for r in registros] for campo in esquema}

    return pa.table(columnas, schema=esquema)


def _extraer_fecha_de_key(s3_key: str) -> str:
    """
    Extrae la fecha compacta (YYYYMMDD) del nombre del archivo Bronze en S3.

    Busca el patrón 'fixtures_YYYYMMDD' en la key de S3 para determinar
    la fecha de los datos procesados.

    Args:
        s3_key (str): Ruta completa del objeto en S3.

    Returns:
        str: Fecha en formato YYYYMMDD.

    Raises:
        ValueError: Si no se puede extraer la fecha del nombre del archivo.

    Ejemplo:
        >>> _extraer_fecha_de_key("bronze/year=2026/month=11/fixtures_20261117.json")
        '20261117'
    """
    patron = r"bronze/year=(\d{4})/month=(\d{2})/fixtures_(\d{8})\.json"
    coincidencia = re.fullmatch(patron, s3_key)
    if not coincidencia:
        raise ValueError(
            f"No se pudo extraer la fecha del archivo Bronze: {s3_key}"
        )
    year, month, date_compact = coincidencia.groups()
    parsed_date = datetime.strptime(date_compact, "%Y%m%d")
    if parsed_date.strftime("%Y%m") != f"{year}{month}":
        raise ValueError(f"La partición no coincide con la fecha de Bronze: {s3_key}")
    return date_compact


def _listar_archivos_bronze(bucket: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix="bronze/"):
        for item in page.get("Contents", []):
            key = item["Key"]
            try:
                _extraer_fecha_de_key(key)
            except ValueError:
                continue
            keys.append(key)
    return sorted(keys, key=_extraer_fecha_de_key)


def _guardar_cuarentena(bucket: str, fecha_compacta: str, fixtures: list) -> str:
    fecha = datetime.strptime(fecha_compacta, "%Y%m%d")
    key = (
        f"cuarentena/year={fecha:%Y}/month={fecha:%m}/"
        f"fixtures_{fecha_compacta}_quarantine.json"
    )
    documento = {
        "timestamp_cuarentena": datetime.now(timezone.utc).isoformat(),
        "motivo": "Fixture con estructura o valores inválidos",
        "total_registros": len(fixtures),
        "registros": fixtures,
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(documento, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def _procesar_archivo_bronze(bucket: str, key: str) -> dict:
    fecha_compacta = _extraer_fecha_de_key(key)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    documento = json.loads(response["Body"].read().decode("utf-8"))
    if not isinstance(documento, dict):
        raise ValueError(f"El archivo Bronze no contiene un objeto JSON: {key}")

    respuesta_api = documento.get("respuesta_api", documento)
    if not isinstance(respuesta_api, dict):
        raise ValueError(f"respuesta_api inválida en {key}")
    if respuesta_api.get("errors"):
        raise RuntimeError(f"Bronze contiene errores reportados por API-Football: {key}")
    fixtures = respuesta_api.get("response")
    if not isinstance(fixtures, list):
        raise ValueError(f"response debe ser una lista en {key}")
    if not fixtures:
        logger.info("Sin fixtures en %s; no se genera un Parquet vacío.", key)
        return {"source_key": key, "silver_key": None, "valid": 0, "invalid": 0}

    fecha_objetivo = datetime.strptime(fecha_compacta, "%Y%m%d").date()
    validos = []
    invalidos = []
    for fixture in fixtures:
        valido = _validar_fixture(fixture)
        if valido:
            fixture_date = datetime.fromisoformat(
                fixture["fixture"]["date"].replace("Z", "+00:00")
            )
            valido = (
                fixture_date.tzinfo is not None
                and fixture_date.astimezone(timezone.utc).date() == fecha_objetivo
            )
            if not valido:
                logger.warning(
                    "Fixture inválido — fixture.date no corresponde al día UTC del archivo %s.",
                    fecha_compacta,
                )
        (validos if valido else invalidos).append(fixture)
    cuarentena_key = None
    if invalidos:
        cuarentena_key = _guardar_cuarentena(bucket, fecha_compacta, invalidos)
        logger.warning("%d fixture(s) enviados a %s", len(invalidos), cuarentena_key)
    if not validos:
        raise ValueError(f"Todos los fixtures de {key} son inválidos.")

    registros = [_aplanar_fixture(fixture) for fixture in validos]
    temporadas = {registro["season"] for registro in registros}
    if len(temporadas) != 1:
        raise ValueError(f"Un archivo Bronze contiene varias temporadas: {temporadas}")
    temporada = temporadas.pop()

    tabla = _construir_tabla_parquet(registros)
    buffer = BytesIO()
    pq.write_table(tabla, buffer, compression="snappy")
    key_silver = f"silver/season={temporada}/fixtures_{fecha_compacta}.parquet"
    s3_client.put_object(
        Bucket=bucket,
        Key=key_silver,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    return {
        "source_key": key,
        "silver_key": key_silver,
        "valid": len(validos),
        "invalid": len(invalidos),
        "season": temporada,
    }


def _publicar_marcador_gold(bucket: str, reason: str) -> str:
    key = (
        f"{GOLD_READY_PREFIX}"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    )
    marker = {
        "reason": reason,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(marker).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def lambda_handler(event: dict, context) -> dict:
    """Transforma uno o todos los archivos Bronze y notifica Gold al finalizar."""
    logger.info("=== INICIO: Transformación Silver de Football ===")
    logger.info("Evento recibido: %s", json.dumps(event, default=str))
    bucket = os.environ["BUCKET_NAME"]
    backfill = isinstance(event, dict) and event.get("mode") == "backfill"

    if backfill:
        keys = _listar_archivos_bronze(bucket)
        if not keys:
            raise FileNotFoundError("No hay archivos Bronze para procesar.")
    else:
        record = event["Records"][0]["s3"]
        event_bucket = record["bucket"]["name"]
        if event_bucket != bucket:
            raise ValueError(
                f"El evento proviene de {event_bucket}, no del bucket configurado {bucket}."
            )
        key = unquote_plus(record["object"]["key"])
        if not key.startswith("bronze/"):
            raise ValueError(f"Se esperaba un objeto Bronze, recibido: {key}")
        keys = [key]

    results = [_procesar_archivo_bronze(bucket, key) for key in keys]
    written = [result for result in results if result["silver_key"]]
    marker_key = None
    if written:
        marker_key = _publicar_marcador_gold(
            bucket,
            "backfill" if backfill else f"bronze:{written[0]['source_key']}",
        )

    summary = {
        "archivos_procesados": len(results),
        "archivos_silver": len(written),
        "fixtures_validos": sum(result["valid"] for result in results),
        "fixtures_invalidos": sum(result["invalid"] for result in results),
        "silver_keys": [result["silver_key"] for result in written],
        "gold_ready_marker": marker_key,
    }
    logger.info("Transformación Silver completada: %s", summary)
    logger.info("=== FIN: Transformación Silver de Football ===")
    return {"statusCode": 200, "body": summary}
