"""Crea KPIs de partidos finalizados y vistas de consulta en Athena."""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena_client = boto3.client("athena")
s3_client = boto3.client("s3")

ATHENA_POLLING_INTERVAL_SECONDS = 2
ATHENA_MAX_WAIT_SECONDS = 75
FINAL_STATUSES = ("FT", "AET", "PEN")
GOLD_PREFIX = "gold/kpis/"
GOLD_READY_PREFIX = "control/gold-ready/"


def _validate_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", identifier):
        raise ValueError(f"Identificador SQL inválido: {identifier!r}")
    return identifier


def _extract_s3_info(event: dict) -> tuple[str, str]:
    record = event["Records"][0]["s3"]
    return (
        record["bucket"]["name"],
        unquote_plus(record["object"]["key"]),
    )


def _run_athena_query(
    query: str, database: str, output_location: str
) -> str:
    logger.info("Ejecutando Athena en %s:\n%s", database, query)
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    query_id = response["QueryExecutionId"]
    elapsed = 0

    while elapsed < ATHENA_MAX_WAIT_SECONDS:
        execution = athena_client.get_query_execution(QueryExecutionId=query_id)
        status = execution["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return query_id
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "sin detalle de Athena")
            raise RuntimeError(
                f"Consulta Athena {state}: {reason} (QueryID: {query_id})"
            )
        time.sleep(ATHENA_POLLING_INTERVAL_SECONDS)
        elapsed += ATHENA_POLLING_INTERVAL_SECONDS

    raise TimeoutError(
        f"Athena excedió {ATHENA_MAX_WAIT_SECONDS}s (QueryID: {query_id})"
    )


def _build_drop_query(database: str, gold_table: str) -> str:
    database = _validate_identifier(database)
    gold_table = _validate_identifier(gold_table)
    return f"DROP TABLE IF EXISTS {database}.{gold_table}"


def _build_drop_view_query(database: str, view_name: str) -> str:
    database = _validate_identifier(database)
    view_name = _validate_identifier(view_name)
    return f"DROP VIEW IF EXISTS {database}.{view_name}"


def _build_ctas_query(
    database: str,
    bucket: str,
    silver_table: str = "football_silver",
    gold_table: str = "football_kpis_gold",
) -> str:
    database = _validate_identifier(database)
    silver_table = _validate_identifier(silver_table)
    gold_table = _validate_identifier(gold_table)
    if not re.fullmatch(r"[a-z0-9.-]+", bucket):
        raise ValueError(f"Nombre de bucket inválido: {bucket!r}")

    return f"""
    CREATE TABLE {database}.{gold_table}
    WITH (
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        external_location = 's3://{bucket}/{GOLD_PREFIX}'
    ) AS
    WITH partidos_por_equipo AS (
        SELECT
            season,
            home_team_id AS team_id,
            home_team_name AS team_name,
            goals_home AS goals_for,
            goals_away AS goals_against,
            CASE WHEN goals_home > goals_away THEN 1 ELSE 0 END AS wins,
            CASE WHEN goals_home = goals_away THEN 1 ELSE 0 END AS draws,
            CASE WHEN goals_home < goals_away THEN 1 ELSE 0 END AS losses
        FROM {database}.{silver_table}
        WHERE status_short IN ('FT', 'AET', 'PEN')
          AND goals_home IS NOT NULL AND goals_away IS NOT NULL

        UNION ALL

        SELECT
            season,
            away_team_id AS team_id,
            away_team_name AS team_name,
            goals_away AS goals_for,
            goals_home AS goals_against,
            CASE WHEN goals_away > goals_home THEN 1 ELSE 0 END AS wins,
            CASE WHEN goals_away = goals_home THEN 1 ELSE 0 END AS draws,
            CASE WHEN goals_away < goals_home THEN 1 ELSE 0 END AS losses
        FROM {database}.{silver_table}
        WHERE status_short IN ('FT', 'AET', 'PEN')
          AND goals_home IS NOT NULL AND goals_away IS NOT NULL
    )
    SELECT
        season,
        team_id,
        team_name,
        COUNT(*) AS matches_played,
        SUM(wins) AS wins,
        SUM(draws) AS draws,
        SUM(losses) AS losses,
        ROUND(100.0 * SUM(wins) / COUNT(*), 2) AS win_percentage,
        SUM(goals_for) AS goals_for,
        SUM(goals_against) AS goals_against,
        SUM(goals_for) - SUM(goals_against) AS goal_difference,
        ROUND(1.0 * SUM(goals_for) / COUNT(*), 2) AS avg_goals_per_match
    FROM partidos_por_equipo
    GROUP BY season, team_id, team_name
    """


def _build_effectiveness_view_query(
    database: str, gold_table: str = "football_kpis_gold"
) -> str:
    database = _validate_identifier(database)
    gold_table = _validate_identifier(gold_table)
    return f"""
    CREATE OR REPLACE VIEW {database}.football_effectiveness AS
    SELECT
        season,
        team_id,
        team_name,
        matches_played,
        wins,
        draws,
        losses,
        win_percentage AS effectiveness_percentage,
        goals_for,
        goals_against,
        goal_difference,
        avg_goals_per_match
    FROM {database}.{gold_table}
    """


def _build_match_view_query(
    database: str, silver_table: str = "football_silver"
) -> str:
    database = _validate_identifier(database)
    silver_table = _validate_identifier(silver_table)
    return f"""
    CREATE OR REPLACE VIEW {database}.football_match_kpis AS
    SELECT
        fixture_id,
        season,
        "date",
        home_team_id AS team_id,
        home_team_name AS team_name,
        away_team_id AS opponent_id,
        away_team_name AS opponent_name,
        goals_home AS goals_for,
        goals_away AS goals_against,
        CASE
            WHEN goals_home > goals_away THEN 'W'
            WHEN goals_home = goals_away THEN 'D'
            ELSE 'L'
        END AS result
    FROM {database}.{silver_table}
    WHERE status_short IN ('FT', 'AET', 'PEN')
      AND goals_home IS NOT NULL AND goals_away IS NOT NULL

    UNION ALL

    SELECT
        fixture_id,
        season,
        "date",
        away_team_id AS team_id,
        away_team_name AS team_name,
        home_team_id AS opponent_id,
        home_team_name AS opponent_name,
        goals_away AS goals_for,
        goals_home AS goals_against,
        CASE
            WHEN goals_away > goals_home THEN 'W'
            WHEN goals_away = goals_home THEN 'D'
            ELSE 'L'
        END AS result
    FROM {database}.{silver_table}
    WHERE status_short IN ('FT', 'AET', 'PEN')
      AND goals_home IS NOT NULL AND goals_away IS NOT NULL
    """


def _delete_gold_data(bucket: str) -> int:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = [
        {"Key": item["Key"]}
        for page in paginator.paginate(Bucket=bucket, Prefix=GOLD_PREFIX)
        for item in page.get("Contents", [])
    ]
    for offset in range(0, len(keys), 1000):
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": keys[offset:offset + 1000], "Quiet": True},
        )
        errors = response.get("Errors", [])
        if errors:
            raise RuntimeError(f"No se pudieron eliminar objetos Gold: {errors}")
    return len(keys)


def lambda_handler(event: dict, context) -> dict:
    logger.info("=== INICIO: Cálculo de KPIs Gold de Football ===")
    logger.info("Evento recibido: %s", json.dumps(event, default=str))

    bucket = os.environ["BUCKET_NAME"]
    database = _validate_identifier(os.environ["ATHENA_DATABASE"])
    output_location = os.environ["ATHENA_OUTPUT_LOCATION"]
    silver_table = _validate_identifier(
        os.environ.get("SILVER_TABLE", "football_silver")
    )
    gold_table = _validate_identifier(
        os.environ.get("GOLD_TABLE", "football_kpis_gold")
    )

    source_bucket, source_key = _extract_s3_info(event)
    if source_bucket != bucket or not source_key.startswith(GOLD_READY_PREFIX):
        raise ValueError(
            "Gold solo procesa marcadores control/gold-ready/ del bucket configurado."
        )

    for view_name in ("football_effectiveness", "football_match_kpis"):
        _run_athena_query(
            _build_drop_view_query(database, view_name),
            database,
            output_location,
        )
    drop_query = _build_drop_query(database, gold_table)
    _run_athena_query(drop_query, database, output_location)

    deleted = _delete_gold_data(bucket)
    logger.info("Objetos anteriores de Gold eliminados: %d", deleted)

    ctas_query = _build_ctas_query(database, bucket, silver_table, gold_table)
    ctas_query_id = _run_athena_query(ctas_query, database, output_location)
    effectiveness_query_id = _run_athena_query(
        _build_effectiveness_view_query(database, gold_table),
        database,
        output_location,
    )
    match_query_id = _run_athena_query(
        _build_match_view_query(database, silver_table),
        database,
        output_location,
    )

    result = {
        "statusCode": 200,
        "body": {
            "database": database,
            "table": gold_table,
            "views": ["football_effectiveness", "football_match_kpis"],
            "gold_location": f"s3://{bucket}/{GOLD_PREFIX}",
            "query_ids": {
                "ctas": ctas_query_id,
                "effectiveness_view": effectiveness_query_id,
                "match_view": match_query_id,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }
    logger.info("KPIs Gold y vistas recreadas correctamente.")
    logger.info("=== FIN: Cálculo de KPIs Gold de Football ===")
    return result
