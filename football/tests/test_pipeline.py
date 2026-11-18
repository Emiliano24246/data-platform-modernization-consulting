import importlib.util
import io
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import URLError


ROOT = Path(__file__).resolve().parents[1]

boto3_stub = types.ModuleType("boto3")
boto3_stub.client = lambda *_args, **_kwargs: Mock()
sys.modules["boto3"] = boto3_stub

try:
    import pyarrow  # noqa: F401
except ImportError:
    arrow_stub = types.ModuleType("pyarrow")
    arrow_stub.Table = object
    arrow_stub.field = lambda name, data_type: types.SimpleNamespace(
        name=name, type=data_type
    )
    arrow_stub.int64 = lambda: "int64"
    arrow_stub.string = lambda: "string"
    arrow_stub.schema = lambda fields: fields
    arrow_stub.table = lambda columns, schema: types.SimpleNamespace(
        columns=columns, schema=schema, num_rows=len(next(iter(columns.values()), [])),
        num_columns=len(columns),
    )
    parquet_stub = types.ModuleType("pyarrow.parquet")
    parquet_stub.write_table = Mock()
    arrow_stub.parquet = parquet_stub
    sys.modules["pyarrow"] = arrow_stub
    sys.modules["pyarrow.parquet"] = parquet_stub


def load_handler(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


extract = load_handler(
    "football_extract_handler", ROOT / "src" / "extract_bronze" / "handler.py"
)
silver = load_handler(
    "football_silver_handler", ROOT / "src" / "transform_silver" / "handler.py"
)
gold = load_handler(
    "football_gold_handler", ROOT / "src" / "kpis_gold" / "handler.py"
)


def fixture(status="FT", home_goals=2, away_goals=1):
    return {
        "fixture": {
            "id": 101,
            "date": "2026-11-17T18:00:00+00:00",
            "status": {"short": status, "long": "Match Finished"},
        },
        "league": {"id": 1, "name": "World Cup", "season": 2026},
        "teams": {
            "home": {"id": 10, "name": "Home"},
            "away": {"id": 20, "name": "Away"},
        },
        "goals": {"home": home_goals, "away": away_goals},
    }


class ExtractTests(unittest.TestCase):
    def test_handler_queries_previous_utc_day_unless_target_date_is_provided(self):
        response = {"response": []}
        with (
            patch.dict("os.environ", {"BUCKET_NAME": "test-bucket"}),
            patch.object(extract, "_obtener_api_key", return_value="secret"),
            patch.object(extract, "_llamar_api_football", return_value=response) as call_api,
            patch.object(extract, "_guardar_en_s3") as save,
        ):
            result = extract.lambda_handler({"target_date": "2026-11-17"}, None)

        self.assertEqual(call_api.call_args.args[-1], "2026-11-17")
        self.assertEqual(result["body"]["fecha"], "2026-11-17")
        self.assertEqual(save.call_args.args[1], "bronze/year=2026/month=11/fixtures_20261117.json")

    def test_api_retries_network_errors_and_raises_api_errors(self):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read.return_value = b'{"response": [], "errors": []}'
        with (
            patch.object(extract.urllib.request, "urlopen", side_effect=[URLError("temporary"), response]) as urlopen,
            patch.object(extract.time, "sleep"),
        ):
            data = extract._llamar_api_football("secret", "api.example", "1", "2026", "2026-11-17")
        self.assertEqual(data["response"], [])
        self.assertEqual(urlopen.call_count, 2)

        error_response = Mock()
        error_response.__enter__ = Mock(return_value=error_response)
        error_response.__exit__ = Mock(return_value=False)
        error_response.read.return_value = b'{"response": [], "errors": {"token": "invalid"}}'
        with patch.object(extract.urllib.request, "urlopen", return_value=error_response):
            with self.assertRaisesRegex(RuntimeError, "API-Football reportó errores"):
                extract._llamar_api_football("secret", "api.example", "1", "2026", "2026-11-17")


class SilverTests(unittest.TestCase):
    def test_unplayed_fixture_is_valid_but_final_fixture_requires_score(self):
        unplayed = fixture("NS", None, None)
        self.assertTrue(silver._validar_fixture(unplayed))
        flattened = silver._aplanar_fixture(unplayed)
        self.assertIsNone(flattened["goals_home"])
        self.assertEqual(flattened["season"], 2026)

        final_without_score = fixture("FT", None, None)
        self.assertFalse(silver._validar_fixture(final_without_score))
        negative_score = fixture("FT", -1, 0)
        self.assertFalse(silver._validar_fixture(negative_score))
        missing_goal_fields = fixture()
        missing_goal_fields.pop("goals")
        self.assertFalse(silver._validar_fixture(missing_goal_fields))

    def test_invalid_key_or_partition_date_is_rejected(self):
        self.assertEqual(
            silver._extraer_fecha_de_key(
                "bronze/year=2026/month=11/fixtures_20261117.json"
            ),
            "20261117",
        )
        with self.assertRaises(ValueError):
            silver._extraer_fecha_de_key(
                "bronze/year=2025/month=11/fixtures_20261117.json"
            )
        with self.assertRaises(ValueError):
            silver._extraer_fecha_de_key(
                "bronze/year=2026/month=11/fixtures_20261117.json.bak"
            )

    def test_parquet_season_is_a_partition_not_a_duplicate_column(self):
        records = [silver._aplanar_fixture(fixture())]
        table = silver._construir_tabla_parquet(records)
        field_names = [field.name for field in table.schema]
        self.assertNotIn("season", field_names)
        self.assertIn("status_short", field_names)

    def test_handler_accepts_unplayed_fixtures_and_publishes_one_marker(self):
        document = {"respuesta_api": {"response": [fixture("NS", None, None)]}}
        body = io.BytesIO(json.dumps(document).encode())
        with (
            patch.dict("os.environ", {"BUCKET_NAME": "test-bucket"}),
            patch.object(silver.s3_client, "get_object", return_value={"Body": body}),
            patch.object(silver, "_construir_tabla_parquet", return_value=Mock()),
            patch.object(silver.pq, "write_table", side_effect=lambda _table, buffer, **_kw: buffer.write(b"parquet")),
            patch.object(silver, "_publicar_marcador_gold", return_value="marker") as marker,
            patch.object(silver.s3_client, "put_object") as put_object,
        ):
            result = silver.lambda_handler({
                "Records": [{
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "bronze%2Fyear%3D2026%2Fmonth%3D11%2Ffixtures_20261117.json"},
                    }
                }]
            }, None)

        self.assertEqual(result["body"]["fixtures_validos"], 1)
        self.assertEqual(result["body"]["gold_ready_marker"], "marker")
        marker.assert_called_once()
        put_object.assert_called_once()
        self.assertEqual(
            put_object.call_args.kwargs["Key"],
            "silver/season=2026/fixtures_20261117.parquet",
        )

    def test_backfill_sorts_bronze_keys_and_publishes_once(self):
        keys = [
            "bronze/year=2026/month=11/fixtures_20261117.json",
            "bronze/year=2026/month=11/fixtures_20261115.json",
            "bronze/year=2026/month=11/fixtures_20261116.json",
        ]
        paginator = Mock()
        paginator.paginate.return_value = [{
            "Contents": [{"Key": key} for key in keys]
        }]
        silver.s3_client.get_paginator.return_value = paginator
        ordered = []
        with (
            patch.dict("os.environ", {"BUCKET_NAME": "test-bucket"}),
            patch.object(silver, "_procesar_archivo_bronze", side_effect=lambda _bucket, key: (
                ordered.append(key) or {"source_key": key, "silver_key": key, "valid": 1, "invalid": 0}
            )),
            patch.object(silver, "_publicar_marcador_gold", return_value="marker") as marker,
        ):
            result = silver.lambda_handler({"mode": "backfill"}, None)

        self.assertEqual(ordered, sorted(keys, key=silver._extraer_fecha_de_key))
        self.assertEqual(result["body"]["archivos_silver"], 3)
        marker.assert_called_once_with("test-bucket", "backfill")


class GoldTests(unittest.TestCase):
    def test_ctas_and_views_filter_to_completed_matches(self):
        query = gold._build_ctas_query(
            "football_db", "test-bucket", "football_silver", "football_gold"
        )
        match_view = gold._build_match_view_query("football_db")
        self.assertIn("status_short IN ('FT', 'AET', 'PEN')", query)
        self.assertIn("goals_home IS NOT NULL", query)
        self.assertIn("football_db.football_silver", query)
        self.assertIn("UNION ALL", match_view)
        self.assertIn('"date"', match_view)
        self.assertNotIn("ORDER BY", query.upper())
        self.assertFalse(query.rstrip().endswith(";"))

    def test_gold_cleanup_paginates_and_batches_deletes(self):
        paginator = Mock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": f"gold/kpis/{index}.parquet"} for index in range(1001)]}
        ]
        gold.s3_client.get_paginator.return_value = paginator
        gold.s3_client.delete_objects.return_value = {}

        self.assertEqual(gold._delete_gold_data("test-bucket"), 1001)
        self.assertEqual(gold.s3_client.delete_objects.call_count, 2)

    def test_gold_propagates_drop_failure_without_cleaning_s3(self):
        with (
            patch.dict("os.environ", {
                "BUCKET_NAME": "test-bucket",
                "ATHENA_DATABASE": "football_db",
                "ATHENA_OUTPUT_LOCATION": "s3://test-bucket/athena-results/",
            }),
            patch.object(gold, "_run_athena_query", side_effect=RuntimeError("drop failed")),
            patch.object(gold, "_delete_gold_data") as cleanup,
        ):
            with self.assertRaisesRegex(RuntimeError, "drop failed"):
                gold.lambda_handler({
                    "Records": [{
                        "s3": {
                            "bucket": {"name": "test-bucket"},
                            "object": {"key": "control/gold-ready/refresh.json"},
                        }
                    }]
                }, None)
        cleanup.assert_not_called()


if __name__ == "__main__":
    unittest.main()
