import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


def _load_transform_silver():
    boto3_stub = types.ModuleType("boto3")
    boto3_stub.client = Mock(return_value=Mock())

    botocore_stub = types.ModuleType("botocore")
    botocore_stub.__path__ = []
    exceptions_stub = types.ModuleType("botocore.exceptions")

    class ClientError(Exception):
        pass

    exceptions_stub.ClientError = ClientError
    botocore_stub.exceptions = exceptions_stub

    mocked_modules = {
        "boto3": boto3_stub,
        "botocore": botocore_stub,
        "botocore.exceptions": exceptions_stub,
    }

    try:
        import pandas  # noqa: F401
    except ImportError:
        pandas_stub = types.ModuleType("pandas")
        pandas_stub.DataFrame = Mock()
        mocked_modules["pandas"] = pandas_stub

    module_path = (
        Path(__file__).parents[1]
        / "src"
        / "transform_silver"
        / "handler.py"
    )
    spec = importlib.util.spec_from_file_location(
        "asteroid_transform_silver", module_path
    )
    module = importlib.util.module_from_spec(spec)

    with patch.dict(sys.modules, mocked_modules):
        spec.loader.exec_module(module)

    return module


transform_silver = _load_transform_silver()


class AplanarRespuestaNeoWsTests(unittest.TestCase):
    def setUp(self):
        self.payload = {
            "near_earth_objects": {
                "2026-11-17": [
                    {
                        "id": "123",
                        "name": "Test asteroid",
                        "estimated_diameter": {
                            "kilometers": {
                                "estimated_diameter_min": 0.1,
                                "estimated_diameter_max": 0.2,
                            }
                        },
                        "close_approach_data": [
                            {
                                "close_approach_date": "2026-11-17",
                                "relative_velocity": {
                                    "kilometers_per_hour": "55000",
                                },
                                "miss_distance": {"kilometers": "7500000"},
                                "orbiting_body": "Earth",
                            }
                        ],
                    }
                ]
            }
        }

    def test_flattens_raw_neows_response(self):
        records = transform_silver.aplanar_respuesta_neows(self.payload)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["neo_id"], "123")

    def test_flattens_bronze_audit_envelope(self):
        bronze_document = {
            "_metadata": {"fecha_consulta": "2026-11-17"},
            "payload": self.payload,
        }

        records = transform_silver.aplanar_respuesta_neows(bronze_document)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["neo_id"], "123")

    def test_handler_processes_bronze_audit_envelope(self):
        bronze_document = {
            "_metadata": {"fecha_consulta": "2026-11-17"},
            "payload": self.payload,
        }
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "asteroid-data-lake"},
                        "object": {
                            "key": "bronze/year=2026/month=11/neo_20261117.json",
                        },
                    }
                }
            ]
        }

        with patch.object(
            transform_silver,
            "leer_json_desde_s3",
            return_value=bronze_document,
        ), patch.object(
            transform_silver,
            "guardar_parquet_en_s3",
            return_value="silver/year=2026/month=11/neo_20261117.parquet",
        ) as guardar_parquet:
            result = transform_silver.lambda_handler(event, None)

        self.assertEqual(result["body"]["valid_records"], 1)
        guardar_parquet.assert_called_once()

    def test_rejects_non_object_bronze_payload(self):
        with self.assertRaisesRegex(ValueError, "payload.*objeto JSON"):
            transform_silver.aplanar_respuesta_neows(
                {"_metadata": {}, "payload": None}
            )

    def test_handler_fails_for_non_object_bronze_payload(self):
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "asteroid-data-lake"},
                        "object": {
                            "key": "bronze/year=2026/month=11/neo_20261117.json",
                        },
                    }
                }
            ]
        }

        with patch.object(
            transform_silver,
            "leer_json_desde_s3",
            return_value={"_metadata": {}, "payload": None},
        ), self.assertRaisesRegex(ValueError, "payload.*objeto JSON"):
            transform_silver.lambda_handler(event, None)


if __name__ == "__main__":
    unittest.main()
