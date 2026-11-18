import importlib.util
import math
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parents[1]


class ClientError(Exception):
    def __init__(self, response):
        self.response = response
        super().__init__(str(response))


try:
    import boto3  # noqa: F401
except ImportError:
    boto3_stub = types.ModuleType("boto3")
    boto3_stub.client = lambda *_args, **_kwargs: Mock()
    sys.modules["boto3"] = boto3_stub

    botocore_stub = types.ModuleType("botocore")
    exceptions_stub = types.ModuleType("botocore.exceptions")
    exceptions_stub.ClientError = ClientError
    botocore_stub.exceptions = exceptions_stub
    sys.modules["botocore"] = botocore_stub
    sys.modules["botocore.exceptions"] = exceptions_stub


def load_handler(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


extract = load_handler(
    "binance_extract_handler", ROOT / "src" / "extract_bronze" / "handler.py"
)
silver = load_handler(
    "binance_silver_handler", ROOT / "src" / "transform_silver" / "handler.py"
)
gold = load_handler(
    "binance_gold_handler", ROOT / "src" / "signals_gold" / "handler.py"
)


def daily_kline(date, close=100.0):
    open_time = int(
        datetime.strptime(date, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp() * 1000
    )
    return [
        open_time, str(close), str(close + 2), str(close - 2), str(close),
        "10", open_time + 86_400_000 - 1, "1000", 10, "4", "400", "0",
    ]


class ExtractTests(unittest.TestCase):
    def test_api_url_is_limited_to_requested_utc_day(self):
        target = datetime(2026, 10, 1, tzinfo=timezone.utc)
        params = parse_qs(urlparse(extract._build_api_url(
            "BTCUSDT", "1d", target
        )).query)

        self.assertEqual(params["startTime"], ["1790812800000"])
        self.assertEqual(params["endTime"], ["1790899199999"])
        self.assertEqual(params["limit"], ["1"])

    def test_daily_validator_rejects_a_different_or_open_candle(self):
        target = datetime(2024, 3, 15, tzinfo=timezone.utc)
        wrong_day = daily_kline("2024-03-16")
        with self.assertRaisesRegex(ValueError, "open_time"):
            extract._validate_daily_kline([wrong_day], target)

        with self.assertRaisesRegex(ValueError, "12 campos"):
            extract._validate_daily_kline([wrong_day[:6]], target)

    def test_api_retries_transient_network_failure(self):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.getcode.return_value = 200
        response.read.return_value = b"[]"
        with (
            patch.object(extract.urllib.request, "urlopen", side_effect=[URLError("temporary"), response]) as urlopen,
            patch.object(extract.time, "sleep"),
        ):
            self.assertEqual(extract._call_binance_api("https://example.test"), [])
        self.assertEqual(urlopen.call_count, 2)


class SilverTests(unittest.TestCase):
    def test_bronze_history_is_sorted_even_when_s3_lists_objects_out_of_order(self):
        dates = ["2024-03-15", "2024-03-16", "2024-03-17"]
        keys = [
            f"bronze/year=2024/month=03/BTCUSDT_{date.replace('-', '')}.json"
            for date in dates
        ]
        silver.s3_client.get_paginator.return_value.paginate.return_value = [{
            "Contents": [{"Key": key} for key in reversed(keys)]
        }]
        bronze_by_key = {
            key: {"raw_klines": [daily_kline(date, 100 + index)]}
            for index, (date, key) in enumerate(zip(dates, keys))
        }
        with patch.object(
            silver, "_read_bronze_json",
            side_effect=lambda _bucket, key: bronze_by_key[key],
        ):
            history = silver._load_bronze_history("test-bucket", "BTCUSDT")

        self.assertEqual(history["date"].tolist(), dates)
        self.assertEqual(history["close"].tolist(), [100.0, 101.0, 102.0])

    def test_kline_validation_rejects_bad_shape_and_inconsistent_ohlc(self):
        with self.assertRaisesRegex(ValueError, "12 campos"):
            silver._parse_kline_to_dict(daily_kline("2024-03-15")[:6])

        data = silver._parse_kline_to_dict(daily_kline("2024-03-15"))
        data["high"] = "50"
        valid, errors = silver._validate_kline(data, "20240315")
        self.assertFalse(valid)
        self.assertTrue(any("high" in error for error in errors))

    def test_indicator_formulas_match_an_independent_seeded_reference(self):
        values = [100.0 + math.sin(index / 3) * 4 + index * 0.2
                  for index in range(80)]
        series = silver.pd.Series(values, dtype="float64")

        rsi = silver._calculate_rsi(series, 14)
        gains = [max(values[i] - values[i - 1], 0) for i in range(1, 15)]
        losses = [max(values[i - 1] - values[i], 0) for i in range(1, 15)]
        avg_gain = sum(gains) / 14
        avg_loss = sum(losses) / 14
        expected_rsi = [float("nan")] * len(values)
        expected_rsi[14] = 100 - 100 / (1 + avg_gain / avg_loss)
        for index in range(15, len(values)):
            gain = max(values[index] - values[index - 1], 0)
            loss = max(values[index - 1] - values[index], 0)
            avg_gain = (13 * avg_gain + gain) / 14
            avg_loss = (13 * avg_loss + loss) / 14
            expected_rsi[index] = (
                100 - 100 / (1 + avg_gain / avg_loss)
                if avg_loss
                else (100.0 if avg_gain else 0.0)
            )
        self.assertTrue(math.isnan(rsi.iloc[13]))
        for actual, expected in zip(rsi, expected_rsi):
            if math.isnan(expected):
                self.assertTrue(math.isnan(actual))
            else:
                self.assertAlmostEqual(actual, expected, places=12)

        upper, middle, lower = silver._calculate_bollinger_bands(series, 20, 2)
        window = values[60:80]
        mean = sum(window) / 20
        variance = sum((value - mean) ** 2 for value in window) / 20
        deviation = math.sqrt(variance)
        self.assertAlmostEqual(middle.iloc[-1], mean, places=12)
        self.assertAlmostEqual(upper.iloc[-1], mean + 2 * deviation, places=12)
        self.assertAlmostEqual(lower.iloc[-1], mean - 2 * deviation, places=12)

        macd, signal, histogram = silver._calculate_macd(series, 12, 26, 9)

        def seeded_ema(period):
            output = [float("nan")] * len(values)
            output[period - 1] = sum(values[:period]) / period
            alpha = 2 / (period + 1)
            for index in range(period, len(values)):
                output[index] = alpha * values[index] + (1 - alpha) * output[index - 1]
            return output

        fast, slow = seeded_ema(12), seeded_ema(26)
        reference_macd = [float("nan")] * len(values)
        for index in range(25, len(values)):
            reference_macd[index] = fast[index] - slow[index]
        reference_signal = [float("nan")] * len(values)
        reference_signal[33] = sum(reference_macd[25:34]) / 9
        alpha = 2 / 10
        for index in range(34, len(values)):
            reference_signal[index] = (
                alpha * reference_macd[index]
                + (1 - alpha) * reference_signal[index - 1]
            )
        self.assertAlmostEqual(macd.iloc[-1], reference_macd[-1], places=12)
        self.assertAlmostEqual(signal.iloc[-1], reference_signal[-1], places=12)
        self.assertAlmostEqual(
            histogram.iloc[-1], reference_macd[-1] - reference_signal[-1],
            places=12,
        )

    def test_invalid_bronze_is_quarantined_then_fails_the_invocation(self):
        key = "bronze/year=2024/month=03/BTCUSDT_20240315.json"
        malformed = {"raw_klines": [daily_kline("2024-03-15")[:6]]}
        with (
            patch.object(silver, "_list_bronze_json", return_value=[("20240315", key)]),
            patch.object(silver, "_read_bronze_json", return_value=malformed),
            patch.object(silver, "_save_to_quarantine") as quarantine,
        ):
            with self.assertRaisesRegex(ValueError, "12 campos"):
                silver._load_bronze_history("bucket", "BTCUSDT")
        quarantine.assert_called_once()

    def test_backfill_writes_all_dates_and_only_one_gold_marker(self):
        dates = ["2024-03-15", "2024-03-16", "2024-03-17"]
        rows = silver.pd.DataFrame([
            {
                "date": date, "symbol": "BTCUSDT", "open": 100.0,
                "high": 102.0, "low": 98.0, "close": 100.0 + index,
                "volume": 10.0,
            }
            for index, date in enumerate(dates)
        ])
        keys = [(date.replace("-", ""), f"bronze/{date}.json") for date in dates]
        with (
            patch.object(silver, "BUCKET_NAME", "test-bucket"),
            patch.object(silver, "_list_bronze_json", return_value=keys),
            patch.object(silver, "_load_bronze_history", return_value=rows),
            patch.object(silver, "_save_silver_parquet", side_effect=["a", "b", "c"]) as save,
            patch.object(silver, "_write_gold_ready_marker", return_value="marker") as marker,
        ):
            result = silver.lambda_handler({"mode": "backfill"}, None)

        self.assertEqual(save.call_count, 3)
        marker.assert_called_once_with("test-bucket", "backfill")
        self.assertEqual(result["dates_written"], 3)
        self.assertEqual(result["gold_ready_marker"], "marker")


class GoldTests(unittest.TestCase):
    def test_ctas_is_parameterized_and_has_no_unneeded_ordering(self):
        query = gold._build_ctas_query(
            "marketmatrix_db", "binance_gold", "binance_silver", "data-bucket"
        )
        self.assertIn('FROM marketmatrix_db.binance_silver', query)
        self.assertIn('SELECT\n        "date"', query)
        self.assertNotIn("ORDER BY", query.upper())
        self.assertFalse(query.rstrip().endswith(";"))

    def test_gold_propagates_drop_failure_instead_of_reporting_success(self):
        with (
            patch.object(gold, "BUCKET_NAME", "test-bucket"),
            patch.object(gold, "ATHENA_OUTPUT_LOCATION", "s3://test-bucket/athena-results/"),
            patch.object(gold, "_drop_gold_table_if_exists", side_effect=RuntimeError("drop failed")),
            patch.object(gold, "_delete_gold_s3_data") as cleanup,
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

    def test_s3_cleanup_api_errors_are_not_suppressed(self):
        paginator = Mock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "gold/signals/old.parquet"}]}]
        gold.s3_client.get_paginator.return_value = paginator
        gold.s3_client.delete_objects.return_value = {
            "Errors": [{"Key": "gold/signals/old.parquet", "Code": "AccessDenied"}]
        }

        with self.assertRaisesRegex(RuntimeError, "no pudo eliminar"):
            gold._delete_gold_s3_data("test-bucket", "gold/signals/")


if __name__ == "__main__":
    unittest.main()
