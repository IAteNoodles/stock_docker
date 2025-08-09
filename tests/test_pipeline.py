import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import date

from stock_pipeline.fetch_data import (
    fetch_historical_data_marketstack,
    process_tickers,
    _env,
    _dates_set,
    _last_1000_window,
    _range_within_last_1000,
    get_latest_value,
)


class TestPipeline(unittest.TestCase):
    """Unit tests for the stock pipeline with mocked network calls and optional DB."""

    # ---- Pure/unit helpers ----

    def test_env_sanitization(self):
        os.environ["TEST_QUOTED"] = '  " value "  '
        self.assertEqual(_env("TEST_QUOTED"), "value")
        self.assertEqual(_env("MISSING_VAR", "default"), "default")

    def test_date_helpers(self):
        s = _dates_set("2025-01-01", "2025-01-03")
        self.assertEqual(len(s), 3)
        self.assertIn("2025-01-02", s)
        start, end = _last_1000_window()
        self.assertRegex(start, r"^\d{4}-\d{2}-\d{2}$")
        self.assertRegex(end, r"^\d{4}-\d{2}-\d{2}$")
        today = date.today().strftime("%Y-%m-%d")
        self.assertEqual(end, today)
        self.assertTrue(_range_within_last_1000(today, today))

    # ---- Network fetch (mocked HTTP) ----

    @patch("stock_pipeline.fetch_data.requests.get")
    def test_fetch_historical_data_marketstack_mocked(self, mock_get):
        mock_get.return_value.raise_for_status.return_value = None
        mock_get.return_value.json.return_value = {"data": [
            {"symbol": "MOCK", "date": "2025-01-01T00:00:00+0000", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 100}
        ]}
        data = fetch_historical_data_marketstack(["MOCK"], "2025-01-01", "2025-01-02")
        self.assertIn("MOCK", data)
        self.assertEqual(len(data["MOCK"]), 1)

    @patch("stock_pipeline.fetch_data.requests.get")
    def test_fetch_historical_handles_provider_error(self, mock_get):
        mock_get.return_value.raise_for_status.return_value = None
        mock_get.return_value.json.return_value = {"error": {"message": "bad key"}}
        data = fetch_historical_data_marketstack(["ERR"], "2025-01-01", "2025-01-02")
        self.assertEqual(data["ERR"], [])

    @patch("stock_pipeline.fetch_data.requests.get")
    def test_fetch_historical_ignores_non_dict_items(self, mock_get):
        mock_get.return_value.raise_for_status.return_value = None
        mock_get.return_value.json.return_value = {"data": ["bad", 123, {"symbol": "OK", "date": "2025-01-01T00:00:00+0000"}]}
        res = fetch_historical_data_marketstack(["OK"], "2025-01-01", "2025-01-02")
        self.assertEqual(len(res["OK"]), 1)

    # ---- DB dependent tests (skipped when DB not available) ----

    def test_insert_stock_data_skips_incomplete(self):
        """insert_stock_data should skip rows missing symbol/date and not raise."""
        from stock_pipeline.fetch_from_db import connect_to_db, create_stock_data_table, insert_stock_data

        conn = connect_to_db()
        if conn is None:
            self.skipTest("PostgreSQL not available; skipping DB-dependent test")
        try:
            with conn.cursor() as cursor:
                create_stock_data_table(cursor)
                rows = [
                    {"symbol": "TST", "date": "2025-01-02T00:00:00+0000", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 10},
                    {"date": "2025-01-03T00:00:00+0000", "open": 1},  # missing symbol -> skip
                    {"symbol": "TST", "open": 1},  # missing date -> skip
                ]
                insert_stock_data(cursor, None, rows)
            conn.commit()
        finally:
            conn.close()

    @patch("stock_pipeline.fetch_data.fetch_historical_data_marketstack")
    def test_process_tickers_db_first(self, mock_fetch):
        """process_tickers must not call API when data present in DB for range."""
        from stock_pipeline.fetch_from_db import connect_to_db, create_stock_data_table, insert_stock_data
        conn = connect_to_db()
        if conn is None:
            self.skipTest("PostgreSQL not available; skipping DB-dependent test")
        try:
            with conn.cursor() as cursor:
                create_stock_data_table(cursor)
                insert_stock_data(cursor, None, [{
                    "symbol": "MOCK2", "date": "2025-02-01T00:00:00+0000",
                    "open": 2, "high": 2, "low": 2, "close": 2, "volume": 20
                }])
            conn.commit()
        finally:
            conn.close()

        succeeded, failed = process_tickers(["MOCK2"], "2025-02-01", "2025-02-02")
        self.assertIn("MOCK2", succeeded)
        mock_fetch.assert_not_called()

    def test_history_crud_and_cooldown(self):
        """Create history row, update fields, and verify cooldown prevents immediate fetch."""
        from stock_pipeline.fetch_from_db import (
            connect_to_db, create_stock_data_table, create_history_table, ensure_history_symbol,
            get_history, update_history_latest_date, touch_history_last_tried, list_history_entries,
        )
        conn = connect_to_db()
        if conn is None:
            self.skipTest("PostgreSQL not available; skipping DB-dependent test")
        sym = "HISTX"
        try:
            with conn.cursor() as cursor:
                create_stock_data_table(cursor)
                create_history_table(cursor)
                ensure_history_symbol(cursor, sym)
                update_history_latest_date(cursor, sym, "2025-01-05")
                touch_history_last_tried(cursor, sym)
            conn.commit()
            with conn.cursor() as cursor:
                hist = get_history(cursor, sym)
                self.assertIsNotNone(hist)
                self.assertIsNotNone(hist.get("latest_data_date"))
                self.assertIsNotNone(hist.get("last_tried_to_fetch_date"))
                entries = list_history_entries(cursor)
                self.assertTrue(any(e.get("symbol") == sym for e in entries))
        finally:
            conn.close()

    def test_get_stock_data_and_existing_dates(self):
        from stock_pipeline.fetch_from_db import connect_to_db, create_stock_data_table, insert_stock_data, get_existing_dates, get_stock_data
        conn = connect_to_db()
        if conn is None:
            self.skipTest("PostgreSQL not available; skipping DB-dependent test")
        sym = "EXIST1"
        try:
            with conn.cursor() as cursor:
                create_stock_data_table(cursor)
                insert_stock_data(cursor, None, [{
                    "symbol": sym, "date": "2025-03-01T00:00:00+0000",
                    "open": 3, "high": 3, "low": 3, "close": 3, "volume": 30
                }])
            conn.commit()
            with conn.cursor() as cursor:
                present = get_existing_dates(cursor, sym, "2025-03-01", "2025-03-02")
                self.assertIn("2025-03-01", present)
            rows = get_stock_data(sym, "2025-03-01", "2025-03-02")
            self.assertEqual(len(rows), 1)
        finally:
            conn.close()

    # ---- Background worker test (no DB, full mocks) ----

    @patch("stock_pipeline.fetch_data.time.sleep", return_value=None)
    @patch("stock_pipeline.fetch_data.process_tickers")
    @patch("stock_pipeline.fetch_data.list_history_entries")
    @patch("stock_pipeline.fetch_data.create_history_table")
    @patch("stock_pipeline.fetch_data.connect_to_db")
    def test_background_worker_due_symbol(self, mock_conn, mock_create_tbl, mock_list, mock_process, _mock_sleep):
        class DummyCursor:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False
        class DummyConn:
            def cursor(self):
                return DummyCursor()
            def close(self):
                pass
            def commit(self):
                pass
        mock_conn.return_value = DummyConn()

        # First call returns one due symbol, then empty to exit quickly
        calls = {"n": 0}
        def list_side_effect(_cur):
            if calls["n"] == 0:
                calls["n"] += 1
                return [{"symbol": "DUE1", "latest_data_date": None, "last_tried_to_fetch_date": None}]
            return []
        mock_list.side_effect = list_side_effect

        stop_event = MagicMock()
        # stop_event.wait returns False on first no-entries sleep(0), then True to exit
        def wait_side_effect(timeout):
            return True  # cause immediate exit when called
        stop_event.wait.side_effect = wait_side_effect

        # Run a single iteration path
        get_latest_value(stop_event=stop_event, default_idle_seconds=0)
        mock_process.assert_called_with(["DUE1"], unittest.mock.ANY, unittest.mock.ANY)


if __name__ == "__main__":
    unittest.main()
