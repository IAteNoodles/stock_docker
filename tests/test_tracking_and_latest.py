import unittest
from unittest.mock import patch

from stock_pipeline.fetch_data import get_or_refresh_latest, refresh_tracked_symbol


class TestLatestAndTracking(unittest.TestCase):
    @patch("stock_pipeline.fetch_data.connect_to_db")
    @patch("stock_pipeline.fetch_data.fetch_latest_data_marketstack", return_value={"AAA": {"symbol": "AAA", "date": "2025-01-01T00:00:00+0000", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}})
    @patch("stock_pipeline.fetch_data.create_stock_data_table")
    @patch("stock_pipeline.fetch_data.update_history_latest_date")
    @patch("stock_pipeline.fetch_data.insert_stock_data")
    @patch("stock_pipeline.fetch_data.create_latest_table")
    @patch("stock_pipeline.fetch_data.get_latest_cache", return_value=None)
    @patch("stock_pipeline.fetch_data.upsert_latest_cache")
    def test_get_or_refresh_latest_fetches_and_upserts(self, _upsert_latest, _get_cache, _create_latest, _insert, _update_hist, _create_stock, mock_fetch, mock_conn):
        class DummyCursor:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False
        class DummyConn:
            def cursor(self):
                return DummyCursor()
            def commit(self):
                pass
            def close(self):
                pass
        mock_conn.return_value = DummyConn()

        item = get_or_refresh_latest("AAA")
        self.assertIsNotNone(item)
        self.assertEqual(item["symbol"], "AAA")

    @patch("stock_pipeline.fetch_data.connect_to_db")
    @patch("stock_pipeline.fetch_data.create_history_table")
    @patch("stock_pipeline.fetch_data.create_latest_table")
    @patch("stock_pipeline.fetch_data.create_tracked_symbols_table")
    @patch("stock_pipeline.fetch_data.ensure_history_symbol")
    @patch("stock_pipeline.fetch_data.get_history", return_value={"last_tried_to_fetch_date": None})
    @patch("stock_pipeline.fetch_data.get_latest_cache", return_value=None)
    @patch("stock_pipeline.fetch_data.touch_history_last_tried")
    @patch("stock_pipeline.fetch_data.get_or_refresh_latest", return_value={"symbol": "TTT"})
    @patch("stock_pipeline.fetch_data.set_tracked_last_fetched")
    def test_refresh_tracked_symbol_path(self, _set_lf, _get_or_refresh, _touch, _get_cache, _get_hist, _ensure_hist, _create_tracked, _create_latest, _create_hist, mock_conn):
        class DummyCursor:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False
        class DummyConn:
            def cursor(self):
                return DummyCursor()
            def commit(self):
                pass
            def close(self):
                pass
        mock_conn.return_value = DummyConn()

        out = refresh_tracked_symbol(" ttt ")
        self.assertEqual(out, {"symbol": "TTT"})


if __name__ == "__main__":
    unittest.main()
