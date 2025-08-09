import unittest
from unittest.mock import patch
from datetime import date

from stock_pipeline.fetch_data import (
    _dates_set,
    _last_1000_window,
    _range_within_last_1000,
    _select_api_key_index_once,
    _current_api_key,
)


class TestFetchDataHelpers(unittest.TestCase):
    def test_dates_set(self):
        s = _dates_set("2025-01-01", "2025-01-03")
        self.assertEqual(s, {"2025-01-01", "2025-01-02", "2025-01-03"})

    def test_last_1000_window(self):
        start, end = _last_1000_window()
        self.assertRegex(start, r"^\d{4}-\d{2}-\d{2}$")
        self.assertRegex(end, r"^\d{4}-\d{2}-\d{2}$")
        self.assertEqual(end, date.today().strftime("%Y-%m-%d"))

    def test_range_within_last_1000(self):
        today = date.today().strftime("%Y-%m-%d")
        self.assertTrue(_range_within_last_1000(today, today))

    @patch("stock_pipeline.fetch_data.connect_to_db")
    @patch("stock_pipeline.fetch_data.create_api_key_history_table")
    @patch("stock_pipeline.fetch_data.upsert_api_keys_history")
    @patch("stock_pipeline.fetch_data.select_usable_api_key_idx", return_value=1)
    def test_select_api_key_index_once(self, _sel, _up, _crt, mock_conn):
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
        idx = _select_api_key_index_once(["a", "b"])
        self.assertEqual(idx, 1)

    @patch("stock_pipeline.fetch_data._select_api_key_index_once", return_value=0)
    def test_current_api_key(self, _sel_once):
        # reset module-level cache
        import importlib
        import stock_pipeline.fetch_data as fd
        importlib.reload(fd)
        self.assertEqual(fd._current_api_key(["K0", "K1"]), "K0")


if __name__ == "__main__":
    unittest.main()
