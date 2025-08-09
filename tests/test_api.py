import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient

from stock_pipeline.api import app


class TestAPI(unittest.TestCase):
    @patch("stock_pipeline.api.process_tickers", return_value=(set(["ABC"]), set()))
    @patch("stock_pipeline.api.get_stock_data", return_value=[{"symbol": "ABC", "date": "2025-01-01", "open": 1.0, "high": 1.2, "low": 0.9, "close": 1.1, "volume": 1000}])
    def test_get_stocks_success(self, mock_get_stock_data, mock_process):
        client = TestClient(app)
        resp = client.get("/stocks", params={"symbols": "ABC", "start_date": "2025-01-01", "end_date": "2025-01-02"})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("ABC", data)
        self.assertEqual(len(data["ABC"]), 1)
        mock_process.assert_called_once()
        mock_get_stock_data.assert_called()

    def test_get_stocks_missing_params(self):
        client = TestClient(app)
        resp = client.get("/stocks", params={"symbols": "ABC"})
        self.assertEqual(resp.status_code, 400)

    def test_health(self):
        client = TestClient(app)
        resp = client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"status": "ok"})
