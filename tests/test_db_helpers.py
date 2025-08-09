import unittest
from datetime import datetime, timedelta, timezone

from stock_pipeline.fetch_from_db import (
    connect_to_db,
    upsert_api_keys_history,
    select_usable_api_key_idx,
    mark_api_key_limit_reached_idx,
    list_api_key_history,
    get_latest_cache,
    upsert_latest_cache,
)
from stock_pipeline.db_schema import (
    create_api_key_history_table,
    create_latest_table,
)


class TestDBHelpers(unittest.TestCase):
    def setUp(self):
        self.conn = connect_to_db()
        if self.conn is None:
            self.skipTest("PostgreSQL not available; skipping DB-dependent tests")

    def tearDown(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

    def test_api_key_history_selection_and_marking(self):
        keys = ["k1", "k2"]
        with self.conn.cursor() as cursor:
            create_api_key_history_table(cursor)
            upsert_api_keys_history(cursor, keys)
        self.conn.commit()

        with self.conn.cursor() as cursor:
            idx = select_usable_api_key_idx(cursor, len(keys))
            self.assertIn(idx, (0, 1))
            # mark the chosen idx limited, then pick the next
            mark_api_key_limit_reached_idx(cursor, idx)
        self.conn.commit()

        with self.conn.cursor() as cursor:
            next_idx = select_usable_api_key_idx(cursor, len(keys))
            # If first was chosen, next should be second; otherwise vice versa
            if idx == 0:
                self.assertEqual(next_idx, 1)
            else:
                self.assertEqual(next_idx, 0)
        self.conn.commit()

        with self.conn.cursor() as cursor:
            rows = list_api_key_history(cursor)
        self.conn.commit()
        self.assertEqual(len(rows), 2)

    def test_latest_cache_upsert_and_get(self):
        sym = "LC1"
        data = {"symbol": sym, "value": 123}
        now_iso = datetime.now(timezone.utc).isoformat()
        with self.conn.cursor() as cursor:
            create_latest_table(cursor)
            upsert_latest_cache(cursor, sym, data, last_updated=now_iso)
        self.conn.commit()

        with self.conn.cursor() as cursor:
            row = get_latest_cache(cursor, sym)
        self.conn.commit()
        self.assertIsNotNone(row)
        self.assertEqual(row["symbol"], sym)
        self.assertIsNotNone(row.get("last_updated"))
        self.assertEqual(row.get("data", {}).get("value"), 123)


if __name__ == "__main__":
    unittest.main()
