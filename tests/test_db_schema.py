import unittest
from stock_pipeline.db_schema import (
    create_stock_data_table,
    create_history_table,
    create_latest_table,
    create_api_key_history_table,
    create_tracked_symbols_table,
)
from stock_pipeline.fetch_from_db import connect_to_db


class TestDBSchema(unittest.TestCase):
    def setUp(self):
        self.conn = connect_to_db()
        if self.conn is None:
            self.skipTest("PostgreSQL not available; skipping schema tests")

    def tearDown(self):
        if self.conn:
            self.conn.close()

    def test_schema_creation_idempotent(self):
        with self.conn.cursor() as cursor:
            create_stock_data_table(cursor)
            create_history_table(cursor)
            create_latest_table(cursor)
            create_api_key_history_table(cursor)
            create_tracked_symbols_table(cursor)
        self.conn.commit()
        # Run again to ensure idempotency
        with self.conn.cursor() as cursor:
            create_stock_data_table(cursor)
            create_history_table(cursor)
            create_latest_table(cursor)
            create_api_key_history_table(cursor)
            create_tracked_symbols_table(cursor)
        self.conn.commit()


if __name__ == "__main__":
    unittest.main()
