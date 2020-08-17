import json
from unittest import TestCase
from unittest import mock

import testing.postgresql

from config import *

pgsql = testing.postgresql.Postgresql(cache_initialized_db=True, port=int(AWS_RDS_PORT))

from optimization_platform.src.jobs import sync_orders


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

        @property
        def headers(self):
            return {}

    if "orders.json?updated_at_min=" in args[0]:
        with open("tests/data/test_order_agent/orders_updated_at.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)
    elif "orders.json" in args[0]:
        with open("tests/data/test_order_agent/orders.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)
    elif "checkouts.json?updated_at_min=" in args[0]:
        with open("tests/data/test_order_agent/checkouts.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)
    else:
        with open("tests/data/test_order_agent/checkouts.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)


from optimization_platform.src.jobs.sync_orders import rds_data_store, main, logger

logger.disabled = True


class TestProductAgent(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestProductAgent, self).__init__(*args, **kwargs)

    def setUp(self):
        self.rds_data_store = rds_data_store
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def tearDown(self):
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def _add_new_clients(self):
        csv_file_name = "tests/data/test_sync_products/shops.csv"
        cursor = self.rds_data_store.conn.cursor()
        sql = "COPY shops FROM STDIN DELIMITER ',' CSV HEADER"
        with open(csv_file_name, "r") as fp:
            cursor.copy_expert(sql, fp)
        self.rds_data_store.conn.commit()
        cursor.close()

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_main(self, x):
        self._add_new_clients()
        main()

        result = self.rds_data_store.run_custom_sql("select * from orders")
        len_result = len(result)
        expected_len_result = 100
        self.assertEqual(first=len_result, second=expected_len_result)

        main()

        result = self.rds_data_store.run_custom_sql("select * from orders")
        len_result = len(result)
        expected_len_result = 100
        self.assertEqual(first=len_result, second=expected_len_result)

    def test_init(self):
        with mock.patch.object(sync_orders, "main", return_value=42):
            with mock.patch.object(sync_orders, "__name__", "__main__"):
                with mock.patch.object(sync_orders.sys, 'exit') as mock_exit:
                    sync_orders.init()
                    self.assertEqual(first=mock_exit.call_args[0][0], second=42)
