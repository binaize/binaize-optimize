import json
from unittest import TestCase
from unittest import mock

import testing.postgresql

from config import *
from optimization_platform.src.agents.order_agent import OrderAgent
from utils.data_store.rds_data_store import RDSDataStore


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


pgsql = testing.postgresql.Postgresql(cache_initialized_db=True, port=int(AWS_RDS_PORT))
rds_data_store = RDSDataStore(host=AWS_RDS_HOST,
                              port=AWS_RDS_PORT,
                              dbname=AWS_RDS_DBNAME,
                              user=AWS_RDS_USER,
                              password=AWS_RDS_PASSWORD)


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

    def _add_new_client(self):
        csv_file_name = "tests/data/test_order_agent/shops.csv"
        cursor = self.rds_data_store.conn.cursor()
        sql = "COPY shops FROM STDIN DELIMITER ',' CSV HEADER"
        with open(csv_file_name, "r") as fp:
            cursor.copy_expert(sql, fp)
        self.rds_data_store.conn.commit()
        cursor.close()

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_sync_orders(self, x):
        self._add_new_client()
        result = OrderAgent.sync_orders(shop_id="binaize-dev-watch.myshopify.com", data_store=self.rds_data_store)
        result = result[0]
        expected_result = 45
        self.assertEqual(first=result, second=expected_result)
        result = OrderAgent.sync_orders(shop_id="binaize-dev-watch.myshopify.com", data_store=self.rds_data_store)
        result = result[0]
        expected_result = 10
        self.assertEqual(first=result, second=expected_result)
