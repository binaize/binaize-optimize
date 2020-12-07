import json
from unittest import TestCase
from unittest import mock

import testing.postgresql

from config import *
from optimization_platform.src.agents.client_agent import ClientAgent
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
        with open("tests/data/test_order_3.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)
    elif "orders.json" in args[0]:
        with open("tests/data/test_order_1.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)
    elif "checkouts.json?updated_at_min=" in args[0]:
        with open("tests/data/test_order_4.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)
    else:
        with open("tests/data/test_order_2.json", "r") as fp:
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

    def _add_new_client(self, client_id, full_name, company_name, hashed_password, disabled, shopify_app_eg_url,
                        client_timezone):
        timestamp = 1590673060
        status = ClientAgent.add_new_client(data_store=self.rds_data_store, client_id=client_id,
                                            full_name=full_name,
                                            company_name=company_name, hashed_password=hashed_password,
                                            disabled=disabled, client_timezone=client_timezone,
                                            shopify_app_eg_url=shopify_app_eg_url,
                                            creation_timestamp=timestamp)
        expected_status = True
        self.assertEqual(first=status, second=expected_status)
        return status

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_sync_orders(self, x):
        self._add_new_client(client_id="test_client_id",
                             full_name="test_full_name",
                             company_name="test_company_name", hashed_password="test_hashed_password",
                             disabled=False, shopify_app_eg_url="test_shopify_app_eg_url",
                             client_timezone="test_client_timezone")
        result = OrderAgent.sync_orders(client_id="test_client_id", data_store=self.rds_data_store)
        expected_result = 10
        self.assertEqual(first=result[0], second=expected_result)
        result = OrderAgent.sync_orders(client_id="test_client_id", data_store=self.rds_data_store)
        expected_result = 4
        self.assertEqual(first=result[0], second=expected_result)
