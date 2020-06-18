import json
from unittest import TestCase
from unittest import mock

import testing.postgresql

from config import *
from optimization_platform.src.agents.client_agent import ClientAgent
from optimization_platform.src.agents.product_agent import ProductAgent
from utils.data_store.rds_data_store import RDSDataStore


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if "updated_at_min" in args[0]:
        with open("tests/data/test_product_1.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)
    else:
        with open("tests/data/test_product_2.json", "r") as fp:
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

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_sync_products(self, mock_get):
        ClientAgent.add_new_client(data_store=self.rds_data_store, client_id="test_client_id",
                                   full_name="test_full_name",
                                   company_name="test_company_name", hashed_password="test_hashed_password",
                                   disabled=False)
        ClientAgent.add_shopify_credentials_to_existing_client(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               shopify_app_eg_url="test_shopify_app_eg_url",
                                                               shopify_app_shared_secret="test_shopify_app_shared_secret")
        result = ProductAgent.sync_products(client_id="test_client_id", data_store=self.rds_data_store)
        expected_result = 8
        self.assertEqual(first=result, second=expected_result)
        result = ProductAgent.sync_products(client_id="test_client_id", data_store=self.rds_data_store)
        expected_result = 1
        self.assertEqual(first=result, second=expected_result)
