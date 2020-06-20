import json
from unittest import TestCase
from unittest import mock

import testing.postgresql

from config import *
from optimization_platform.src.agents.client_agent import ClientAgent
from optimization_platform.src.jobs import sync_products


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

from optimization_platform.src.jobs.sync_products import rds_data_store, main, logger

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
    def test_shit(self, x):
        self._add_new_client(client_id="test_client_id_1",
                             full_name="test_full_name_1",
                             company_name="test_company_name_1", hashed_password="test_hashed_password_!",
                             disabled=False, shopify_app_eg_url="test_shopify_app_eg_url_1",
                             client_timezone="test_client_timezone_1")
        self._add_new_client(client_id="test_client_id_2",
                             full_name="test_full_name_2",
                             company_name="test_company_name_2", hashed_password="test_hashed_password_2",
                             disabled=False, shopify_app_eg_url="test_shopify_app_eg_url_2",
                             client_timezone="test_client_timezone_2")
        main()

        result = self.rds_data_store.run_custom_sql("select * from products")
        len_result = len(result)
        expected_len_result = 16
        self.assertEqual(first=len_result, second=expected_len_result)

        main()

        result = self.rds_data_store.run_custom_sql("select * from products")
        len_result = len(result)
        expected_len_result = 16
        self.assertEqual(first=len_result, second=expected_len_result)

    def test_init(self):
        with mock.patch.object(sync_products, "main", return_value=42):
            with mock.patch.object(sync_products, "__name__", "__main__"):
                with mock.patch.object(sync_products.sys, 'exit') as mock_exit:
                    sync_products.init()
                    self.assertEqual(first=mock_exit.call_args[0][0], second=42)
