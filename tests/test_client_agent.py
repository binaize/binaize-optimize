from unittest import TestCase

import testing.postgresql

from config import *
from optimization_platform.src.agents.client_agent import ClientAgent
from utils.data_store.rds_data_store import RDSDataStore

pgsql = testing.postgresql.Postgresql(cache_initialized_db=True, port=int(AWS_RDS_PORT))
rds_data_store = RDSDataStore(host=AWS_RDS_HOST,
                              port=AWS_RDS_PORT,
                              dbname=AWS_RDS_DBNAME,
                              user=AWS_RDS_USER,
                              password=AWS_RDS_PASSWORD)


class TestClientAgent(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestClientAgent, self).__init__(*args, **kwargs)

    def setUp(self):
        self.rds_data_store = rds_data_store
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def tearDown(self):
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def test_add_new_client(self):
        ClientAgent.add_new_client(data_store=self.rds_data_store, client_id="test_client_id",
                                   full_name="test_full_name",
                                   company_name="test_company_name", hashed_password="test_hashed_password",
                                   disabled=False)
        result = self.rds_data_store.run_custom_sql("select * from clients")
        expected_result = [(
            'test_client_id', 'test_full_name', 'test_company_name', 'test_hashed_password', False, None,
            None, None, None)]
        self.assertListEqual(list1=expected_result, list2=result)

    def test_get_client_details_for_client_id(self):
        ClientAgent.add_new_client(data_store=self.rds_data_store, client_id="test_client_id",
                                   full_name="test_full_name",
                                   company_name="test_company_name", hashed_password="test_hashed_password",
                                   disabled=False)
        result = ClientAgent.get_client_details_for_client_id(data_store=self.rds_data_store,
                                                              client_id="test_client_id")
        expected_result = {'client_id': 'test_client_id', 'full_name': 'test_full_name',
                           'company_name': 'test_company_name', 'hashed_password': 'test_hashed_password',
                           'disabled': False, 'shopify_app_api_key': None, 'shopify_app_password': None,
                           'shopify_app_eg_url': None, 'shopify_app_shared_secret': None}
        self.assertDictEqual(d1=result, d2=expected_result)

    def test_add_shopify_credentials_to_existing_client(self):
        self.test_add_new_client()
        ClientAgent.add_shopify_credentials_to_existing_client(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               shopify_app_api_key=9,
                                                               shopify_app_password="test_shopify_app_password",
                                                               shopify_app_eg_url="test_shopify_app_eg_url",
                                                               shopify_app_shared_secret="test_shopify_app_shared_secret")
        result = self.rds_data_store.run_custom_sql("select * from clients")
        expected_result = [(
            'test_client_id', 'test_full_name', 'test_company_name', 'test_hashed_password', False,
            "9",
            "test_shopify_app_password", "test_shopify_app_eg_url", "test_shopify_app_shared_secret")]
        self.assertListEqual(list1=expected_result, list2=result)

    def test_get_all_client_ids(self):
        ClientAgent.add_new_client(data_store=self.rds_data_store, client_id="test_client_id_1",
                                   full_name="test_full_name_1",
                                   company_name="test_company_name_1", hashed_password="test_hashed_password_1",
                                   disabled=False)
        ClientAgent.add_new_client(data_store=self.rds_data_store, client_id="test_client_id_2",
                                   full_name="test_full_name_2",
                                   company_name="test_company_name_2", hashed_password="test_hashed_password_2",
                                   disabled=False)
        result = ClientAgent.get_all_client_ids(data_store=self.rds_data_store)
        expected_result = ['test_client_id_1', 'test_client_id_2']
        self.assertCountEqual(first=result,second=expected_result)


