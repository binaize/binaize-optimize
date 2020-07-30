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

    def _add_new_client(self, client_id, full_name, company_name, hashed_password, disabled, shopify_app_eg_url,
                        client_timezone):
        timestamp = 1590673060
        status = ClientAgent.add_new_client(data_store=self.rds_data_store, client_id=client_id,
                                            full_name=full_name,
                                            company_name=company_name, hashed_password=hashed_password,
                                            disabled=disabled, client_timezone=client_timezone,
                                            shopify_app_eg_url=shopify_app_eg_url,
                                            creation_timestamp=timestamp)
        return status

    def _add_multiple_clients(self):
        # f = open("./tests/data/test_client_agent/clients.csv", 'r')
        csv_file_name = "./tests/data/test_client_agent/clients.csv"
        cursor = self.rds_data_store.conn.cursor()
        sql = "COPY clients FROM STDIN DELIMITER ',' CSV HEADER"
        cursor.copy_expert(sql, open(csv_file_name, "r"))
        self.rds_data_store.conn.commit()
        cursor.close()

    def test_add_new_client(self):
        self._add_multiple_clients()
        status = self._add_new_client(client_id="test_client_id",
                                      full_name="test_full_name_1",
                                      company_name="test_company_name_1", hashed_password="test_hashed_password_1",
                                      disabled=False, shopify_app_eg_url="test_shopify_app_eg_url_1",
                                      client_timezone="Asia/Kolkata")
        expected_status = True
        self.assertEqual(first=status, second=expected_status)
        result = self.rds_data_store.run_custom_sql("select * from clients")
        result = list(result[0])
        result[-1] = result[-1].isoformat()
        expected_result = ['test_client_id', 'test_full_name_1', 'test_company_name_1', 'test_hashed_password_1', False,
                           'test_shopify_app_eg_url_1', 'Asia/Kolkata', '2020-05-28T19:07:40+05:30']

        self.assertListEqual(list1=expected_result, list2=result)

        status = self._add_new_client(client_id="test_client_id",
                                      full_name="test_full_name",
                                      company_name="test_company_name", hashed_password="test_hashed_password",
                                      disabled=False, shopify_app_eg_url="test_shopify_app_eg_url",
                                      client_timezone="Asia/Kolkata")
        expected_status = None
        self.assertEqual(first=status, second=expected_status)
        result = self.rds_data_store.run_custom_sql("select * from clients")
        result = list(result[0])
        result[-1] = result[-1].isoformat()
        expected_result = ['test_client_id', 'test_full_name_1', 'test_company_name_1', 'test_hashed_password_1', False,
                           'test_shopify_app_eg_url_1', 'Asia/Kolkata', '2020-05-28T19:07:40+05:30']
        self.assertListEqual(list1=expected_result, list2=result)

    def test_get_client_details_for_client_id(self):
        status = self._add_new_client(client_id="test_client_id",
                                      full_name="test_full_name",
                                      company_name="test_company_name", hashed_password="test_hashed_password",
                                      disabled=False, shopify_app_eg_url="test_shopify_app_eg_url",
                                      client_timezone="test_client_timezone")
        expected_status = True
        self.assertEqual(first=status, second=expected_status)

        result = ClientAgent.get_client_details_for_client_id(data_store=self.rds_data_store,
                                                              client_id="test_client_id")
        expected_result = {'client_id': 'test_client_id', 'full_name': 'test_full_name',
                           'company_name': 'test_company_name', 'hashed_password': 'test_hashed_password',
                           'disabled': False, 'shopify_app_eg_url': 'test_shopify_app_eg_url',
                           'client_timezone': 'test_client_timezone', 'creation_time': '2020-05-28'}
        self.assertDictEqual(d1=result, d2=expected_result)

    def test_get_all_client_ids(self):
        self._add_new_client(client_id="test_client_id_1",
                             full_name="test_full_name_1",
                             company_name="test_company_name_1", hashed_password="test_hashed_password_1",
                             disabled=False, shopify_app_eg_url="test_shopify_app_eg_url_1",
                             client_timezone="test_client_timezone")
        self._add_new_client(client_id="test_client_id_2",
                             full_name="test_full_name_2",
                             company_name="test_company_name_2", hashed_password="test_hashed_password_2",
                             disabled=False, shopify_app_eg_url="test_shopify_app_eg_url_2",
                             client_timezone="test_client_timezone")
        result = ClientAgent.get_all_client_ids(data_store=self.rds_data_store)
        expected_result = ['test_client_id_1', 'test_client_id_2']
        self.assertCountEqual(first=result, second=expected_result)
