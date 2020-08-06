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

    def _add_new_client(self, shopify_store, shopify_access_token, hashed_password):
        timestamp = 1590673060
        status = ClientAgent.add_new_client(data_store=self.rds_data_store, shopify_domain=shopify_store,
                                            shopify_access_token=shopify_access_token, hashed_password=hashed_password,
                                            creation_timestamp=timestamp)
        return status

    def _add_multiple_clients(self):
        csv_file_name = "tests/data/test_client_agent/clients.csv"
        cursor = self.rds_data_store.conn.cursor()
        sql = "COPY clients FROM STDIN DELIMITER ',' CSV HEADER"
        cursor.copy_expert(sql, open(csv_file_name, "r"))
        self.rds_data_store.conn.commit()
        cursor.close()

    def test_add_new_client(self):
        status = self._add_new_client(shopify_store="binaize-dev-watch",
                                      shopify_access_token="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",
                                      hashed_password="test_hashed_password")
        expected_status = True
        self.assertEqual(first=status, second=expected_status)
        result = self.rds_data_store.run_custom_sql("select * from clients")
        result = list(result[0])
        result[-1] = result[-1].isoformat()
        expected_result = ['39985610915', 'binaize-dev-watch.myshopify.com', 'dev.watches.binaize.com', 'Binaize Labs',
                           'binaizeshopify@gmail.com', 'test_hashed_password', False, 'America/New_York',
                           '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', 'Austin', 'US', 'Texas',
                           '2020-05-28T19:07:40+05:30']

        self.assertListEqual(list1=expected_result, list2=result)

        status = self._add_new_client(shopify_store="binaize-dev-watch",
                                      shopify_access_token="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",
                                      hashed_password="test_hashed_password")
        expected_status = None
        self.assertEqual(first=status, second=expected_status)
        result = self.rds_data_store.run_custom_sql("select * from clients")
        result = list(result[0])
        result[-1] = result[-1].isoformat()
        expected_result = ['39985610915', 'binaize-dev-watch.myshopify.com', 'dev.watches.binaize.com', 'Binaize Labs',
                           'binaizeshopify@gmail.com', 'test_hashed_password', False, 'America/New_York',
                           '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', 'Austin', 'US', 'Texas',
                           '2020-05-28T19:07:40+05:30']
        self.assertListEqual(list1=expected_result, list2=result)

    def test_get_client_details_for_client_id(self):
        self._add_multiple_clients()

        result = ClientAgent.get_client_details_for_client_id(data_store=self.rds_data_store,
                                                              client_id="39985610920")
        expected_result = {'client_id': '39985610920', 'shopify_domain': 'binaize-dev-watch.myshopify.com',
                           'shop_domain': 'dev.watches.binaize.com', 'shop_owner': 'Binaize Labs',
                           'email_id': 'binaizeshopify@gmail.com', 'hashed_password': "test_hashed_password'",
                           'disabled': False, 'client_timezone': "'America/New_York",
                           'shopify_access_token': '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', 'city': 'Austin',
                           'country': 'US', 'province': 'Texas', 'creation_time': '2020-05-28'}
        self.assertDictEqual(d1=result, d2=expected_result)

    def test_get_all_client_ids(self):
        self._add_multiple_clients()
        result = ClientAgent.get_all_client_ids(data_store=self.rds_data_store)
        expected_result = ['39985610920', '39985610917']
        self.assertCountEqual(first=result, second=expected_result)

    def test_delete_client_for_client_id(self):
        self._add_multiple_clients()
        status = ClientAgent.delete_client_for_client_id(data_store=self.rds_data_store, client_id="39985610920")
        result = ClientAgent.get_all_client_ids(data_store=self.rds_data_store)
        expected_result = ['39985610920', '39985610917']
        self.assertCountEqual(first=result, second=expected_result)
