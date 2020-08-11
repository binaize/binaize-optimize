from unittest import TestCase

import testing.postgresql
from unittest import mock
from config import *
from optimization_platform.src.agents.shop_agent import ShopAgent
from utils.data_store.rds_data_store import RDSDataStore
import json

pgsql = testing.postgresql.Postgresql(cache_initialized_db=True, port=int(AWS_RDS_PORT))
rds_data_store = RDSDataStore(host=AWS_RDS_HOST,
                              port=AWS_RDS_PORT,
                              dbname=AWS_RDS_DBNAME,
                              user=AWS_RDS_USER,
                              password=AWS_RDS_PASSWORD)


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

    with open("tests/data/test_shop_agent/store_details.json", "r") as fp:
        data = json.load(fp)
    return MockResponse(data, 200)


class TestShopAgent(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestShopAgent, self).__init__(*args, **kwargs)

    def setUp(self):
        self.rds_data_store = rds_data_store
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def tearDown(self):
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def _add_new_shop(self, shop_id, shopify_access_token, hashed_password):
        timestamp = 1590673060
        status = ShopAgent.add_new_shop(data_store=self.rds_data_store, shop_id=shop_id,
                                        shopify_access_token=shopify_access_token, hashed_password=hashed_password,
                                        creation_timestamp=timestamp)
        return status

    def _add_multiple_shops(self):
        csv_file_name = "tests/data/test_shop_agent/shops.csv"
        cursor = self.rds_data_store.conn.cursor()
        sql = "COPY shops FROM STDIN DELIMITER ',' CSV HEADER"
        with open(csv_file_name, "r") as fp:
            cursor.copy_expert(sql, fp)
        self.rds_data_store.conn.commit()
        cursor.close()

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_add_new_client(self, x):
        status = self._add_new_shop(shop_id="binaize-dev-watch.myshopify.com",
                                    shopify_access_token="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",
                                    hashed_password="test_hashed_password")
        expected_status = True
        self.assertEqual(first=status, second=expected_status)
        result = self.rds_data_store.run_custom_sql("select * from shops")
        result = list(result[0])
        result[-1] = result[-1].isoformat()
        expected_result = ['binaize-dev-watch.myshopify.com', 'dev.watches.binaize.com', 'Binaize Labs',
                           'binaizeshopify@gmail.com', 'test_hashed_password', False, 'America/New_York',
                           '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', 'Austin', 'US', 'Texas', "",
                           '2020-05-28T19:07:40+05:30']

        self.assertListEqual(list1=expected_result, list2=result)

        status = self._add_new_shop(shop_id="binaize-dev-watch.myshopify.com",
                                    shopify_access_token="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",
                                    hashed_password="test_hashed_password")
        expected_status = True
        self.assertEqual(first=status, second=expected_status)
        result = self.rds_data_store.run_custom_sql("select * from shops")
        result = list(result[0])
        result[-1] = result[-1].isoformat()
        expected_result = ['binaize-dev-watch.myshopify.com', 'dev.watches.binaize.com', 'Binaize Labs',
                           'binaizeshopify@gmail.com', 'test_hashed_password', False, 'America/New_York',
                           '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', 'Austin', 'US', 'Texas', "",
                           '2020-05-28T19:07:40+05:30']
        self.assertListEqual(list1=expected_result, list2=result)

    def test_get_shop_details_for_shop_id(self):
        self._add_multiple_shops()

        result = ShopAgent.get_shop_details_for_shop_id(data_store=self.rds_data_store,
                                                        shop_id='binaize-dev-watch.myshopify.com')
        expected_result = {'shop_id': 'binaize-dev-watch.myshopify.com',
                           'shop_domain': 'dev.watches.binaize.com', 'shop_owner': 'Binaize Labs',
                           'email_id': 'binaizeshopify@gmail.com', 'hashed_password': "test_hashed_password",
                           'disabled': False, 'timezone': "America/New_York",
                           'shopify_access_token': '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', 'city': 'Austin',
                           'country': 'US', 'province': 'Texas', 'shopify_nonce': 'shopify_nonce_dev',
                           'creation_time': '2020-05-28'}
        self.assertDictEqual(d1=result, d2=expected_result)

    def test_get_all_shop_ids(self):
        self._add_multiple_shops()
        result = ShopAgent.get_all_shop_ids(data_store=self.rds_data_store)
        expected_result = ['binaize-dev-watch.myshopify.com', 'binaize-staging-watch.myshopify.com']
        self.assertCountEqual(first=result, second=expected_result)

    def test_delete_client_for_client_id(self):
        self._add_multiple_shops()
        status = ShopAgent.delete_shop_for_shop_id(data_store=self.rds_data_store,
                                                   shop_id='binaize-dev-watch.myshopify.com')
        result = ShopAgent.get_all_shop_ids(data_store=self.rds_data_store)
        expected_result = ['binaize-staging-watch.myshopify.com']
        self.assertCountEqual(first=result, second=expected_result)

    def test_get_shopify_details_for_shop_id(self):
        self._add_multiple_shops()
        result = ShopAgent.get_shopify_details_for_shop_id(data_store=self.rds_data_store,
                                                           shop_id="binaize-dev-watch.myshopify.com")
        expected_result = {'shop_id': 'binaize-dev-watch.myshopify.com',
                           'shopify_access_token': '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',
                           'shopify_nonce': 'shopify_nonce_dev'}
        self.assertDictEqual(d1=result, d2=expected_result)

    def test_upsert_shopify_nonce_for_shop_id(self):
        ShopAgent.upsert_shopify_nonce_for_shop_id(data_store=self.rds_data_store, shop_id="test_shop_id",
                                                   shopify_nonce="test_shopify_nonce")
        result = self.rds_data_store.run_custom_sql("select * from shops")
        expected_result = [
            ('test_shop_id', None, None, None, None, None, None, None, None, None, None, 'test_shopify_nonce', None)]

        self.assertCountEqual(first=result, second=expected_result)
        self._add_multiple_shops()
        ShopAgent.upsert_shopify_nonce_for_shop_id(data_store=self.rds_data_store,
                                                   shop_id="binaize-dev-watch.myshopify.com",
                                                   shopify_nonce="dev_shopify_nonce")
        result = self.rds_data_store.run_custom_sql(
            "select * from shops where shop_id='binaize-dev-watch.myshopify.com'")
        result = result[0][:-1]
        expected_result = ('binaize-dev-watch.myshopify.com', 'dev.watches.binaize.com', 'Binaize Labs',
                           'binaizeshopify@gmail.com', 'test_hashed_password', False, 'America/New_York',
                           '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', 'Austin', 'US', 'Texas', 'dev_shopify_nonce')
        self.assertEqual(first=result, second=expected_result)
