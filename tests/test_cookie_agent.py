from unittest import TestCase

import testing.postgresql

from config import *
from optimization_platform.src.agents.cookie_agent import CookieAgent
from utils.data_store.rds_data_store import RDSDataStore

pgsql = testing.postgresql.Postgresql(cache_initialized_db=True, port=int(AWS_RDS_PORT))
rds_data_store = RDSDataStore(host=AWS_RDS_HOST,
                              port=AWS_RDS_PORT,
                              dbname=AWS_RDS_DBNAME,
                              user=AWS_RDS_USER,
                              password=AWS_RDS_PASSWORD)


class TestCookieAgent(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestCookieAgent, self).__init__(*args, **kwargs)

    def setUp(self):
        self.rds_data_store = rds_data_store
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def tearDown(self):
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def test_register_event_for_client(self):
        CookieAgent.register_cookie_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                               session_id="test_session_id", shopify_x="test_shopify_x",
                                               cart_token="test_cart_token",
                                               creation_time=1590570923)
        result = self.rds_data_store.run_select_sql("select * from cookie ")
        result = list(result[0])
        result[-1] = result[-1].isoformat()
        expected_result = ['test_client_id', 'test_session_id', 'test_shopify_x', 'test_cart_token',
                           '2020-05-27T14:45:23+05:30']

        self.assertListEqual(list1=expected_result, list2=result)
