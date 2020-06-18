from unittest import TestCase

import testing.postgresql

from config import *
from optimization_platform.src.agents.visit_agent import VisitAgent
from utils.data_store.rds_data_store import RDSDataStore

pgsql = testing.postgresql.Postgresql(cache_initialized_db=True, port=int(AWS_RDS_PORT))
rds_data_store = RDSDataStore(host=AWS_RDS_HOST,
                              port=AWS_RDS_PORT,
                              dbname=AWS_RDS_DBNAME,
                              user=AWS_RDS_USER,
                              password=AWS_RDS_PASSWORD)


class TestVisitAgent(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestVisitAgent, self).__init__(*args, **kwargs)

    def setUp(self):
        self.rds_data_store = rds_data_store
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def tearDown(self):
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def test_register_visit_for_client(self):
        status = VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             session_id="test_session_id",
                                             event_name="test_event_name", url="test_url",
                                             creation_time=1590570923)
        expected_status = True
        self.assertEqual(first=status, second=expected_status)
        result = self.rds_data_store.run_select_sql("select * from visits")
        result = list(result[0])
        result[-2] = result[-2].isoformat()
        expected_result = ['test_client_id', 'test_session_id',
                           'test_event_name', '2020-05-27T14:45:23+05:30', 'test_url']
        self.assertListEqual(list1=expected_result, list2=result)
        self.rds_data_store.run_select_sql("drop table visits ")
        status = VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                                      session_id="test_session_id",
                                                      event_name="test_event_name", url="test_url",
                                                      creation_time=1590570923)
        expected_status = None
        self.assertEqual(first=status, second=expected_status)
