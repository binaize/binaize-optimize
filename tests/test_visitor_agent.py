from unittest import TestCase

import testing.postgresql

from config import *
from optimization_platform.src.agents.visitor_agent import VisitorAgent
from utils.data_store.rds_data_store import RDSDataStore

pgsql = testing.postgresql.Postgresql(cache_initialized_db=True, port=int(AWS_RDS_PORT))
rds_data_store = RDSDataStore(host=AWS_RDS_HOST,
                              port=AWS_RDS_PORT,
                              dbname=AWS_RDS_DBNAME,
                              user=AWS_RDS_USER,
                              password=AWS_RDS_PASSWORD)


class TestVisitorAgent(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestVisitorAgent, self).__init__(*args, **kwargs)

    def setUp(self):
        self.rds_data_store = rds_data_store
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def tearDown(self):
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def test_register_visitor_for_client(self):
        status = VisitorAgent.register_visitor_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                                          session_id="test_session_id", ip="435.423.4234.4234",
                                                          city="BLR", region="KA", country="IN", lat="2.334",
                                                          long="3.556", timezone="Asia/Kolkata", browser="Safari",
                                                          os="Mac OS", device="Macbook", fingerprint="5234523453245",
                                                          creation_time=1590570923)
        expected_status = True
        self.assertEqual(first=status, second=expected_status)
        result = self.rds_data_store.run_select_sql("select * from visitors ")
        length = len(result)
        expected_length = 1
        self.assertEqual(first=length, second=expected_length)
        result = list(result[0])
        result[-1] = result[-1].isoformat()
        expected_result = ['test_client_id', 'test_session_id', '435.423.4234.4234', 'BLR', 'KA', 'IN', '2.334',
                           '3.556', 'Asia/Kolkata', 'Safari', 'Mac OS', 'Macbook', '5234523453245',
                           '2020-05-27T14:45:23+05:30']

        self.assertListEqual(list1=expected_result, list2=result)

        status = VisitorAgent.register_visitor_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                                          session_id="test_session_id", ip="435.423.4234.4234",
                                                          city="BLR", region="KA", country="IN", lat="2.334",
                                                          long="3.556", timezone="Asia/Kolkata", browser="Safari",
                                                          os="Mac OS", device="Macbook", fingerprint="5234523453245",
                                                          creation_time=1590570966)

        expected_status = True
        self.assertEqual(first=status, second=expected_status)
        result = self.rds_data_store.run_select_sql("select * from visitors ")
        length = len(result)
        expected_length = 2
        self.assertEqual(first=length, second=expected_length)
        result = list(result[1])
        result[-1] = result[-1].isoformat()
        expected_result = ['test_client_id', 'test_session_id', '435.423.4234.4234', 'BLR', 'KA', 'IN', '2.334',
                           '3.556', 'Asia/Kolkata', 'Safari', 'Mac OS', 'Macbook', '5234523453245',
                           '2020-05-27T14:46:06+05:30']

        self.assertListEqual(list1=expected_result, list2=result)
