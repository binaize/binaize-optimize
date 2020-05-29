import datetime
from unittest import TestCase

import psycopg2
import testing.postgresql

from optimization_platform.src.agents.event_agent import EventAgent
from utils.data_store.rds_data_store import RDSDataStore


class TestEventAgent(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestEventAgent, self).__init__(*args, **kwargs)
        self.pgsql = testing.postgresql.Postgresql(cache_initialized_db=True)
        self.assertIsNotNone(self.pgsql)
        params = self.pgsql.dsn()
        self.assertEqual('test', params['database'])
        self.assertEqual('127.0.0.1', params['host'])
        self.assertEqual(self.pgsql.settings['port'], params['port'])
        self.assertEqual('postgres', params['user'])
        self.rds_data_store = RDSDataStore(host=params['host'],
                                           port=params['port'],
                                           dbname=params["database"],
                                           user=params["user"],
                                           password=None)
        self.rds_data_store.run_create_table_sql(open("rds_tables.sql", "r").read())

    def test_register_event_for_client(self):
        EventAgent.register_event_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id", variation_id="test_variation_id",
                                             event_name="test_event_name",
                                             creation_time=1590570923)
        result = self.rds_data_store.run_custom_sql("select * from events")
        expected_result = [('test_variation_id', 'test_client_id', 'test_experiment_id', 'test_session_id',
                            'test_event_name', datetime.datetime(2020, 5, 27, 9, 15, 23,
                                                                 tzinfo=psycopg2.tz.FixedOffsetTimezone(offset=330,
                                                                                                        name=None)))]

        self.assertListEqual(list1=expected_result, list2=result)
