from unittest import TestCase

import testing.postgresql

from optimization_platform.src.agents.dashboard_agent import DashboardAgent
from optimization_platform.src.agents.event_agent import EventAgent
from optimization_platform.src.agents.variation_agent import VariationAgent
from utils.data_store.rds_data_store import RDSDataStore


class TestDashboardAgent(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDashboardAgent, self).__init__(*args, **kwargs)
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

    def _create_event(self, variation_1, variation_2):
        timestamp = 1590673060
        variation_id_1 = variation_1["variation_id"]
        variation_id_2 = variation_2["variation_id"]
        EventAgent.register_event_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_1", variation_id=variation_id_1,
                                             event_name="served",
                                             creation_time=timestamp)
        EventAgent.register_event_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_1", variation_id=variation_id_1,
                                             event_name="clicked",
                                             creation_time=timestamp + 10)
        EventAgent.register_event_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_2", variation_id=variation_id_2,
                                             event_name="served",
                                             creation_time=timestamp + 20)
        EventAgent.register_event_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_3", variation_id=variation_id_2,
                                             event_name="served",
                                             creation_time=timestamp + 30)
        EventAgent.register_event_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_3", variation_id=variation_id_2,
                                             event_name="clicked",
                                             creation_time=timestamp + 40)
        EventAgent.register_event_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_3", variation_id=variation_id_2,
                                             event_name="served",
                                             creation_time=timestamp + 50)
        EventAgent.register_event_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_3", variation_id=variation_id_2,
                                             event_name="clicked",
                                             creation_time=timestamp + 60)

    def _create_variation(self):
        variation_1 = VariationAgent.create_variation_for_client_id_and_experiment_id(data_store=self.rds_data_store,
                                                                                      client_id="test_client_id",
                                                                                      experiment_id="test_experiment_id",
                                                                                      variation_name="test_variation_name_1",
                                                                                      traffic_percentage=50)
        variation_2 = VariationAgent.create_variation_for_client_id_and_experiment_id(data_store=self.rds_data_store,
                                                                                      client_id="test_client_id",
                                                                                      experiment_id="test_experiment_id",
                                                                                      variation_name="test_variation_name_2",
                                                                                      traffic_percentage=50)
        return variation_1, variation_2

    def test_get_session_count_per_variation_over_time(self):
        """events and variations table both has are empty"""

        result = DashboardAgent.get_session_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                          client_id="test_client_id",
                                                                          experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """variations table has data but events table does not have"""

        variation_1, variation_2 = self._create_variation()

        result = DashboardAgent.get_session_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                          client_id="test_client_id",
                                                                          experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)

        result = DashboardAgent.get_session_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                          client_id="test_client_id",
                                                                          experiment_id="test_experiment_id")
        expected_result = {'date': ['May 28'],
                           'session_count': {'test_variation_name_1': [1], 'test_variation_name_2': [3]}}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = DashboardAgent.get_session_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                          client_id="test_client_id",
                                                                          experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

    def test_get_visitor_count_per_variation_over_time(self):
        """events and variations table both has are empty"""

        result = DashboardAgent.get_visitor_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                          client_id="test_client_id",
                                                                          experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """variations table has data but events table does not have"""

        variation_1, variation_2 = self._create_variation()

        result = DashboardAgent.get_visitor_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                          client_id="test_client_id",
                                                                          experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)

        result = DashboardAgent.get_visitor_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                          client_id="test_client_id",
                                                                          experiment_id="test_experiment_id")
        expected_result = {'date': ['May 28'],
                           'visitor_count': {'test_variation_name_1': [1], 'test_variation_name_2': [2]}}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = DashboardAgent.get_visitor_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                          client_id="test_client_id",
                                                                          experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

    def test_get_conversion_rate_per_variation_over_time(self):
        """events and variations table both has are empty"""

        result = DashboardAgent.get_conversion_rate_per_variation_over_time(data_store=self.rds_data_store,
                                                                            client_id="test_client_id",
                                                                            experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """variations table has data but events table does not have"""

        variation_1, variation_2 = self._create_variation()

        result = DashboardAgent.get_conversion_rate_per_variation_over_time(data_store=self.rds_data_store,
                                                                            client_id="test_client_id",
                                                                            experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)

        result = DashboardAgent.get_conversion_rate_per_variation_over_time(data_store=self.rds_data_store,
                                                                            client_id="test_client_id",
                                                                            experiment_id="test_experiment_id")
        expected_result = {'date': ['May 28'],
                           'conversion': {'test_variation_name_1': [1.0], 'test_variation_name_2': [0.5]}}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = DashboardAgent.get_conversion_rate_per_variation_over_time(data_store=self.rds_data_store,
                                                                            client_id="test_client_id",
                                                                            experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

    def test_get_conversion_rate_of_experiment(self):
        """events and variations table both has are empty"""

        result = DashboardAgent.get_conversion_rate_of_experiment(data_store=self.rds_data_store,
                                                                  client_id="test_client_id",
                                                                  experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """variations table has data but events table does not have"""

        variation_1, variation_2 = self._create_variation()

        result = DashboardAgent.get_conversion_rate_of_experiment(data_store=self.rds_data_store,
                                                                  client_id="test_client_id",
                                                                  experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)

        result = DashboardAgent.get_conversion_rate_of_experiment(data_store=self.rds_data_store,
                                                                  client_id="test_client_id",
                                                                  experiment_id="test_experiment_id")
        expected_result = [
            {'variation_name': 'test_variation_name_1', 'variation_id': variation_1["variation_id"],
             'num_session': 1, 'num_visitor': 1, 'visitor_converted': 1, 'conversion': 1.0},
            {'variation_name': 'test_variation_name_2', 'variation_id': variation_2["variation_id"],
             'num_session': 3, 'num_visitor': 2, 'visitor_converted': 1, 'conversion': 0.5}]

        self.assertCountEqual(first=result, second=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = DashboardAgent.get_conversion_rate_of_experiment(data_store=self.rds_data_store,
                                                                  client_id="test_client_id",
                                                                  experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)
