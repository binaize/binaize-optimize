import datetime
from unittest import TestCase
from unittest import mock
from unittest.mock import Mock

import pytz
import testing.postgresql

from config import *
from optimization_platform.src.analytics.experiment import ExperimentAnalytics
from optimization_platform.src.agents.event_agent import EventAgent
from optimization_platform.src.agents.variation_agent import VariationAgent
from optimization_platform.src.agents.visit_agent import VisitAgent
from utils.data_store.rds_data_store import RDSDataStore

pgsql = testing.postgresql.Postgresql(cache_initialized_db=True, port=int(AWS_RDS_PORT))
rds_data_store = RDSDataStore(host=AWS_RDS_HOST,
                              port=AWS_RDS_PORT,
                              dbname=AWS_RDS_DBNAME,
                              user=AWS_RDS_USER,
                              password=AWS_RDS_PASSWORD)

datetime_mock = Mock(wraps=datetime.datetime)
tz = pytz.timezone("Asia/Kolkata")
datetime_now = tz.localize(datetime.datetime(2020, 5, 30, 13, 0, 0, 0))
datetime_mock.now.return_value = datetime_now


class TestExperimentAnalytics(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestExperimentAnalytics, self).__init__(*args, **kwargs)

    def setUp(self):
        self.rds_data_store = rds_data_store
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def tearDown(self):
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

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

    @mock.patch('datetime.datetime', new=datetime_mock)
    def test_get_session_count_per_variation_over_time(self):
        """events and variations table both has are empty"""

        result = ExperimentAnalytics.get_session_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """variations table has data but events table does not have"""

        variation_1, variation_2 = self._create_variation()

        result = ExperimentAnalytics.get_session_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)

        result = ExperimentAnalytics.get_session_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id")
        expected_result = {'date': ['May 28'],
                           'session_count': {'test_variation_name_1': [1], 'test_variation_name_2': [3]}}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = ExperimentAnalytics.get_session_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

    @mock.patch('datetime.datetime', new=datetime_mock)
    def test_get_visitor_count_per_variation_over_time(self):
        """events and variations table both has are empty"""

        result = ExperimentAnalytics.get_visitor_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """variations table has data but events table does not have"""

        variation_1, variation_2 = self._create_variation()

        result = ExperimentAnalytics.get_visitor_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)

        result = ExperimentAnalytics.get_visitor_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id")
        expected_result = {'date': ['May 28'],
                           'visitor_count': {'test_variation_name_1': [1], 'test_variation_name_2': [2]}}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = ExperimentAnalytics.get_visitor_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

    @mock.patch('datetime.datetime', new=datetime_mock)
    def test_get_conversion_rate_per_variation_over_time(self):
        """events and variations table both has are empty"""

        result = ExperimentAnalytics.get_conversion_rate_per_variation_over_time(data_store=self.rds_data_store,
                                                                                 client_id="test_client_id",
                                                                                 experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """variations table has data but events table does not have"""

        variation_1, variation_2 = self._create_variation()

        result = ExperimentAnalytics.get_conversion_rate_per_variation_over_time(data_store=self.rds_data_store,
                                                                                 client_id="test_client_id",
                                                                                 experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)

        result = ExperimentAnalytics.get_conversion_rate_per_variation_over_time(data_store=self.rds_data_store,
                                                                                 client_id="test_client_id",
                                                                                 experiment_id="test_experiment_id")
        expected_result = {'date': ['May 28'],
                           'conversion': {'test_variation_name_1': [1.0], 'test_variation_name_2': [0.5]}}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = ExperimentAnalytics.get_conversion_rate_per_variation_over_time(data_store=self.rds_data_store,
                                                                                 client_id="test_client_id",
                                                                                 experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

    @mock.patch('datetime.datetime', new=datetime_mock)
    def test_get_conversion_rate_of_experiment(self):
        """events and variations table both has are empty"""

        result = ExperimentAnalytics.get_conversion_rate_of_experiment(data_store=self.rds_data_store,
                                                                       client_id="test_client_id",
                                                                       experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """variations table has data but events table does not have"""

        variation_1, variation_2 = self._create_variation()

        result = ExperimentAnalytics.get_conversion_rate_of_experiment(data_store=self.rds_data_store,
                                                                       client_id="test_client_id",
                                                                       experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)

        result = ExperimentAnalytics.get_conversion_rate_of_experiment(data_store=self.rds_data_store,
                                                                       client_id="test_client_id",
                                                                       experiment_id="test_experiment_id")

        for record in result:
            record.pop("variation_id", None)
        expected_result = [
            {'variation_name': 'test_variation_name_2',
             'num_session': 3, 'num_visitor': 2, 'visitor_converted': 1, 'goal_conversion': 50.0,
             'sales_conversion': 2.45},
            {'variation_name': 'test_variation_name_1',
             'num_session': 1, 'num_visitor': 1, 'visitor_converted': 1, 'goal_conversion': 100.0,
             'sales_conversion': 2.45}]

        self.assertCountEqual(first=result, second=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = ExperimentAnalytics.get_conversion_rate_of_experiment(data_store=self.rds_data_store,
                                                                       client_id="test_client_id",
                                                                       experiment_id="test_experiment_id")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)
