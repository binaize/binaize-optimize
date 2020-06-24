import datetime
from unittest import TestCase
from unittest import mock
from unittest.mock import Mock

import pytz
import testing.postgresql
import json
import uuid

from config import *
from optimization_platform.src.analytics.experiment.experiment_analytics import ExperimentAnalytics
from optimization_platform.src.agents.event_agent import EventAgent
from optimization_platform.src.agents.variation_agent import VariationAgent
from optimization_platform.src.agents.order_agent import OrderAgent
from utils.data_store.rds_data_store import RDSDataStore
from optimization_platform.src.agents.cookie_agent import CookieAgent
from optimization_platform.src.agents.client_agent import ClientAgent


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if "orders.json" in args[0]:
        with open("tests/data/test_experiment_order.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)
    elif "checkouts.json" in args[0]:
        with open("tests/data/test_experiment_checkout.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)


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

    def _add_new_client(self, client_id, full_name, company_name, hashed_password, disabled, shopify_app_eg_url,
                        client_timezone):
        timestamp = 1590673060
        ClientAgent.add_new_client(data_store=self.rds_data_store, client_id=client_id,
                                   full_name=full_name,
                                   company_name=company_name, hashed_password=hashed_password,
                                   disabled=disabled, client_timezone=client_timezone,
                                   shopify_app_eg_url=shopify_app_eg_url,
                                   creation_timestamp=timestamp)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def _create_orders(self, x):
        self._add_new_client(client_id="test_client_id",
                             full_name="test_full_name",
                             company_name="test_company_name", hashed_password="test_hashed_password",
                             disabled=False, shopify_app_eg_url="test_shopify_app_eg_url",
                             client_timezone="test_client_timezone")
        OrderAgent.sync_orders(client_id="test_client_id", data_store=self.rds_data_store)

    def _create_cookie(self):
        CookieAgent.register_cookie_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                               session_id="test_session_id_1", cart_token="cart_token_1",
                                               creation_time=1590570923)
        CookieAgent.register_cookie_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                               session_id="test_session_id_1", cart_token="cart_token_2",
                                               creation_time=1590570923)
        CookieAgent.register_cookie_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                               session_id="test_session_id_1", cart_token="cart_token_3",
                                               creation_time=1590570923)
        CookieAgent.register_cookie_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                               session_id="test_session_id_4", cart_token="cart_token_4",
                                               creation_time=1590570923)

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

    def _create_random_event(self, variation_1, variation_2):
        timestamp = 1590673060
        variation_id_1 = variation_1["variation_id"]
        variation_id_2 = variation_2["variation_id"]

        session_id = uuid.uuid4().hex
        EventAgent.register_event_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             experiment_id="test_experiment_id",
                                             session_id=session_id, variation_id=variation_id_1,
                                             event_name="served",
                                             creation_time=timestamp)
        EventAgent.register_event_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             experiment_id="test_experiment_id",
                                             session_id=session_id, variation_id=variation_id_1,
                                             event_name="clicked",
                                             creation_time=timestamp + 60)
        session_id = uuid.uuid4().hex
        EventAgent.register_event_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             experiment_id="test_experiment_id",
                                             session_id=session_id, variation_id=variation_id_2,
                                             event_name="served",
                                             creation_time=timestamp)
        EventAgent.register_event_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             experiment_id="test_experiment_id",
                                             session_id=session_id, variation_id=variation_id_2,
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
                                                                               experiment_id="test_experiment_id",
                                                                               timezone_str="Asia/Kolkata")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """variations table has data but events table does not have"""

        variation_1, variation_2 = self._create_variation()

        result = ExperimentAnalytics.get_session_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id",
                                                                               timezone_str="Asia/Kolkata")
        expected_result = {'date': ['May 24', 'May 25', 'May 26', 'May 27', 'May 28', 'May 29', 'May 30'],
                           'session_count': {'test_variation_name_1': [0, 0, 0, 0, 0, 0, 0],
                                             'test_variation_name_2': [0, 0, 0, 0, 0, 0, 0]}}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)

        result = ExperimentAnalytics.get_session_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id",
                                                                               timezone_str="Asia/Kolkata")
        expected_result = {'date': ['May 24', 'May 25', 'May 26', 'May 27', 'May 28', 'May 29', 'May 30'],
                           'session_count': {'test_variation_name_1': [0, 0, 0, 0, 1, 0, 0],
                                             'test_variation_name_2': [0, 0, 0, 0, 3, 0, 0]}}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = ExperimentAnalytics.get_session_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id",
                                                                               timezone_str="Asia/Kolkata")
        expected_result = {'date': ['May 24', 'May 25', 'May 26', 'May 27', 'May 28', 'May 29', 'May 30'],
                           'session_count': {'test_variation_name_1': [0, 0, 0, 0, 0, 0, 0],
                                             'test_variation_name_2': [0, 0, 0, 0, 0, 0, 0]}}
        self.assertDictEqual(d1=result, d2=expected_result)

    @mock.patch('datetime.datetime', new=datetime_mock)
    def test_get_visitor_count_per_variation_over_time(self):
        """events and variations table both has are empty"""

        result = ExperimentAnalytics.get_visitor_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id",
                                                                               timezone_str="Asia/Kolkata")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """variations table has data but events table does not have"""

        variation_1, variation_2 = self._create_variation()

        result = ExperimentAnalytics.get_visitor_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id",
                                                                               timezone_str="Asia/Kolkata")
        expected_result = {'date': ['May 24', 'May 25', 'May 26', 'May 27', 'May 28', 'May 29', 'May 30'],
                           'visitor_count': {'test_variation_name_1': [0, 0, 0, 0, 0, 0, 0],
                                             'test_variation_name_2': [0, 0, 0, 0, 0, 0, 0]}}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)

        result = ExperimentAnalytics.get_visitor_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id",
                                                                               timezone_str="Asia/Kolkata")
        expected_result = {'date': ['May 24', 'May 25', 'May 26', 'May 27', 'May 28', 'May 29', 'May 30'],
                           'visitor_count': {'test_variation_name_1': [0, 0, 0, 0, 1, 0, 0],
                                             'test_variation_name_2': [0, 0, 0, 0, 2, 0, 0]}}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = ExperimentAnalytics.get_visitor_count_per_variation_over_time(data_store=self.rds_data_store,
                                                                               client_id="test_client_id",
                                                                               experiment_id="test_experiment_id",
                                                                               timezone_str="Asia/Kolkata")
        expected_result = {'date': ['May 24', 'May 25', 'May 26', 'May 27', 'May 28', 'May 29', 'May 30'],
                           'visitor_count': {'test_variation_name_1': [0, 0, 0, 0, 0, 0, 0],
                                             'test_variation_name_2': [0, 0, 0, 0, 0, 0, 0]}}
        self.assertDictEqual(d1=result, d2=expected_result)

    @mock.patch('datetime.datetime', new=datetime_mock)
    def test_get_conversion_rate_per_variation_over_time(self):
        """events and variations table both has are empty"""

        result = ExperimentAnalytics.get_conversion_rate_per_variation_over_time(data_store=self.rds_data_store,
                                                                                 client_id="test_client_id",
                                                                                 experiment_id="test_experiment_id",
                                                                                 timezone_str="Asia/Kolkata")
        expected_result = {}
        self.assertDictEqual(d1=result, d2=expected_result)

        """variations table has data but events table does not have"""

        variation_1, variation_2 = self._create_variation()

        result = ExperimentAnalytics.get_conversion_rate_per_variation_over_time(data_store=self.rds_data_store,
                                                                                 client_id="test_client_id",
                                                                                 experiment_id="test_experiment_id",
                                                                                 timezone_str="Asia/Kolkata")
        expected_result = {'date': ['May 24', 'May 25', 'May 26', 'May 27', 'May 28', 'May 29', 'May 30'],
                           'conversion': {'test_variation_name_1': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                                          'test_variation_name_2': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)

        result = ExperimentAnalytics.get_conversion_rate_per_variation_over_time(data_store=self.rds_data_store,
                                                                                 client_id="test_client_id",
                                                                                 experiment_id="test_experiment_id",
                                                                                 timezone_str="Asia/Kolkata")
        expected_result = {'date': ['May 24', 'May 25', 'May 26', 'May 27', 'May 28', 'May 29', 'May 30'],
                           'conversion': {'test_variation_name_1': [0.0, 0.0, 0.0, 0.0, 99.01, 0.0, 0.0],
                                          'test_variation_name_2': [0.0, 0.0, 0.0, 0.0, 49.75, 0.0, 0.0]}}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = ExperimentAnalytics.get_conversion_rate_per_variation_over_time(data_store=self.rds_data_store,
                                                                                 client_id="test_client_id",
                                                                                 experiment_id="test_experiment_id",
                                                                                 timezone_str="Asia/Kolkata")
        expected_result = {'date': ['May 24', 'May 25', 'May 26', 'May 27', 'May 28', 'May 29', 'May 30'],
                           'conversion': {'test_variation_name_1': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                                          'test_variation_name_2': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}}
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
        for record in result:
            record.pop("variation_id", None)
        expected_result = [
            {'variation_name': 'test_variation_name_1',
             'num_session': 0, 'num_visitor': 0, 'goal_conversion_count': 0, 'sales_conversion_count': 0,
             'goal_conversion': 0.0, 'sales_conversion': 0.0},
            {'variation_name': 'test_variation_name_2',
             'num_session': 0, 'num_visitor': 0, 'goal_conversion_count': 0, 'sales_conversion_count': 0,
             'goal_conversion': 0.0, 'sales_conversion': 0.0}]
        self.assertCountEqual(first=result, second=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)
        self._create_orders()
        self._create_cookie()

        result = ExperimentAnalytics.get_conversion_rate_of_experiment(data_store=self.rds_data_store,
                                                                       client_id="test_client_id",
                                                                       experiment_id="test_experiment_id")

        for record in result:
            record.pop("variation_id", None)
        expected_result = [
            {'variation_name': 'test_variation_name_1', 'num_session': 1, 'num_visitor': 1, 'goal_conversion_count': 1,
             'sales_conversion_count': 8, 'goal_conversion': 99.01, 'sales_conversion': 100.0},
            {'variation_name': 'test_variation_name_2', 'num_session': 3, 'num_visitor': 2, 'goal_conversion_count': 1,
             'sales_conversion_count': 0, 'goal_conversion': 49.75, 'sales_conversion': 0.0}]

        self.assertCountEqual(first=result, second=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = ExperimentAnalytics.get_conversion_rate_of_experiment(data_store=self.rds_data_store,
                                                                       client_id="test_client_id",
                                                                       experiment_id="test_experiment_id")
        for record in result:
            record.pop("variation_id", None)
        expected_result = [
            {'variation_name': 'test_variation_name_1',
             'num_session': 0, 'num_visitor': 0, 'goal_conversion_count': 0, 'sales_conversion_count': 0,
             'goal_conversion': 0.0, 'sales_conversion': 0.0},
            {'variation_name': 'test_variation_name_2',
             'num_session': 0, 'num_visitor': 0, 'goal_conversion_count': 0, 'sales_conversion_count': 0,
             'goal_conversion': 0.0, 'sales_conversion': 0.0}]
        self.assertCountEqual(first=result, second=expected_result)

    @mock.patch('datetime.datetime', new=datetime_mock)
    def test_get_summary_of_experiment(self):
        """events and variations table both has are empty"""

        result = ExperimentAnalytics.get_summary_of_experiment(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               experiment_id="test_experiment_id")
        expected_result = {'status': '<strong> SUMMARY : </strong> Not enough visitors on the website.',
                           'conclusion': '<strong> STATUS : </strong> Not enough visitors on the website.',
                           'recommendation': "<strong> RECOMMENDATION : </strong> <span style = 'color: blue; font-size: 16px;'><strong>  CONTINUE </strong></span> the Experiment."}
        self.assertDictEqual(d1=result, d2=expected_result)

        """variations table has data but events table does not have"""

        variation_1, variation_2 = self._create_variation()

        result = ExperimentAnalytics.get_summary_of_experiment(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               experiment_id="test_experiment_id")
        expected_result = {'status': '<strong> SUMMARY : </strong> Not enough visitors on the website.',
                           'conclusion': '<strong> STATUS : </strong> Not enough visitors on the website.',
                           'recommendation': "<strong> RECOMMENDATION : </strong> <span style = 'color: blue; font-size: 16px;'><strong>  CONTINUE </strong></span> the Experiment."}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self._create_event(variation_1, variation_2)
        self._create_orders()
        self._create_cookie()

        result = ExperimentAnalytics.get_summary_of_experiment(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               experiment_id="test_experiment_id")

        expected_result = {
            'status': "<strong> SUMMARY : </strong><span style = 'color: blue; font-size: 16px;'><strong> test_variation_name_1 </strong></span> is winning. It is <span style = 'color: blue; font-size: 16px;'><strong> 49.75% </strong></span> better than the others.",
            'conclusion': "<strong> STATUS : </strong> There is <span style = 'color: red; font-size: 16px;'><strong> NOT ENOUGH</strong></span> evidence to conclude the experiment (It is <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> yet statistically significant).To be statistically confident, we need <strong> 1566 </strong> more visitors.Based on recent visitor trend, experiment should run for another <strong> 22 </strong> days.",
            'recommendation': "<strong> RECOMMENDATION : </strong> <span style = 'color: blue; font-size: 16px;'><strong>  CONTINUE </strong></span> the Experiment."}

        self.assertDictEqual(d1=result, d2=expected_result)

        for i in range(100):
            self._create_random_event(variation_1, variation_2)

        result = ExperimentAnalytics.get_summary_of_experiment(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               experiment_id="test_experiment_id")

        expected_result = {
            'status': "<strong> SUMMARY : </strong><span style = 'color: blue; font-size: 16px;'><strong> test_variation_name_1 </strong></span> is winning. It is <span style = 'color: blue; font-size: 16px;'><strong> 0.98% </strong></span> better than the others.",
            'conclusion': "<strong> STATUS : </strong> There is <span style = 'color: red; font-size: 16px;'><strong> NOT ENOUGH</strong></span> evidence to conclude the experiment (It is <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> yet statistically significant).To be statistically confident, we need <strong> 1366 </strong> more visitors.Based on recent visitor trend, experiment should run for another <strong> 1 </strong> days.",
            'recommendation': "<strong> RECOMMENDATION : </strong> <span style = 'color: blue; font-size: 16px;'><strong>  CONTINUE </strong></span> the Experiment."}
        self.assertDictEqual(d1=result, d2=expected_result)

        for i in range(900):
            self._create_random_event(variation_1, variation_2)

        result = ExperimentAnalytics.get_summary_of_experiment(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               experiment_id="test_experiment_id")

        expected_result = {
            'status': "<strong> SUMMARY : </strong><span style = 'color: blue; font-size: 16px;'><strong> test_variation_name_1 </strong></span> is winning. It is <span style = 'color: blue; font-size: 16px;'><strong> 0.1% </strong></span> better than the others.",
            'conclusion': "<strong> STATUS : </strong> There is <span style = 'color: green; font-size: 16px;'><strong> ENOUGH </strong></span> evidence to conclude the experiment. There is <span style = 'color: red; font-size: 16px;'><strong> NO CLEAR WINNER </strong></span>. We are <span style = 'color: red; font-size: 16px;'><strong> 68.26% </strong></span> confident that <span style = 'color: blue; font-size: 16px;'><strong> test_variation_name_1 </strong></span> is the best.",
            'recommendation': "<strong> RECOMMENDATION : </strong> <span style = 'color: green; font-size: 16px;'><strong>  STOP </strong></span> the Experiment."}
        self.assertDictEqual(d1=result, d2=expected_result)

        """events and variations table both has data"""

        self.rds_data_store.run_update_sql(query="truncate table events")

        result = ExperimentAnalytics.get_summary_of_experiment(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               experiment_id="test_experiment_id")
        expected_result = {'status': '<strong> SUMMARY : </strong> Not enough visitors on the website.',
                           'conclusion': '<strong> STATUS : </strong> Not enough visitors on the website.',
                           'recommendation': "<strong> RECOMMENDATION : </strong> <span style = 'color: blue; font-size: 16px;'><strong>  CONTINUE </strong></span> the Experiment."}
        self.assertDictEqual(d1=result, d2=expected_result)
