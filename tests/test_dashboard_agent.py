import datetime
from unittest import TestCase
from unittest import mock
from unittest.mock import Mock

import pytz
import testing.postgresql

from config import *
from optimization_platform.src.agents.dashboard_agent import DashboardAgent
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


class TestDashboardAgent(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDashboardAgent, self).__init__(*args, **kwargs)

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

    def _create_visit_event(self):
        timestamp = 1590673060
        VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             session_id="test_session_id_1", event_name="home", creation_time=timestamp,
                                             url="url_1")
        VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             session_id="test_session_id_2", event_name="collection",
                                             creation_time=timestamp + 10,
                                             url="url_2")
        VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             session_id="test_session_id_2", event_name="product",
                                             creation_time=timestamp + 20,
                                             url="url_3")
        VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             session_id="test_session_id_4", event_name="cart",
                                             creation_time=timestamp + 30,
                                             url="url_4")
        VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             session_id="test_session_id_5", event_name="checkout",
                                             creation_time=timestamp + 40,
                                             url="url_5")
        VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             session_id="test_session_id_6", event_name="checkout",
                                             creation_time=timestamp + 50,
                                             url="url_6")
        VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             session_id="test_session_id_7", event_name="home",
                                             creation_time=timestamp + 60,
                                             url="url_7")
        VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             session_id="test_session_id_8", event_name="home",
                                             creation_time=timestamp + 70,
                                             url="url_8")
        VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             session_id="test_session_id_9", event_name="collection",
                                             creation_time=timestamp + 80,
                                             url="url_9")
        VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                             session_id="test_session_id_10", event_name="prooduct",
                                             creation_time=timestamp + 90,
                                             url="url_10")

    @mock.patch('datetime.datetime', new=datetime_mock)
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

    @mock.patch('datetime.datetime', new=datetime_mock)
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

    @mock.patch('datetime.datetime', new=datetime_mock)
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

    @mock.patch('datetime.datetime', new=datetime_mock)
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

    @mock.patch('datetime.datetime', new=datetime_mock)
    def test_get_shop_funnel_analytics(self):
        """visits table is empty"""

        result = DashboardAgent.get_shop_funnel_analytics(data_store=self.rds_data_store,
                                                          client_id="test_client_id")
        expected_result = {
            'pages': ['Home Page', 'Collection Page', 'Product Page', 'Cart Page', 'Checkout Page', 'Purchase'],
            'shop_funnel': {'count': [0, 0, 0, 0, 0, 0], 'percentage': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]},
            'summary': 'This is a shop funnel summary', 'conclusion': 'This is shop funnel conclusion'}

        self.assertDictEqual(d1=result, d2=expected_result)

        """visits table has data"""

        self._create_visit_event()

        result = DashboardAgent.get_shop_funnel_analytics(data_store=self.rds_data_store,
                                                          client_id="test_client_id")
        expected_result = {
            'pages': ['Home Page', 'Collection Page', 'Product Page', 'Cart Page', 'Checkout Page', 'Purchase'],
            'shop_funnel': {'count': [3, 2, 1, 1, 2, 0], 'percentage': [75.0, 50.0, 25.0, 25.0, 50.0, 0.0]},
            'summary': 'This is a shop funnel summary', 'conclusion': 'This is shop funnel conclusion'}

        self.assertDictEqual(d1=result, d2=expected_result)

    def test_get_product_conversion_analytics(self):
        result = DashboardAgent.get_product_conversion_analytics(data_store=self.rds_data_store,
                                                                 client_id="test_client_id")
        expected_result = {
            'products': ['Tissot T Race', 'Tissot T Classic', 'Tissot T Sport', 'Tissot 1853', 'Ordinary Watch',
                         'Titan Classic Watch', 'IWC Watch'],
            'product_conversion': {'visitor_count': [1156, 900, 600, 1456, 800, 500, 760],
                                   'conversion_count': [20, 12, 37, 29, 9, 13, 11],
                                   'conversion_percentage': [1.78, 1.33, 6.12, 1.99, 1.12, 2.41, 1.44]},
            'summary': 'This is a product conversion summary', 'conclusion': 'This is product conversion conclusion'}

        self.assertDictEqual(d1=result, d2=expected_result)

    def test_get_landing_page_analytics(self):
        result = DashboardAgent.get_landing_page_analytics(data_store=self.rds_data_store,
                                                           client_id="test_client_id")
        expected_result = {'pages': ['Home Page', 'Product Page', 'Blog Page'],
                           'landing_conversion': {'visitor_count': [11560, 9000, 6000],
                                                  'conversion_count': [200, 120, 370],
                                                  'conversion_percentage': [4.32, 5.34, 8.28]},
                           'summary': 'This is a landing conversion summary',
                           'conclusion': 'This is landing conversion conclusion'}

        self.assertDictEqual(d1=result, d2=expected_result)
