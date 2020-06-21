import datetime
import json
from unittest import TestCase
from unittest import mock
from unittest.mock import Mock

import pytz
import testing.postgresql

from config import *
from optimization_platform.src.agents.client_agent import ClientAgent
from optimization_platform.src.agents.order_agent import OrderAgent
from optimization_platform.src.agents.product_agent import ProductAgent
from optimization_platform.src.agents.visit_agent import VisitAgent
from optimization_platform.src.analytics.conversion import ConversionAnalytics
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


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if "orders.json" in args[0]:
        with open("tests/data/test_conversion_order.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)
    elif "products.json" in args[0]:
        with open("tests/data/test_conversion_product.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)
    elif "checkouts.json" in args[0]:
        with open("tests/data/test_conversion_checkout.json", "r") as fp:
            data = json.load(fp)
        return MockResponse(data, 200)


class TestConversionAnalytics(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestConversionAnalytics, self).__init__(*args, **kwargs)

    def setUp(self):
        self.rds_data_store = rds_data_store
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def tearDown(self):
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def _create_visit_event(self):
        timestamp = 1590673060
        with open("tests/data/test_visits.json", "r") as fp:
            visits = json.load(fp)
        i = 0
        for visit in visits:
            i += 10
            VisitAgent.register_visit_for_client(data_store=self.rds_data_store, client_id="test_client_id",
                                                 session_id=visit["session_id"], event_name=visit["event_name"],
                                                 creation_time=timestamp + i,
                                                 url=visit["url"])

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
    def _create_products(self, x):
        self._add_new_client(client_id="test_client_id",
                             full_name="test_full_name",
                             company_name="test_company_name", hashed_password="test_hashed_password",
                             disabled=False, shopify_app_eg_url="test_shopify_app_eg_url",
                             client_timezone="test_client_timezone")
        ProductAgent.sync_products(client_id="test_client_id", data_store=self.rds_data_store)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def _create_orders(self, x):
        self._add_new_client(client_id="test_client_id",
                             full_name="test_full_name",
                             company_name="test_company_name", hashed_password="test_hashed_password",
                             disabled=False, shopify_app_eg_url="test_shopify_app_eg_url",
                             client_timezone="test_client_timezone")
        OrderAgent.sync_orders(client_id="test_client_id", data_store=self.rds_data_store)

    # @mock.patch('datetime.datetime', new=datetime_mock)
    def test_get_shop_funnel_analytics(self):
        start_date_str = "2020-05-28T00-00-01"
        end_date_str = "2020-06-28T23-59-59"
        """visits,products and orders table are empty"""

        result = ConversionAnalytics.get_shop_funnel_analytics(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               start_date_str=start_date_str,
                                                               end_date_str=end_date_str)

        expected_result = {
            'pages': ['Home Page', 'Collection Page', 'Product Page', 'Cart Page', 'Checkout Page', 'Purchase'],
            'shop_funnel': {'count': [0, 0, 0, 0, 0, 0], 'percentage': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]},
            'summary': '<strong> SUMMARY </strong> There are NOT enough visits registered on the website',
            'conclusion': '<strong> CONCLUSION </strong> Wait for the customers to interact with your website'}

        self.assertDictEqual(d1=result, d2=expected_result)

        """visits and orders table are empty but products table has data"""
        self._create_products()
        result = ConversionAnalytics.get_shop_funnel_analytics(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               start_date_str=start_date_str,
                                                               end_date_str=end_date_str)
        expected_result = {
            'pages': ['Home Page', 'Collection Page', 'Product Page', 'Cart Page', 'Checkout Page', 'Purchase'],
            'shop_funnel': {'count': [0, 0, 0, 0, 0, 0], 'percentage': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]},
            'summary': '<strong> SUMMARY </strong> There are NOT enough visits registered on the website',
            'conclusion': '<strong> CONCLUSION </strong> Wait for the customers to interact with your website'}

        self.assertDictEqual(d1=result, d2=expected_result)

        """visits table is empty but products and orders table has data"""
        self._create_orders()

        result = ConversionAnalytics.get_shop_funnel_analytics(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               start_date_str=start_date_str,
                                                               end_date_str=end_date_str)
        expected_result = {
            'pages': ['Home Page', 'Collection Page', 'Product Page', 'Cart Page', 'Checkout Page', 'Purchase'],
            'shop_funnel': {'count': [0, 0, 0, 0, 7, 5], 'percentage': [0.0, 0.0, 0.0, 0.0, 99.86, 71.33]},
            'summary': '<strong> SUMMARY </strong> There are NOT enough visits registered on the website',
            'conclusion': '<strong> CONCLUSION </strong> Wait for the customers to interact with your website'}

        self.assertDictEqual(d1=result, d2=expected_result)

        """all tables have data"""

        self._create_visit_event()

        result = ConversionAnalytics.get_shop_funnel_analytics(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               start_date_str=start_date_str,
                                                               end_date_str=end_date_str)
        expected_result = {
            'pages': ['Home Page', 'Collection Page', 'Product Page', 'Cart Page', 'Checkout Page', 'Purchase'],
            'shop_funnel': {'count': [30, 20, 15, 10, 7, 5], 'percentage': [99.97, 66.64, 49.98, 33.32, 23.33, 16.66]},
            'summary': '<strong> SUMMARY </strong> Home Page has maximum churn of 33.33%',
            'conclusion': '<strong> CONCLUSION </strong> Experiment with different creatives/copies for Home Page'}

        self.assertDictEqual(d1=result, d2=expected_result)

    @mock.patch('datetime.datetime', new=datetime_mock)
    def test_get_product_conversion_analytics(self):
        start_date_str = "2020-05-28T00-00-01"
        end_date_str = "2020-06-28T23-59-59"
        """visits,products and orders table are empty"""

        result = ConversionAnalytics.get_product_conversion_analytics(data_store=self.rds_data_store,
                                                                      client_id="test_client_id",
                                                                      start_date_str=start_date_str,
                                                                      end_date_str=end_date_str)

        expected_result = {'products': [], 'product_conversion': {'visitor_count': [], 'conversion_count': [],
                                                                  'conversion_percentage': []},
                           'summary': '<strong> SUMMARY </strong> There are NOT enough visits registered on the website',
                           'conclusion': '<strong> CONCLUSION </strong> Wait for the customers to interact with your website'}

        self.assertDictEqual(d1=result, d2=expected_result)

        """visits and orders table are empty but products table has data"""
        self._create_products()
        result = ConversionAnalytics.get_product_conversion_analytics(data_store=self.rds_data_store,
                                                                      client_id="test_client_id",
                                                                      start_date_str=start_date_str,
                                                                      end_date_str=end_date_str)
        expected_result = {'products': ['product_title_1', 'product_title_2', 'product_title_3'],
                           'product_conversion': {'visitor_count': [0, 0, 0], 'conversion_count': [0, 0, 0],
                                                  'conversion_percentage': [0.0, 0.0, 0.0]},
                           'summary': '<strong> SUMMARY </strong> There are NOT enough visits registered on the website',
                           'conclusion': '<strong> CONCLUSION </strong> Wait for the customers to interact with your website'}

        self.assertDictEqual(d1=result, d2=expected_result)

        """visits table is empty but products and orders table has data"""
        self._create_orders()

        result = ConversionAnalytics.get_product_conversion_analytics(data_store=self.rds_data_store,
                                                                      client_id="test_client_id",
                                                                      start_date_str=start_date_str,
                                                                      end_date_str=end_date_str)
        expected_result = {'products': ['product_title_1', 'product_title_2', 'product_title_3'],
                           'product_conversion': {'visitor_count': [0, 0, 0], 'conversion_count': [10, 8, 2],
                                                  'conversion_percentage': [99.99, 99.99, 99.99]},
                           'summary': '<strong> SUMMARY </strong> There are NOT enough visits registered on the website',
                           'conclusion': '<strong> CONCLUSION </strong> Wait for the customers to interact with your website'}

        self.assertDictEqual(d1=result, d2=expected_result)

        """all tables have data"""

        self._create_visit_event()

        result = ConversionAnalytics.get_product_conversion_analytics(data_store=self.rds_data_store,
                                                                      client_id="test_client_id",
                                                                      start_date_str=start_date_str,
                                                                      end_date_str=end_date_str)
        expected_result = {'products': ['product_title_1', 'product_title_2', 'product_title_3'],
                           'product_conversion': {'visitor_count': [5, 5, 5], 'conversion_count': [10, 8, 2],
                                                  'conversion_percentage': [99.99, 99.99, 39.92]},
                           'summary': '<strong> SUMMARY </strong> product_title_3 has minimum conversion of 39.92%',
                           'conclusion': '<strong> CONCLUSION </strong> Experiment with different creatives/copies for product_title_3'}

        self.assertDictEqual(d1=result, d2=expected_result)

    def test_get_landing_page_analytics(self):
        start_date_str = "2019-05-27T00-00-01"
        end_date_str = "2021-07-28T23-59-59"
        """visits,products and orders table are empty"""

        result = ConversionAnalytics.get_landing_page_analytics(data_store=self.rds_data_store,
                                                                client_id="test_client_id",
                                                                start_date_str=start_date_str,
                                                                end_date_str=end_date_str)

        expected_result = {'pages': [], 'landing_conversion': {'visitor_count': [], 'conversion_count': [],
                                                               'conversion_percentage': []},
                           'summary': '<strong> SUMMARY </strong> There are NOT enough visits registered on the website',
                           'conclusion': '<strong> CONCLUSION </strong> Wait for the customers to interact with your website'}

        self.assertDictEqual(d1=result, d2=expected_result)

        """visits and orders table are empty but products table has data"""
        self._create_products()
        result = ConversionAnalytics.get_landing_page_analytics(data_store=self.rds_data_store,
                                                                client_id="test_client_id",
                                                                start_date_str=start_date_str,
                                                                end_date_str=end_date_str)
        expected_result = {'pages': [], 'landing_conversion': {'visitor_count': [], 'conversion_count': [],
                                                               'conversion_percentage': []},
                           'summary': '<strong> SUMMARY </strong> There are NOT enough visits registered on the website',
                           'conclusion': '<strong> CONCLUSION </strong> Wait for the customers to interact with your website'}

        self.assertDictEqual(d1=result, d2=expected_result)

        """visits table is empty but products and orders table has data"""
        self._create_orders()

        result = ConversionAnalytics.get_landing_page_analytics(data_store=self.rds_data_store,
                                                                client_id="test_client_id",
                                                                start_date_str=start_date_str,
                                                                end_date_str=end_date_str)
        expected_result = {'pages': [], 'landing_conversion': {'visitor_count': [], 'conversion_count': [],
                                                               'conversion_percentage': []},
                           'summary': '<strong> SUMMARY </strong> There are NOT enough visits registered on the website',
                           'conclusion': '<strong> CONCLUSION </strong> Wait for the customers to interact with your website'}

        self.assertDictEqual(d1=result, d2=expected_result)

        """all tables have data"""

        self._create_visit_event()

        result = ConversionAnalytics.get_landing_page_analytics(data_store=self.rds_data_store,
                                                                client_id="test_client_id",
                                                                start_date_str=start_date_str,
                                                                end_date_str=end_date_str)
        expected_result = {'pages': ['Collections Page', 'Home Page', 'Product Page'],
                           'landing_conversion': {'visitor_count': [8, 21, 6], 'conversion_count': [2, 6, 2],
                                                  'conversion_percentage': [24.97, 28.56, 33.28]},
                           'summary': '<strong> SUMMARY </strong> Collections Page has minimum conversion of 24.97%',
                           'conclusion': '<strong> CONCLUSION </strong> Experiment with different creatives/copies for Collections Page'}

        self.assertDictEqual(d1=result, d2=expected_result)
