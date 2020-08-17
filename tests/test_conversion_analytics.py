import datetime
import json
from unittest import TestCase
from unittest import mock
from unittest.mock import Mock

import pytz
import testing.postgresql

from config import *
from optimization_platform.src.agents.shop_agent import ShopAgent
from optimization_platform.src.agents.order_agent import OrderAgent
from optimization_platform.src.agents.product_agent import ProductAgent
from optimization_platform.src.agents.visit_agent import VisitAgent
from optimization_platform.src.analytics.conversion.conversion_analytics import ConversionAnalytics
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

        @property
        def headers(self):
            return {}

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
        ShopAgent.add_new_client(data_store=self.rds_data_store, client_id=client_id,
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
        ProductAgent.sync_products(shop_id="test_client_id", data_store=self.rds_data_store)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def _create_orders(self, x):
        self._add_new_client(client_id="test_client_id",
                             full_name="test_full_name",
                             company_name="test_company_name", hashed_password="test_hashed_password",
                             disabled=False, shopify_app_eg_url="test_shopify_app_eg_url",
                             client_timezone="test_client_timezone")
        OrderAgent.sync_orders(shop_id="test_client_id", data_store=self.rds_data_store)

    @mock.patch('datetime.datetime', new=datetime_mock)
    def test_get_shop_funnel_analytics(self):
        start_date_str = "2020-05-28T00-00-01"
        end_date_str = "2020-06-28T23-59-59"
        """visits,products and orders table are empty"""

        result = ConversionAnalytics.get_shop_funnel_analytics(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               start_date_str=start_date_str,
                                                               end_date_str=end_date_str,
                                                               timezone_str="Asia/Kolkata")

        expected_result = {
            'pages': ['Home Page', 'Collection Page', 'Product Page', 'Cart Page', 'Checkout Page', 'Purchase'],
            'shop_funnel': {'visitor_count': [0, 0, 0, 0, 0, 0], 'percentage': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]},
            'summary': "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> enough visits registered on the website",
            'conclusion': "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT </strong></span> for the customers to interact with your website"}

        self.assertDictEqual(d1=result, d2=expected_result)

        """visits and orders table are empty but products table has data"""
        self._create_products()
        result = ConversionAnalytics.get_shop_funnel_analytics(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               start_date_str=start_date_str,
                                                               end_date_str=end_date_str,
                                                               timezone_str="Asia/Kolkata")
        expected_result = {
            'pages': ['Home Page', 'Collection Page', 'Product Page', 'Cart Page', 'Checkout Page', 'Purchase'],
            'shop_funnel': {'visitor_count': [0, 0, 0, 0, 0, 0], 'percentage': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]},
            'summary': "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> enough visits registered on the website",
            'conclusion': "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT </strong></span> for the customers to interact with your website"}

        self.assertDictEqual(d1=result, d2=expected_result)

        """visits table is empty but products and orders table has data"""
        self._create_orders()

        result = ConversionAnalytics.get_shop_funnel_analytics(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               start_date_str=start_date_str,
                                                               end_date_str=end_date_str,
                                                               timezone_str="Asia/Kolkata")
        expected_result = {
            'pages': ['Home Page', 'Collection Page', 'Product Page', 'Cart Page', 'Checkout Page', 'Purchase'],
            'shop_funnel': {'visitor_count': [0, 0, 0, 0, 7, 5], 'percentage': [0.0, 0.0, 0.0, 0.0, 100.0, 71.43]},
            'summary': "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> enough visits registered on the website",
            'conclusion': "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT </strong></span> for the customers to interact with your website"}

        self.assertDictEqual(d1=result, d2=expected_result)

        """all tables have data"""

        self._create_visit_event()

        result = ConversionAnalytics.get_shop_funnel_analytics(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               start_date_str=start_date_str,
                                                               end_date_str=end_date_str,
                                                               timezone_str="Asia/Kolkata")
        expected_result = {
            'pages': ['Home Page', 'Collection Page', 'Product Page', 'Cart Page', 'Checkout Page', 'Purchase'],
            'shop_funnel': {'visitor_count': [30, 20, 15, 10, 7, 5],
                            'percentage': [100.0, 66.67, 50.0, 33.33, 23.33, 16.67]},
            'summary': "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong> Home Page </strong></span> has the highest churn of <span style = 'color: blue; font-size: 16px;'><strong> 33.33% </strong></span>",
            'conclusion': "<strong> CONCLUSION : </strong> Experiment with different creatives/copies for <span style = 'color: blue; font-size: 16px;'><strong> Home Page </strong></span>"}

        self.assertDictEqual(d1=result, d2=expected_result)

        """visits table does not exist"""

        self.rds_data_store.run_custom_sql("drop table visits")

        result = ConversionAnalytics.get_shop_funnel_analytics(data_store=self.rds_data_store,
                                                               client_id="test_client_id",
                                                               start_date_str=start_date_str,
                                                               end_date_str=end_date_str,
                                                               timezone_str="Asia/Kolkata")

        expected_result = {'pages': [], 'shop_funnel': {'visitor_count': [], 'percentage': []},
                           'summary': "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> enough visits registered on the website",
                           'conclusion': "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT </strong></span> for the customers to interact with your website"}

        self.assertDictEqual(d1=result, d2=expected_result)

    @mock.patch('datetime.datetime', new=datetime_mock)
    def test_get_product_conversion_analytics(self):
        start_date_str = "2020-05-28T00-00-01"
        end_date_str = "2020-06-28T23-59-59"
        """visits,products and orders table are empty"""

        result = ConversionAnalytics.get_product_conversion_analytics(data_store=self.rds_data_store,
                                                                      client_id="test_client_id",
                                                                      start_date_str=start_date_str,
                                                                      end_date_str=end_date_str,
                                                                      timezone_str="Asia/Kolkata")

        expected_result = {'products': [], 'product_conversion': {'non_conversion_count': [], 'conversion_count': [],
                                                                  'conversion_percentage': []},
                           'summary': "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> enough visits registered on the website",
                           'conclusion': "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT </strong></span> for the customers to interact with your website"}
        self.assertDictEqual(d1=result, d2=expected_result)

        """visits and orders table are empty but products table has data"""
        self._create_products()
        result = ConversionAnalytics.get_product_conversion_analytics(data_store=self.rds_data_store,
                                                                      client_id="test_client_id",
                                                                      start_date_str=start_date_str,
                                                                      end_date_str=end_date_str,
                                                                      timezone_str="Asia/Kolkata")
        expected_result = {'tags': ['tag1', 'tag2', 'no-tag'], 'results': [
            {'products': ['product_title_1', 'product_title_2', 'product_title_3'],
             'product_conversion': {'non_conversion_count': [0, 0, 0], 'conversion_count': [0, 0, 0],
                                    'conversion_percentage': [0.0, 0.0, 0.0]},
             'summary': "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> enough visits registered on the website",
             'conclusion': "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT </strong></span> for the customers to interact with your website"},
            {'products': ['product_title_1'],
             'product_conversion': {'non_conversion_count': [0], 'conversion_count': [0],
                                    'conversion_percentage': [0.0]},
             'summary': "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> enough visits registered on the website",
             'conclusion': "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT </strong></span> for the customers to interact with your website"},
            {'products': ['product_title_4'],
             'product_conversion': {'non_conversion_count': [0], 'conversion_count': [0],
                                    'conversion_percentage': [0.0]},
             'summary': "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> enough visits registered on the website",
             'conclusion': "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT </strong></span> for the customers to interact with your website"}]}
        self.assertDictEqual(d1=result, d2=expected_result)

        """visits table is empty but products and orders table has data"""
        self._create_orders()

        result = ConversionAnalytics.get_product_conversion_analytics(data_store=self.rds_data_store,
                                                                      client_id="test_client_id",
                                                                      start_date_str=start_date_str,
                                                                      end_date_str=end_date_str,
                                                                      timezone_str="Asia/Kolkata")
        expected_result = {'tags': ['tag1', 'tag2', 'no-tag'], 'results': [
            {'products': ['product_title_1', 'product_title_2', 'product_title_3'],
             'product_conversion': {'non_conversion_count': [0, 0, 0], 'conversion_count': [5, 4, 1],
                                    'conversion_percentage': [100.0, 100.0, 100.0]},
             'summary': "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong> product_title_1 </strong></span> has the least conversion of <span style = 'color: blue; font-size: 16px;'><strong> 100.0% </strong></span>",
             'conclusion': "<strong> CONCLUSION : </strong> Experiment with different creatives/copies for<span style = 'color: blue; font-size: 16px;'><strong> product_title_1 </strong></span>"},
            {'products': ['product_title_1'],
             'product_conversion': {'non_conversion_count': [0], 'conversion_count': [5],
                                    'conversion_percentage': [100.0]},
             'summary': "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong> product_title_1 </strong></span> has the least conversion of <span style = 'color: blue; font-size: 16px;'><strong> 100.0% </strong></span>",
             'conclusion': "<strong> CONCLUSION : </strong> Experiment with different creatives/copies for<span style = 'color: blue; font-size: 16px;'><strong> product_title_1 </strong></span>"},
            {'products': ['product_title_4'],
             'product_conversion': {'non_conversion_count': [0], 'conversion_count': [0],
                                    'conversion_percentage': [0.0]},
             'summary': "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong> product_title_4 </strong></span> has the least conversion of <span style = 'color: blue; font-size: 16px;'><strong> 0.0% </strong></span>",
             'conclusion': "<strong> CONCLUSION : </strong> Experiment with different creatives/copies for<span style = 'color: blue; font-size: 16px;'><strong> product_title_4 </strong></span>"}]}

        self.assertDictEqual(d1=result, d2=expected_result)

        """all tables have data"""

        self._create_visit_event()

        result = ConversionAnalytics.get_product_conversion_analytics(data_store=self.rds_data_store,
                                                                      client_id="test_client_id",
                                                                      start_date_str=start_date_str,
                                                                      end_date_str=end_date_str,
                                                                      timezone_str="Asia/Kolkata")
        expected_result = {'tags': ['tag1', 'tag2', 'no-tag'], 'results': [
            {'products': ['product_title_1', 'product_title_2', 'product_title_3'],
             'product_conversion': {'non_conversion_count': [0, 1, 4], 'conversion_count': [5, 4, 1],
                                    'conversion_percentage': [100.0, 80.0, 20.0]},
             'summary': "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong> product_title_3 </strong></span> has the least conversion of <span style = 'color: blue; font-size: 16px;'><strong> 20.0% </strong></span>",
             'conclusion': "<strong> CONCLUSION : </strong> Experiment with different creatives/copies for<span style = 'color: blue; font-size: 16px;'><strong> product_title_3 </strong></span>"},
            {'products': ['product_title_1'],
             'product_conversion': {'non_conversion_count': [0], 'conversion_count': [5],
                                    'conversion_percentage': [100.0]},
             'summary': "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong> product_title_1 </strong></span> has the least conversion of <span style = 'color: blue; font-size: 16px;'><strong> 100.0% </strong></span>",
             'conclusion': "<strong> CONCLUSION : </strong> Experiment with different creatives/copies for<span style = 'color: blue; font-size: 16px;'><strong> product_title_1 </strong></span>"},
            {'products': ['product_title_4'],
             'product_conversion': {'non_conversion_count': [0], 'conversion_count': [0],
                                    'conversion_percentage': [0.0]},
             'summary': "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong> product_title_4 </strong></span> has the least conversion of <span style = 'color: blue; font-size: 16px;'><strong> 0.0% </strong></span>",
             'conclusion': "<strong> CONCLUSION : </strong> Experiment with different creatives/copies for<span style = 'color: blue; font-size: 16px;'><strong> product_title_4 </strong></span>"}]}
        self.assertDictEqual(d1=result, d2=expected_result)

    def test_get_landing_page_analytics(self):
        start_date_str = "2019-05-27T00-00-01"
        end_date_str = "2021-07-28T23-59-59"
        """visits,products and orders table are empty"""

        result = ConversionAnalytics.get_landing_page_analytics(data_store=self.rds_data_store,
                                                                client_id="test_client_id",
                                                                start_date_str=start_date_str,
                                                                end_date_str=end_date_str, timezone_str="Asia/Kolkata")

        expected_result = {'pages': ['Home Page', 'Collections Page', 'Product Page'],
                           'landing_conversion': {'non_conversion_count': [0, 0, 0], 'conversion_count': [0, 0, 0],
                                                  'conversion_percentage': [0.0, 0.0, 0.0]},
                           'summary': "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> enough visits registered on the website",
                           'conclusion': "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT </strong></span> for the customers to interact with your website"}

        self.assertDictEqual(d1=result, d2=expected_result)

        """visits and orders table are empty but products table has data"""
        self._create_products()
        result = ConversionAnalytics.get_landing_page_analytics(data_store=self.rds_data_store,
                                                                client_id="test_client_id",
                                                                start_date_str=start_date_str,
                                                                end_date_str=end_date_str,
                                                                timezone_str="Asia/Kolkata")
        expected_result = {'pages': ['Home Page', 'Collections Page', 'Product Page'],
                           'landing_conversion': {'non_conversion_count': [0, 0, 0], 'conversion_count': [0, 0, 0],
                                                  'conversion_percentage': [0.0, 0.0, 0.0]},
                           'summary': "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> enough visits registered on the website",
                           'conclusion': "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT </strong></span> for the customers to interact with your website"}

        self.assertDictEqual(d1=result, d2=expected_result)

        """visits table is empty but products and orders table has data"""
        self._create_orders()

        result = ConversionAnalytics.get_landing_page_analytics(data_store=self.rds_data_store,
                                                                client_id="test_client_id",
                                                                start_date_str=start_date_str,
                                                                end_date_str=end_date_str,
                                                                timezone_str="Asia/Kolkata")
        expected_result = {'pages': ['Home Page', 'Collections Page', 'Product Page'],
                           'landing_conversion': {'non_conversion_count': [0, 0, 0], 'conversion_count': [6, 2, 2],
                                                  'conversion_percentage': [100.0, 100.0, 100.0]},
                           'summary': "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong> Home Page </strong></span> has the least conversion of <span style = 'color: blue; font-size: 16px;'><strong> 100.0% </strong></span>",
                           'conclusion': "<strong> CONCLUSION : </strong> Experiment with different creatives/copies for <span style = 'color: blue; font-size: 16px;'><strong> Home Page </strong></span>"}

        self.assertDictEqual(d1=result, d2=expected_result)

        """all tables have data"""

        self._create_visit_event()

        result = ConversionAnalytics.get_landing_page_analytics(data_store=self.rds_data_store,
                                                                client_id="test_client_id",
                                                                start_date_str=start_date_str,
                                                                end_date_str=end_date_str,
                                                                timezone_str="Asia/Kolkata")
        expected_result = {'pages': ['Home Page', 'Collections Page', 'Product Page'],
                           'landing_conversion': {'non_conversion_count': [19, 8, 1], 'conversion_count': [6, 2, 2],
                                                  'conversion_percentage': [24.0, 20.0, 66.67]},
                           'summary': "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong> Collections Page </strong></span> has the least conversion of <span style = 'color: blue; font-size: 16px;'><strong> 20.0% </strong></span>",
                           'conclusion': "<strong> CONCLUSION : </strong> Experiment with different creatives/copies for <span style = 'color: blue; font-size: 16px;'><strong> Collections Page </strong></span>"}

        self.assertDictEqual(d1=result, d2=expected_result)
