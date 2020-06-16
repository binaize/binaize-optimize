import datetime
from unittest import TestCase
from unittest.mock import Mock, patch

import pytz
import testing.postgresql
from fastapi.testclient import TestClient
from optimization_platform.src.agents.event_agent import EventAgent
from optimization_platform.src.agents.variation_agent import VariationAgent
from optimization_platform.src.agents.visit_agent import VisitAgent

from config import AWS_RDS_PORT

pgsql = testing.postgresql.Postgresql(cache_initialized_db=True, port=int(AWS_RDS_PORT))
params = pgsql.dsn()

from optimization_platform.deployment.server import app, logger

logger.disabled = True

client = TestClient(app)
datetime_mock = Mock(wraps=datetime.datetime)
tz = pytz.timezone("Asia/Kolkata")
datetime_now = tz.localize(datetime.datetime(2020, 5, 30, 13, 0, 0, 0))
datetime_mock.now.return_value = datetime_now


class TestServer(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestServer, self).__init__(*args, **kwargs)

    def setUp(self):
        with open("rds_tables.sql", "r") as fp:
            app.rds_data_store.run_create_table_sql(fp.read())

    def tearDown(self):
        app.rds_data_store.run_create_table_sql("drop schema public cascade")
        app.rds_data_store.run_create_table_sql("create schema public")

    def test_home_page(self):
        response = client.get("/")
        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)
        response_json = response.json()
        expected_response_json = {'status': 200,
                                  'message': {'title': 'Binaize API', 'description': 'apis for the binaize service',
                                              'version': '2.5.0'}
                                  }
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    def _sign_up_new_client(self):
        response = client.post(
            "/api/v1/schemas/client/sign_up",
            headers={"accept": "application/json"},
            json={"client_id": "test_client", "company_name": "test_company_name", "full_name": "test_full_name",
                  "disabled": False,
                  "password": "test_password"},
        )
        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)
        response_json = response.json()
        expected_response_json = {"status": "200",
                                  "message": "Sign up for new client with client_id test_client is successful."}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)
        response = client.post(
            "/api/v1/schemas/client/sign_up",
            headers={"accept": "application/json"},
            json={"client_id": "test_client", "company_name": "test_company_name", "full_name": "test_full_name",
                  "disabled": False,
                  "password": "test_password"}
        )
        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)
        response_json = response.json()
        expected_response_json = {"status": "409", "message": "Client_id test_client is already registered."}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    def test_sign_up_new_client(self):
        self._sign_up_new_client()

    def test_login_and_get_access_token(self):
        response = client.post(
            "/api/v1/schemas/client/sign_up",
            headers={"accept": "application/json"},
            json={"client_id": "test_client", "company_name": "test_company_name", "full_name": "test_full_name",
                  "disabled": False,
                  "password": "test_password"},
        )

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=fake_client&password=test_password&scope=&client_id=&client_secret="
        )
        status_code = response.status_code
        expected_status_code = 401
        self.assertEqual(first=status_code, second=expected_status_code)

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=fake_password&scope=&client_id=&client_secret="
        )
        status_code = response.status_code
        expected_status_code = 401
        self.assertEqual(first=status_code, second=expected_status_code)

    def test_get_client_details(self):
        """ one active and one disabled client signed up"""
        client.post(
            "/api/v1/schemas/client/sign_up",
            headers={"accept": "application/json"},
            json={"client_id": "test_client", "company_name": "test_company_name", "full_name": "test_full_name",
                  "disabled": False,
                  "password": "test_password"},
        )
        client.post(
            "/api/v1/schemas/client/sign_up",
            headers={"accept": "application/json"},
            json={"client_id": "disabled_client", "company_name": "test_company_name", "full_name": "test_full_name",
                  "disabled": True,
                  "password": "disabled_password"},
        )

        """get access token for the disabled client"""
        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=disabled_client&password=disabled_password&scope=&client_id=&client_secret="
        )

        access_token = response.json()["access_token"]

        response = client.get(
            "/api/v1/schemas/client/details",
            headers={
                "Authorization": "Bearer " + access_token},
        )

        status_code = response.status_code
        expected_status_code = 400
        self.assertEqual(first=status_code, second=expected_status_code)
        response_json = response.json()
        expected_response_json = {"detail": "Inactive user"}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

        """get access token for the active client"""

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        access_token = response.json()["access_token"]

        response = client.get(
            "/api/v1/schemas/client/details",
            headers={"Authorization": "Bearer " + access_token}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        """send an expired access token for active user"""

        response = client.post(
            "/api/v1/schemas/client/details",
            headers={
                "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0X2NsaWVudCIsImV4cCI6MTU5MDc2NTI0OX0.4dU20UbYFHM5H_qdccx7ELVoEwijqtZrsCtkKvMJOTE"}

        )

        status_code = response.status_code
        expected_status_code = 405
        self.assertEqual(first=status_code, second=expected_status_code)
        response_json = response.json()
        expected_response_json = {"detail": "Method Not Allowed"}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

        app.rds_data_store.run_create_table_sql("truncate table clients")

        response = client.get(
            "/api/v1/schemas/client/details",
            headers={"Authorization": "Bearer " + access_token}
        )

        status_code = response.status_code
        expected_status_code = 401
        self.assertEqual(first=status_code, second=expected_status_code)
        response_json = response.json()
        expected_response_json = {"detail": "Could not validate credentials"}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    def test_add_shopify_credentials_to_logged_in_client(self):
        self._sign_up_new_client()

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )

        access_token = response.json()["access_token"]

        response = client.post(
            "/api/v1/schemas/client/add-credential",
            headers={"Authorization": "Bearer " + access_token},
            json={"shopify_app_api_key": "test_shopify_app_api_key",
                  "shopify_app_password": "test_shopify_app_password",
                  "shopify_app_eg_url": "test_shopify_app_eg_url",
                  "shopify_app_shared_secret": "test_shopify_app_shared_secret"}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response_json = response.json()
        expected_response_json = {"status": "200",
                                  "message": "Addition of Shopify Credentials for client_id test_client is successful."}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    @patch('datetime.datetime', new=datetime_mock)
    def test_add_experiment(self):
        self._sign_up_new_client()

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )

        access_token = response.json()["access_token"]

        response = client.post(
            "/api/v1/schemas/experiment/create",
            headers={"Authorization": "Bearer " + access_token},
            json={"experiment_name": "test_experiment_name",
                  "page_type": "test_page_type",
                  "experiment_type": "test_experiment_type",
                  "status": "test_status"}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response_json = response.json()
        response_json.pop("experiment_id", None)
        expected_response_json = {'experiment_name': 'test_experiment_name', 'page_type': 'test_page_type',
                                  'experiment_type': 'test_experiment_type', 'status': 'test_status',
                                  'client_id': 'test_client',
                                  'creation_time': '2020-05-30T07:30:00+00:00',
                                  'last_updation_time': '2020-05-30T07:30:00+00:00'}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    @patch('datetime.datetime', new=datetime_mock)
    def test_list_experiments(self):
        self._sign_up_new_client()

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        access_token = response.json()["access_token"]

        client.post(
            "/api/v1/schemas/experiment/create",
            headers={"Authorization": "Bearer " + access_token},
            json={"experiment_name": "test_experiment_name_1",
                  "page_type": "test_page_type_1",
                  "experiment_type": "test_experiment_type_1",
                  "status": "test_status_1"}
        )
        client.post(
            "/api/v1/schemas/experiment/create",
            headers={"Authorization": "Bearer " + access_token},
            json={"experiment_name": "test_experiment_name_2",
                  "page_type": "test_page_type_2",
                  "experiment_type": "test_experiment_type_2",
                  "status": "test_status_2"}
        )
        client.post(
            "/api/v1/schemas/experiment/create",
            headers={"Authorization": "Bearer " + access_token},
            json={"experiment_name": "test_experiment_name_3",
                  "page_type": "test_page_type_3",
                  "experiment_type": "test_experiment_type_3",
                  "status": "test_status_3"}
        )

        response = client.get(
            "/api/v1/schemas/experiment/list",
            headers={"Authorization": "Bearer " + access_token}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response_json = response.json()
        for x in response_json:
            x.pop("experiment_id", None)

        expected_response_json = [{'experiment_name': 'test_experiment_name_1', 'page_type': 'test_page_type_1',
                                   'experiment_type': 'test_experiment_type_1', 'status': 'test_status_1',
                                   'client_id': 'test_client', 'creation_time': '30-May-2020',
                                   'last_updation_time': '30-May-2020'},
                                  {'experiment_name': 'test_experiment_name_2', 'page_type': 'test_page_type_2',
                                   'experiment_type': 'test_experiment_type_2', 'status': 'test_status_2',
                                   'client_id': 'test_client', 'creation_time': '30-May-2020',
                                   'last_updation_time': '30-May-2020'},
                                  {'experiment_name': 'test_experiment_name_3', 'page_type': 'test_page_type_3',
                                   'experiment_type': 'test_experiment_type_3', 'status': 'test_status_3',
                                   'client_id': 'test_client', 'creation_time': '30-May-2020',
                                   'last_updation_time': '30-May-2020'}]
        self.assertCountEqual(first=response_json, second=expected_response_json)

    def test_add_variation(self):
        self._sign_up_new_client()
        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.post(
            "/api/v1/schemas/experiment/create",
            headers={"Authorization": "Bearer " + access_token},
            json={"experiment_name": "",
                  "page_type": "test_page_type_1",
                  "experiment_type": "test_experiment_type_1",
                  "status": "test_status_1"}
        )

        experiment_id = response.json()["experiment_id"]

        response = client.post(
            "/api/v1/schemas/variation/create",
            headers={"Authorization": "Bearer " + access_token},
            json={
                "experiment_id": experiment_id,
                "variation_name": "test_variation",
                "traffic_percentage": 0
            }
        )

        response_json = response.json()
        response_json.pop("variation_id", None)
        expected_response_json = {"experiment_id": experiment_id,
                                  "client_id": "test_client"}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    @patch('datetime.datetime', new=datetime_mock)
    def test_get_variation_id_to_redirect(self):
        self._sign_up_new_client()
        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.post(
            "/api/v1/schemas/experiment/create",
            headers={"Authorization": "Bearer " + access_token},
            json={"experiment_name": "",
                  "page_type": "test_page_type_1",
                  "experiment_type": "test_experiment_type_1",
                  "status": "test_status_1"}
        )

        experiment_id = response.json()["experiment_id"]

        client.post(
            "/api/v1/schemas/variation/create",
            headers={"Authorization": "Bearer " + access_token},
            json={
                "experiment_id": experiment_id,
                "variation_name": "test_variation_1",
                "traffic_percentage": 0
            }
        )

        client.post(
            "/api/v1/schemas/variation/create",
            headers={"Authorization": "Bearer " + access_token},
            json={
                "experiment_id": experiment_id,
                "variation_name": "test_variation_2",
                "traffic_percentage": 0
            }
        )

        client.post(
            "/api/v1/schemas/variation/create",
            headers={"Authorization": "Bearer " + access_token},
            json={
                "experiment_id": experiment_id,
                "variation_name": "test_variation_3",
                "traffic_percentage": 0
            }
        )

        response = client.get(
            "/api/v1/schemas/variation/redirection",
            headers={"Authorization": "Bearer " + access_token},
            params={"client_id": "test_client",
                    "experiment_id": experiment_id,
                    "session_id": "test_session_id"}
        )
        response_json = response.json()
        variation_id = response_json["variation_id"]
        response_json.pop("variation_id", None)
        expected_response_json = {"client_id": "test_client",
                                  "experiment_id": experiment_id}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

        result = app.rds_data_store.run_select_sql("select * from events")
        result = list(result[0])
        result[-1] = result[-1].isoformat()
        expected_result = [variation_id, 'test_client', experiment_id,
                           'test_session_id', 'served', '2020-05-30T13:00:00+05:30']
        self.assertCountEqual(first=result, second=expected_result)

    @patch('datetime.datetime', new=datetime_mock)
    def test_register_event(self):
        self._sign_up_new_client()

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.post(
            "/api/v1/schemas/event/register",
            headers={"Authorization": "Bearer " + access_token},
            json={
                "client_id": "test_client",
                "experiment_id": "test_experiment_id",
                "variation_id": "test_variation_id",
                "session_id": "test_session_id",
                "event_name": "test_event_name"
            }
        )
        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)
        response_json = response.json()
        expected_response_json = {'status': '200',
                                  'message': 'Event registration for client_id test_client is successful.'}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    @patch('datetime.datetime', new=datetime_mock)
    def test_register_visit(self):
        self._sign_up_new_client()

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.post(
            "/api/v1/schemas/visit/register",
            headers={"Authorization": "Bearer " + access_token},
            json={
                "client_id": "test_client",
                "session_id": "test_session_id",
                "event_name": "test_event_name",
                "url": "test_url"
            }
        )
        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)
        response_json = response.json()
        expected_response_json = {'status': '200',
                                  'message': 'Visit registration for client_id test_client and event name test_event_name is successful.'}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

        result = app.rds_data_store.run_select_sql("select * from visits")
        result = list(result[0])
        result[-2] = result[-2].isoformat()
        expected_result = ['test_client', 'test_session_id',
                           'test_event_name', '2020-05-30T13:00:00+05:30', 'test_url']
        self.assertCountEqual(first=result, second=expected_result)

    @patch('datetime.datetime', new=datetime_mock)
    def test_register_cookie(self):
        self._sign_up_new_client()

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.post(
            "/api/v1/schemas/cookie/register",
            headers={"Authorization": "Bearer " + access_token},
            json={
                "client_id": "test_client",
                "session_id": "test_session_id",
                "shopify_x": "test_shopify_x",
                "cart_token": "test_cart_token"
            }
        )
        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)
        response_json = response.json()
        expected_response_json = {'status': '200',
                                  'message': 'Cookie registration for client_id test_client is successful.'}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

        result = app.rds_data_store.run_select_sql("select * from cookie")
        result = list(result[0])
        result[-1] = result[-1].isoformat()
        expected_result = ['test_client', 'test_session_id', 'test_shopify_x', 'test_cart_token',
                           '2020-05-30T13:00:00+05:30']
        self.assertCountEqual(first=result, second=expected_result)

    def _create_event(self, variation_1, variation_2):
        timestamp = 1590673060
        variation_id_1 = variation_1["variation_id"]
        variation_id_2 = variation_2["variation_id"]
        EventAgent.register_event_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_1", variation_id=variation_id_1,
                                             event_name="served",
                                             creation_time=timestamp)
        EventAgent.register_event_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_1", variation_id=variation_id_1,
                                             event_name="clicked",
                                             creation_time=timestamp + 10)
        EventAgent.register_event_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_2", variation_id=variation_id_2,
                                             event_name="served",
                                             creation_time=timestamp + 20)
        EventAgent.register_event_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_3", variation_id=variation_id_2,
                                             event_name="served",
                                             creation_time=timestamp + 30)
        EventAgent.register_event_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_3", variation_id=variation_id_2,
                                             event_name="clicked",
                                             creation_time=timestamp + 40)
        EventAgent.register_event_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_3", variation_id=variation_id_2,
                                             event_name="served",
                                             creation_time=timestamp + 50)
        EventAgent.register_event_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             experiment_id="test_experiment_id",
                                             session_id="test_session_id_3", variation_id=variation_id_2,
                                             event_name="clicked",
                                             creation_time=timestamp + 60)

    def _create_variation(self):
        variation_1 = VariationAgent.create_variation_for_client_id_and_experiment_id(data_store=app.rds_data_store,
                                                                                      client_id="test_client",
                                                                                      experiment_id="test_experiment_id",
                                                                                      variation_name="test_variation_name_1",
                                                                                      traffic_percentage=50)
        variation_2 = VariationAgent.create_variation_for_client_id_and_experiment_id(data_store=app.rds_data_store,
                                                                                      client_id="test_client",
                                                                                      experiment_id="test_experiment_id",
                                                                                      variation_name="test_variation_name_2",
                                                                                      traffic_percentage=50)
        return variation_1, variation_2

    @patch('datetime.datetime', new=datetime_mock)
    def test_get_session_count_for_dashboard(self):
        self._sign_up_new_client()
        variation_1, variation_2 = self._create_variation()
        self._create_event(variation_1, variation_2)

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.get(
            "/api/v1/schemas/report/session-count",
            headers={
                "Authorization": "Bearer " + access_token},
            params={"client_id": "test_client",
                    "experiment_id": "test_experiment_id"}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response_json = response.json()
        expected_response_json = {'date': ['May 28'],
                                  'session_count': {'test_variation_name_1': [1], 'test_variation_name_2': [3]}}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    @patch('datetime.datetime', new=datetime_mock)
    def test_get_visitor_count_for_dashboard(self):
        self._sign_up_new_client()
        variation_1, variation_2 = self._create_variation()
        self._create_event(variation_1, variation_2)

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.get(
            "/api/v1/schemas/report/visitor-count",
            headers={
                "Authorization": "Bearer " + access_token},
            params={"client_id": "test_client",
                    "experiment_id": "test_experiment_id"}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response_json = response.json()
        expected_response_json = {'date': ['May 28'],
                                  'visitor_count': {'test_variation_name_1': [1], 'test_variation_name_2': [2]}}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    @patch('datetime.datetime', new=datetime_mock)
    def test_get_conversion_rate_for_dashboard(self):
        self._sign_up_new_client()
        variation_1, variation_2 = self._create_variation()
        self._create_event(variation_1, variation_2)

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.get(
            "/api/v1/schemas/report/conversion-rate",
            headers={
                "Authorization": "Bearer " + access_token},
            params={"client_id": "test_client",
                    "experiment_id": "test_experiment_id"}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response_json = response.json()
        expected_response_json = {'date': ['May 28'],
                                  'conversion': {'test_variation_name_1': [1.0], 'test_variation_name_2': [0.5]}}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    @patch('datetime.datetime', new=datetime_mock)
    def test_get_conversion_table_for_dashboard(self):
        self._sign_up_new_client()
        variation_1, variation_2 = self._create_variation()
        self._create_event(variation_1, variation_2)

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.get(
            "/api/v1/schemas/report/conversion-table",
            headers={
                "Authorization": "Bearer " + access_token},
            params={"client_id": "test_client",
                    "experiment_id": "test_experiment_id"}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response_json = response.json()
        expected_response_json = [
            {'variation_name': 'test_variation_name_1', 'variation_id': variation_1["variation_id"],
             'num_session': 1, 'num_visitor': 1, 'visitor_converted': 1, 'conversion': 1.0},
            {'variation_name': 'test_variation_name_2', 'variation_id': variation_2["variation_id"],
             'num_session': 3, 'num_visitor': 2, 'visitor_converted': 1, 'conversion': 0.5}]
        self.assertCountEqual(first=response_json, second=expected_response_json)

    def test_get_experiment_summary(self):
        self._sign_up_new_client()

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.get(
            "/api/v1/schemas/report/experiment-summary",
            headers={
                "Authorization": "Bearer " + access_token},
            params={"experiment_id": "test_experiment_id"}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response_json = response.json()
        expected_response_json = {'status': 'Variation Blue is winning. It is 6% better than the others.',
                                  'conclusion': 'There is NOT enough evidence to conclude the experiment (It is NOT yet statistically significant).',
                                  'recommendation': 'Recommendation: CONTINUE the Experiment.'}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    def _create_visit_event(self):
        timestamp = 1590673060
        VisitAgent.register_visit_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             session_id="test_session_id_1", event_name="home", creation_time=timestamp,
                                             url="url_1")
        VisitAgent.register_visit_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             session_id="test_session_id_2", event_name="collection",
                                             creation_time=timestamp + 10,
                                             url="url_2")
        VisitAgent.register_visit_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             session_id="test_session_id_2", event_name="product",
                                             creation_time=timestamp + 20,
                                             url="url_3")
        VisitAgent.register_visit_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             session_id="test_session_id_4", event_name="cart",
                                             creation_time=timestamp + 30,
                                             url="url_4")
        VisitAgent.register_visit_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             session_id="test_session_id_5", event_name="checkout",
                                             creation_time=timestamp + 40,
                                             url="url_5")
        VisitAgent.register_visit_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             session_id="test_session_id_6", event_name="checkout",
                                             creation_time=timestamp + 50,
                                             url="url_6")
        VisitAgent.register_visit_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             session_id="test_session_id_7", event_name="home",
                                             creation_time=timestamp + 60,
                                             url="url_7")
        VisitAgent.register_visit_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             session_id="test_session_id_8", event_name="home",
                                             creation_time=timestamp + 70,
                                             url="url_8")
        VisitAgent.register_visit_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             session_id="test_session_id_9", event_name="collection",
                                             creation_time=timestamp + 80,
                                             url="url_9")
        VisitAgent.register_visit_for_client(data_store=app.rds_data_store, client_id="test_client",
                                             session_id="test_session_id_10", event_name="prooduct",
                                             creation_time=timestamp + 90,
                                             url="url_10")

    @patch('datetime.datetime', new=datetime_mock)
    def test_get_shop_funnel_analytics_for_dashboard(self):
        self._sign_up_new_client()
        self._create_visit_event()

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.get(
            "/api/v1/schemas/report/shop-funnel",
            headers={
                "Authorization": "Bearer " + access_token},
            params={"client_id": "test_client"}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response_json = response.json()
        expected_response_json = {
            'pages': ['Home Page', 'Collection Page', 'Product Page', 'Cart Page', 'Checkout Page', 'Purchase'],
            'shop_funnel': {'count': [3, 2, 1, 1, 2, 0], 'percentage': [75.0, 50.0, 25.0, 25.0, 50.0, 0.0]}}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    @patch('datetime.datetime', new=datetime_mock)
    def test_get_product_conversion_analytics_for_dashboard(self):
        self._sign_up_new_client()
        self._create_visit_event()

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.get(
            "/api/v1/schemas/report/product-conversion",
            headers={
                "Authorization": "Bearer " + access_token},
            params={"client_id": "test_client"}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response_json = response.json()
        expected_response_json = {
            'products': ['Tissot T Race', 'Tissot T Classic', 'Tissot T Sport', 'Tissot 1853', 'Ordinary Watch',
                         'Titan Classic Watch', 'IWC Watch'],
            'product_conversion': {'visitor_count': [1156, 900, 600, 1456, 800, 500, 760],
                                   'convertion_count': [20, 12, 37, 29, 9, 13, 11],
                                   'convertion_percentage': [1.78, 1.33, 6.12, 1.99, 1.12, 2.41, 1.44]}}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    @patch('datetime.datetime', new=datetime_mock)
    def test_get_landing_page_analytics_for_dashboard(self):
        self._sign_up_new_client()
        self._create_visit_event()

        response = client.post(
            "/api/v1/schemas/client/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        access_token = response.json()["access_token"]

        response = client.get(
            "/api/v1/schemas/report/landing-page",
            headers={
                "Authorization": "Bearer " + access_token},
            params={"client_id": "test_client"}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response_json = response.json()
        expected_response_json = {'pages': ['Home Page', 'Product Page', 'Blog Page'],
                                  'landing_conversion': {'convertion_percentage': [4.32, 5.34, 2.28]}}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)