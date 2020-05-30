from unittest import TestCase

import testing.postgresql
from fastapi.testclient import TestClient

pgsql = testing.postgresql.Postgresql(cache_initialized_db=True, port=55895)
params = pgsql.dsn()
from optimization_platform.deployment.server import app, logger

logger.disabled = True

client = TestClient(app)

app.rds_data_store.run_create_table_sql(open("rds_tables.sql", "r").read())


class TestServer(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestServer, self).__init__(*args, **kwargs)

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

    def test_sign_up_new_client(self):
        app.rds_data_store.run_create_table_sql("truncate table clients")
        response = client.post(
            "/sign_up",
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
            "/sign_up",
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

    def test_login_and_get_access_token(self):
        app.rds_data_store.run_create_table_sql("truncate table clients")
        response = client.post(
            "/sign_up",
            headers={"accept": "application/json"},
            json={"client_id": "test_client", "company_name": "test_company_name", "full_name": "test_full_name",
                  "disabled": False,
                  "password": "test_password"},
        )

        response = client.post(
            "/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )
        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        response = client.post(
            "/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=fake_client&password=test_password&scope=&client_id=&client_secret="
        )
        status_code = response.status_code
        expected_status_code = 401
        self.assertEqual(first=status_code, second=expected_status_code)

        response = client.post(
            "/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=fake_password&scope=&client_id=&client_secret="
        )
        status_code = response.status_code
        expected_status_code = 401
        self.assertEqual(first=status_code, second=expected_status_code)

    def test_get_client_details(self):
        app.rds_data_store.run_create_table_sql("truncate table clients")

        """ one active and one disabled client signed up"""
        client.post(
            "/sign_up",
            headers={"accept": "application/json"},
            json={"client_id": "test_client", "company_name": "test_company_name", "full_name": "test_full_name",
                  "disabled": False,
                  "password": "test_password"},
        )
        client.post(
            "/sign_up",
            headers={"accept": "application/json"},
            json={"client_id": "disabled_client", "company_name": "test_company_name", "full_name": "test_full_name",
                  "disabled": True,
                  "password": "disabled_password"},
        )

        """get access token for the disabled client"""
        response = client.post(
            "/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=disabled_client&password=disabled_password&scope=&client_id=&client_secret="
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        access_token = response.json()["access_token"]

        response = client.get(
            "/get_client_details",
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
            "/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        access_token = response.json()["access_token"]

        response = client.get(
            "/get_client_details",
            headers={"Authorization": "Bearer " + access_token}
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        """send an expired access token for active user"""

        response = client.post(
            "/get_client_details",
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
            "/get_client_details",
            headers={"Authorization": "Bearer " + access_token}
        )

        status_code = response.status_code
        expected_status_code = 401
        self.assertEqual(first=status_code, second=expected_status_code)
        response_json = response.json()
        expected_response_json = {"detail": "Could not validate credentials"}
        self.assertDictEqual(d1=response_json, d2=expected_response_json)

    def test_add_shopify_credentials_to_logged_in_client(self):
        self.test_sign_up_new_client()

        response = client.post(
            "/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            json="grant_type=password&username=test_client&password=test_password&scope=&client_id=&client_secret="
        )

        status_code = response.status_code
        expected_status_code = 200
        self.assertEqual(first=status_code, second=expected_status_code)

        access_token = response.json()["access_token"]

        response = client.post(
            "/add_shopify_credentials",
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
