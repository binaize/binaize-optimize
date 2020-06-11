from unittest import TestCase

import testing.postgresql

from config import *
from optimization_platform.src.agents.experiment_agent import ExperimentAgent
from utils.data_store.rds_data_store import RDSDataStore

pgsql = testing.postgresql.Postgresql(cache_initialized_db=True, port=int(AWS_RDS_PORT))
rds_data_store = RDSDataStore(host=AWS_RDS_HOST,
                              port=AWS_RDS_PORT,
                              dbname=AWS_RDS_DBNAME,
                              user=AWS_RDS_USER,
                              password=AWS_RDS_PASSWORD)


class TestExperimentAgent(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestExperimentAgent, self).__init__(*args, **kwargs)

    def setUp(self):
        self.rds_data_store = rds_data_store
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def tearDown(self):
        with open("rds_tables.sql", "r") as fp:
            self.rds_data_store.run_create_table_sql(fp.read())

    def test_create_experiment_for_client_id(self):
        result = ExperimentAgent.create_experiment_for_client_id(data_store=self.rds_data_store,
                                                                 client_id="test_client_id",
                                                                 experiment_name="test_experiment_name",
                                                                 page_type="test_page_type",
                                                                 experiment_type="test_experiment_type", status=False,
                                                                 creation_time=1590570923,
                                                                 last_updation_time=1590570923)
        expected_result = {'client_id': 'test_client_id', 'experiment_name': 'test_experiment_name',
                           'page_type': 'test_page_type', 'experiment_type': 'test_experiment_type', 'status': False,
                           'creation_time': '2020-05-27T09:15:23+00:00',
                           'last_updation_time': '2020-05-27T09:15:23+00:00',
                           'experiment_id': result['experiment_id']}

        self.assertDictEqual(d1=result, d2=expected_result)

    def test_get_experiments_for_client_id(self):
        ExperimentAgent.create_experiment_for_client_id(data_store=self.rds_data_store,
                                                        client_id="test_client_id",
                                                        experiment_name="test_experiment_name_1",
                                                        page_type="test_page_type",
                                                        experiment_type="test_experiment_type", status=False,
                                                        creation_time=1590570923,
                                                        last_updation_time=1590570923)
        ExperimentAgent.create_experiment_for_client_id(data_store=self.rds_data_store,
                                                        client_id="test_client_id",
                                                        experiment_name="test_experiment_name_2",
                                                        page_type="test_page_type",
                                                        experiment_type="test_experiment_type", status=False,
                                                        creation_time=1590570923,
                                                        last_updation_time=1590570923)
        result = ExperimentAgent.get_experiments_for_client_id(data_store=self.rds_data_store,
                                                               client_id="test_client_id")

        expected_result = [{'client_id': 'test_client_id',
                            'experiment_name': 'test_experiment_name_1', 'status': 'false',
                            'page_type': 'test_page_type', 'experiment_type': 'test_experiment_type',
                            'creation_time': '27-May-2020', 'last_updation_time': '27-May-2020'},
                           {'client_id': 'test_client_id',
                            'experiment_name': 'test_experiment_name_2', 'status': 'false',
                            'page_type': 'test_page_type', 'experiment_type': 'test_experiment_type',
                            'creation_time': '27-May-2020', 'last_updation_time': '27-May-2020'}]

        for i in range(len(result)):
            result[i].pop('experiment_id', None)

        self.assertCountEqual(first=result, second=expected_result)
