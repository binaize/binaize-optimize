from unittest import TestCase

import testing.postgresql

from optimization_platform.src.agents.experiment_agent import ExperimentAgent
from utils.data_store.rds_data_store import RDSDataStore


class TestExperimentAgent(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestExperimentAgent, self).__init__(*args, **kwargs)
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

    def test_create_experiment_for_client_id(self):
        result = ExperimentAgent.create_experiment_for_client_id(data_store=self.rds_data_store,
                                                                 client_id="test_client_id",
                                                                 experiment_name="test_experiment_name",
                                                                 page_type="test_page_type",
                                                                 experiment_type="test_experiment_type", status=False,
                                                                 created_on_timestamp=1590570923,
                                                                 last_updated_on_timestamp=1590570923)
        expected_result = {'client_id': 'test_client_id', 'experiment_name': 'test_experiment_name',
                           'page_type': 'test_page_type', 'experiment_type': 'test_experiment_type', 'status': False,
                           'creation_time': '2020-05-27 09:15:23', 'last_updation_time': '2020-05-27 09:15:23',
                           'experiment_id': result['experiment_id']}

        self.assertDictEqual(d1=result, d2=expected_result)

    def test_get_experiments_for_client_id(self):
        ExperimentAgent.create_experiment_for_client_id(data_store=self.rds_data_store,
                                                        client_id="test_client_id",
                                                        experiment_name="test_experiment_name_1",
                                                        page_type="test_page_type",
                                                        experiment_type="test_experiment_type", status=False,
                                                        created_on_timestamp=1590570923,
                                                        last_updated_on_timestamp=1590570923)
        ExperimentAgent.create_experiment_for_client_id(data_store=self.rds_data_store,
                                                        client_id="test_client_id",
                                                        experiment_name="test_experiment_name_2",
                                                        page_type="test_page_type",
                                                        experiment_type="test_experiment_type", status=False,
                                                        created_on_timestamp=1590570923,
                                                        last_updated_on_timestamp=1590570923)
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
            expected_result[i]['experiment_id'] = result[i]['experiment_id']

        self.assertEqual(first=result, second=expected_result)
