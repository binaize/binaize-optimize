from unittest import TestCase

import testing.postgresql

from utils.data_store.rds_data_store import RDSDataStore


class TestRDSDataStore(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestRDSDataStore, self).__init__(*args, **kwargs)
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

    def test_run_create_table_sql(self):
        status = self.rds_data_store.run_create_table_sql("CREATE TABLE hello(id int, value varchar(256))")
        self.assertEqual(status, None)

    def test_run_select_sql(self):
        self.rds_data_store.run_create_table_sql("CREATE TABLE hello(id int, value varchar(256))")
        self.rds_data_store.run_insert_into_sql("INSERT INTO hello values(1, 'hello'), (2, 'ciao')")
        mobile_records = self.rds_data_store.run_select_sql('SELECT * FROM hello ORDER BY id')
        self.assertListEqual(mobile_records, [(1, 'hello'), (2, 'ciao')])

    def test_run_insert_into_sql(self):
        self.rds_data_store.run_create_table_sql("CREATE TABLE hello(id int, value varchar(256))")
        status = self.rds_data_store.run_insert_into_sql("INSERT INTO hello values(1, 'hello'), (2, 'ciao')")
        self.assertEqual(status, None)

    def test_run_update_sql(self):
        self.rds_data_store.run_create_table_sql("CREATE TABLE hello(id int, value varchar(256))")
        self.rds_data_store.run_insert_into_sql("INSERT INTO hello values(1, 'hello'), (2, 'ciao')")
        status = self.rds_data_store.run_update_sql("UPDATE hello SET value='yoo' where id=2")
        self.assertEqual(status, None)
        mobile_records = self.rds_data_store.run_select_sql('SELECT * FROM hello ORDER BY id')
        self.assertListEqual(mobile_records, [(1, 'hello'), (2, 'yoo')])

    def test_run_custom_sql(self):
        self.rds_data_store.run_create_table_sql("CREATE TABLE hello(id int, value varchar(256))")
        self.rds_data_store.run_create_table_sql("CREATE TABLE hi(id int, value varchar(256))")
        self.rds_data_store.run_insert_into_sql("INSERT INTO hello values(1, 'hello'), (2, 'ciao')")
        self.rds_data_store.run_insert_into_sql("INSERT INTO hi values(1, 'bye'), (2, 'jao')")
        mobile_records = self.rds_data_store.run_custom_sql("select * from hello union select * from hi")
        self.assertListEqual(mobile_records, [(2, 'ciao'), (1, 'bye'), (2, 'jao'), (1, 'hello')])
