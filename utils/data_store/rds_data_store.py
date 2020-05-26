import pandas as pd
import psycopg2


class RDSDataStore(object):
    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = psycopg2.connect(host=self.host,
                                     port=self.port,
                                     dbname=self.dbname,
                                     user=self.user,
                                     password=self.password)

    def _run_sql_to_get_data(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        mobile_records = cursor.fetchall()
        cursor.close()
        return mobile_records

    def _run_sql_to_push_data(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        cursor.close()
        return None

    def run_select_sql(self, query):
        mobile_records = self._run_sql_to_get_data(query=query)
        return mobile_records

    def run_insert_into_sql(self, query):
        return self._run_sql_to_push_data(query=query)

    def run_update_sql(self, query):
        return self._run_sql_to_push_data(query=query)

    def run_custom_sql(self, query):
        mobile_records = self._run_sql_to_get_data(query=query)
        return mobile_records

    def run_create_table_sql(self, query):
        return self._run_sql_to_push_data(query=query)
