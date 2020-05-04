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

    def read_record_from_data_store(self, **args):
        """
        Read table into PANDAS dataframe

        TODO complete docstring

        """

        table = args["table"]
        columns = args["columns"]
        column = ",".join(columns)
        where = args["where"]

        sql = """ SELECT {column} from {table} where {where}"""
        query = sql.format(column=column, table=table, where=where)

        cursor = self.conn.cursor()
        cursor.execute(query)

        self.conn.commit()  # <- We MUST commit to reflect the inserted data
        mobile_records = cursor.fetchall()
        cursor.close()
        df = None
        if len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = columns
        return df

    def insert_record_to_data_store(self, **args):
        def lst2pgarr(alist):
            return '{' + ','.join(alist) + '}'

        table = args["table"]
        columns_value_dict = args["columns_value_dict"]

        columns = list(columns_value_dict.keys())
        column = ",".join(columns)

        values = [
            columns_value_dict[key] if type(columns_value_dict[key]) != list else lst2pgarr(columns_value_dict[key]) for
            key in columns]
        value = str(tuple(values))

        sql = """INSERT INTO {table} ({column}) \
                       VALUES {value}"""

        query = sql.format(table=table, column=column, value=value)

        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        cursor.close()
        return

    def update_record_in_data_store(self, **args):
        table = args["table"]
        columns_value_dict = args["columns_value_dict"]
        where = args["where"]

        set_string_list = list()
        for column, value in columns_value_dict.items():
            if type(value) == str:
                temp = "{column}='{value}'".format(column=column, value=value)
            else:
                temp = "{column}={value}".format(column=column, value=value)
            set_string_list.append(temp)
        set_string = ",".join(set_string_list)

        sql = """ UPDATE {table} SET {set_string} where {where}"""
        query = sql.format(set_string=set_string, table=table, where=where)
        # query = " UPDATE clients SET shopify_app_api_key=shopify_app_api_key,shopify_app_password=shopify_app_password,shopify_app_eg_url=shopify_app_eg_url,shopify_app_shared_secret=shopify_app_shared_secret where client_id=string"
        # print(query)

        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        cursor.close()
        return
