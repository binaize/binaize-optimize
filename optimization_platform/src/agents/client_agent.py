import pandas as pd

from config import *


class ClientAgent(object):

    @classmethod
    def add_new_client(cls, data_store, client_id, full_name, company_name, hashed_password, disabled):
        table = TABLE_CLIENTS
        columns_value_dict = {"client_id": client_id, "full_name": full_name, "company_name": company_name,
                              "hashed_password": hashed_password, "disabled": disabled}

        columns = list(columns_value_dict.keys())
        column = ",".join(columns)
        values = [columns_value_dict[key] for key in columns]
        value = str(tuple(values))

        sql = """INSERT INTO {table} ({column}) VALUES {value}"""
        query = sql.format(table=table, column=column, value=value)
        data_store.run_insert_into_sql(query=query)

    @classmethod
    def get_client_details_for_client_id(cls, data_store, client_id):
        table = TABLE_CLIENTS
        columns = ["client_id", "full_name", "company_name", "hashed_password", "disabled",
                   "shopify_app_api_key", "shopify_app_password", "shopify_app_eg_url", "shopify_app_shared_secret"]
        where = "client_id='{client_id}'".format(client_id=client_id)

        column = ",".join(columns)
        sql = """ SELECT {column} from {table} where {where}"""
        query = sql.format(column=column, table=table, where=where)
        mobile_records = data_store.run_select_sql(query=query)
        df = None
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = columns
        client_details = None
        if df is not None:
            client_details = df[columns].to_dict(orient="records")[0]
        return client_details

    @classmethod
    def add_shopify_credentials_to_existing_client(cls, data_store, client_id, shopify_app_api_key,
                                                   shopify_app_password,
                                                   shopify_app_eg_url, shopify_app_shared_secret):
        table = TABLE_CLIENTS

        columns_value_dict = {"shopify_app_api_key": shopify_app_api_key,
                              "shopify_app_password": shopify_app_password,
                              "shopify_app_eg_url": shopify_app_eg_url,
                              "shopify_app_shared_secret": shopify_app_shared_secret}

        where = "client_id='{client_id}'".format(client_id=client_id)

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

        data_store.run_update_sql(query=query)
