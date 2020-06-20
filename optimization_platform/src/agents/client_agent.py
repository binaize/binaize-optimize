import pandas as pd

from config import *
from utils.date_utils import DateUtils


class ClientAgent(object):

    @classmethod
    def add_new_client(cls, data_store, client_id, full_name, company_name, hashed_password, disabled,
                       shopify_app_eg_url, client_timezone,
                       creation_timestamp):
        table = TABLE_CLIENTS
        creation_time_utc_str = DateUtils.convert_timestamp_to_utc_iso_string(creation_timestamp)
        columns_value_dict = {"client_id": client_id, "full_name": full_name, "company_name": company_name,
                              "hashed_password": hashed_password, "disabled": disabled,
                              "shopify_app_eg_url": shopify_app_eg_url,
                              "client_timezone": client_timezone, "creation_time": creation_time_utc_str}

        columns = list(columns_value_dict.keys())
        column = ",".join(columns)
        values = [columns_value_dict[key] for key in columns]
        value = str(tuple(values))

        sql = """INSERT INTO {table} ({column}) VALUES {value}""".format(table=table, column=column, value=value)
        status = data_store.run_insert_into_sql(query=sql)
        return status

    @classmethod
    def get_client_details_for_client_id(cls, data_store, client_id):
        table = TABLE_CLIENTS
        columns = ["client_id", "full_name", "company_name", "hashed_password", "disabled",
                   "shopify_app_eg_url", "client_timezone", "creation_time"]
        where = "client_id='{client_id}'".format(client_id=client_id)

        column = ",".join(columns)
        sql = """ SELECT {column} from {table} where {where}""".format(column=column, table=table, where=where)
        mobile_records = data_store.run_select_sql(query=sql)
        df = None
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = columns
        client_details = None
        if df is not None:
            client_details = df[columns].to_dict(orient="records")[0]
            client_details["creation_time"] = DateUtils.convert_datetime_to_conversion_dashboard_date_string(
                datetime_obj=client_details["creation_time"], timezone_str="UTC")
        return client_details

    @classmethod
    def get_all_client_ids(cls, data_store):
        table = TABLE_CLIENTS
        columns = ["client_id"]
        column = ",".join(columns)
        sql = """ select {column} from {table}""".format(column=column, table=table)
        mobile_records = data_store.run_select_sql(query=sql)
        df = None
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = columns
        client_ids = None
        if df is not None:
            client_ids = df[columns[0]].tolist()
        return client_ids
