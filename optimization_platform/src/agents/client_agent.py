import pandas as pd
import requests

from config import *
from utils.date_utils import DateUtils


class ClientAgent(object):

    @classmethod
    def add_new_client(cls, data_store, shopify_domain, shopify_access_token, hashed_password, creation_timestamp):
        table = TABLE_CLIENTS
        creation_time_utc_str = DateUtils.convert_timestamp_to_utc_iso_string(creation_timestamp)
        STORE_DETAILS_URL = "https://{shopify_store}/admin/api/2020-07/shop.json".format(
            shopify_store=shopify_domain)
        shop_details_response = requests.get(STORE_DETAILS_URL, headers={
            "X-Shopify-Access-Token": shopify_access_token
        })
        shop_details = shop_details_response.json()
        client_id = shop_details["shop"]["id"]
        shopify_domain = shop_details["shop"]["myshopify_domain"]
        shop_domain = shop_details["shop"]["domain"]
        shop_owner = shop_details["shop"]["shop_owner"]
        email_id = shop_details["shop"]["customer_email"]
        disabled = False
        client_timezone = shop_details["shop"]["iana_timezone"]
        city = shop_details["shop"]["city"]
        country = shop_details["shop"]["country"]
        province = shop_details["shop"]["province"]
        columns_value_dict = {"client_id": client_id,
                              "shopify_domain": shopify_domain,
                              "shop_domain": shop_domain,
                              "shop_owner": shop_owner,
                              "email_id": email_id,
                              "hashed_password": hashed_password,
                              "disabled": disabled,
                              "client_timezone": client_timezone,
                              "shopify_access_token": shopify_access_token,
                              "city": city,
                              "country": country,
                              "province": province,
                              "creation_time": creation_time_utc_str}

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
        columns = ["client_id", "shopify_domain", "shop_domain", "shop_owner", "email_id", "hashed_password",
                   "disabled", "client_timezone", "shopify_access_token", "city", "country", "province",
                   "creation_time"]

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

    @classmethod
    def delete_client_for_client_id(cls, data_store, client_id):
        table = TABLE_CLIENTS
        sql = """ delete from {table} where client_id='{client_id}'""".format(table=table, client_id=client_id)
        status = data_store.run_update_sql(query=sql)
        return status
