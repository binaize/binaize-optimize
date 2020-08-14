import pandas as pd
import requests

from config import *
from utils.date_utils import DateUtils


class ShopAgent(object):

    @classmethod
    def add_new_shop(cls, data_store, shop_id, shopify_access_token, hashed_password, creation_timestamp):
        table = TABLE_SHOPS
        creation_time_utc_str = DateUtils.convert_timestamp_to_utc_iso_string(creation_timestamp)
        STORE_DETAILS_URL = "https://{shopify_store}/admin/api/2020-07/shop.json".format(
            shopify_store=shop_id)
        shop_details_response = requests.get(STORE_DETAILS_URL, headers={
            "X-Shopify-Access-Token": shopify_access_token
        })
        shop_details = shop_details_response.json()
        shop_id = shop_details["shop"]["myshopify_domain"]
        shop_domain = shop_details["shop"]["domain"]
        shop_owner = shop_details["shop"]["shop_owner"]
        email_id = shop_details["shop"]["customer_email"]
        disabled = False
        timezone = shop_details["shop"]["iana_timezone"]
        city = shop_details["shop"]["city"]
        country = shop_details["shop"]["country"]
        province = shop_details["shop"]["province"]
        columns_value_dict = {"shop_id": shop_id,
                              "shop_domain": shop_domain,
                              "shop_owner": shop_owner,
                              "email_id": email_id,
                              "hashed_password": hashed_password,
                              "disabled": disabled,
                              "timezone": timezone,
                              "shopify_access_token": shopify_access_token,
                              "city": city,
                              "country": country,
                              "province": province,
                              "shopify_nonce": "",
                              "creation_time": creation_time_utc_str}

        columns = list(columns_value_dict.keys())
        column = ",".join(columns)
        values = [columns_value_dict[key] for key in columns]
        value = str(tuple(values))

        cls.delete_shop_for_shop_id(data_store=data_store, shop_id=shop_id)

        sql = """INSERT INTO {table} ({column}) VALUES {value}""".format(table=table, column=column, value=value)
        status = data_store.run_insert_into_sql(query=sql)
        return status

    @classmethod
    def get_shop_details_for_shop_id(cls, data_store, shop_id):
        table = TABLE_SHOPS
        columns = ["shop_id", "shop_domain", "shop_owner", "email_id", "hashed_password",
                   "disabled", "timezone", "shopify_access_token", "city", "country", "province",
                   "shopify_nonce", "creation_time"]

        where = "shop_id='{shop_id}'".format(shop_id=shop_id)

        column = ",".join(columns)
        sql = """ SELECT {column} from {table} where {where}""".format(column=column, table=table, where=where)
        mobile_records = data_store.run_select_sql(query=sql)
        df = None
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = columns
        shop_details = None
        if df is not None:
            shop_details = df[columns].to_dict(orient="records")[0]
            if shop_details["creation_time"] is not None:
                shop_details["creation_time"] = DateUtils.convert_datetime_to_conversion_dashboard_date_string(
                    datetime_obj=shop_details["creation_time"], timezone_str="UTC")
            else:
                shop_details["creation_time"] = None
        return shop_details

    @classmethod
    def get_all_shop_ids(cls, data_store):
        table = TABLE_SHOPS
        columns = ["shop_id"]
        column = ",".join(columns)
        sql = """ select {column} from {table}""".format(column=column, table=table)
        mobile_records = data_store.run_select_sql(query=sql)
        df = None
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = columns
        shop_ids = None
        if df is not None:
            shop_ids = df[columns[0]].tolist()
        return shop_ids

    @classmethod
    def delete_shop_for_shop_id(cls, data_store, shop_id):
        table = TABLE_SHOPS
        sql = """ delete from {table} where shop_id='{shop_id}'""".format(table=table, shop_id=shop_id)
        status = data_store.run_update_sql(query=sql)
        return status

    @classmethod
    def get_shopify_details_for_shop_id(cls, data_store, shop_id):
        table = TABLE_SHOPS
        columns = ["shop_id", "shopify_access_token", "shopify_nonce"]
        column = ",".join(columns)
        sql = """ select {column} from {table} where shop_id='{shop_id}'""".format(column=column, table=table,
                                                                                   shop_id=shop_id)
        mobile_records = data_store.run_select_sql(query=sql)
        df = None
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = columns
        shopify_details = None
        if df is not None:
            shopify_details = df[columns].to_dict(orient="records")[0]
        return shopify_details

    @classmethod
    def upsert_shopify_nonce_for_shop_id(cls, data_store, shop_id, shopify_nonce):
        table = TABLE_SHOPS
        columns_value_dict = {"shop_id": shop_id,
                              "shopify_nonce": shopify_nonce}
        columns = list(columns_value_dict.keys())
        column = ",".join(columns)
        values = [columns_value_dict[key] for key in columns]
        value = str(tuple(values))

        sql = """INSERT INTO {table} ({column}) VALUES {value} ON CONFLICT (shop_id)""" \
              """ DO UPDATE SET shopify_nonce='{shopify_nonce}'""".format(
            table=table, column=column, value=value, shopify_nonce=shopify_nonce)

        status = data_store.run_insert_into_sql(query=sql)
        return status
