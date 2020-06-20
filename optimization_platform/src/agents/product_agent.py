import datetime

import requests

from optimization_platform.src.agents.client_agent import ClientAgent
from utils.data_store.rds_data_store import IteratorFile
from utils.date_utils import DateUtils
from config import TABLE_PRODUCTS


class ProductAgent(object):

    @classmethod
    def sync_products(cls, data_store, client_id):
        table = TABLE_PRODUCTS
        columns = ['client_id', 'product_id', "product_title", "product_handle", "variant_id", "variant_title",
                   "variant_price", "updated_at"]

        sql = "select max(updated_at) from {table} where client_id = '{client_id}'".format(table=table,
                                                                                           client_id=client_id)
        mobile_records = data_store.run_select_sql(query=sql)
        updated_at = None
        max_datetime = mobile_records[0][0]
        if max_datetime is not None:
            max_datetime += datetime.timedelta(microseconds=0)
            max_datetime_utc = DateUtils.change_timezone(datetime_obj=max_datetime, timezone_str="UTC")
            updated_at = DateUtils.convert_datetime_to_iso_string(datetime_obj=max_datetime_utc)

        client_details = ClientAgent.get_client_details_for_client_id(data_store=data_store, client_id=client_id)
        shared_url = client_details["shopify_app_eg_url"]
        base_url = "/".join(shared_url.split("/")[:-1])
        product_url = "{base_url}/products.json".format(base_url=base_url)
        if updated_at is not None:
            product_url = "{base_url}/products.json?updated_at_min={updated_at}".format(
                base_url=base_url, updated_at=updated_at)
        r = requests.get(product_url)
        product_list = r.json()["products"]

        variant_list = list()
        variant_id_list = list()
        for product in product_list:
            variants = product["variants"]
            for variant in variants:
                variant_dict = dict()
                variant_dict["client_id"] = client_id
                variant_dict["product_id"] = product["id"]
                variant_dict["product_title"] = product["title"]
                variant_dict["product_handle"] = product["handle"]
                variant_dict["variant_id"] = variant["id"]
                variant_dict["variant_title"] = variant["title"]
                variant_dict["variant_price"] = float(variant["price"])
                variant_dict["updated_at"] = variant["updated_at"]
                variant_list.append(variant_dict)
                variant_id_list.append(variant["id"])

        if updated_at is not None and len(variant_list) > 0:
            s = ",".join(["%s" for i in range(len(variant_id_list))])
            query = """delete from {table} where client_id = '{client_id}' and variant_id in ({s})""".format(
                table=table,
                client_id=client_id,
                s=s)
            data_store.run_batch_delete_sql(query=query, data_list=variant_id_list)

        if len(variant_list) > 0:
            s = "\t".join(["{}" for i in range(len(variant_list[0].keys()))])

            file = IteratorFile((s.format(variant[columns[0]], variant[columns[1]],
                                          variant[columns[2]], variant[columns[3]],
                                          variant[columns[4]], variant[columns[5]],
                                          variant[columns[6]], variant[columns[7]])
                                 for variant in variant_list))
            data_store.run_batch_insert_sql(file=file, table=table, columns=columns)

        variant_id_count = len(variant_id_list)
        return variant_id_count
