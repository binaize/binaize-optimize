import datetime

import requests

from optimization_platform.src.agents.client_agent import ClientAgent
from utils.data_store.rds_data_store import IteratorFile
from utils.date_utils import DateUtils
from config import TABLE_ORDERS


class OrderAgent(object):

    @classmethod
    def sync_orders(cls, data_store, client_id):
        table = TABLE_ORDERS
        columns = ["client_id", "order_id", "email_id", "cart_token", "product_id",
                   "variant_id", "variant_quantity", "variant_price", "updated_at", "payment_status", "landing_page"]
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
        order_url = "{base_url}/orders.json".format(base_url=base_url)
        if updated_at is not None:
            order_url = "{base_url}/orders.json?updated_at_min={updated_at}".format(base_url=base_url,
                                                                                    updated_at=updated_at)
        r = requests.get(order_url)
        order_list = r.json()["orders"]

        variant_list = list()
        order_id_list = list()
        for order in order_list:
            items = order["line_items"]
            for item in items:
                variant_dict = dict()
                variant_dict["client_id"] = client_id
                variant_dict["order_id"] = order["id"]
                variant_dict["email_id"] = order["email"]
                variant_dict["cart_token"] = order["cart_token"]
                variant_dict["product_id"] = item["product_id"]
                variant_dict["variant_id"] = item["variant_id"]
                variant_dict["variant_quantity"] = int(item["quantity"])
                variant_dict["variant_price"] = float(item["price"])
                variant_dict["updated_at"] = order["updated_at"]
                variant_dict["payment_status"] = True
                variant_dict["landing_page"] = order["landing_site"]
                variant_list.append(variant_dict)
            order_id_list.append(order["id"])

        base_url = "/".join(shared_url.split("/")[:-1])
        checkout_url = "{base_url}/checkouts.json".format(base_url=base_url)
        if updated_at is not None:
            checkout_url = "{base_url}/checkouts.json?updated_at_min={updated_at}".format(base_url=base_url,
                                                                                          updated_at=updated_at)
        r = requests.get(checkout_url)
        checkout_list = r.json()["checkouts"]
        for checkout in checkout_list:
            items = checkout["line_items"]
            for item in items:
                variant_dict = dict()
                variant_dict["client_id"] = client_id
                variant_dict["order_id"] = checkout["id"]
                variant_dict["email_id"] = checkout["email"]
                variant_dict["cart_token"] = checkout["cart_token"]
                variant_dict["product_id"] = item["product_id"]
                variant_dict["variant_id"] = item["variant_id"]
                variant_dict["variant_quantity"] = int(item["quantity"])
                variant_dict["variant_price"] = float(item["price"])
                variant_dict["updated_at"] = checkout["updated_at"]
                variant_dict["payment_status"] = False
                variant_dict["landing_page"] = checkout["landing_site"]
                variant_list.append(variant_dict)
            order_id_list.append(checkout["id"])

        if updated_at is not None and len(variant_list) > 0:
            s = ",".join(["%s" for i in range(len(order_id_list))])
            query = """delete from {table} where client_id = '{client_id}' and order_id in ({s})""".format(
                table=table,
                client_id=client_id,
                s=s)
            data_store.run_batch_delete_sql(query=query, data_list=order_id_list)

        if len(variant_list) > 0:
            s = "\t".join(["{}" for i in range(len(variant_list[0].keys()))])

            file = IteratorFile((s.format(variant[columns[0]], variant[columns[1]],
                                          variant[columns[2]], variant[columns[3]],
                                          variant[columns[4]], variant[columns[5]],
                                          variant[columns[6]], variant[columns[7]],
                                          variant[columns[8]], variant[columns[9]], variant[columns[10]])
                                 for variant in variant_list))
            data_store.run_batch_insert_sql(file=file, table=table, columns=columns)

        order_id_count = len(order_id_list)
        return order_id_count
