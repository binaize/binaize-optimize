import datetime

import requests

from optimization_platform.src.agents.shop_agent import ShopAgent
from utils.data_store.rds_data_store import IteratorFile
from utils.date_utils import DateUtils
from config import TABLE_ORDERS
import uuid


class OrderAgent(object):

    @classmethod
    def sync_orders(cls, data_store, client_id):
        table = TABLE_ORDERS
        columns = ["client_id", "order_id", "email_id", "cart_token", "product_id",
                   "variant_id", "variant_quantity", "variant_price", "updated_at", "payment_status", "landing_page"]
        sql = "select max(updated_at) from {table} where client_id = '{client_id}' and payment_status=TRUE".format(
            table=table,
            client_id=client_id)
        mobile_records = data_store.run_select_sql(query=sql)
        order_updated_at = None
        max_datetime = mobile_records[0][0]
        if max_datetime is not None:
            max_datetime += datetime.timedelta(seconds=1)
            max_datetime_utc = DateUtils.change_timezone(datetime_obj=max_datetime, timezone_str="UTC")
            order_updated_at = DateUtils.convert_datetime_to_iso_string(datetime_obj=max_datetime_utc)

        client_details = ShopAgent.get_shop_details_for_shop_id(data_store=data_store, shop_id=client_id)
        shared_url = client_details["shopify_app_eg_url"]
        base_url = "/".join(shared_url.split("/")[:6])
        order_url = "{base_url}/orders.json?limit=250".format(base_url=base_url)
        if order_updated_at is not None:
            order_url = "{base_url}/orders.json?updated_at_min={updated_at}".format(base_url=base_url,
                                                                                    updated_at=order_updated_at)
        r = requests.get(order_url)
        order_list = r.json()["orders"]

        def get_next_url(base_url, header):
            link_header = header.get('Link')
            rel_next_tag = 'rel="next"'
            if link_header is not None and rel_next_tag in link_header:
                next_field = link_header.split(",")[-1]
                url = next_field.split(";")[0][1:-1]
                ext = "/".join(url.split("/")[6:])
                next_url = "{base_url}/{ext}".format(base_url=base_url, ext=ext)
                return next_url
            return None

        while True:
            header = r.headers
            order_url = get_next_url(base_url, header)
            if order_url is None:
                break
            r = requests.get(order_url)
            order_list += r.json()["orders"]

        variant_list = list()
        order_id_list = list()
        session_cart_time_list = list()
        for order in order_list:
            items = order["line_items"]
            order_id = order["id"]
            email_id = order["email"]
            cart_token = order["cart_token"]
            updated_at = order["updated_at"]
            landing_page = order["landing_site"]
            if cart_token is None or len(cart_token) == 0:
                cart_token = uuid.uuid4().hex
                session_id = uuid.uuid4().hex
                session_cart_time_list.append((session_id, cart_token, updated_at))
            for item in items:
                variant_dict = dict()
                variant_dict["client_id"] = client_id
                variant_dict["order_id"] = order_id
                variant_dict["email_id"] = email_id
                variant_dict["cart_token"] = cart_token
                variant_dict["product_id"] = item["product_id"]
                variant_dict["variant_id"] = item["variant_id"]
                variant_dict["variant_quantity"] = int(item["quantity"])
                variant_dict["variant_price"] = float(item["price"])
                variant_dict["updated_at"] = updated_at
                variant_dict["payment_status"] = True
                variant_dict["landing_page"] = landing_page
                variant_list.append(variant_dict)
            order_id_list.append(order["id"])

        base_url = "/".join(shared_url.split("/")[:6])
        checkout_url = "{base_url}/checkouts.json?limit=250".format(base_url=base_url)
        r = requests.get(checkout_url)
        checkout_list = r.json()["checkouts"]

        def get_next_url(base_url, header):
            link_header = header.get('Link')
            rel_next_tag = 'rel="next"'
            if link_header is not None and rel_next_tag in link_header:
                next_field = link_header.split(",")[-1]
                url = next_field.split(";")[0][1:-1]
                ext = "/".join(url.split("/")[6:])
                next_url = "{base_url}/{ext}".format(base_url=base_url, ext=ext)
                return next_url
            return None

        while True:
            header = r.headers
            checkout_url = get_next_url(base_url, header)
            if checkout_url is None:
                break
            r = requests.get(checkout_url)
            checkout_list += r.json()["checkouts"]

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

        if len(variant_list) > 0:
            s = ",".join(["%s" for i in range(len(order_id_list))])
            query = """delete from {table} where client_id = '{client_id}' and payment_status=FALSE""".format(
                table=table,
                client_id=client_id)
            data_store.run_custom_sql(query=query)
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
        return order_id_count, session_cart_time_list
