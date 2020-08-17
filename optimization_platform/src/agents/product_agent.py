import datetime

import requests

from optimization_platform.src.agents.shop_agent import ShopAgent
from utils.data_store.rds_data_store import IteratorFile
from utils.date_utils import DateUtils
from config import TABLE_PRODUCTS


class ProductAgent(object):

    @classmethod
    def sync_products(cls, data_store, shop_id):
        table = TABLE_PRODUCTS
        columns = ['shop_id', 'product_id', "product_title", "product_handle", "variant_id", "variant_title",
                   "variant_price", "updated_at", "tags"]

        sql = "select max(updated_at) from {table} where shop_id = '{shop_id}'".format(table=table,
                                                                                       shop_id=shop_id)
        mobile_records = data_store.run_select_sql(query=sql)
        updated_at = None
        max_datetime = mobile_records[0][0]
        if max_datetime is not None:
            max_datetime += datetime.timedelta(microseconds=0)
            max_datetime_utc = DateUtils.change_timezone(datetime_obj=max_datetime, timezone_str="UTC")
            updated_at = DateUtils.convert_datetime_to_iso_string(datetime_obj=max_datetime_utc)

        shop_details = ShopAgent.get_shop_details_for_shop_id(data_store=data_store, shop_id=shop_id)
        shopify_access_token = shop_details["shopify_access_token"]
        product_url = "https://{shop_id}/admin/api/2020-04/products.json?limit=250".format(shop_id=shop_id)
        if updated_at is not None:
            product_url = "https://{shop_id}/admin/api/2020-04/products.json?updated_at_min={updated_at}".format(
                shop_id=shop_id,
                updated_at=updated_at)
        r = requests.get(product_url, headers={
            "X-Shopify-Access-Token": shopify_access_token
        })
        product_list = r.json()["products"]

        def get_next_url(shop_id, header):
            link_header = header.get('Link')
            rel_next_tag = 'rel="next"'
            if link_header is not None and rel_next_tag in link_header:
                next_field = link_header.split(",")[-1]
                url = next_field.split(";")[0][1:-1]
                ext = "/".join(url.split("/")[6:])
                next_url = "https://{shop_id}/admin/api/2020-04/{ext}".format(shop_id=shop_id, ext=ext)
                return next_url
            return None

        while True:
            header = r.headers
            product_url = get_next_url(shop_id, header)
            if product_url is None:
                break
            r = requests.get(product_url, headers={
                "X-Shopify-Access-Token": shopify_access_token
            })
            product_list += r.json()["products"]

        variant_list = list()
        variant_id_list = list()
        for product in product_list:
            variants = product["variants"]
            for variant in variants:
                variant_dict = dict()
                variant_dict["shop_id"] = shop_id
                variant_dict["product_id"] = product["id"]
                variant_dict["product_title"] = product["title"]
                variant_dict["product_handle"] = product["handle"]
                variant_dict["variant_id"] = variant["id"]
                variant_dict["variant_title"] = variant["title"]
                variant_dict["variant_price"] = float(variant["price"])
                variant_dict["updated_at"] = variant["updated_at"]
                variant_dict["tags"] = product["tags"]
                variant_list.append(variant_dict)
                variant_id_list.append(variant["id"])

        if updated_at is not None and len(variant_list) > 0:
            s = ",".join(["%s" for i in range(len(variant_id_list))])
            query = """delete from {table} where shop_id = '{shop_id}' and variant_id in ({s})""".format(
                table=table,
                shop_id=shop_id,
                s=s)
            data_store.run_batch_delete_sql(query=query, data_list=variant_id_list)

        if len(variant_list) > 0:
            s = "\t".join(["{}" for i in range(len(variant_list[0].keys()))])

            file = IteratorFile((s.format(variant[columns[0]], variant[columns[1]],
                                          variant[columns[2]], variant[columns[3]],
                                          variant[columns[4]], variant[columns[5]],
                                          variant[columns[6]], variant[columns[7]],
                                          variant[columns[8]])
                                 for variant in variant_list))
            data_store.run_batch_insert_sql(file=file, table=table, columns=columns)

        variant_id_count = len(variant_id_list)
        return variant_id_count
