import pandas as pd

from utils.date_utils import DateUtils


class ConversionAnalytics(object):

    @classmethod
    def get_shop_funnel_analytics(cls, data_store, client_id, start_date_str, end_date_str):
        start_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=start_date_str,
                                                                           timezone_str="Asia/Kolkata")
        end_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=end_date_str,
                                                                         timezone_str="Asia/Kolkata")
        sql = \
            """
                select '1' as id, 'Home Page' as event,count(distinct(session_id))
                    from visits
                where 
                    client_id = '{client_id}'
                    and event_name = 'home'
                    and creation_time > '{start_date}' 
                    and creation_time < '{end_date}' 
                union
                (select '2' as id, 'Collection Page' as event,count(distinct(session_id))
                    from visits
                where 
                    client_id = '{client_id}'
                    and event_name = 'collection'
                    and creation_time > '{start_date}' 
                    and creation_time < '{end_date}')
                union
                (select '3' as id, 'Product Page' as event,count(distinct(session_id))
                    from visits
                where 
                    client_id = '{client_id}'
                    and event_name = 'product'
                    and creation_time > '{start_date}' 
                    and creation_time < '{end_date}')
                union
                (select '4' as id, 'Cart Page' as event,count(distinct(session_id))
                    from visits
                where 
                    client_id = '{client_id}'
                    and event_name = 'cart'
                    and creation_time > '{start_date}' 
                    and creation_time < '{end_date}')
                union
                (select '5' as id, 'Checkout Page' as event,count(distinct(order_id))
                    from orders
                where 
                    client_id = '{client_id}'
                    and updated_at > '{start_date}' 
                    and updated_at < '{end_date}')
                union
                (select '6' as id, 'Purchase' as event,count(distinct(order_id))
                    from orders
                where 
                    client_id = '{client_id}'
                    and payment_status = True
                    and updated_at > '{start_date}' 
                    and updated_at < '{end_date}')

            """.format(client_id=client_id, start_date=start_date, end_date=end_date)
        mobile_records = data_store.run_custom_sql(sql)
        result = {}
        temp_dict = dict()
        summary = "<strong> SUMMARY </strong> There are NOT enough visits registered on the website"
        conclusion = "<strong> CONCLUSION </strong> Wait for the customers to interact with your website"
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = ["id", "pages", "count"]
            df = df.sort_values(['id'])
            df["percentage"] = df["count"] * 100 / (max(df["count"]) + 0.01)
            df["percentage"] = df["percentage"].map(lambda x: min(100.00, round(x, 2)))
            result["pages"] = df["pages"].tolist()
            visitor_count = df["count"].tolist()
            temp_dict["visitor_count"] = visitor_count
            temp_dict["percentage"] = df["percentage"].tolist()
            result["shop_funnel"] = temp_dict
            if sum(visitor_count[:4]) > 0:
                diff_list = list()
                for a in zip(df["percentage"], df["percentage"][1:]):
                    diff_list.append(a[0] - a[1])
                max_idx = diff_list.index(max(diff_list))
                summary = "<strong> SUMMARY </strong> {page_type} has maximum churn of {drop}%".format(
                    page_type=result["pages"][max_idx],
                    drop=round(diff_list[max_idx], 2))
                conclusion = "<strong> CONCLUSION </strong> Experiment with different creatives/copies for {page_type}".format(
                    page_type=result["pages"][max_idx])
            result["summary"] = summary
            result["conclusion"] = conclusion

        return result

    @classmethod
    def get_product_conversion_analytics(cls, data_store, client_id, start_date_str, end_date_str):
        start_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=start_date_str,
                                                                           timezone_str="Asia/Kolkata")
        end_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=end_date_str,
                                                                         timezone_str="Asia/Kolkata")
        sql = \
            """
                select url, count(distinct(session_id)) as session_count
                    from visits
                where 
                    client_id = '{client_id}'
                    and event_name = 'product'
                    and creation_time > '{start_date}' 
                    and creation_time < '{end_date}'
                group by
                    url
            """.format(client_id=client_id, start_date=start_date, end_date=end_date)
        mobile_records = data_store.run_custom_sql(sql)
        visits_df = None
        if mobile_records is not None and len(mobile_records) > 0:
            visits_df = pd.DataFrame.from_records(mobile_records)
            visits_df.columns = ["url", "visitor_count"]
            visits_df["product_handle"] = visits_df["url"].map(lambda x: x.split("/")[-1])
            visits_df.drop(['url'], axis=1, inplace=True)
        sql = \
            """
                select product_title, product_handle, product_id
                    from products
                where 
                    client_id = '{client_id}'
            """.format(client_id=client_id, start_date=start_date, end_date=end_date)
        mobile_records = data_store.run_custom_sql(sql)
        products_df = None
        if mobile_records is not None and len(mobile_records) > 0:
            products_df = pd.DataFrame.from_records(mobile_records)
            products_df.columns = ["product_title", "product_handle", "product_id"]
            products_df.drop_duplicates(keep="first", inplace=True)

        sql = \
            """
                select product_id, sum(variant_quantity) as conversion_count
                    from orders
                where 
                    client_id = '{client_id}' 
                    and payment_status = True
                    and updated_at > '{start_date}' 
                    and updated_at < '{end_date}'
                group by
                    product_id
            """.format(client_id=client_id, start_date=start_date, end_date=end_date)
        mobile_records = data_store.run_custom_sql(sql)
        orders_df = None
        if mobile_records is not None and len(mobile_records) > 0:
            orders_df = pd.DataFrame.from_records(mobile_records)
            orders_df.columns = ["product_id", "conversion_count"]

        visits_product_df = None
        if visits_df is not None and products_df is not None:
            visits_product_df = pd.merge(visits_df, products_df,
                                         how='outer', left_on=["product_handle"], right_on=["product_handle"])
        elif products_df is not None:
            visits_product_df = products_df
            visits_product_df["visitor_count"] = 0

        df = None
        if orders_df is not None and visits_product_df is not None:
            df = pd.merge(visits_product_df, orders_df,
                          how='left', left_on=["product_id"], right_on=["product_id"])
        elif visits_product_df is not None:
            df = visits_product_df
            df["conversion_count"] = 0

        products = list()
        visitor_count = list()
        conversion_count = list()
        percentage_list = list()
        summary = "<strong> SUMMARY </strong> There are NOT enough visits registered on the website"
        conclusion = "<strong> CONCLUSION </strong> Wait for the customers to interact with your website"
        if df is not None:
            df["conversion_count"] = df["conversion_count"].fillna(0).astype(int)
            df["visitor_count"] = df["visitor_count"].fillna(0).astype(int)
            df["conversion_percentage"] = df["conversion_count"] * 100 / (df["visitor_count"] + 0.01)
            df["conversion_percentage"] = df["conversion_percentage"].map(lambda x: round(x, 2))
            df = df.sort_values(["product_handle"])
            products = df["product_title"].tolist()
            visitor_count = df["visitor_count"].tolist()
            conversion_count = df["conversion_count"].tolist()
            percentage_list = df["conversion_percentage"].tolist()
            for i in range(len(products)):
                if percentage_list[i] > 100:
                    if conversion_count[i] == 0:
                        percentage_list[i] = 0.0
                    else:
                        percentage_list[i] = 99.99
            product_list = products
            min_idx = percentage_list.index(min(percentage_list))
            if min(percentage_list) < 99.99 and sum(percentage_list) > 0.0:
                summary = "<strong> SUMMARY </strong> {product} has minimum conversion of {conversion}%".format(
                    product=product_list[min_idx],
                    conversion=percentage_list[
                        min_idx])
                conclusion = "<strong> CONCLUSION </strong> Experiment with different creatives/copies for {product}".format(
                    product=product_list[min_idx])
        result = dict()
        result["products"] = products
        temp_dict = dict()
        temp_dict["visitor_count"] = visitor_count
        temp_dict["conversion_count"] = conversion_count
        temp_dict["conversion_percentage"] = percentage_list
        result["product_conversion"] = temp_dict
        result["summary"] = summary
        result["conclusion"] = conclusion

        return result

    @classmethod
    def get_landing_page_analytics(cls, data_store, client_id, start_date_str, end_date_str):
        start_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=start_date_str,
                                                                           timezone_str="Asia/Kolkata")
        end_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=end_date_str,
                                                                         timezone_str="Asia/Kolkata")

        sql = \
            """ 
                select t.event_name, count(*)
                from (
                    select distinct on (session_id) session_id, url, event_name
                        from visits
                    where 
                        client_id = '{client_id}'
                        and event_name in ('collection','home','product')
                        and creation_time > '{start_date}' 
                        and creation_time < '{end_date}') t
                group by event_name
            """.format(client_id=client_id, start_date=start_date, end_date=end_date)
        mobile_records = data_store.run_custom_sql(sql)

        visits_df = None
        if mobile_records is not None and len(mobile_records) > 0:
            visits_df = pd.DataFrame.from_records(mobile_records)
            visits_df.columns = ["event_name", "visitor_count"]

        sql = \
            """
                select landing_page, count(*)
                    FROM orders
                where 
                    client_id = '{client_id}' 
                    and payment_status = True
                    and updated_at > '{start_date}' 
                    and updated_at < '{end_date}'
                group by landing_page
            """.format(client_id=client_id, start_date=start_date, end_date=end_date)
        mobile_records = data_store.run_custom_sql(sql)

        orders_df = None
        if mobile_records is not None and len(mobile_records) > 0:
            orders_df = pd.DataFrame.from_records(mobile_records)
            orders_df.columns = ["landing_page", "conversion_count"]

            def process_landing_page(x):
                if "product" in x:
                    return "product"
                elif "collection" in x:
                    return "collection"
                return "home"

            orders_df["page_type"] = orders_df["landing_page"].map(process_landing_page)

        df = None
        if orders_df is not None and visits_df is not None:
            df = pd.merge(visits_df, orders_df, how="left", left_on="event_name", right_on="page_type")
            df["conversion_count"] = df["conversion_count"].fillna(0).astype(int)
        event_dict = {"home": "Home Page", "product": "Product Page", "collection": "Collections Page"}
        pages = list()
        visitor_count = list()
        conversion_count = list()
        conversion_percentage = list()
        summary = "<strong> SUMMARY </strong> There are NOT enough visits registered on the website"
        conclusion = "<strong> CONCLUSION </strong> Wait for the customers to interact with your website"
        result = dict()
        if df is not None:
            pages = [event_dict[val] for val in df["event_name"].tolist()]
            visitor_count = df["visitor_count"].tolist()
            conversion_count = df["conversion_count"].tolist()
            df["conversion_percentage"] = df["conversion_count"] * 100 / (df["visitor_count"] + 0.01)
            conversion_percentage = df["conversion_percentage"].map(lambda x: min(100, round(x, 2))).tolist()
            min_idx = conversion_percentage.index(min(conversion_percentage))
            summary = "<strong> SUMMARY </strong> {product} has minimum conversion of {conversion}%".format(
                product=pages[min_idx],
                conversion=conversion_percentage[
                    min_idx])
            conclusion = "<strong> CONCLUSION </strong> Experiment with different creatives/copies for {product}".format(
                product=pages[min_idx])
        result["pages"] = pages
        temp_dict = dict()
        temp_dict["visitor_count"] = visitor_count
        temp_dict["conversion_count"] = conversion_count
        temp_dict["conversion_percentage"] = conversion_percentage
        result["landing_conversion"] = temp_dict
        result["summary"] = summary
        result["conclusion"] = conclusion
        return result
