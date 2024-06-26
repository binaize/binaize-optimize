import pandas as pd

from utils.date_utils import DateUtils


def get_visits_df(client_id, data_store, end_date, start_date):
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
        visits_df["product_handle"] = visits_df["url"].map(lambda x: x.split("/")[-1].strip())
        visits_df.drop(['url'], axis=1, inplace=True)
        visits_df = visits_df.groupby('product_handle').agg({
            'visitor_count': [('visitor_count', lambda x: x.sum())]
        })
        visits_df.columns = visits_df.columns.droplevel()
        visits_df["product_handle"] = visits_df.index
        visits_df.reset_index(inplace=True, drop=True)
    return visits_df


def get_products_df(client_id, data_store, end_date, start_date):
    sql = \
        """
            select distinct product_title, product_handle, product_id, tags
                from products
            where 
                client_id = '{client_id}'
        """.format(client_id=client_id, start_date=start_date, end_date=end_date)
    mobile_records = data_store.run_custom_sql(sql)
    products_df = None
    if mobile_records is not None and len(mobile_records) > 0:
        products_df = pd.DataFrame.from_records(mobile_records)
        products_df.columns = ["product_title", "product_handle", "product_id", "tags"]
    return products_df


def get_orders_df(client_id, data_store, end_date, start_date):
    # sql = \
    #     """
    #         select product_id, sum(variant_quantity) as conversion_count
    #             from orders
    #         where
    #             client_id = '{client_id}'
    #             and payment_status = True
    #             and updated_at > '{start_date}'
    #             and updated_at < '{end_date}'
    #         group by
    #             product_id
    #     """.format(client_id=client_id, start_date=start_date, end_date=end_date)

    sql = \
        """
            select product_id, count(distinct(order_id)) as conversion_count
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
    return orders_df


def get_product_conversion_analytics(data_store, client_id, start_date_str, end_date_str, timezone_str):
    start_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=start_date_str,
                                                                       timezone_str=timezone_str)
    end_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=end_date_str,
                                                                     timezone_str=timezone_str)

    visits_df = get_visits_df(client_id, data_store, end_date, start_date)
    products_df = get_products_df(client_id, data_store, end_date, start_date)
    orders_df = get_orders_df(client_id, data_store, end_date, start_date)

    products = list()
    non_conversion_count = list()
    conversion_count = list()
    percentage_list = list()
    conclusion, summary = get_description_for_data_not_enough()

    if products_df is None:
        result = construct_result(conclusion, conversion_count, percentage_list, products, summary, non_conversion_count)
        return result

    visits_product_df = products_df
    if visits_df is None:
        visits_product_df["visitor_count"] = 0
    else:
        visits_product_df = pd.merge(products_df, visits_df,
                                     how='left', left_on=["product_handle"], right_on=["product_handle"])
        visits_product_df["visitor_count"] = visits_product_df["visitor_count"].fillna(0).astype(int)

    df = visits_product_df
    if orders_df is None:
        df["conversion_count"] = 0
    else:
        df = pd.merge(visits_product_df, orders_df,
                      how='left', left_on=["product_id"], right_on=["product_id"])
        df["conversion_count"] = df["conversion_count"].fillna(0).astype(int)

    df["non_conversion_count"] = df["visitor_count"] - df["conversion_count"]
    df["non_conversion_count"] = df["non_conversion_count"].map(lambda x: 0 if x < 0 else x)
    df["visitor_count"] = df["non_conversion_count"] + df["conversion_count"]
    df["adjusted_visitor_count"] = df["visitor_count"].map(lambda x: x + 0.01 if x == 0 else x)
    df["conversion_percentage"] = df["conversion_count"] * 100 / (df["adjusted_visitor_count"])
    df["conversion_percentage"] = df["conversion_percentage"].map(lambda x: round(x, 2))
    df = df.sort_values(["product_handle"])

    df["tags"] = df["tags"].map(lambda x: ["no-tag"] if len(x) == 0 else [s.strip() for s in x.split(",")])
    # df["tags"] = df["tags"].map(lambda x: ["no-tag"] if len(x) == 0 else x.split(","))

    import functools
    import operator

    tag_list = list(set(functools.reduce(operator.concat, df["tags"])))
    if "no-tag" in tag_list:
        tag_list.remove("no-tag")
        tag_list = sorted(tag_list)
        tag_list = tag_list + ["no-tag"]

    final_result = dict()
    result_list = list()
    for tag in tag_list:
        temp_df = df[df["tags"].map(set([tag]).issubset)]
        products = temp_df["product_title"].tolist()
        non_conversion_count = temp_df["non_conversion_count"].tolist()
        conversion_count = temp_df["conversion_count"].tolist()
        percentage_list = temp_df["conversion_percentage"].tolist()

        if orders_df is None and visits_df is None:
            result = construct_result(conclusion, conversion_count, percentage_list, products, summary,
                                      non_conversion_count)
        else:
            min_idx = percentage_list.index(min(percentage_list))
            min_product = products[min_idx]
            min_percentage = percentage_list[min_idx]
            conclusion, summary = get_description_for_enough_visitors(min_percentage, min_product)
            result = construct_result(conclusion, conversion_count, percentage_list, products, summary,
                                      non_conversion_count)
        result_list.append(result)
    final_result["tags"] = tag_list
    final_result["results"] = result_list
    return final_result


def construct_result(conclusion, conversion_count, percentage_list, products, summary, non_conversion_count):
    result = dict()
    result["products"] = products
    temp_dict = dict()
    temp_dict["non_conversion_count"] = non_conversion_count
    temp_dict["conversion_count"] = conversion_count
    temp_dict["conversion_percentage"] = percentage_list
    result["product_conversion"] = temp_dict
    result["summary"] = summary
    result["conclusion"] = conclusion
    return result


def get_description_for_enough_visitors(min_percentage, min_product):
    summary = "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong> {product}" \
              " </strong></span> has the least conversion of <span style = 'color: blue; font-size: 16px;'><strong>" \
              " {conversion}% </strong></span>".format(
        product=min_product,
        conversion=min_percentage)
    conclusion = "<strong> CONCLUSION : </strong> Experiment with different creatives/copies for" \
                 "<span style = 'color: blue; font-size: 16px;'><strong> {product} </strong></span>".format(
        product=min_product)
    return conclusion, summary


def get_description_for_data_not_enough():
    summary = "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT " \
              "</strong></span> enough visits registered on the website"
    conclusion = "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT " \
                 "</strong></span> for the customers to interact with your website"
    return conclusion, summary
