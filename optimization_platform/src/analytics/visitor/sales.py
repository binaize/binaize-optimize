import pandas as pd
from utils.date_utils import DateUtils


def get_location_df():
    location_df = pd.read_json("utils/us-states.json", dtype=False)
    return location_df


def get_sales_conversion_df(client_id, data_store, start_time, end_time):
    sql = \
        """ select
                    table1.session_id,
                    table2.order_id,
                    table2.variant_quantity,
                    table2.variant_price
                from
                    (
                    select
                        session_id,
                        cart_token
                    from
                        cookies
                    where
                        client_id = '{client_id}'
                    )
                    table1
                    right join
                        (
                        select
                            variant_id,
                            variant_quantity,
                            cart_token,
                            order_id,
                            variant_price
                        from
                            orders
                        where
                            client_id = '{client_id}'
                            and payment_status = True
                            and updated_at > '{start_time}'
                            and updated_at < '{end_time}'
                        )
                        table2
                        on (table1.cart_token = table2.cart_token)
                        
        """.format(client_id=client_id, start_time=start_time, end_time=end_time)
    mobile_records = data_store.run_custom_sql(sql)
    sales_conv_df = None
    if mobile_records is not None and len(mobile_records) > 0:
        sales_conv_df = pd.DataFrame.from_records(mobile_records)
        sales_conv_df.columns = ["session_id", "order_id", "variant_quantity", "variant_price"]
    return sales_conv_df


def get_visitors_df(data_store, client_id, start_time, end_time):
    sql = \
        """
        select
            session_id,
            device,
            browser,
            os,
            region
        from
            visitors
        where
            client_id = '{client_id}'
            and creation_time > '{start_time}'
            and creation_time < '{end_time}'
            
        """.format(client_id=client_id, start_time=start_time, end_time=end_time)
    mobile_records = data_store.run_custom_sql(sql)
    visitors_df = None
    if mobile_records is not None and len(mobile_records) > 0:
        visitors_df = pd.DataFrame.from_records(mobile_records)
        visitors_df.columns = ["session_id", "device", "browser", "os", "region"]
    return visitors_df


def get_sales_analytics(data_store, client_id, start_date_str, end_date_str, timezone_str):
    start_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=start_date_str,
                                                                       timezone_str=timezone_str)
    end_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=end_date_str,
                                                                     timezone_str=timezone_str)
    visitors_df = get_visitors_df(data_store, client_id, start_date, end_date)
    sales_conv_df = get_sales_conversion_df(client_id, data_store, start_date, end_date)

    result = dict()
    if visitors_df is None:
        return result

    df = visitors_df
    if sales_conv_df is None:
        df["order_id"] = 0
        df["total_order_value"] = 0.0
    else:
        sales_conv_df["order_total_price"] = sales_conv_df["variant_quantity"] * sales_conv_df["variant_price"]
        sales_conv_df = sales_conv_df.groupby('order_id').agg({
            'session_id': [('session_id', lambda x: list(x)[0])],
            'order_total_price': [('total_order_value', lambda x: sum(x))]
        })
        sales_conv_df.columns = sales_conv_df.columns.droplevel()
        sales_conv_df["order_id"] = sales_conv_df.index
        sales_conv_df.reset_index(inplace=True, drop=True)
        df = pd.merge(visitors_df, sales_conv_df,
                      how='left', left_on=["session_id"],
                      right_on=["session_id"])
        df["order_id"] = df["order_id"].fillna(0).astype(int)
        df["total_order_value"] = df["total_order_value"].fillna(0).astype(float)

    result["device"] = get_aggregate_for_column(df, "device")
    result["browser"] = get_aggregate_for_column(df, "browser")
    result["os"] = get_aggregate_for_column(df, "os")

    location_df = get_location_df()
    result["region"] = get_aggregate_for_region(df, location_df)

    return result


def get_aggregate_for_column(df, column):
    result_session_df = df.groupby(column).agg({
        'session_id': [('session_count', lambda x: len(x)), ('visitor_count', lambda x: len(set(x)))]
    })
    result_session_df.columns = result_session_df.columns.droplevel()
    result_session_df[column] = result_session_df.index
    result_session_df.reset_index(inplace=True, drop=True)
    result_conv_df = df[df["order_id"] != 0].drop_duplicates()
    result_conv_df = result_conv_df.groupby(column).agg({
        'session_id': [('sales_conversion_count', lambda x: len(set(x)))],
        'order_id': [('order_count', lambda x: len(set(x)))],
        'total_order_value': [('total_sales', lambda x: sum(x))]
    })
    result_conv_df.columns = result_conv_df.columns.droplevel()
    result_conv_df[column] = result_conv_df.index
    result_conv_df.reset_index(inplace=True, drop=True)
    result_df = pd.merge(result_session_df, result_conv_df,
                         how='left', left_on=[column],
                         right_on=[column])
    result_df["sales_conversion_count"] = result_df["sales_conversion_count"].fillna(0).astype(int)
    result_df["order_count"] = result_df["order_count"].fillna(0).astype(int)
    result_df["total_sales"] = result_df["total_sales"].fillna(0)

    result_df["sales_conversion"] = result_df["sales_conversion_count"] * 100 / (result_df["visitor_count"] + 0.01)
    result_df["sales_conversion"] = result_df["sales_conversion"].map(lambda x: min(100.00, round(x, 2)))
    result_df["avg_order_value"] = result_df["total_sales"] / (result_df["order_count"] + 0.01)
    result_df["avg_order_value"] = result_df["avg_order_value"].map(lambda x: round(x, 2))

    result = result_df.to_dict(orient="records")
    return result


def get_aggregate_for_region(df, location_df):
    column = "region"
    result_session_df = df.groupby(column).agg({
        'session_id': [('session_count', lambda x: len(x)), ('visitor_count', lambda x: len(set(x)))]
    })
    result_session_df.columns = result_session_df.columns.droplevel()
    result_session_df[column] = result_session_df.index
    result_session_df.reset_index(inplace=True, drop=True)
    result_conv_df = df[df["order_id"] != 0].drop_duplicates()
    result_conv_df = result_conv_df.groupby(column).agg({
        'session_id': [('sales_conversion_count', lambda x: len(set(x)))],
        'order_id': [('order_count', lambda x: len(set(x)))],
        'total_order_value': [('total_sales', lambda x: sum(x))]
    })
    result_conv_df.columns = result_conv_df.columns.droplevel()
    result_conv_df[column] = result_conv_df.index
    result_conv_df.reset_index(inplace=True, drop=True)
    result_df = pd.merge(result_session_df, result_conv_df,
                         how='left', left_on=[column],
                         right_on=[column])
    result_df = pd.merge(location_df, result_df,
                         how='left', left_on=["region_name"],
                         right_on=[column])

    result_df.fillna(0, inplace=True)
    result_df["sales_conversion_count"] = result_df["sales_conversion_count"].fillna(0).astype(int)
    result_df["order_count"] = result_df["order_count"].fillna(0).astype(int)
    result_df["total_sales"] = result_df["total_sales"].fillna(0)


    result_df["sales_conversion"] = result_df["sales_conversion_count"] * 100 / (result_df["visitor_count"] + 0.01)
    result_df["sales_conversion"] = result_df["sales_conversion"].map(lambda x: min(100.00, round(x, 2)))
    result_df["avg_order_value"] = result_df["total_sales"] / (result_df["order_count"] + 0.01)
    result_df["avg_order_value"] = result_df["avg_order_value"].map(lambda x: round(x, 2))

    result_df["properties"] = list(
        zip(result_df["region_name"], result_df["session_count"], result_df["visitor_count"], result_df["total_sales"],
            result_df["sales_conversion_count"], result_df["order_count"], result_df["sales_conversion"],
            result_df["avg_order_value"]))

    def create_properties(x):
        x = list(x)
        value = {"name": x[0], "session_count": x[1], "visitor_count": x[2], "total_sales": x[3],
                 "sales_conversion_count": x[4], "order_count": x[5], "sales_conversion": x[6],
                 "avg_order_value": x[7]}
        return value

    result_df["properties"] = result_df["properties"].map(create_properties)

    dic = result_df[["type", "id", "properties", "geometry"]].to_dict(orient="records")

    result = dict()
    result["type"] = "FeatureCollection"
    result["features"] = dic
    return result
