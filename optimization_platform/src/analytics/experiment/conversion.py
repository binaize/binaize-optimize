import pandas as pd

from utils.date_utils import DateUtils


def get_events_df(client_id, data_store, date_range_list, experiment_id, event_name):
    sql_list = list()
    for start_date, end_date, client_date in date_range_list:
        sql_temp = \
            """
            select
                variation_id,
                count(session_id) as num_session,
                count(distinct(session_id)) as num_visitor,
                '{date}' as date,
                '{start_date}' as keydate
            from
                events
            where
                creation_time > '{start_date}'
                and creation_time < '{end_date}'
                and client_id = '{client_id}'
                and experiment_id = '{experiment_id}'
                and event_name = '{event_name}'
            group by
                variation_id
            """.format(client_id=client_id, experiment_id=experiment_id, start_date=start_date,
                       end_date=end_date,
                       date=client_date, event_name=event_name)

        sql_list.append(sql_temp)
    sql = " union ".join(sql_list)
    mobile_records = data_store.run_custom_sql(sql)
    events_df = None
    if mobile_records is not None and len(mobile_records) > 0:
        events_df = pd.DataFrame.from_records(mobile_records)
        events_df.columns = ["variation_id", "session_count", "visitor_count", "date", "iso_date"]
    return events_df


def get_variations_df(data_store, client_id, experiment_id):
    sql = \
        """
            select
                variation_name,
                variation_id
            from
                variations
            where
                client_id = '{client_id}'
                and experiment_id = '{experiment_id}'
        """.format(client_id=client_id, experiment_id=experiment_id)
    mobile_records = data_store.run_custom_sql(sql)
    variations_df = None
    if mobile_records is not None and len(mobile_records) > 0:
        variations_df = pd.DataFrame.from_records(mobile_records)
        variations_df.columns = ["variation_name", "variation_id"]
    return variations_df


def get_sales_conversion_df(client_id, data_store, date_range_list, experiment_id):
    sql_list = list()
    for start_date, end_date, client_date in date_range_list:
        sql_temp = \
            """ select distinct
                    cookie_order_table.session_id,
                    events_table.variation_id,
                    cookie_order_table.cart_token,
                    cookie_order_table.variant_id,
                    cookie_order_table.variant_quantity,
                    '{date}' as date,
                    '{start_date}' as keydate
                from
                (select
                        table1.session_id,
                        table2.cart_token,
                        table2.variant_id,
                        table2.variant_quantity
                    from
                        (
                        select
                            session_id,
                            cart_token
                        from
                            cookies
                        where
                            client_id = '{client_id}'
                            and creation_time > '{start_date}'
                            and creation_time < '{end_date}'
                        )
                        table1
                        inner join
                            (
                            select
                                variant_id,
                                variant_quantity,
                                cart_token
                            from
                                orders
                            where
                                client_id = '{client_id}'
                                and payment_status = True
                                and updated_at > '{start_date}'
                                and updated_at < '{end_date}'
                            )
                            table2
                            on (table1.cart_token = table2.cart_token)) cookie_order_table
                            left join(
                            select
                                variation_id,
                                session_id
                            from events
                            where
                                client_id = '{client_id}'
                                and experiment_id = '{experiment_id}'
                                and creation_time > '{start_date}'
                                and creation_time < '{end_date}'
                            ) events_table
                            on (cookie_order_table.session_id = events_table.session_id)
            """.format(client_id=client_id, experiment_id=experiment_id, start_date=start_date,
                       end_date=end_date,
                       date=client_date)
        sql_list.append(sql_temp)
    sql = " union ".join(sql_list)
    mobile_records = data_store.run_custom_sql(sql)
    sales_conv_df = None
    if mobile_records is not None and len(mobile_records) > 0:
        sales_conv_df = pd.DataFrame.from_records(mobile_records)
        sales_conv_df.columns = ["session_id", "variation_id", "cart_token", "variant_id",
                                 "variant_quantity", "date", "iso_date"]
        sales_conv_df = sales_conv_df[sales_conv_df['variation_id'].notna()]
        if len(sales_conv_df) == 0:
            sales_conv_df = None

    return sales_conv_df


def get_conversion_per_variation_over_time(data_store, client_id, experiment_id, timezone_str):
    date_range_list = DateUtils.get_date_range_in_utc_str(timezone_str)
    date_list = [x[-1] for x in date_range_list]
    iso_date_list = [x[0] for x in date_range_list]

    date_df = pd.DataFrame({"date": date_list, "iso_date": iso_date_list})
    served_df = get_events_df(client_id, data_store, date_range_list, experiment_id, "served")
    clicked_df = get_events_df(client_id, data_store, date_range_list, experiment_id, "clicked")
    sales_df = get_sales_conversion_df(client_id, data_store, date_range_list, experiment_id)
    variations_df = get_variations_df(data_store, client_id, experiment_id)
    result = dict()
    if variations_df is None:
        return result

    date_variations_df = pd.merge(date_df.assign(key=1), variations_df.assign(key=1), on='key').drop('key', 1)
    date_served_df = date_variations_df
    if served_df is None:
        date_served_df["visitor_count"] = 0
        date_served_df["session_count"] = 0

    else:
        date_served_df = pd.merge(date_variations_df, served_df,
                                  how='left', left_on=["date", "iso_date", "variation_id"],
                                  right_on=["date", "iso_date", "variation_id"])
        date_served_df["visitor_count"] = date_served_df["visitor_count"].fillna(0).astype(int)
        date_served_df["session_count"] = date_served_df["session_count"].fillna(0).astype(int)

    date_served_clicked_df = date_served_df
    if clicked_df is None:
        date_served_clicked_df["goal_conversion_count"] = 0
    else:
        clicked_df.drop(columns=["session_count"], inplace=True)
        clicked_df.rename(columns={'visitor_count': 'goal_conversion_count'}, inplace=True)
        date_served_clicked_df = pd.merge(date_served_df, clicked_df,
                                          how='left', left_on=["date", "iso_date", "variation_id"],
                                          right_on=["date", "iso_date", "variation_id"])
        date_served_clicked_df["goal_conversion_count"] = date_served_clicked_df["goal_conversion_count"].fillna(
            0).astype(int)

    df = date_served_clicked_df
    if sales_df is None:
        df["sales_conversion_count"] = 0
    else:
        sales_df = sales_df.groupby(["date", "iso_date", "variation_id"]).agg({
            'session_id': [('sales_conversion_count', lambda x: len(set(x)))]
        })
        sales_df.columns = sales_df.columns.droplevel()
        sales_df.reset_index(inplace=True)
        df = pd.merge(date_served_clicked_df, sales_df,
                      how='left', left_on=["date", "iso_date", "variation_id"],
                      right_on=["date", "iso_date", "variation_id"])
        df["sales_conversion_count"] = df["sales_conversion_count"].fillna(0).astype(int)

    df["non_goal_conversion_count"] = df["visitor_count"] - df["goal_conversion_count"]
    df["non_goal_conversion_count"] = df["non_goal_conversion_count"].map(lambda x: 0 if x < 0 else x)
    df["visitor_count"] = df["non_goal_conversion_count"] + df["goal_conversion_count"]
    df["non_sales_conversion_count"] = df["visitor_count"] - df["sales_conversion_count"]
    df["non_sales_conversion_count"] = df["non_sales_conversion_count"].map(lambda x: 0 if x < 0 else x)
    df["visitor_count"] = df["non_sales_conversion_count"] + df["sales_conversion_count"]
    df["adjusted_visitor_count"] = df["visitor_count"].map(lambda x: x + 0.01 if x == 0 else x)
    df["sales_conversion_percentage"] = df["sales_conversion_count"] * 100 / df["adjusted_visitor_count"]
    df["sales_conversion_percentage"] = df["sales_conversion_percentage"].map(lambda x: round(x, 2))
    df["goal_conversion_percentage"] = df["goal_conversion_count"] * 100 / (df["adjusted_visitor_count"])
    df["goal_conversion_percentage"] = df["goal_conversion_percentage"].map(lambda x: round(x, 2))

    variation_names = df["variation_name"].unique()
    variation_session_count_dict = dict()
    variation_visitor_count_dict = dict()
    variation_goal_conversion_count_dict = dict()
    variation_goal_conversion_percentage_dict = dict()
    variation_sales_conversion_count_dict = dict()
    variation_sales_conversion_percentage_dict = dict()
    for variation_name in variation_names:
        variation_df = df[df["variation_name"] == variation_name].copy()
        variation_df.sort_values(['iso_date'], inplace=True)
        variation_session_count_dict[variation_name] = variation_df["session_count"].tolist()
        variation_visitor_count_dict[variation_name] = variation_df["visitor_count"].tolist()
        variation_goal_conversion_count_dict[variation_name] = variation_df["goal_conversion_count"].tolist()
        variation_goal_conversion_percentage_dict[variation_name] = variation_df["goal_conversion_percentage"].tolist()
        variation_sales_conversion_count_dict[variation_name] = variation_df["sales_conversion_count"].tolist()
        variation_sales_conversion_percentage_dict[variation_name] = variation_df[
            "sales_conversion_percentage"].tolist()
    result["date"] = date_list
    result["session_count"] = variation_session_count_dict
    result["visitor_count"] = variation_visitor_count_dict
    result["goal_conversion_count"] = variation_goal_conversion_count_dict
    result["goal_conversion_percentage"] = variation_goal_conversion_percentage_dict
    result["sales_conversion_count"] = variation_sales_conversion_count_dict
    result["sales_conversion_percentage"] = variation_sales_conversion_percentage_dict
    return result
