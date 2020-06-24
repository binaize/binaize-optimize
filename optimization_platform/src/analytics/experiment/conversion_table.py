import pandas as pd


def get_sales_conversion_df(client_id, data_store, experiment_id):
    sql = \
        """ select distinct
                cookie_order_table.session_id,
                events_table.variation_id,
                cookie_order_table.cart_token,
                cookie_order_table.variant_id,
                cookie_order_table.variant_quantity
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
                        ) events_table
                        on (cookie_order_table.session_id = events_table.session_id)
        """.format(client_id=client_id, experiment_id=experiment_id)
    mobile_records = data_store.run_custom_sql(sql)
    sales_conv_df = None
    if mobile_records is not None and len(mobile_records) > 0:
        sales_conv_df = pd.DataFrame.from_records(mobile_records)
        sales_conv_df.columns = ["session_id", "variation_id", "cart_token", "variant_id",
                                 "variant_quantity"]
    return sales_conv_df


def get_events_df(data_store, experiment_id, client_id):
    sql = \
        """
        select
            table1.variation_id,
            table2.num_session,
            table2.num_visitor,
            table1.num_conversion
        from
            (
                select
                    variation_id,
                    count(distinct(session_id)) as num_conversion 
                from
                    events 
                where
                    client_id = '{client_id}' 
                    and experiment_id = '{experiment_id}' 
                    and event_name = 'clicked' 
                group by
                    variation_id
            )
            table1 
            join
                (
                    select
                        variation_id,
                        count(session_id) as num_session,
                        count(distinct(session_id)) as num_visitor 
                    from
                        events 
                    where
                        client_id = '{client_id}' 
                        and experiment_id = '{experiment_id}' 
                        and event_name = 'served'
                    group by
                        variation_id
                )
                table2 
                on (table1.variation_id = table2.variation_id)
        """.format(client_id=client_id, experiment_id=experiment_id)
    mobile_records = data_store.run_custom_sql(sql)
    events_df = None
    if mobile_records is not None and len(mobile_records) > 0:
        events_df = pd.DataFrame.from_records(mobile_records)
        events_df.columns = ["variation_id", "num_session", "num_visitor",
                             "goal_conversion_count"]
    return events_df


def get_variations_df(data_store, experiment_id, client_id):
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


def get_conversion_table_of_experiment(data_store, client_id, experiment_id):
    events_df = get_events_df(data_store, experiment_id, client_id)
    variations_df = get_variations_df(data_store, experiment_id, client_id)
    sales_conv_df = get_sales_conversion_df(client_id, data_store, experiment_id)

    result = dict()
    if variations_df is None:
        return result

    variation_events_df = variations_df
    if events_df is None:
        variation_events_df["num_session"] = 0
        variation_events_df["num_visitor"] = 0
        variation_events_df["goal_conversion_count"] = 0
    else:
        variation_events_df = pd.merge(variations_df, events_df,
                                       how='left', left_on=["variation_id"],
                                       right_on=["variation_id"])
        variation_events_df["num_session"] = variation_events_df["num_session"].fillna(0).astype(int)
        variation_events_df["num_visitor"] = variation_events_df["num_visitor"].fillna(0).astype(int)
        variation_events_df["goal_conversion_count"] = variation_events_df["goal_conversion_count"].fillna(0).astype(
            int)

    df = variation_events_df
    if sales_conv_df is None:
        df["sales_conversion_count"] = 0
    else:
        sales_conv_df = sales_conv_df.groupby('variation_id').agg({
            'variant_quantity': [('sales_conversion_count', lambda x: x.sum())]
        })
        sales_conv_df.columns = sales_conv_df.columns.droplevel()
        sales_conv_df["variation_id"] = sales_conv_df.index
        sales_conv_df.reset_index(inplace=True, drop=True)
        df = pd.merge(variation_events_df, sales_conv_df,
                      how='left', left_on=["variation_id"],
                      right_on=["variation_id"])
        df["sales_conversion_count"] = df["sales_conversion_count"].fillna(0).astype(int)

    df["goal_conversion"] = df["goal_conversion_count"] * 100 / (df["num_visitor"] + 0.01)
    df["goal_conversion"] = df["goal_conversion"].map(lambda x: min(100.00, round(x, 2)))
    df["sales_conversion"] = df["sales_conversion_count"] * 100 / (df["num_visitor"] + 0.01)
    df["sales_conversion"] = df["sales_conversion"].map(lambda x: min(100.00, round(x, 2)))

    result = df.to_dict(orient="records")
    return result
