import pandas as pd

from utils.date_utils import DateUtils


class ExperimentAnalytics(object):
    @classmethod
    def get_session_count_per_variation_over_time(cls, data_store, client_id, experiment_id):
        date_list = DateUtils.get_date_range_in_utc_str("Asia/Kolkata")
        sql_list = list()
        for start_date, end_date, client_date in date_list:
            sql_temp = \
                """   
                select
                    variation_id,
                    count(session_id) as num_session,
                    '{date}' as date,
                    '{start_date}' as keydate 
                from
                    events 
                where
                    creation_time > '{start_date}' 
                    and creation_time < '{end_date}' 
                    and client_id = '{client_id}' 
                    and experiment_id = '{experiment_id}'
                    and event_name = 'served' 
                group by
                    variation_id
                """.format(client_id=client_id, experiment_id=experiment_id, start_date=start_date, end_date=end_date,
                           date=client_date)

            sql_list.append(sql_temp)
        sql1 = " union ".join(sql_list)
        sql = \
            """
            select
                variation_table.variation_name,
                overview_table.variation_id,
                overview_table.num_session,
                overview_table.date,
                overview_table.keydate 
            from
                (
                    {sql1} 
                )
                as overview_table 
                left outer join
                    (
                        select
                            variation_name,
                            variation_id 
                        from
                            variations 
                        where
                            client_id = '{client_id}' 
                            and experiment_id = '{experiment_id}' 
                    )
                    as variation_table 
                    ON overview_table.variation_id = variation_table.variation_id
            """.format(sql1=sql1, client_id=client_id, experiment_id=experiment_id)

        mobile_records = data_store.run_custom_sql(sql)
        result = dict()
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = ["variation_name", "variation_id", "num_session", "date", "keydate"]
            session_df = df.sort_values(['keydate'])
            variation_ids = session_df["variation_name"].unique()
            variation_dict = dict()
            for variation_id in variation_ids:
                d = session_df[session_df["variation_name"] == variation_id]
                session_count = d["num_session"].tolist()
                variation_dict[variation_id] = session_count
            result["date"] = d["date"].tolist()
            result["session_count"] = variation_dict
        return result

    @classmethod
    def get_visitor_count_per_variation_over_time(cls, data_store, client_id, experiment_id):
        date_list = DateUtils.get_date_range_in_utc_str("Asia/Kolkata")
        sql_list = list()
        for start_date, end_date, client_date in date_list:
            sql_temp = \
                """ 
                select
                    variation_id,
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
                group by
                    variation_id
                """.format(client_id=client_id, experiment_id=experiment_id,
                           start_date=start_date, end_date=end_date, date=client_date)

            sql_list.append(sql_temp)
        sql1 = " union ".join(sql_list)
        sql = \
            """
            select
                variation_table.variation_name,
                overview_table.variation_id,
                overview_table.num_visitor,
                overview_table.date,
                overview_table.keydate 
            from
                (
                    {sql1} 
                )
                as overview_table 
                left outer join
                    (
                        select
                            variation_name,
                            variation_id 
                        from
                            variations 
                        where
                            client_id = '{client_id}' 
                            and experiment_id = '{experiment_id}'
                    )
                    as variation_table 
                    ON overview_table.variation_id = variation_table.variation_id
            """.format(sql1=sql1, client_id=client_id, experiment_id=experiment_id)
        mobile_records = data_store.run_custom_sql(sql)
        result = dict()
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = ["variation_name", "variation_id", "num_visitor", "date", "keydate"]
            session_df = df.sort_values(['keydate'])
            variation_ids = session_df["variation_name"].unique()
            variation_dict = dict()
            for variation_id in variation_ids:
                d = session_df[session_df["variation_name"] == variation_id]
                session_count = d["num_visitor"].tolist()
                variation_dict[variation_id] = session_count
            result["date"] = d["date"].tolist()
            result["visitor_count"] = variation_dict
        return result

    @classmethod
    def get_conversion_rate_per_variation_over_time(cls, data_store, client_id, experiment_id):
        date_list = DateUtils.get_date_range_in_utc_str("Asia/Kolkata")
        sql_list = list()
        for start_date, end_date, client_date in date_list:
            sql_temp = \
                """
                select
                    variation_id,
                    count(distinct(session_id)) as num_visitor,
                    event_name,
                    '{date}' as date,
                    '{start_date}' as keydate 
                from
                    events 
                where
                    creation_time > '{start_date}' 
                    and creation_time < '{end_date}' 
                    and client_id = '{client_id}' 
                    and experiment_id = '{experiment_id}' 
                group by
                    variation_id,
                    event_name
                """.format(client_id=client_id, experiment_id=experiment_id, start_date=start_date, end_date=end_date,
                           date=client_date)

            sql_list.append(sql_temp)
        sql1 = " union ".join(sql_list)
        sql = \
            """
            select
                variation_table.variation_name,
                overview_table.variation_id,
                overview_table.num_visitor,
                overview_table.event_name,
                overview_table.date,
                overview_table.keydate 
            from
                (
                    {sql1} 
                )
                as overview_table 
                left outer join
                    (
                        select
                            variation_name,
                            variation_id 
                        from
                            variations 
                        where
                            client_id = '{client_id}' 
                            and experiment_id = '{experiment_id}'
                    )
                    as variation_table 
                    ON overview_table.variation_id = variation_table.variation_id
            """.format(sql1=sql1, client_id=client_id, experiment_id=experiment_id)
        mobile_records = data_store.run_custom_sql(sql)
        result = dict()
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = ["variation_name", "variation_id", "num_session", "event_name", "date", "keydate"]
            conversion_df = df.sort_values(['keydate'])
            served_df = conversion_df[conversion_df["event_name"] == "served"].copy()
            clicked_df = conversion_df[conversion_df["event_name"] == "clicked"].copy()
            conversion_df = pd.merge(served_df, clicked_df, on=["variation_id", "date", "variation_name"])
            conversion_df["conversion"] = conversion_df["num_session_y"] / conversion_df["num_session_x"]
            conversion_df["conversion"] = conversion_df["conversion"].map(lambda x: min(100.00, round(x * 100, 2)))
            variation_ids = conversion_df["variation_name"].unique()
            variation_dict = dict()
            for variation_id in variation_ids:
                d = conversion_df[conversion_df["variation_name"] == variation_id]
                temp_df = pd.DataFrame({"date": df["date"].unique().tolist()})
                d = pd.merge(d, temp_df, how="right", on=["date"])
                d["conversion"] = d["conversion"].fillna(0.0)
                session_count = d["conversion"].tolist()
                variation_dict[variation_id] = session_count
                result["date"] = d["date"].tolist()
                result["conversion"] = variation_dict
        return result

    @classmethod
    def get_conversion_rate_of_experiment(cls, data_store, client_id, experiment_id):
        sql = \
            """
            select
                variation_table.variation_name,
                overview_table.variation_id,
                overview_table.num_session,
                overview_table.num_visitor,
                overview_table.num_conversion,
                overview_table.conversion 
            from
                (
                    select
                        table1.variation_id,
                        table2.num_session,
                        table2.num_visitor,
                        table1.num_conversion,
                        cast(table1.num_conversion as float) / table2.num_visitor as conversion 
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
                )
                as overview_table 
                left outer join
                    (
                        select
                            variation_name,
                            variation_id 
                        from
                            variations 
                        where
                            client_id = '{client_id}' 
                            and experiment_id = '{experiment_id}'
                    )
                    as variation_table 
                    ON overview_table.variation_id = variation_table.variation_id
            """.format(client_id=client_id, experiment_id=experiment_id)

        mobile_records = data_store.run_custom_sql(sql)
        result = {}
        if mobile_records is not None and len(mobile_records) > 0:
            goal_conv_df = pd.DataFrame.from_records(mobile_records)
            goal_conv_df.columns = ["variation_name", "variation_id", "num_session", "num_visitor",
                                    "goal_conversion_count",
                                    "goal_conversion"]
            goal_conv_df["goal_conversion"] = goal_conv_df["goal_conversion"].map(
                lambda x: min(100.00, round(x * 100, 2)))

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
                sales_conv_df = sales_conv_df.groupby('variation_id').agg({
                    'variant_quantity': [('sales_conversion_count', lambda x: x.sum())]
                })
                sales_conv_df.columns = sales_conv_df.columns.droplevel()
                sales_conv_df["variation_id"] = sales_conv_df.index
                sales_conv_df.reset_index(inplace=True, drop=True)

            if sales_conv_df is None:
                goal_conv_df["sales_conversion"] = pd.Series([0.00 for i in range(len(goal_conv_df))])
                goal_conv_df["sales_conversion_count"] = pd.Series([0 for i in range(len(goal_conv_df))])
            else:
                goal_conv_df = pd.merge(goal_conv_df, sales_conv_df, how="left", on="variation_id")
                goal_conv_df.fillna(value=0.0, inplace=True)
                goal_conv_df["sales_conversion_count"] = goal_conv_df["sales_conversion_count"].astype(int)
                goal_conv_df["sales_conversion"] = goal_conv_df["sales_conversion_count"] * 100 / goal_conv_df[
                    "num_visitor"]
                goal_conv_df["sales_conversion"] = goal_conv_df["sales_conversion"].map(
                    lambda x: min(100.00, round(x, 2)))

            result = goal_conv_df.to_dict(orient="records")
        return result

    @classmethod
    def get_summary_of_experiment(cls, data_store, client_id, experiment_id):
        yo = cls.get_conversion_rate_of_experiment(data_store=data_store, client_id=client_id,
                                                   experiment_id=experiment_id)
        df = pd.DataFrame(yo, columns=["variation_name", "variation_id", "num_session", "num_visitor",
                                       "goal_conversion_count", "goal_conversion", "sales_conversion_count",
                                       "sales_conversion"])
        sql = \
            """
                select
                    max(creation_time),
                    min(creation_time)
                from
                    events 
                where
                    client_id = '{client_id}' 
                    and experiment_id = '{experiment_id}'
            """.format(client_id=client_id, experiment_id=experiment_id)
        records = data_store.run_custom_sql(sql)
        if records[0][0] is None or records[0][0] is None:
            result = dict()
            result["status"] = "<strong> SUMMARY : </strong> Not enough visitors on the website."
            result["conclusion"] = "<strong> STATUS : </strong> Not enough visitors on the website."
            result["recommendation"] = "<strong> RECOMMENDATION : </strong> <strong> CONTINUE </strong> the Experiment."
            return result
        delta = records[0][0] - records[0][1]
        variation_names = df["variation_name"].tolist()
        visitor_converted = df["goal_conversion_count"].tolist()
        visitor_count = df["num_visitor"].tolist()
        num_days = delta.days

        if len(variation_names) == 0:
            result = dict()
            result["status"] = "<strong> SUMMARY : </strong> Not enough visitors on the website."
            result["conclusion"] = "<strong> STATUS : </strong> Not enough visitors on the website."
            result["recommendation"] = "<strong> RECOMMENDATION : </strong> <strong> CONTINUE </strong> the Experiment."
            return result

        from optimization_platform.src.optim.abtest import ABTest
        ab = ABTest(arm_name_list=variation_names, conversion_count_list=visitor_converted,
                    session_count_list=visitor_count, num_days=num_days)

        best_variation = ab.get_best_arm()
        confidence = ab.get_best_arm_confidence()
        confidence_percentage = round(confidence * 100, 2)
        betterness_score = ab.get_betterness_score()
        betterness_percentage = round(betterness_score * 100, 2)
        remaining_sample_size = ab.get_estimated_sample_size()
        remaining_time = ab.get_remaining_time()
        remaining_days = int(remaining_time) + 1

        status = "<strong> SUMMARY : </strong><span style = 'color: blue; font-size: 16px;'><strong> {variation} </strong></span> is winning." \
                 " It is <span style = 'color: blue; font-size: 16px;'><strong> {betterness_percentage}% </strong></span> better than the others.".format(
            variation=best_variation,
            betterness_percentage=betterness_percentage)

        conclusion = "<strong> STATUS : </strong> There is <span style = 'color: red; font-size: 16px;'><strong> NOT ENOUGH</strong></span>" \
                     " evidence to conclude the experiment " \
                     "(It is <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> yet" \
                     " statistically significant)." \
                     "To be statistically confident, we need <strong> {remaining_sample_size} </strong> more visitors." \
                     "Based on recent visitor trend, experiment should run for another <strong> {remaining_days} </strong> days.".format(
            remaining_sample_size=remaining_sample_size, remaining_days=remaining_days)
        recommendation = "<strong> RECOMMENDATION : </strong> <span style = 'color: blue; font-size: 16px;'><strong>  CONTINUE </strong></span> the Experiment."
        if remaining_sample_size < 0:
            conclusion = "<strong> STATUS : </strong> There is <span style = 'color: green; font-size: 16px;'><strong> ENOUGH </strong></span>" \
                         " evidence to conclude the experiment. " \
                         "There is <span style = 'color: red; font-size: 16px;'><strong> NO CLEAR WINNER </strong></span>. " \
                         "We are <span style = 'color: red; font-size: 16px;'><strong> {confidence_percentage}%" \
                         " </strong></span> confident that <span style = 'color: blue; font-size: 16px;'><strong> {variation} </strong></span> " \
                         "is the best.".format(confidence_percentage=confidence_percentage, variation=best_variation)
            recommendation = "<strong> RECOMMENDATION : </strong> <span style = 'color: green; font-size: 16px;'><strong>  STOP </strong></span> the Experiment."
        if confidence > 0.95:
            conclusion = "<strong> STATUS : </strong> There is <span style = 'color: green; font-size: 16px;'> <strong> ENOUGH </strong></span>" \
                         " evidence to conclude the experiment. " \
                         "We have a <span style = 'color: green; font-size: 16px;'><strong> WINNER </strong></span>. " \
                         "We are <span style = 'color: green; font-size: 16px;'><strong> {confidence_percentage}% </strong></span> confident " \
                         "that <span style = 'color: blue; font-size: 16px;'><strong> {variation} </strong></span> is the best.".format(
                confidence_percentage=confidence_percentage, variation=best_variation)
            recommendation = "<strong> RECOMMENDATION : </strong> <span style = 'color: green; font-size: 16px;'><strong>  STOP </strong></span> the Experiment."

            # <span style = 'color: #388e3c; font-size: 16px;'> SUMMARY </span>

        result = dict()
        result["status"] = status
        result["conclusion"] = conclusion
        result["recommendation"] = recommendation
        return result
