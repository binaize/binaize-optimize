import pandas as pd

from utils.data_store.rds_data_store import RDSDataStore
from utils.date_utils import DateUtils


# noinspection SqlResolve
class DashboardAgent(object):
    @classmethod
    def get_session_count_per_variation_over_time(cls, data_store: RDSDataStore, client_id, experiment_id):
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
            conversion_df["conversion"] = conversion_df["conversion"].map(lambda x: round(x, 2))
            variation_ids = conversion_df["variation_name"].unique()
            variation_dict = dict()
            for variation_id in variation_ids:
                d = conversion_df[conversion_df["variation_name"] == variation_id]
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
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = ["variation_name", "variation_id", "num_session", "num_visitor",
                          "visitor_converted",
                          "conversion"]
            df["conversion"] = df["conversion"].map(lambda x: round(x, 2))
            result = df.to_dict(orient="records")
        return result

    @classmethod
    def get_shop_funnel_analytics(cls, data_store, client_id):
        sql = \
            """
                select '1' as id, 'Home Page' as event,count(distinct(session_id)) as blah
                    from visits
                where 
                    client_id = '{client_id}' and
                    event_name = 'home'
                union
                (select '2' as id, 'Collection Page' as event,count(distinct(session_id)) as blahblah
                    from visits
                where 
                    client_id = '{client_id}' and
                    event_name = 'collection')
                union
                (select '3' as id, 'Product Page' as event,count(distinct(session_id)) as blahblahd
                    from visits
                where 
                    client_id = '{client_id}' and
                    event_name = 'product')
                union
                (select '4' as id, 'Cart Page' as event,count(distinct(session_id)) as blahblahdd
                    from visits
                where 
                    client_id = '{client_id}' and
                    event_name = 'cart')
                union
                (select '5' as id, 'Checkout Page' as event,count(distinct(session_id)) as blahblah
                    from visits
                where 
                    client_id = '{client_id}' and
                    event_name = 'checkout')
                union
                (select '6' as id, 'Purchase' as event,count(distinct(session_id))/3 as blahblah
                    from visits
                where 
                    client_id = '{client_id}' and
                    event_name = 'checkout')

            """.format(client_id=client_id)
        mobile_records = data_store.run_custom_sql(sql)
        result = {}
        temp_dict = dict()
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = ["id", "pages", "count"]
            df = df.sort_values(['id'])
            df["percentage"] = df["count"] * 100 / (max(df["count"]) + 1)
            df["percentage"] = df["percentage"].map(lambda x: round(x, 2))
            result["pages"] = df["pages"].tolist()
            temp_dict["count"] = df["count"].tolist()
            temp_dict["percentage"] = df["percentage"].tolist()
        result["shop_funnel"] = temp_dict
        return result

    @classmethod
    def get_product_conversion_analytics(cls, data_store, client_id):
        result = {
            "products": [
                "Tissot T Race",
                "Tissot T Classic",
                "Tissot T Sport",
                "Tissot 1853",
                "Ordinary Watch",
                "Titan Classic Watch",
                "IWC Watch"
            ],
            "product_conversion": {
                "visitor_count": [
                    1156,
                    900,
                    600,
                    1456,
                    800,
                    500,
                    760
                ],
                "convertion_count": [
                    20,
                    12,
                    37,
                    29,
                    9,
                    13,
                    11

                ],
                "convertion_percentage": [
                    1.78,
                    1.33,
                    6.12,
                    1.99,
                    1.12,
                    2.41,
                    1.44

                ]
            }
        }
        return result

    @classmethod
    def get_landing_page_analytics(cls, data_store, client_id):
        result = {
            "pages": [
                "Home Page",
                "Product Page",
                "Blog Page"
            ],
            "landing_conversion": {
                "convertion_percentage": [
                    4.32,
                    5.34,
                    2.28
                ]
            }
        }
        return result