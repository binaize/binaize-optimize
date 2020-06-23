from utils.date_utils import DateUtils
import pandas as pd


def get_visitor_count_per_variation_over_time(data_store, client_id, experiment_id):
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
