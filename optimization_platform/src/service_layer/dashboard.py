import pandas as pd

from utils.date_utils import get_date_range_in_utc_str


def get_session_count_per_variation_over_time(data_store, client_id, experiment_id):
    date_list = get_date_range_in_utc_str("Asia/Kolkata")
    sql_list = list()
    for start_date, end_date, client_date in date_list:
        sql_temp = """ select
                      variation_id, count(session_id) as num_session, '{date}' as date, '{start_date}' as keydate
                  from events 
                  where
                      creation_time  > '{start_date}' and
                      creation_time < '{end_date}' and
                      client_id = '{client_id}' and
                      experiment_id = '{experiment_id}'
                  group by 
                      variation_id  """.format(client_id=client_id, experiment_id=experiment_id,
                                               start_date=start_date, end_date=end_date, date=client_date)

        sql_list.append(sql_temp)
    sql1 = " union ".join(sql_list)
    sql = "select variation_table.variation_name, overview_table.variation_id, overview_table.num_session, overview_table.date, overview_table.keydate from ( " + sql1 + """ ) as overview_table left outer join (select variation_name, variation_id from variations where 
                  client_id = '{client_id}' and
                  experiment_id = '{experiment_id}') as variation_table
                  ON overview_table.variation_id = variation_table.variation_id """.format(client_id=client_id,
                                                                                           experiment_id=experiment_id)
    mobile_records = data_store.run_sql(sql)
    df = pd.DataFrame.from_records(mobile_records)
    df.columns = ["variation_name", "variation_id", "num_session", "date", "keydate"]
    session_df = df.sort_values(['keydate'])
    variation_ids = session_df["variation_name"].unique()
    variation_dict = dict()
    for variation_id in variation_ids:
        d = session_df[session_df["variation_name"] == variation_id]
        session_count = d["num_session"].tolist()
        variation_dict[variation_id] = session_count
    result = dict()
    result["date"] = d["date"].tolist()
    result["session_count"] = variation_dict
    return result


def get_visitor_count_per_variation_over_time(data_store, client_id, experiment_id):
    date_list = get_date_range_in_utc_str("Asia/Kolkata")
    sql_list = list()
    for start_date, end_date, client_date in date_list:
        sql_temp = """ select
                      variation_id, count(distinct(session_id)) as num_visitor, '{date}' as date, '{start_date}' as keydate
                  from events 
                  where
                      creation_time  > '{start_date}' and
                      creation_time < '{end_date}' and
                      client_id = '{client_id}' and
                      experiment_id = '{experiment_id}'
                  group by 
                      variation_id  """.format(client_id=client_id, experiment_id=experiment_id,
                                               start_date=start_date, end_date=end_date, date=client_date)

        sql_list.append(sql_temp)
    sql1 = " union ".join(sql_list)
    sql = "select variation_table.variation_name, overview_table.variation_id, overview_table.num_visitor, overview_table.date, overview_table.keydate from ( " + sql1 + """ ) as overview_table left outer join (select variation_name, variation_id from variations where 
                      client_id = '{client_id}' and
                      experiment_id = '{experiment_id}') as variation_table
                      ON overview_table.variation_id = variation_table.variation_id """.format(client_id=client_id,
                                                                                               experiment_id=experiment_id)
    mobile_records = data_store.run_sql(sql)
    df = pd.DataFrame.from_records(mobile_records)
    df.columns = ["variation_name", "variation_id", "num_visitor", "date", "keydate"]
    session_df = df.sort_values(['keydate'])
    variation_ids = session_df["variation_name"].unique()
    variation_dict = dict()
    for variation_id in variation_ids:
        d = session_df[session_df["variation_name"] == variation_id]
        session_count = d["num_visitor"].tolist()
        variation_dict[variation_id] = session_count
    result = dict()
    result["date"] = d["date"].tolist()
    result["visitor_count"] = variation_dict
    return result


def get_conversion_rate_per_variation_over_time(data_store, client_id, experiment_id):
    date_list = get_date_range_in_utc_str("Asia/Kolkata")
    sql_list = list()
    for start_date, end_date, client_date in date_list:
        sql_temp = """ select
                      variation_id, count(session_id) as num_session, event_name,
                      '{date}' as date, '{start_date}' as keydate
                  from events 
                  where
                      creation_time  > '{start_date}' and
                      creation_time < '{end_date}' and
                      client_id = '{client_id}' and
                      experiment_id = '{experiment_id}' 
                  group by 
                      variation_id,event_name  """.format(client_id=client_id,
                                                          experiment_id=experiment_id,
                                                          start_date=start_date, end_date=end_date, date=client_date)

        sql_list.append(sql_temp)
    sql1 = " union ".join(sql_list)
    sql = "select variation_table.variation_name, overview_table.variation_id, overview_table.num_session, overview_table.event_name, overview_table.date, overview_table.keydate from ( " + sql1 + """ ) as overview_table left outer join (select variation_name, variation_id from variations where 
                          client_id = '{client_id}' and
                          experiment_id = '{experiment_id}') as variation_table
                          ON overview_table.variation_id = variation_table.variation_id """.format(client_id=client_id,
                                                                                                   experiment_id=experiment_id)
    mobile_records = data_store.run_sql(sql)
    df = pd.DataFrame.from_records(mobile_records)
    df.columns = ["variation_name", "variation_id", "num_session", "event_name", "date", "keydate"]
    conversion_df = df.sort_values(['keydate'])
    served_df = conversion_df[conversion_df["event_name"] == "served"].copy()
    clicked_df = conversion_df[conversion_df["event_name"] == "clicked"].copy()
    conversion_df = pd.merge(served_df, clicked_df, on=["variation_id", "date","variation_name"])
    conversion_df["conversion"] = conversion_df["num_session_y"] / conversion_df["num_session_x"]
    conversion_df["conversion"] = conversion_df["conversion"].map(lambda x: round(x, 2))
    variation_ids = conversion_df["variation_name"].unique()
    variation_dict = dict()
    for variation_id in variation_ids:
        d = conversion_df[conversion_df["variation_name"] == variation_id]
        session_count = d["conversion"].tolist()
        variation_dict[variation_id] = session_count
    result = dict()
    result["date"] = d["date"].tolist()
    result["conversion"] = variation_dict
    return result


def get_conversion_rate_of_experiment(data_store, client_id, experiment_id):
    sql = """ select variation_table.variation_name,overview_table.variation_id, overview_table.num_session, overview_table.num_visitor, 
            overview_table.num_conversion , overview_table.conversion
            from (select 
            table1.variation_id, table2.num_session, table2.num_visitor, 
            table1.num_conversion , cast(table1.num_conversion as float)/table2.num_visitor as conversion
            from (select
              variation_id, count(distinct(session_id)) as num_conversion
          from events 
          where
              client_id = '{client_id}' and
              experiment_id = '{experiment_id}' and
              event_name = 'clicked'
          group by 
              variation_id) table1
              join (select
              variation_id, count(session_id) as num_session, count(distinct(session_id)) as num_visitor
          from events 
          where
              client_id = '{client_id}' and
              experiment_id = '{experiment_id}'
          group by 
              variation_id) table2 on (table1.variation_id=table2.variation_id)) as overview_table
              left outer join (select variation_name, variation_id from variations where 
              client_id = '{client_id}' and
              experiment_id = '{experiment_id}') as variation_table
              ON overview_table.variation_id = variation_table.variation_id""".format(client_id=client_id,
                                                                                      experiment_id=experiment_id)

    mobile_records = data_store.run_sql(sql=sql)
    df = pd.DataFrame.from_records(mobile_records)
    df.columns = df.columns = ["variation_name", "variation_id", "num_session", "num_visitor", "visitor_converted",
                               "conversion"]
    df["conversion"] = df["conversion"].map(lambda x: round(x, 2))
    result = df.to_dict(orient="records")
    return result
