import pandas as pd

from utils.date_utils import get_date_range_in_utc_str


def get_session_count_per_variation(data_store, client_id, experiment_id):
    df_list = list()
    date_list = get_date_range_in_utc_str("Asia/Kolkata")
    for start_date, end_date, client_date in date_list:
        sql = """ select
                      variation_id, count(session_id) as num_session 
                  from events 
                  where
                      creation_time  > '{start_date}' and
                      creation_time < '{end_date}' and
                      client_id = '{client_id}' and
                      experiment_id = '{experiment_id}'
                  group by 
                      variation_id  """.format(client_id=client_id, experiment_id=experiment_id, start_date=start_date,
                                               end_date=end_date)
        mobile_records = data_store.run_sql(sql=sql)
        df = pd.DataFrame.from_records(mobile_records)
        df.columns = ["variation_id", "num_session"]
        df["date"] = client_date
        df_list.append(df)
    session_df = pd.concat(df_list, axis=0)
    variation_ids = session_df["variation_id"].unique()
    variation_dict = dict()
    for variation_id in variation_ids:
        d = session_df[session_df["variation_id"] == variation_id]
        session_count = d["num_session"].tolist()
        variation_dict[variation_id] = session_count
    result = dict()
    result["date"] = d["date"].tolist()
    result["session_count"] = variation_dict
    return result


def get_visitor_count_per_variation(data_store, client_id, experiment_id):
    df_list = list()
    date_list = get_date_range_in_utc_str("Asia/Kolkata")
    for start_date, end_date, client_date in date_list:
        sql = """ select
                      variation_id, count(distinct(session_id)) as num_visitor 
                  from events 
                  where
                      creation_time  > '{start_date}' and
                      creation_time < '{end_date}' and
                      client_id = '{client_id}' and
                      experiment_id = '{experiment_id}'
                  group by 
                      variation_id  """.format(client_id=client_id, experiment_id=experiment_id, start_date=start_date,
                                               end_date=end_date)
        mobile_records = data_store.run_sql(sql=sql)
        df = pd.DataFrame.from_records(mobile_records)
        df.columns = ["variation_id", "num_visitor"]
        df["date"] = client_date
        df_list.append(df)
    session_df = pd.concat(df_list, axis=0)
    variation_ids = session_df["variation_id"].unique()
    variation_dict = dict()
    for variation_id in variation_ids:
        d = session_df[session_df["variation_id"] == variation_id]
        session_count = d["num_visitor"].tolist()
        variation_dict[variation_id] = session_count
    result = dict()
    result["date"] = d["date"].tolist()
    result["visitor_count"] = variation_dict
    return result


def get_conversion_rate_per_variation(data_store, client_id, experiment_id):
    df_list = list()
    date_list = get_date_range_in_utc_str("Asia/Kolkata")
    for start_date, end_date, client_date in date_list:
        sql = """ select
                          variation_id, count(session_id) as num_session, event_name
                      from events 
                      where
                          creation_time  > '{start_date}' and
                          creation_time < '{end_date}' and
                          client_id = '{client_id}' and
                          experiment_id = '{experiment_id}'
                      group by 
                          variation_id, event_name""".format(client_id=client_id, experiment_id=experiment_id,
                                                              start_date=start_date,
                                                              end_date=end_date)
        mobile_records = data_store.run_sql(sql=sql)
        df = pd.DataFrame.from_records(mobile_records)
        df.columns = ["variation_id", "num_session", "event_name"]
        df["date"] = client_date
        df_list.append(df)
    conversion_df = pd.concat(df_list, axis=0)
    served_df = conversion_df[conversion_df["event_name"] == "served"].copy()
    clicked_df = conversion_df[conversion_df["event_name"] == "clicked"].copy()
    conversion_df = pd.merge(served_df, clicked_df, on=["variation_id", "date"])
    conversion_df["conversion"] = conversion_df["num_session_y"] / conversion_df["num_session_x"]
    conversion_df["conversion"] = conversion_df["conversion"].map(lambda x: round(x, 2))
    variation_ids = conversion_df["variation_id"].unique()
    variation_dict = dict()
    for variation_id in variation_ids:
        d = conversion_df[conversion_df["variation_id"] == variation_id]
        session_count = d["conversion"].tolist()
        variation_dict[variation_id] = session_count
    result = dict()
    result["date"] = d["date"].tolist()
    result["conversion"] = variation_dict
    return result
