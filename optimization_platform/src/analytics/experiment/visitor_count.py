from utils.date_utils import DateUtils
import pandas as pd

from utils.date_utils import DateUtils
import pandas as pd


def get_events_df(client_id, data_store, date_range_list, experiment_id):
    sql_list = list()
    for start_date, end_date, client_date in date_range_list:
        sql_temp = \
            """ 
            select
                variation_id,
                count(distinct(session_id)) as num_session,
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
            """.format(client_id=client_id, experiment_id=experiment_id,
                       start_date=start_date, end_date=end_date, date=client_date)

        sql_list.append(sql_temp)
    sql = " union ".join(sql_list)
    mobile_records = data_store.run_custom_sql(sql)
    events_df = None
    if mobile_records is not None and len(mobile_records) > 0:
        events_df = pd.DataFrame.from_records(mobile_records)
        events_df.columns = ["variation_id", "visitor_count", "date", "iso_date"]
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


def get_visitor_count_per_variation_over_time(data_store, client_id, experiment_id, timezone_str):
    date_range_list = DateUtils.get_date_range_in_utc_str(timezone_str)
    date_list = [x[-1] for x in date_range_list]
    iso_date_list = [x[0] for x in date_range_list]

    date_df = pd.DataFrame({"date": date_list, "iso_date": iso_date_list})
    events_df = get_events_df(client_id, data_store, date_range_list, experiment_id)
    variations_df = get_variations_df(data_store, client_id, experiment_id)
    result = dict()

    if variations_df is None:
        result = dict()
        return result

    date_variations_df = pd.merge(date_df.assign(key=1), variations_df.assign(key=1), on='key').drop('key', 1)
    df = date_variations_df
    if events_df is None:
        df["visitor_count"] = 0
    else:
        df = pd.merge(date_variations_df, events_df,
                      how='left', left_on=["date", "iso_date", "variation_id"],
                      right_on=["date", "iso_date", "variation_id"])
        df["visitor_count"] = df["visitor_count"].fillna(0).astype(int)

    df.sort_values(['date'], inplace=True)
    variation_names = df["variation_name"].unique()
    variation_dict = dict()
    for variation_name in variation_names:
        d = df[df["variation_name"] == variation_name]
        session_count = d["visitor_count"].tolist()
        variation_dict[variation_name] = session_count
    result["date"] = d["date"].tolist()
    result["visitor_count"] = variation_dict

    return result
