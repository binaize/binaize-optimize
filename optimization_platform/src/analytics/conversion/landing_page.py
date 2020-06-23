import pandas as pd

from utils.date_utils import DateUtils


def get_orders_df(client_id, data_store, end_date, start_date):
    sql = \
        """
            select landing_page, count(*)
                FROM orders
            where 
                client_id = '{client_id}' 
                and payment_status = True
                and updated_at > '{start_date}' 
                and updated_at < '{end_date}'
            group by landing_page
        """.format(client_id=client_id, start_date=start_date, end_date=end_date)
    mobile_records = data_store.run_custom_sql(sql)
    orders_df = None
    if mobile_records is not None and len(mobile_records) > 0:
        orders_df = pd.DataFrame.from_records(mobile_records)
        orders_df.columns = ["landing_page", "conversion_count"]

        def process_landing_page(x):
            if "product" in x or "checkout" in x:
                return "product"
            elif "collection" in x:
                return "collection"
            return "home"

        orders_df["page_type"] = orders_df["landing_page"].map(process_landing_page)
        orders_df.drop(['landing_page'], axis=1, inplace=True)

        orders_df = orders_df.groupby('page_type').agg({
            'conversion_count': [('conversion_count', lambda x: x.sum())]
        })
        orders_df.columns = orders_df.columns.droplevel()
        orders_df["page_type"] = orders_df.index
        orders_df.reset_index(inplace=True, drop=True)
    return orders_df


def get_visits_df(client_id, data_store, end_date, start_date):
    sql = \
        """ 
            select session_id, url, event_name
                from visits
            where 
                client_id = '{client_id}'
                and event_name in ('collection','home','product')
                and creation_time > '{start_date}' 
                and creation_time < '{end_date}'
            order by creation_time asc
        """.format(client_id=client_id, start_date=start_date, end_date=end_date)
    mobile_records = data_store.run_custom_sql(sql)
    visits_df = None
    if mobile_records is not None and len(mobile_records) > 0:
        visits_df = pd.DataFrame.from_records(mobile_records)
        visits_df.columns = ["session_id", "url", "event_name"]
        visits_df.drop_duplicates(subset=["session_id"], keep='first', inplace=True)
        visits_df = visits_df.groupby('event_name').agg({
            'session_id': [('visitor_count', lambda x: len(x))]
        })
        visits_df.columns = visits_df.columns.droplevel()
        visits_df["event_name"] = visits_df.index
        visits_df.reset_index(inplace=True, drop=True)
    return visits_df


def get_landing_page_analytics(data_store, client_id, start_date_str, end_date_str, timezone_str):
    start_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=start_date_str,
                                                                       timezone_str=timezone_str)
    end_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=end_date_str,
                                                                     timezone_str=timezone_str)

    visits_df = get_visits_df(client_id, data_store, end_date, start_date)
    orders_df = get_orders_df(client_id, data_store, end_date, start_date)

    page_df = pd.DataFrame({"id": [1, 2, 3],
                            "page_type": ["home", "collection", "product"],
                            "page_name": ["Home Page", "Collections Page", "Product Page"]})

    conclusion, summary = get_description_for_data_not_enough()

    visits_page_df = page_df
    if visits_df is None:
        visits_page_df["visitor_count"] = 0
    else:
        visits_page_df = pd.merge(page_df, visits_df,
                                  how='left', left_on=["page_type"], right_on=["event_name"])
        visits_page_df["visitor_count"] = visits_page_df["visitor_count"].fillna(0).astype(int)

    df = visits_page_df
    if orders_df is None:
        df["conversion_count"] = 0
    else:
        df = pd.merge(visits_page_df, orders_df,
                      how='left', left_on=["page_type"], right_on=["page_type"])
        df["conversion_count"] = df["conversion_count"].fillna(0).astype(int)

    df["conversion_percentage"] = df["conversion_count"] * 100 / (df["visitor_count"] + 0.01)
    df["conversion_percentage"] = df["conversion_percentage"].map(lambda x: min(100, round(x, 2)))
    df = df.sort_values(["id"])

    pages = df["page_name"].tolist()
    visitor_count = df["visitor_count"].tolist()
    conversion_count = df["conversion_count"].tolist()
    conversion_percentage = df["conversion_percentage"].tolist()

    if visits_df is None and orders_df is None:
        result = construct_result(conclusion, conversion_count, conversion_percentage, pages, summary, visitor_count)
        return result

    min_idx = conversion_percentage.index(min(conversion_percentage))
    min_page = pages[min_idx]
    min_conversion = conversion_percentage[min_idx]
    conclusion, summary = get_description_for_enough_visitors(conclusion, min_conversion, min_page, summary)

    result = construct_result(conclusion, conversion_count, conversion_percentage, pages, summary, visitor_count)
    return result


def construct_result(conclusion, conversion_count, conversion_percentage, pages, summary, visitor_count):
    result = dict()
    temp_dict = dict()
    result["pages"] = pages
    temp_dict["visitor_count"] = visitor_count
    temp_dict["conversion_count"] = conversion_count
    temp_dict["conversion_percentage"] = conversion_percentage
    result["landing_conversion"] = temp_dict
    result["summary"] = summary
    result["conclusion"] = conclusion
    return result


def get_description_for_enough_visitors(conclusion, min_conversion, min_page, summary):
    summary = "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong> {page} </strong></span>" \
              " has the least conversion of <span style = 'color: blue; font-size: 16px;'><strong> " \
              "{conversion}% </strong></span>".format(page=min_page, conversion=min_conversion)
    conclusion = "<strong> CONCLUSION : </strong> Experiment with different creatives/copies" \
                 " for <span style = 'color: blue; font-size: 16px;'><strong> {page} </strong></span>".format(
        page=min_page)
    return conclusion, summary


def get_description_for_data_not_enough():
    summary = "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT" \
              " </strong></span> enough visits registered on the website"
    conclusion = "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT " \
                 "</strong></span> for the customers to interact with your website"
    return conclusion, summary
