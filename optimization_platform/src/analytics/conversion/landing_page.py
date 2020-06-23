from utils.date_utils import DateUtils
import pandas as pd


def get_landing_page_analytics(data_store, client_id, start_date_str, end_date_str, timezone_str):
    start_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=start_date_str,
                                                                       timezone_str=timezone_str)
    end_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=end_date_str,
                                                                     timezone_str=timezone_str)

    sql = \
        """ 
            select t.event_name, count(*)
            from (
                select distinct on (session_id) session_id, url, event_name
                    from visits
                where 
                    client_id = '{client_id}'
                    and event_name in ('collection','home','product')
                    and creation_time > '{start_date}' 
                    and creation_time < '{end_date}') t
            group by event_name
        """.format(client_id=client_id, start_date=start_date, end_date=end_date)
    mobile_records = data_store.run_custom_sql(sql)

    visits_df = None
    if mobile_records is not None and len(mobile_records) > 0:
        visits_df = pd.DataFrame.from_records(mobile_records)
        visits_df.columns = ["event_name", "visitor_count"]

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
            if "product" in x:
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

    df = None
    if orders_df is not None and visits_df is not None:
        df = pd.merge(visits_df, orders_df, how="left", left_on="event_name", right_on="page_type")
        df["conversion_count"] = df["conversion_count"].fillna(0).astype(int)
    event_dict = {"home": "Home Page", "product": "Product Page", "collection": "Collections Page"}
    pages = list()
    visitor_count = list()
    conversion_count = list()
    conversion_percentage = list()
    summary = "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT" \
              " </strong></span> enough visits registered on the website"
    conclusion = "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT " \
                 "</strong></span> for the customers to interact with your website"
    result = dict()
    if df is not None:
        pages = [event_dict[val] for val in df["event_name"].tolist()]
        visitor_count = df["visitor_count"].tolist()
        conversion_count = df["conversion_count"].tolist()
        df["conversion_percentage"] = df["conversion_count"] * 100 / (df["visitor_count"] + 0.01)
        conversion_percentage = df["conversion_percentage"].map(lambda x: min(100, round(x, 2))).tolist()
        min_idx = conversion_percentage.index(min(conversion_percentage))
        summary = "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong> {product} </strong></span>" \
                  " has minimum conversion of <span style = 'color: blue; font-size: 16px;'><strong> " \
                  "{conversion}% </strong></span>".format(
            product=pages[min_idx],
            conversion=conversion_percentage[
                min_idx])
        conclusion = "<strong> CONCLUSION : </strong> Experiment with different creatives/copies" \
                     " for <span style = 'color: blue; font-size: 16px;'><strong> {product} </strong></span>".format(
            product=pages[min_idx])
    result["pages"] = pages
    temp_dict = dict()
    temp_dict["visitor_count"] = visitor_count
    temp_dict["conversion_count"] = conversion_count
    temp_dict["conversion_percentage"] = conversion_percentage
    result["landing_conversion"] = temp_dict
    result["summary"] = summary
    result["conclusion"] = conclusion
    return result
