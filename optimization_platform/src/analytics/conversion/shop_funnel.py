from utils.date_utils import DateUtils
import pandas as pd


def get_shop_funnel_analytics(data_store, client_id, start_date_str, end_date_str, timezone_str):
    start_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=start_date_str,
                                                                       timezone_str=timezone_str)
    end_date = DateUtils.convert_conversion_datestring_to_iso_string(datetime_str=end_date_str,
                                                                     timezone_str=timezone_str)
    sql = \
        """
            select '1' as id, 'Home Page' as event,count(distinct(session_id))
                from visits
            where 
                client_id = '{client_id}'
                and event_name = 'home'
                and creation_time > '{start_date}' 
                and creation_time < '{end_date}' 
            union
            (select '2' as id, 'Collection Page' as event,count(distinct(session_id))
                from visits
            where 
                client_id = '{client_id}'
                and event_name = 'collection'
                and creation_time > '{start_date}' 
                and creation_time < '{end_date}')
            union
            (select '3' as id, 'Product Page' as event,count(distinct(session_id))
                from visits
            where 
                client_id = '{client_id}'
                and event_name = 'product'
                and creation_time > '{start_date}' 
                and creation_time < '{end_date}')
            union
            (select '4' as id, 'Cart Page' as event,count(distinct(session_id))
                from visits
            where 
                client_id = '{client_id}'
                and event_name = 'cart'
                and creation_time > '{start_date}' 
                and creation_time < '{end_date}')
            union
            (select '5' as id, 'Checkout Page' as event,count(distinct(order_id))
                from orders
            where 
                client_id = '{client_id}'
                and updated_at > '{start_date}' 
                and updated_at < '{end_date}')
            union
            (select '6' as id, 'Purchase' as event,count(distinct(order_id))
                from orders
            where 
                client_id = '{client_id}'
                and payment_status = True
                and updated_at > '{start_date}' 
                and updated_at < '{end_date}')

        """.format(client_id=client_id, start_date=start_date, end_date=end_date)
    mobile_records = data_store.run_custom_sql(sql)
    result = {}
    temp_dict = dict()
    summary = "<strong> SUMMARY : </strong> There are <span style = 'color: red; font-size: 16px;'><strong> NOT" \
              " </strong></span> enough visits registered on the website"
    conclusion = "<strong> CONCLUSION : </strong> <span style = 'color: blue; font-size: 16px;'><strong> WAIT " \
                 "</strong></span> for the customers to interact with your website"
    if mobile_records is not None and len(mobile_records) > 0:
        df = pd.DataFrame.from_records(mobile_records)
        df.columns = ["id", "pages", "count"]
        df = df.sort_values(['id'])
        df["percentage"] = df["count"] * 100 / (max(df["count"]) + 0.01)
        df["percentage"] = df["percentage"].map(lambda x: min(100.00, round(x, 2)))
        result["pages"] = df["pages"].tolist()
        visitor_count = df["count"].tolist()
        temp_dict["visitor_count"] = visitor_count
        temp_dict["percentage"] = df["percentage"].tolist()
        result["shop_funnel"] = temp_dict
        if sum(visitor_count[:4]) > 0:
            diff_list = list()
            for a in zip(df["percentage"], df["percentage"][1:]):
                diff_list.append(a[0] - a[1])
            max_idx = diff_list.index(max(diff_list))
            summary = "<strong> SUMMARY : </strong> <span style = 'color: blue; font-size: 16px;'><strong>" \
                      " {page_type} </strong></span> has maximum churn of <span style = 'color: blue; font-size: 16px;'><strong> " \
                      "{drop}% </strong></span>".format(
                page_type=result["pages"][max_idx],
                drop=round(diff_list[max_idx], 2))
            conclusion = "<strong> CONCLUSION : </strong> Experiment with different creatives/copies for " \
                         "<span style = 'color: blue; font-size: 16px;'><strong> {page_type} </strong></span>".format(
                page_type=result["pages"][max_idx])
        result["summary"] = summary
        result["conclusion"] = conclusion

    return result
