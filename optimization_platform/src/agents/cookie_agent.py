from utils.date_utils import DateUtils


class CookieAgent(object):

    @classmethod
    def register_cookie_for_client(cls, data_store, client_id, session_id, shopify_x, cart_token, creation_time):
        table = "cookie"

        creation_time_utc_str = DateUtils.convert_timestamp_to_utc_iso_string(creation_time)
        columns_value_dict = {"client_id": client_id,
                              "session_id": session_id, "shopify_x": shopify_x, "cart_token": cart_token,
                              "creation_time": creation_time_utc_str}
        columns = list(columns_value_dict.keys())
        column = ",".join(columns)
        values = [columns_value_dict[key] for key in columns]
        value = str(tuple(values))

        sql = """insert into {table} ({column}) values {value}""".format(table=table, column=column, value=value)
        data_store.run_insert_into_sql(query=sql)
