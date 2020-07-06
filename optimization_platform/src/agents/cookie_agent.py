from config import TABLE_COOKIES
from utils.data_store.rds_data_store import IteratorFile
from utils.date_utils import DateUtils


class CookieAgent(object):

    @classmethod
    def register_cookie_for_client(cls, data_store, client_id, session_id, cart_token, creation_time):
        table = TABLE_COOKIES

        if len(session_id) == 0 or len(cart_token) == 0:
            return False

        columns = ["client_id", "session_id", "cart_token"]
        where = "client_id='{client_id}' and session_id='{session_id}' and cart_token='{cart_token}'".format(
            client_id=client_id,
            cart_token=cart_token, session_id=session_id)

        column = ",".join(columns)
        sql = """ SELECT {column} from {table} where {where}""".format(column=column, table=table, where=where)

        mobile_records = data_store.run_select_sql(query=sql)
        record_exist = False
        if mobile_records is not None and len(mobile_records) > 0:
            record_exist = True
        status = record_exist
        if not record_exist:
            creation_time_utc_str = DateUtils.convert_timestamp_to_utc_iso_string(creation_time)
            columns_value_dict = {"client_id": client_id,
                                  "session_id": session_id, "cart_token": cart_token,
                                  "creation_time": creation_time_utc_str}
            columns = list(columns_value_dict.keys())
            column = ",".join(columns)
            values = [columns_value_dict[key] for key in columns]
            value = str(tuple(values))

            sql = """insert into {table} ({column}) values {value}""".format(table=table, column=column, value=value)
            status = data_store.run_insert_into_sql(query=sql)
        return status

    @classmethod
    def register_dummy_cookie_for_client(cls, data_store, client_id, session_cart_time_list):
        table = "cookies"
        columns = ["client_id", "session_id", "cart_token", "creation_time"]
        status = False
        if len(session_cart_time_list) > 0:
            s = "\t".join(["{}" for i in range(len(columns))])

            file = IteratorFile((s.format(client_id, session_cart_time[0], session_cart_time[1],
                                          session_cart_time[2])
                                 for session_cart_time in session_cart_time_list))
            status = data_store.run_batch_insert_sql(file=file, table=table, columns=columns)
        return status
