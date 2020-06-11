from config import *
from utils.date_utils import DateUtils


class VisitAgent(object):

    @classmethod
    def register_visit_for_client(cls, data_store, client_id, session_id, event_name, creation_time, url):
        table = TABLE_VISITS

        creation_time_utc_str = DateUtils.convert_timestamp_to_utc_iso_string(creation_time)
        columns_value_dict = {"client_id": client_id,
                              "session_id": session_id, "event_name": event_name,
                              "creation_time": creation_time_utc_str, "url": url}
        columns = list(columns_value_dict.keys())
        column = ",".join(columns)
        values = [columns_value_dict[key] for key in columns]
        value = str(tuple(values))

        sql = """insert into {table} ({column}) values {value}""".format(table=table, column=column, value=value)
        data_store.run_insert_into_sql(query=sql)
