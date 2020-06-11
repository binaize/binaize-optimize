from config import *
from utils.date_utils import DateUtils


class EventAgent(object):

    @classmethod
    def register_event_for_client(cls, data_store, client_id, experiment_id, session_id, variation_id, event_name,
                                  creation_time):
        table = TABLE_EVENTS

        creation_time_utc_str = DateUtils.convert_timestamp_to_utc_iso_string(creation_time)
        columns_value_dict = {"client_id": client_id, "experiment_id": experiment_id,
                              "variation_id": variation_id,
                              "session_id": session_id, "event_name": event_name,
                              "creation_time": creation_time_utc_str}
        columns = list(columns_value_dict.keys())
        column = ",".join(columns)
        values = [columns_value_dict[key] for key in columns]
        value = str(tuple(values))

        sql = """INSERT INTO {table} ({column}) VALUES {value}"""
        query = sql.format(table=table, column=column, value=value)
        data_store.run_insert_into_sql(query=query)
