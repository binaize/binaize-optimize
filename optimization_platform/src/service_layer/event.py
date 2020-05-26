from config import *
from utils.data_store.rds_data_store import RDSDataStore
from utils.date_utils import *


def register_event_for_client(data_store, client_id, experiment_id, session_id, variation_id, event_name,
                              creation_time=None):
    table = TABLE_EVENTS

    if creation_time is None:
        creation_time = int(datetime.datetime.now(pytz.timezone("UTC")).timestamp())

    creation_time_utc_str = utc_timestamp_to_utc_datetime_string(creation_time)
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
