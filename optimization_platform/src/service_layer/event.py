from config import *
from utils.data_store.rds_data_store import RDSDataStore
from utils.date_utils import *


def register_event_for_client(data_store, client_id, experiment_id, session_id, variation_id, event_name,
                              creation_time):
    creation_time_utc_str = utc_timestamp_to_utc_datetime_string(creation_time)

    table = TABLE_EVENTS
    columns_value_dict = {"client_id": client_id, "experiment_id": experiment_id,
                          "variation_id": variation_id,
                          "session_id": session_id, "event_name": event_name, "creation_time": creation_time_utc_str}
    data_store.insert_record_to_data_store(table=table, columns_value_dict=columns_value_dict)
