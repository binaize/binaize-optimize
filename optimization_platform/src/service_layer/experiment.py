import uuid

from config import *
from utils.date_utils import *


def create_experiment_for_client_id(data_store, client_id, experiment_name, page_type, experiment_type, status,
                                    created_on_timestamp, last_updated_on_timestamp):
    experiment_id = uuid.uuid4().hex
    created_on_utc_str = utc_timestamp_to_utc_datetime_string(created_on_timestamp)
    last_updated_on_utc_str = utc_timestamp_to_utc_datetime_string(last_updated_on_timestamp)

    table = TABLE_EXPERIMENTS
    experiment = {"client_id": client_id, "experiment_id": experiment_id, "experiment_name": experiment_name,
                  "page_type": page_type, "experiment_type": experiment_type, "status": status,
                  "creation_time": created_on_utc_str, "last_updation_time": last_updated_on_utc_str}
    try:
        data_store.insert_record_to_data_store(table=table, columns_value_dict=experiment)
    except Exception as e:
        print("create_experiment_for_client_id failed")
        experiment = None
    return experiment


def get_experiments_for_client_id(data_store, client_id):
    table = TABLE_EXPERIMENTS
    columns = ["client_id", "experiment_id", "experiment_name", "status", "page_type", "experiment_type",
               "creation_time",
               "last_updation_time"]
    where = "client_id='{client_id}'".format(client_id=client_id)
    df = data_store.read_record_from_data_store(table=table, columns=columns, where=where)
    df["creation_time"] = df["creation_time"].map(timestampz_to_string)
    df["last_updation_time"] = df["last_updation_time"].map(timestampz_to_string)

    experiments = None
    if df is not None:
        experiments = df.to_dict(orient="records")
    return experiments
