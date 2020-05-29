import uuid

from config import *
from utils.date_utils import DateUtils
import pandas as pd


class ExperimentAgent(object):

    @classmethod
    def create_experiment_for_client_id(cls, data_store, client_id, experiment_name, page_type, experiment_type, status,
                                        created_on_timestamp, last_updated_on_timestamp):
        experiment_id = uuid.uuid4().hex
        created_on_utc_str = DateUtils.convert_timestamp_to_utc_datetime_string(created_on_timestamp)
        last_updated_on_utc_str = DateUtils.convert_timestamp_to_utc_datetime_string(last_updated_on_timestamp)

        table = TABLE_EXPERIMENTS
        experiment = {"client_id": client_id, "experiment_id": experiment_id, "experiment_name": experiment_name,
                      "page_type": page_type, "experiment_type": experiment_type, "status": status,
                      "creation_time": created_on_utc_str, "last_updation_time": last_updated_on_utc_str}
        columns = list(experiment.keys())
        column = ",".join(columns)
        values = [experiment[key] for key in columns]
        value = str(tuple(values))

        sql = """INSERT INTO {table} ({column}) VALUES {value}"""
        query = sql.format(table=table, column=column, value=value)
        data_store.run_insert_into_sql(query=query)
        return experiment

    @classmethod
    def get_experiments_for_client_id(cls, data_store, client_id):
        table = TABLE_EXPERIMENTS
        columns = ["client_id", "experiment_id", "experiment_name", "status", "page_type", "experiment_type",
                   "creation_time",
                   "last_updation_time"]
        where = "client_id='{client_id}'".format(client_id=client_id)
        column = ",".join(columns)
        sql = """ SELECT {column} from {table} where {where}"""
        query = sql.format(column=column, table=table, where=where)
        mobile_records = data_store.run_select_sql(query=query)
        df = None
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = columns
            df["creation_time"] = df["creation_time"].map(DateUtils.timestampz_to_string)
            df["last_updation_time"] = df["last_updation_time"].map(DateUtils.timestampz_to_string)

        experiments = None
        if df is not None:
            experiments = df.to_dict(orient="records")
        return experiments
