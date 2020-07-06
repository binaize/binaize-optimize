import uuid

import pandas as pd

from config import *
from utils.date_utils import DateUtils


class ExperimentAgent(object):

    @classmethod
    def create_experiment_for_client_id(cls, data_store, client_id, experiment_name, page_type, experiment_type, status,
                                        creation_time, last_updation_time):
        experiment_id = uuid.uuid4().hex

        creation_time_utc_str = DateUtils.convert_timestamp_to_utc_iso_string(creation_time)
        last_updation_time_utc_str = DateUtils.convert_timestamp_to_utc_iso_string(last_updation_time)

        table = TABLE_EXPERIMENTS
        experiment = {"client_id": client_id, "experiment_id": experiment_id, "experiment_name": experiment_name,
                      "page_type": page_type, "experiment_type": experiment_type, "status": status,
                      "creation_time": creation_time_utc_str, "last_updation_time": last_updation_time_utc_str}
        columns = list(experiment.keys())
        column = ",".join(columns)
        values = [experiment[key] for key in columns]
        value = str(tuple(values))

        sql = """INSERT INTO {table} ({column}) VALUES {value}""".format(table=table, column=column, value=value)
        status = data_store.run_insert_into_sql(query=sql)
        if status is None:
            experiment = None
        return experiment

    @classmethod
    def get_experiments_for_client_id(cls, data_store, client_id):
        table = TABLE_EXPERIMENTS
        columns = ["client_id", "experiment_id", "experiment_name", "status", "page_type", "experiment_type",
                   "creation_time",
                   "last_updation_time"]
        where = "client_id='{client_id}'".format(client_id=client_id)
        column = ",".join(columns)
        sql = """ SELECT {column} from {table} where {where}""".format(column=column, table=table, where=where)
        mobile_records = data_store.run_select_sql(query=sql)
        df = None
        if mobile_records is not None and len(mobile_records) > 0:
            df = pd.DataFrame.from_records(mobile_records)
            df.columns = columns
            df["creation_time"] = df["creation_time"].map(
                DateUtils.convert_datetime_to_experiment_dashboard_date_string)
            df["last_updation_time"] = df["last_updation_time"].map(
                DateUtils.convert_datetime_to_experiment_dashboard_date_string)
        experiments = None
        if df is not None:
            experiments = df.to_dict(orient="records")
        return experiments

    @classmethod
    def get_latest_experiment_id(cls, data_store, client_id):
        sql = \
            """
            select 
                experiment_id 
            from experiments 
            where 
                client_id = '{client_id}' 
                and last_updation_time = (
                    select 
                        max(last_updation_time) 
                    from 
                        experiments
                    where
                        client_id = '{client_id}')
            """.format(client_id=client_id)
        mobile_records = data_store.run_custom_sql(sql)
        latest_experiment_id = None
        if mobile_records is not None and len(mobile_records) > 0:
            latest_experiment_id = mobile_records[0][0]
        return latest_experiment_id

    @classmethod
    def get_start_time_of_experiment_id(cls, data_store, client_id, experiment_id):
        sql = \
            """
            select 
                creation_time 
            from experiments 
            where 
                client_id = '{client_id}' 
                and experiment_id = '{experiment_id}'
            """.format(client_id=client_id, experiment_id=experiment_id)
        mobile_records = data_store.run_custom_sql(sql)
        creation_time = None
        if mobile_records is not None and len(mobile_records) > 0:
            creation_time = mobile_records[0][0]
        return creation_time
