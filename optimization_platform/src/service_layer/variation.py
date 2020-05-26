import uuid

from config import *
from optimization_platform.src.service_layer.event import register_event_for_client
import pandas as pd


def create_variation_for_client_id_and_experiment_id(data_store, client_id, experiment_id, variation_name,
                                                     traffic_percentage):
    variation_id = uuid.uuid4().hex

    table = TABLE_VARIATIONS
    variation = {"client_id": client_id, "experiment_id": experiment_id, "variation_id": variation_id,
                 "variation_name": variation_name,
                 "traffic_percentage": traffic_percentage}
    columns = list(variation.keys())
    column = ",".join(columns)
    values = [variation[key] for key in columns]
    value = str(tuple(values))

    sql = """INSERT INTO {table} ({column}) VALUES {value}"""
    query = sql.format(table=table, column=column, value=value)
    data_store.run_insert_into_sql(query=query)
    return variation


def get_variation_ids_for_client_id_and_experiment_id(data_store, client_id, experiment_id):
    table = TABLE_VARIATIONS
    columns = ["variation_id"]
    where = "client_id='{client_id}' and experiment_id='{experiment_id}'".format(client_id=client_id,
                                                                                 experiment_id=experiment_id)
    column = ",".join(columns)
    sql = """ SELECT {column} from {table} where {where}"""
    query = sql.format(column=column, table=table, where=where)
    mobile_records = data_store.run_select_sql(query=query)
    if len(mobile_records) > 0:
        df = pd.DataFrame.from_records(mobile_records)
        df.columns = columns

    variations = None
    if df is not None:
        variations = df["variation_id"].tolist()
    return variations


def get_variation_id_to_recommend(data_store, client_id, experiment_id, session_id):
    table = TABLE_EVENTS

    columns = ["client_id", "experiment_id", "session_id", "variation_id"]
    where = "client_id='{client_id}' and experiment_id='{experiment_id}' and session_id='{session_id}'".format(
        client_id=client_id,
        experiment_id=experiment_id, session_id=session_id)

    df = data_store.run_select_sql(table=table, columns=columns, where=where)

    if df is not None and len(df) > 0:
        variation_id_to_recommend = df["variation_id"][0]
    else:

        variation_ids = get_variation_ids_for_client_id_and_experiment_id(data_store=data_store, client_id=client_id,
                                                                          experiment_id=experiment_id)

        import random
        variation_id_to_recommend = random.choice(variation_ids)

    register_event_for_client(data_store=data_store, client_id=client_id, experiment_id=experiment_id,
                              session_id=session_id,
                              variation_id=variation_id_to_recommend, event_name="served")

    variation = {"client_id": client_id, "experiment_id": experiment_id, "variation_id": variation_id_to_recommend}

    return variation
