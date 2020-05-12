import uuid

from pytz import timezone

from config import *
from utils.data_store.rds_data_store import RDSDataStore
from utils.date_utils import *


def add_new_client(data_store, client_id, full_name, company_name, hashed_password, disabled):
    table = TABLE_CLIENTS
    columns_value_dict = {"client_id": client_id, "full_name": full_name, "company_name": company_name,
                          "hashed_password": hashed_password, "disabled": disabled}
    data_store.insert_record_to_data_store(table=table, columns_value_dict=columns_value_dict)


def get_client_details_for_client_id(data_store, client_id):
    table = TABLE_CLIENTS
    columns = ["client_id", "full_name", "company_name", "hashed_password", "disabled", "hashed_password",
               "shopify_app_api_key", "shopify_app_password", "shopify_app_eg_url", "shopify_app_shared_secret"]
    where = "client_id='{client_id}'".format(client_id=client_id)
    df = data_store.read_record_from_data_store(table=table, columns=columns, where=where)
    client_details = None
    if df is not None:
        client_details = df[columns].to_dict(orient="records")[0]
    return client_details


def add_shopify_credentials_to_existing_client(data_store, client_id, shopify_app_api_key, shopify_app_password,
                                               shopify_app_eg_url, shopify_app_shared_secret):
    table = TABLE_CLIENTS

    columns_value_dict = {"shopify_app_api_key": shopify_app_api_key,
                          "shopify_app_password": shopify_app_password,
                          "shopify_app_eg_url": shopify_app_eg_url,
                          "shopify_app_shared_secret": shopify_app_shared_secret}

    where = "client_id='{client_id}'".format(client_id=client_id)

    data_store.update_record_in_data_store(table=table, columns_value_dict=columns_value_dict, where=where)


def create_experiment_for_client_id(data_store, client_id, experiment_name, page_type, experiment_type, status,
                                    created_on_timestamp, last_updated_on_timestamp):
    experiment_id = uuid.uuid4().hex
    created_on_utc_str = utc_timestamp_to_utc_datetime_string(created_on_timestamp)
    last_updated_on_utc_str = utc_timestamp_to_utc_datetime_string(last_updated_on_timestamp)

    table = TABLE_EXPERIMENTS
    experiment = {"client_id": client_id, "experiment_id": experiment_id, "experiment_name": experiment_name,
                  "page_type": page_type, "experiment_type": experiment_type, "status": status,
                  "created_on": created_on_utc_str, "last_updated_on": last_updated_on_utc_str}
    try:
        data_store.insert_record_to_data_store(table=table, columns_value_dict=experiment)
    except Exception as e:
        print("create_experiment_for_client_id failed")
        experiment = None
    return experiment


def get_experiments_for_client_id(data_store, client_id):
    table = TABLE_EXPERIMENTS
    columns = ["client_id", "experiment_id", "experiment_name", "status", "page_type", "experiment_type", "created_on",
               "last_updated_on"]
    where = "client_id='{client_id}'".format(client_id=client_id)
    df = data_store.read_record_from_data_store(table=table, columns=columns, where=where)

    def timestampz_to_string(x):
        localtz = timezone('UTC')
        y = x.astimezone(localtz)
        # return y.strftime("%d-%b-%Y %H:%M:%S")
        return y.strftime("%d-%b-%Y")

    df["created_on"] = df["created_on"].map(timestampz_to_string)
    df["last_updated_on"] = df["last_updated_on"].map(timestampz_to_string)

    experiments = None
    if df is not None:
        experiments = df.to_dict(orient="records")
    return experiments


def create_variation_for_client_id_and_experiment_id(data_store, client_id, experiment_id, variation_name,
                                                     traffic_percentage):
    variation_id = uuid.uuid4().hex

    table = TABLE_VARIATIONS
    variation = {"client_id": client_id, "experiment_id": experiment_id, "variation_id": variation_id,
                 "variation_name": variation_name,
                 "traffic_percentage": traffic_percentage}
    data_store.insert_record_to_data_store(table=table, columns_value_dict=variation)
    # except Exception as e:
    #     print("create_variation_for_client_id_and_experiment_id failed")
    #     variation = None
    return variation


def get_variation_ids_for_client_id_and_experiment_id(data_store, client_id, experiment_id):
    table = TABLE_VARIATIONS
    columns = ["variation_id"]
    where = "client_id='{client_id}' and experiment_id='{experiment_id}'".format(client_id=client_id,
                                                                                 experiment_id=experiment_id)
    df = data_store.read_record_from_data_store(table=table, columns=columns, where=where)
    variations = None
    if df is not None:
        variations = df["variation_id"].tolist()
    return variations


def register_event_for_client(data_store, client_id, experiment_id, session_id, variation_id, event_name):
    table = TABLE_EVENTS
    columns_value_dict = {"client_id": client_id, "experiment_id": experiment_id,
                          "variation_id": variation_id,
                          "session_id": session_id, "event_name": event_name}
    data_store.insert_record_to_data_store(table=table, columns_value_dict=columns_value_dict)


def get_variation_id_to_recommend(data_store, client_id, experiment_id, session_id):
    table = TABLE_EVENTS

    columns = ["client_id", "experiment_id", "session_id", "variation_id"]
    where = "client_id='{client_id}' and experiment_id='{experiment_id}' and session_id='{session_id}'".format(
        client_id=client_id,
        experiment_id=experiment_id, session_id=session_id)

    df = data_store.read_record_from_data_store(table=table, columns=columns, where=where)

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

#
# if __name__ == "__main__":
#     rds_data_store = RDSDataStore(host=AWS_RDS_HOST, port=AWS_RDS_PORT,
#                                   dbname=AWS_RDS_DBNAME,
#                                   user=AWS_RDS_USER,
#                                   password=AWS_RDS_PASSWORD)
#     x = create_experiment_for_client_id(data_store=rds_data_store, client_id="string", experiment_name="string",
#                                         page_type="string", experiment_type="string", status="string",
#                                         created_on_timestamp=1589310054,
#                                         last_updated_on_timestamp=1589310054)
#     print(x)
#
#     f = get_experiments_for_client_id(data_store=rds_data_store, client_id="string")
#     print(f)
