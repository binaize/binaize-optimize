from config import *


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
