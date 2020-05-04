from utils.data_store.rds_data_store import RDSDataStore
from utils.data_store.s3_data_store import S3DataStore
import uuid
from config import *


def add_new_client(data_store, client_id, full_name, company_name, hashed_password, disabled):
    """

    :param data_store: data store object
    :param client_id: id of the smb client
    :param experiment_id: id of the experiment
    :param variation_ids: id of the variation ids for the experiment id
    :return: True in case of success, False in case of failure
    """
    table = "clients"
    columns_value_dict = {"client_id": client_id, "full_name": full_name, "company_name": company_name,
                          "hashed_password": hashed_password, "disabled": disabled}
    data_store.insert_record_to_data_store(table=table, columns_value_dict=columns_value_dict)


def get_client_details_for_client_id(data_store, client_id):
    table = "clients"
    columns = ["client_id", "full_name", "company_name", "hashed_password", "disabled"]
    where = "client_id='{client_id}'".format(client_id=client_id)
    df = data_store.read_record_from_data_store(table=table, columns=columns, where=where)
    client_details = None
    if df is not None:
        client_details = df[columns].to_dict(orient="records")[0]
    return client_details


def add_shopify_credentials_to_existing_client(data_store, client_id, shopify_app_api_key, shopify_app_password,
                                               shopify_app_eg_url, shopify_app_shared_secret):
    table = "clients"

    columns_value_dict = {"shopify_app_api_key": shopify_app_api_key,
                          "shopify_app_password": shopify_app_password,
                          "shopify_app_eg_url": shopify_app_eg_url,
                          "shopify_app_shared_secret": shopify_app_shared_secret}

    where = "client_id='{client_id}'".format(client_id=client_id)

    data_store.update_record_in_data_store(table=table, columns_value_dict=columns_value_dict, where=where)


def main():
    pass
    s3_data_store = S3DataStore(access_key=AWS_ACCESS_KEY_ID,
                                secret_key=AWS_SECRET_ACCESS_KEY)
    x = s3_data_store.create_bucket(bucket_name="binaize-wow")
    print(x)
    d = {"hi": 6}
    x = s3_data_store.write_json_file(bucket_name="binaize-wow", filename="tuhin/file1.json", contents=d)
    print(x)

    x = s3_data_store.write_json_file(bucket_name="binaize-wow", filename="tuhin/file2.json", contents=d)
    print(x)
    f = s3_data_store.read_json_file(bucket_name="binaize-wow", filename="tuhin/file1.json")
    print(f)
    x = s3_data_store.download_file(bucket_name="binaize-wow", src="tuhin/file1.json", target="./x.json")
    print(x)
    x = s3_data_store.list_files(bucket_name="binaize-wow")
    print(x)
    x = s3_data_store.delete_bucket(bucket_name="binaize-wow")
    print(x)

    # rds_data_store = RDSDataStore(host=AWS_RDS_HOST, port=AWS_RDS_PORT,
    #                               dbname=AWS_RDS_DBNAME,
    #                               user=AWS_RDS_USER,
    #                               password=AWS_RDS_PASSWORD)
    #
    # add_shopify_credentials_to_existing_client(rds_data_store, "string1")


if __name__ == "__main__":
    main()
