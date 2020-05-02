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


def main():
    s3_data_store = S3DataStore(src_bucket_name="binaize-dev", access_key=AWS_ACCESS_KEY_ID,
                                secret_key=AWS_SECRET_ACCESS_KEY)
    print(s3_data_store.get_name())
    d = {"hi": 6}
    s3_data_store.write_json_file(filename="tuhin/file.json", contents=d)
    f = s3_data_store.read_json_file(filename="tuhin/file.json")
    print(f)
    s3_data_store.download_folder(src="tuhin", target=".")


if __name__ == "__main__":
    main()
