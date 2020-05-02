from utils.data_store.rds_data_store import RDSDataStore
import uuid


# def get_experiment_ids_for_client_id(data_store, client_id):
#     """
#     :param data_store: data_store object
#     :param client_id: id of the smb client
#     :return: the list of the experiment ids
#     """
#     table = "experiment"
#     columns = ["experiment_id"]
#     where = "client_id='{client_id}'".format(client_id=client_id)
#     df = data_store.read_record_from_data_store(table=table, columns=columns, where=where)
#     experiment_ids = df["experiment_id"].tolist()
#     print(len(experiment_ids))
#     return experiment_ids


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


# def create_experiment_for_client_id(data_store, client_id, experiment_id, variation_ids):
#     """
#
#     :param data_store: data store object
#     :param client_id: id of the smb client
#     :param experiment_id: id of the experiment
#     :param variation_ids: id of the variation ids for the experiment id
#     :return: True in case of success, False in case of failure
#     """
#     table = "experiment"
#     columns_value_dict = {"client_id": client_id, "experiment_id": experiment_id, "variation_list": variation_ids}
#     try:
#         data_store.insert_record_to_data_store(table=table, columns_value_dict=columns_value_dict)
#     except Exception as e:
#         print("create_experiment_for_client_id failed")
#         return False
#     return True


# def get_variation_ids_for_client_id_and_experiment_id(data_store, client_id, experiment_id):
#     table = "experiment"
#     columns = ["variation_list"]
#     where = "client_id='{client_id}' and experiment_id='{experiment_id}'".format(client_id=client_id,
#                                                                                  experiment_id=experiment_id)
#     df = data_store.read_record_from_data_store(table=table, columns=columns, where=where)
#     variation_ids = df["variation_list"].tolist()
#     print(len(variation_ids))
#     return variation_ids
#
#
# def update_experiment():
#     return "g"
#
#
# def get_variation_id_to_route():
#     return "g"
#
#
# def register_event():
#     return "g"
#
#
# def get_result():
#     return "g"


def main():
    postgres_data_store = RDSDataStore(host="localhost",
                                            port="5432",
                                            dbname="binaize",
                                            user="tuhinsharma")

    # client_id = "3fab276ffdf3433a90f6693c6d7f52a4"
    # experiment_id = uuid.uuid4().hex
    # variation_ids = [uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex]
    #
    # print(
    #     create_experiment_for_client_id(data_store=postgres_data_store, client_id=client_id,
    #                                     experiment_id=experiment_id, variation_ids=variation_ids))
    # print(
    #     get_experiment_ids_for_client_id(data_store=postgres_data_store, client_id=client_id))
    #
    # print(get_variation_ids_for_client_id_and_experiment_id(data_store=postgres_data_store,
    #                                                         client_id=client_id,
    #                                                         experiment_id=experiment_id))


if __name__ == "__main__":
    main()
