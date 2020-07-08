import sys

from config import *
from optimization_platform.src.agents.client_agent import ClientAgent
from optimization_platform.src.agents.order_agent import OrderAgent
from utils.data_store.rds_data_store import RDSDataStore
from utils.logger.pylogger import get_logger
from optimization_platform.src.agents.cookie_agent import CookieAgent
from optimization_platform.src.agents.event_agent import EventAgent
from optimization_platform.src.agents.experiment_agent import ExperimentAgent
from optimization_platform.src.agents.variation_agent import VariationAgent
from utils.date_utils import DateUtils

logger = get_logger("sync_orders", "INFO")

rds_data_store = RDSDataStore(host=AWS_RDS_HOST, port=AWS_RDS_PORT,
                              dbname=AWS_RDS_DBNAME,
                              user=AWS_RDS_USER,
                              password=AWS_RDS_PASSWORD)


def main(rds_data_store):
    client_ids = ClientAgent.get_all_client_ids(data_store=rds_data_store)
    logger.info("{hash}".format(hash="".join(["#" for i in range(60)])))
    logger.info("sync orders job for client ids = {client_ids} started".format(client_ids=",".join(client_ids)))
    for client_id in client_ids:
        logger.info("sync orders job for client id = {client_id} started".format(client_id=client_id))
        try:
            _, session_cart_time_list = OrderAgent.sync_orders(data_store=rds_data_store, client_id=client_id)
            CookieAgent.register_dummy_cookie_for_client(data_store=rds_data_store, client_id=client_id,
                                                         session_cart_time_list=session_cart_time_list)
            experiment_id = ExperimentAgent.get_latest_experiment_id(data_store=rds_data_store, client_id=client_id)
            if experiment_id is not None:
                for session_cart_time in session_cart_time_list:
                    session_id = session_cart_time[0]
                    creation_time = DateUtils.convert_utc_iso_format_to_timestamp(datetime_str=session_cart_time[2])
                    variation = VariationAgent.get_variation_id_to_recommend(data_store=rds_data_store,
                                                                             client_id=client_id,
                                                                             experiment_id=experiment_id,
                                                                             session_id=session_id)
                    if variation is not None:
                        EventAgent.register_event_for_client(data_store=rds_data_store,
                                                             client_id=client_id,
                                                             experiment_id=experiment_id,
                                                             session_id=session_id,
                                                             variation_id=variation["variation_id"],
                                                             event_name="served",
                                                             creation_time=creation_time)

            logger.info("sync orders job for client id = {client_id} succeeded".format(client_id=client_id))
        except Exception:
            logger.info("sync orders job for client id = {client_id} failed".format(client_id=client_id))
        logger.info("sync orders job for client id = {client_id} ended".format(client_id=client_id))
    logger.info("sync orders job for client ids = {client_ids} ended".format(client_ids=",".join(client_ids)))
    logger.info("{hash}".format(hash="".join(["#" for i in range(60)])))


def init():
    if __name__ == "__main__":
        sys.exit(main(rds_data_store))


init()
