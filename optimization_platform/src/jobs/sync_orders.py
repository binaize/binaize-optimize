from optimization_platform.src.agents.order_agent import OrderAgent
from optimization_platform.src.agents.client_agent import ClientAgent
from utils.data_store.rds_data_store import RDSDataStore
from config import *
from utils.logger.pylogger import get_logger

logger = get_logger("sync_orders", "INFO")

if __name__ == "__main__":
    rds_data_store = RDSDataStore(host=AWS_RDS_HOST, port=AWS_RDS_PORT,
                                  dbname=AWS_RDS_DBNAME,
                                  user=AWS_RDS_USER,
                                  password=AWS_RDS_PASSWORD)
    client_ids = ClientAgent.get_all_client_ids(data_store=rds_data_store)
    logger.info("{hash}".format(hash="".join(["#" for i in range(60)])))
    logger.info("sync orders job for client ids = {client_ids} started".format(client_ids=",".join(client_ids)))
    for client_id in client_ids:
        logger.info("sync orders job for client id = {client_id} started".format(client_id=client_id))
        OrderAgent.sync_orders(data_store=rds_data_store, client_id=client_id)
        logger.info("sync orders job for client id = {client_id} ended".format(client_id=client_id))
    logger.info("sync orders job for client ids = {client_ids} ended".format(client_ids=",".join(client_ids)))
    logger.info("{hash}".format(hash="".join(["#" for i in range(60)])))


