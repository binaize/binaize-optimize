import sys

from config import *
from optimization_platform.src.agents.shop_agent import ShopAgent
from optimization_platform.src.agents.product_agent import ProductAgent
from utils.data_store.rds_data_store import RDSDataStore
from utils.logger.pylogger import get_logger

logger = get_logger("sync_products", "INFO")

rds_data_store = RDSDataStore(host=AWS_RDS_HOST, port=AWS_RDS_PORT,
                              dbname=AWS_RDS_DBNAME,
                              user=AWS_RDS_USER,
                              password=AWS_RDS_PASSWORD)


def main():
    shop_ids = ShopAgent.get_all_shop_ids(data_store=rds_data_store)
    logger.info("{hash}".format(hash="".join(["#" for i in range(60)])))
    logger.info("sync products job for shop ids = {shop_ids} started".format(shop_ids=",".join(shop_ids)))
    for shop_id in shop_ids:
        logger.info("sync products job for shop id = {shop_id} started".format(shop_id=shop_id))
        try:
            ProductAgent.sync_products(data_store=rds_data_store, shop_id=shop_id)
            logger.info("sync products job for shop id = {shop_id} succeeded".format(shop_id=shop_id))
        except Exception:
            logger.info("sync products job for shop id = {shop_id} failed".format(shop_id=shop_id))
        logger.info("sync products job for shop id = {shop_id} ended".format(shop_id=shop_id))
    logger.info("sync products job for shop ids = {shop_ids} ended".format(shop_ids=",".join(shop_ids)))
    logger.info("{hash}".format(hash="".join(["#" for i in range(60)])))


def init():
    if __name__ == "__main__":
        sys.exit(main())


init()
