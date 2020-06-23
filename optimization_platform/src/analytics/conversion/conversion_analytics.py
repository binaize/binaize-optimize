from optimization_platform.src.analytics.conversion.landing_page import get_landing_page_analytics
from optimization_platform.src.analytics.conversion.product_conversion import get_product_conversion_analytics
from optimization_platform.src.analytics.conversion.shop_funnel import get_shop_funnel_analytics


class ConversionAnalytics(object):

    @classmethod
    def get_shop_funnel_analytics(cls, data_store, client_id, start_date_str, end_date_str, timezone_str):
        return get_shop_funnel_analytics(data_store, client_id, start_date_str, end_date_str, timezone_str)

    @classmethod
    def get_product_conversion_analytics(cls, data_store, client_id, start_date_str, end_date_str, timezone_str):
        return get_product_conversion_analytics(data_store, client_id, start_date_str, end_date_str, timezone_str)

    @classmethod
    def get_landing_page_analytics(cls, data_store, client_id, start_date_str, end_date_str, timezone_str):
        return get_landing_page_analytics(data_store, client_id, start_date_str, end_date_str, timezone_str)
