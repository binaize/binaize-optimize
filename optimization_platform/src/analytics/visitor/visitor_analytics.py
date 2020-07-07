from optimization_platform.src.analytics.visitor.sales import get_sales_analytics


class VisitorAnalytics(object):

    @classmethod
    def get_sales_analytics(cls, data_store, client_id, start_date_str, end_date_str, timezone_str):
        return get_sales_analytics(data_store, client_id, start_date_str, end_date_str, timezone_str)
