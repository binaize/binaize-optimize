from optimization_platform.src.analytics.experiment.conversion_rate import get_conversion_rate_per_variation_over_time, \
    get_conversion_rate_of_experiment
from optimization_platform.src.analytics.experiment.experiment_summary import get_summary_of_experiment
from optimization_platform.src.analytics.experiment.session_count import get_session_count_per_variation_over_time
from optimization_platform.src.analytics.experiment.visitor_count import get_visitor_count_per_variation_over_time


class ExperimentAnalytics(object):
    @classmethod
    def get_session_count_per_variation_over_time(cls, data_store, client_id, experiment_id):
        return get_session_count_per_variation_over_time(data_store, client_id, experiment_id)

    @classmethod
    def get_visitor_count_per_variation_over_time(cls, data_store, client_id, experiment_id):
        return get_visitor_count_per_variation_over_time(data_store, client_id, experiment_id)

    @classmethod
    def get_conversion_rate_per_variation_over_time(cls, data_store, client_id, experiment_id):
        return get_conversion_rate_per_variation_over_time(data_store, client_id, experiment_id)

    @classmethod
    def get_conversion_rate_of_experiment(cls, data_store, client_id, experiment_id):
        return get_conversion_rate_of_experiment(data_store, client_id, experiment_id)

    @classmethod
    def get_summary_of_experiment(cls, data_store, client_id, experiment_id):
        return get_summary_of_experiment(data_store, client_id, experiment_id)
