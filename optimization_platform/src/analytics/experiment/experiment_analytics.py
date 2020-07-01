from optimization_platform.src.analytics.experiment.conversion_table import get_conversion_table_of_experiment
from optimization_platform.src.analytics.experiment.experiment_summary import get_summary_of_experiment
from optimization_platform.src.analytics.experiment.conversion import get_conversion_per_variation_over_time


class ExperimentAnalytics(object):

    @classmethod
    def get_conversion_per_variation_over_time(cls, data_store, client_id, experiment_id, timezone_str):
        return get_conversion_per_variation_over_time(data_store, client_id, experiment_id,
                                                      timezone_str=timezone_str)

    @classmethod
    def get_conversion_table_of_experiment(cls, data_store, client_id, experiment_id):
        return get_conversion_table_of_experiment(data_store, client_id, experiment_id)

    @classmethod
    def get_summary_of_experiment(cls, data_store, client_id, experiment_id):
        return get_summary_of_experiment(data_store, client_id, experiment_id)

