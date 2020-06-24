import pandas as pd

from optimization_platform.src.analytics.experiment.conversion_table import get_conversion_table_of_experiment
from optimization_platform.src.optim.abtest import ABTest


def get_summary_of_experiment(data_store, client_id, experiment_id):
    conversion = get_conversion_table_of_experiment(data_store=data_store, client_id=client_id,
                                                    experiment_id=experiment_id)

    if len(conversion) == 0:
        conclusion, recommendation, status = get_summary_for_data_not_enough()
        result = construct_result(conclusion, recommendation, status)
        return result

    df = pd.DataFrame(conversion, columns=["variation_name", "variation_id", "num_session", "num_visitor",
                                           "goal_conversion_count", "goal_conversion", "sales_conversion_count",
                                           "sales_conversion"])

    end_time, start_time = get_start_time_and_end_time_of_experiment(client_id, data_store, experiment_id)
    if start_time is None or end_time is None:
        conclusion, recommendation, status = get_summary_for_data_not_enough()
        result = construct_result(conclusion, recommendation, status)
        return result
    delta = end_time - start_time
    num_hour = int(delta.seconds / 3600)
    variation_names = df["variation_name"].tolist()
    visitor_converted = df["goal_conversion_count"].tolist()
    visitor_count = df["num_visitor"].tolist()

    if len(variation_names) <= 1:
        conclusion, recommendation, status = get_summary_for_data_not_enough()
        result = construct_result(conclusion, recommendation, status)
        return result

    ab = ABTest(arm_name_list=variation_names, conversion_count_list=visitor_converted,
                session_count_list=visitor_count, num_hour=num_hour)

    best_variation = ab.get_best_arm()
    confidence = ab.get_best_arm_confidence()
    confidence_percentage = round(confidence * 100, 2)
    betterness_score = ab.get_betterness_score()
    betterness_percentage = round(betterness_score * 100, 2)
    remaining_sample_size = ab.get_estimated_sample_size()
    remaining_time = ab.get_remaining_time()
    remaining_days = int(remaining_time) + 1

    conclusion, recommendation, status = get_summary_for_pending_result(best_variation, betterness_percentage,
                                                                        remaining_days,
                                                                        remaining_sample_size)
    if remaining_sample_size < 0:
        conclusion, recommendation, status = get_summary_for_exceeding_deadline(best_variation, betterness_percentage,
                                                                                confidence_percentage)
    if confidence > 0.95:
        conclusion, recommendation, status = get_summary_for_successful_conclusion(best_variation,
                                                                                   betterness_percentage,
                                                                                   confidence_percentage)

    result = construct_result(conclusion, recommendation, status)
    return result


def get_start_time_and_end_time_of_experiment(client_id, data_store, experiment_id):
    sql = \
        """
            select
                min(creation_time),
                max(creation_time)
            from
                events 
            where
                client_id = '{client_id}' 
                and experiment_id = '{experiment_id}'
        """.format(client_id=client_id, experiment_id=experiment_id)
    records = data_store.run_custom_sql(sql)
    start_time = records[0][0]
    end_time = records[0][1]
    return end_time, start_time


def get_summary_for_successful_conclusion(best_variation, betterness_percentage, confidence_percentage):
    status = "<strong> SUMMARY : </strong><span style = 'color: blue; font-size: 16px;'><strong> {variation} </strong></span> is winning." \
             " It is <span style = 'color: blue; font-size: 16px;'><strong> {betterness_percentage}% </strong></span> better than the others.".format(
        variation=best_variation,
        betterness_percentage=betterness_percentage)
    conclusion = "<strong> STATUS : </strong> There is <span style = 'color: green; font-size: 16px;'> <strong> ENOUGH </strong></span>" \
                 " evidence to conclude the experiment. " \
                 "We have a <span style = 'color: green; font-size: 16px;'><strong> WINNER </strong></span>. " \
                 "We are <span style = 'color: green; font-size: 16px;'><strong> {confidence_percentage}% </strong></span> confident " \
                 "that <span style = 'color: blue; font-size: 16px;'><strong> {variation} </strong></span> is the best.".format(
        confidence_percentage=confidence_percentage, variation=best_variation)
    recommendation = "<strong> RECOMMENDATION : </strong> <span style = 'color: green; font-size: 16px;'><strong>  STOP </strong></span> the Experiment."
    return conclusion, recommendation, status


def get_summary_for_exceeding_deadline(best_variation, betterness_percentage, confidence_percentage):
    status = "<strong> SUMMARY : </strong><span style = 'color: blue; font-size: 16px;'><strong> {variation} </strong></span> is winning." \
             " It is <span style = 'color: blue; font-size: 16px;'><strong> {betterness_percentage}% </strong></span> better than the others.".format(
        variation=best_variation,
        betterness_percentage=betterness_percentage)
    conclusion = "<strong> STATUS : </strong> There is <span style = 'color: green; font-size: 16px;'><strong> ENOUGH </strong></span>" \
                 " evidence to conclude the experiment. " \
                 "There is <span style = 'color: red; font-size: 16px;'><strong> NO CLEAR WINNER </strong></span>. " \
                 "We are <span style = 'color: red; font-size: 16px;'><strong> {confidence_percentage}%" \
                 " </strong></span> confident that <span style = 'color: blue; font-size: 16px;'><strong> {variation} </strong></span> " \
                 "is the best.".format(confidence_percentage=confidence_percentage, variation=best_variation)
    recommendation = "<strong> RECOMMENDATION : </strong> <span style = 'color: green; font-size: 16px;'><strong>  STOP </strong></span> the Experiment."
    return conclusion, recommendation, status


def get_summary_for_pending_result(best_variation, betterness_percentage, remaining_days, remaining_sample_size):
    status = "<strong> SUMMARY : </strong><span style = 'color: blue; font-size: 16px;'><strong> {variation} </strong></span> is winning." \
             " It is <span style = 'color: blue; font-size: 16px;'><strong> {betterness_percentage}% </strong></span> better than the others.".format(
        variation=best_variation,
        betterness_percentage=betterness_percentage)
    conclusion = "<strong> STATUS : </strong> There is <span style = 'color: red; font-size: 16px;'><strong> NOT ENOUGH</strong></span>" \
                 " evidence to conclude the experiment " \
                 "(It is <span style = 'color: red; font-size: 16px;'><strong> NOT </strong></span> yet" \
                 " statistically significant)." \
                 "To be statistically confident, we need <strong> {remaining_sample_size} </strong> more visitors." \
                 "Based on recent visitor trend, experiment should run for another <strong> {remaining_days} </strong> days.".format(
        remaining_sample_size=remaining_sample_size, remaining_days=remaining_days)
    recommendation = "<strong> RECOMMENDATION : </strong> <span style = 'color: blue; font-size: 16px;'><strong>  CONTINUE </strong></span> the Experiment."
    return conclusion, recommendation, status


def construct_result(conclusion, recommendation, status):
    result = dict()
    result["status"] = status
    result["conclusion"] = conclusion
    result["recommendation"] = recommendation
    return result


def get_summary_for_data_not_enough():
    status = "<strong> SUMMARY : </strong> Not enough visitors on the website."
    conclusion = "<strong> STATUS : </strong> Not enough visitors on the website."
    recommendation = "<strong> RECOMMENDATION : </strong> <span style = 'color: blue; font-size: 16px;'><strong>  CONTINUE </strong></span> the Experiment."
    return conclusion, recommendation, status
