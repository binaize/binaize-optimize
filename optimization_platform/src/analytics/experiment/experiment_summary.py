from utils.date_utils import DateUtils
import pandas as pd
from optimization_platform.src.analytics.experiment.conversion_rate import get_conversion_rate_of_experiment


def get_summary_of_experiment(data_store, client_id, experiment_id):
    yo = get_conversion_rate_of_experiment(data_store=data_store, client_id=client_id,
                                           experiment_id=experiment_id)
    df = pd.DataFrame(yo, columns=["variation_name", "variation_id", "num_session", "num_visitor",
                                   "goal_conversion_count", "goal_conversion", "sales_conversion_count",
                                   "sales_conversion"])
    sql = \
        """
            select
                max(creation_time),
                min(creation_time)
            from
                events 
            where
                client_id = '{client_id}' 
                and experiment_id = '{experiment_id}'
        """.format(client_id=client_id, experiment_id=experiment_id)
    records = data_store.run_custom_sql(sql)
    result = dict()
    result["status"] = "<strong> SUMMARY : </strong> Not enough visitors on the website."
    result["conclusion"] = "<strong> STATUS : </strong> Not enough visitors on the website."
    result[
        "recommendation"] = "<strong> RECOMMENDATION : </strong> <span style = 'color: blue; font-size: 16px;'><strong>  CONTINUE </strong></span> the Experiment."
    if records[0][0] is None or records[0][0] is None:
        return result
    delta = records[0][0] - records[0][1]
    variation_names = df["variation_name"].tolist()
    visitor_converted = df["goal_conversion_count"].tolist()
    visitor_count = df["num_visitor"].tolist()
    num_days = delta.days

    if len(variation_names) <= 1:
        return result

    from optimization_platform.src.optim.abtest import ABTest
    ab = ABTest(arm_name_list=variation_names, conversion_count_list=visitor_converted,
                session_count_list=visitor_count, num_days=num_days)

    best_variation = ab.get_best_arm()
    confidence = ab.get_best_arm_confidence()
    confidence_percentage = round(confidence * 100, 2)
    betterness_score = ab.get_betterness_score()
    betterness_percentage = round(betterness_score * 100, 2)
    remaining_sample_size = ab.get_estimated_sample_size()
    remaining_time = ab.get_remaining_time()
    remaining_days = int(remaining_time) + 1

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
    if remaining_sample_size < 0:
        conclusion = "<strong> STATUS : </strong> There is <span style = 'color: green; font-size: 16px;'><strong> ENOUGH </strong></span>" \
                     " evidence to conclude the experiment. " \
                     "There is <span style = 'color: red; font-size: 16px;'><strong> NO CLEAR WINNER </strong></span>. " \
                     "We are <span style = 'color: red; font-size: 16px;'><strong> {confidence_percentage}%" \
                     " </strong></span> confident that <span style = 'color: blue; font-size: 16px;'><strong> {variation} </strong></span> " \
                     "is the best.".format(confidence_percentage=confidence_percentage, variation=best_variation)
        recommendation = "<strong> RECOMMENDATION : </strong> <span style = 'color: green; font-size: 16px;'><strong>  STOP </strong></span> the Experiment."
    if confidence > 0.95:
        conclusion = "<strong> STATUS : </strong> There is <span style = 'color: green; font-size: 16px;'> <strong> ENOUGH </strong></span>" \
                     " evidence to conclude the experiment. " \
                     "We have a <span style = 'color: green; font-size: 16px;'><strong> WINNER </strong></span>. " \
                     "We are <span style = 'color: green; font-size: 16px;'><strong> {confidence_percentage}% </strong></span> confident " \
                     "that <span style = 'color: blue; font-size: 16px;'><strong> {variation} </strong></span> is the best.".format(
            confidence_percentage=confidence_percentage, variation=best_variation)
        recommendation = "<strong> RECOMMENDATION : </strong> <span style = 'color: green; font-size: 16px;'><strong>  STOP </strong></span> the Experiment."

        # <span style = 'color: #388e3c; font-size: 16px;'> SUMMARY </span>

    result["status"] = status
    result["conclusion"] = conclusion
    result["recommendation"] = recommendation
    return result
