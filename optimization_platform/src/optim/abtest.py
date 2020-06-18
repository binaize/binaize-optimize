import numpy as np
import statsmodels.stats.power as power
import statsmodels.stats.proportion as ssp

from optimization_platform.src.optim.abstract_optim import AbstractOptim


class ABTest(AbstractOptim):
    def __init__(self, arm_name_list, conversion_count_list, session_count_list, num_days, *args, **kwargs):
        super(ABTest, self).__init__(*args, **kwargs)
        self._arm_name_list = arm_name_list
        self._session_count_list = np.array(session_count_list)
        self._conversion_count_list = np.array(conversion_count_list)
        conversion_list = self._conversion_count_list / self._session_count_list
        max_arm_index = np.argmax(conversion_list)
        combs = [(max_arm_index, i) for i in range(len(arm_name_list)) if i != max_arm_index]
        conf_array = np.zeros(len(combs))
        for i in range(len(combs)):
            comb = combs[i]
            conversion = [self._conversion_count_list[comb[0]], self._conversion_count_list[comb[1]]]
            session = [self._session_count_list[comb[0]], self._session_count_list[comb[1]]]
            result = ssp.proportions_chisquare(count=conversion, nobs=session)
            confidence = 1 - result[1]
            conf_array[i] = confidence
        self._conf_array = conf_array
        self._confidence = np.prod(conf_array)
        self._best_arm_index = max_arm_index
        self._betterness_score = (conversion_list[max_arm_index] - (
                np.sum(conversion_list) - conversion_list[max_arm_index]) / (len(arm_name_list) - 1)) / \
                                 conversion_list[max_arm_index]
        EFFECT_SIZE = 0.1  # 0.1 is low effect. 0.5 is large effect
        ALPHA = 0.05  # Significance level.
        POWER = 0.8  # 1 - acceptable type II error rate. 0.8 is a good choice
        N_BINS = len(arm_name_list)  # Default too. For our case, it will be number of options we have

        chipower = power.GofChisquarePower()
        estimated_sample_size = chipower.solve_power(EFFECT_SIZE, nobs=None, alpha=ALPHA, power=POWER,
                                                     n_bins=N_BINS)
        self._estimated_sample_size = int(estimated_sample_size * 2) - self._session_count_list.sum()
        avg_session_per_day = self._session_count_list.sum() / (num_days+1)
        self._remaining_time = self._estimated_sample_size / avg_session_per_day

    def get_best_arm(self):
        return self._arm_name_list[self._best_arm_index]

    def get_best_arm_confidence(self):
        return self._confidence

    def get_betterness_score(self):
        return self._betterness_score

    def get_remaining_time(self):
        return self._remaining_time

    def get_estimated_sample_size(self):
        return self._estimated_sample_size


