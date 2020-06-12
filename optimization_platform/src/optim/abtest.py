import copy

from optimization_platform.src.optim.abstract_optim import AbstractOptim


class ABTest(AbstractOptim):
    def __init__(self, n_arms):
        self._n_arms = copy.copy(n_arms)

    def select_arm(self):
        pass
