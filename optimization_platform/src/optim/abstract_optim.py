from abc import abstractmethod


class AbstractOptim(object):
    @abstractmethod
    def get_best_arm(self):
        raise NotImplementedError("Method select_arm is not implemented")
