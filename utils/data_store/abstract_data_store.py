from abc import abstractmethod


class AbstractDataStore(object):

    @abstractmethod
    def read_record_from_data_store(self, **args):
        """
        Read table into PANDAS dataframe

        TODO complete docstring

        """
        return

    def insert_record_to_data_store(self, **args):
        """

        """
        return

    def update_record_indata_store(self, **args):
        """

        """
        return

