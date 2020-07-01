from config import *
from utils.date_utils import DateUtils


class EventAgent(object):

    @classmethod
    def register_event_for_client(cls, data_store, client_id, experiment_id, session_id, variation_id, event_name,
                                  creation_time):
        table = TABLE_EVENTS

        creation_time_utc_str = DateUtils.convert_timestamp_to_utc_iso_string(creation_time)
        columns_value_dict = {"client_id": client_id, "experiment_id": experiment_id,
                              "variation_id": variation_id,
                              "session_id": session_id, "event_name": event_name,
                              "creation_time": creation_time_utc_str}
        columns = list(columns_value_dict.keys())
        column = ",".join(columns)
        values = [columns_value_dict[key] for key in columns]
        value = str(tuple(values))

        query = """INSERT INTO {table} ({column}) VALUES {value}""".format(table=table, column=column, value=value)
        status = data_store.run_insert_into_sql(query=query)
        return status

    @classmethod
    def register_event_for_cookie(cls, data_store, client_id, experiment_id, variation_id, session_id, event_name,
                                  creation_time):
        sql = \
            """
            select
                *
            from
                events
            where
                session_id='{session_id}'
                and client_id = '{client_id}'
                and experiment_id = '{experiment_id}'
            """.format(client_id=client_id, experiment_id=experiment_id, session_id=session_id)
        mobile_records = data_store.run_custom_sql(sql)
        status = False
        if mobile_records is not None and len(mobile_records) == 0:
            status = EventAgent.register_event_for_client(data_store=data_store,
                                                          client_id=client_id,
                                                          experiment_id=experiment_id,
                                                          session_id=session_id,
                                                          variation_id=variation_id,
                                                          event_name=event_name,
                                                          creation_time=creation_time)
        return status
