from config import TABLE_COOKIES
from utils.data_store.rds_data_store import IteratorFile
from utils.date_utils import DateUtils


class VisitorAgent(object):

    @classmethod
    def register_visitor_for_client(cls, data_store, client_id, session_id, ip, city, region, country, lat, long,
                                    timezone, browser, os, device, fingerprint, creation_time):
        table = "visitors"

        columns = ["client_id", "session_id", "ip", "city", "region", "country", "lat", "long",
                   "timezone", "browser", "os", "device", "fingerprint"]
        where = "client_id='{client_id}' and session_id='{session_id}' and ip='{ip}' and city='{city}' and region='region'" \
                "and country='{country}' and lat='{lat}' and long='{long}' and timezone='{timezone}'" \
                "and browser='{browser}' and os='{os}' and device='{device}' and fingerprint='{fingerprint}'".format(
            client_id=client_id,
            session_id=session_id, ip=ip, city=city, region=region, country=country, lat=lat, long=long,
            timezone=timezone, browser=browser, os=os, device=device, fingerprint=fingerprint)

        column = ",".join(columns)
        sql = """ SELECT {column} from {table} where {where}""".format(column=column, table=table, where=where)

        mobile_records = data_store.run_select_sql(query=sql)
        record_exist = False
        if mobile_records is not None and len(mobile_records) > 0:
            record_exist = True
        status = record_exist
        if not record_exist:
            creation_time_utc_str = DateUtils.convert_timestamp_to_utc_iso_string(creation_time)
            columns_value_dict = {"client_id": client_id,
                                  "session_id": session_id, "ip": ip, "city": city, "region": region,
                                  "country": country, "lat": lat, "long": long, "timezone": timezone,
                                  "browser": browser, "os": os, "device": device, "fingerprint": fingerprint,
                                  "creation_time": creation_time_utc_str}
            columns = list(columns_value_dict.keys())
            column = ",".join(columns)
            values = [columns_value_dict[key] for key in columns]
            value = str(tuple(values))

            sql = """insert into {table} ({column}) values {value}""".format(table=table, column=column, value=value)
            status = data_store.run_insert_into_sql(query=sql)
        return status
