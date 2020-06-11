import datetime

import pytz


class DateUtils(object):

    @classmethod
    def convert_timestamp_to_utc_iso_string(cls, timestamp):
        tz = pytz.timezone('UTC')
        return datetime.datetime.fromtimestamp(timestamp, tz).isoformat()

    @classmethod
    def timestampz_to_dashboard_formatted_string(cls, timestampz):
        tz = pytz.timezone('UTC')
        y = timestampz.astimezone(tz)
        return y.strftime("%d-%b-%Y")

    @classmethod
    def convert_timestampz_to_iso_string(cls,timestampz):
        return timestampz.isoformat()

    @classmethod
    def get_date_range_in_utc_str(cls, client_timezone_str):
        client_timezone = pytz.timezone(client_timezone_str)
        utc_timezone = pytz.timezone("UTC")
        time_range = list()
        client_datetime_now = datetime.datetime.now(client_timezone)
        numdays = 6
        for i in range(numdays + 1):
            date = client_datetime_now - datetime.timedelta(days=numdays - i)
            client_date = date.strftime('%b %d')
            start_date = date.replace(hour=0, minute=0, second=0)
            start_date = start_date.astimezone(utc_timezone)
            start_date_utc_str = start_date.isoformat()
            end_date = client_datetime_now
            if i != numdays:
                end_date = date.replace(hour=23, minute=59, second=59)
            end_date = end_date.astimezone(utc_timezone)
            end_date_utc_str = end_date.isoformat()
            time_range.append((start_date_utc_str, end_date_utc_str, client_date))
        return time_range

    @classmethod
    def get_timestamp_now(cls):
        timezone = "UTC"
        timestamp = int(datetime.datetime.now(pytz.timezone(timezone)).timestamp())
        return timestamp

    @classmethod
    def change_timezone(cls, src_datetime, timezone):
        return src_datetime.astimezone(pytz.timezone(timezone))
