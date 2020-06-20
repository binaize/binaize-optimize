import datetime

import pytz


class DateUtils(object):

    @classmethod
    def convert_timestamp_to_utc_iso_string(cls, timestamp):
        tz = pytz.timezone('UTC')
        datetime_utc = datetime.datetime.fromtimestamp(timestamp, tz)
        iso_string = cls.convert_datetime_to_iso_string(datetime_obj=datetime_utc)
        return iso_string

    @classmethod
    def convert_datetime_to_experiment_dashboard_date_string(cls, datetime_obj):
        tz = pytz.timezone('UTC')
        y = datetime_obj.astimezone(tz)
        return y.strftime("%d-%b-%Y")

    @classmethod
    def convert_datetime_to_iso_string(cls, datetime_obj):
        return datetime_obj.isoformat()

    @classmethod
    def get_date_range_in_utc_str(cls, client_timezone_str):
        utc_timezone_str = "UTC"
        time_range = list()
        client_datetime_now = cls.get_datetime_now_for_timezone(timezone_str=client_timezone_str)
        numdays = 6
        for i in range(numdays + 1):
            date = client_datetime_now - datetime.timedelta(days=numdays - i)
            client_date = date.strftime('%b %d')
            start_date = date.replace(hour=0, minute=0, second=0)
            start_date = cls.change_timezone(datetime_obj=start_date, timezone_str=utc_timezone_str)
            start_date_utc_str = cls.convert_datetime_to_iso_string(datetime_obj=start_date)
            end_date = client_datetime_now
            if i != numdays:
                end_date = date.replace(hour=23, minute=59, second=59)
            end_date = cls.change_timezone(datetime_obj=end_date, timezone_str=utc_timezone_str)
            end_date_utc_str = cls.convert_datetime_to_iso_string(datetime_obj=end_date)
            time_range.append((start_date_utc_str, end_date_utc_str, client_date))
        return time_range

    @classmethod
    def get_timestamp_now(cls):
        timestamp = int(datetime.datetime.now().timestamp())
        return timestamp

    @classmethod
    def get_datetime_now_for_timezone(cls, timezone_str):
        datetime_now_tz = datetime.datetime.now(pytz.timezone(timezone_str))
        return datetime_now_tz

    @classmethod
    def change_timezone(cls, datetime_obj, timezone_str):
        return datetime_obj.astimezone(pytz.timezone(timezone_str))

    @classmethod
    def convert_dashboard_date_string_to_iso_string(cls, date_string, timezone_str):
        datetime_obj = datetime.datetime.strptime(date_string, '%Y-%m-%d')
        timezone = pytz.timezone(timezone_str)
        datetime_tz = timezone.localize(datetime_obj)
        utc_str = cls.convert_datetime_to_iso_string(datetime_tz)
        return utc_str

    @classmethod
    def convert_datetime_to_conversion_dashboard_date_string(cls, datetime_obj, timezone_str):
        datetime_tz = cls.change_timezone(datetime_obj=datetime_obj, timezone_str=timezone_str)
        datetime_str = datetime_tz.strftime("%Y-%m-%d")
        return datetime_str
