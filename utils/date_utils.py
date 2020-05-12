import datetime


def utc_timestamp_to_utc_datetime_string(x):
    return datetime.datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')
