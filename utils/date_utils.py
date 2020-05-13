import datetime
import pytz


def utc_timestamp_to_utc_datetime_string(x):
    return datetime.datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')


def timestampz_to_string(x):
    localtz = pytz.timezone('UTC')
    y = x.astimezone(localtz)
    # return y.strftime("%d-%b-%Y %H:%M:%S")
    return y.strftime("%d-%b-%Y")
