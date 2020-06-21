import datetime
from unittest import TestCase
from unittest.mock import Mock, patch

import psycopg2
import pytz

from utils.date_utils import DateUtils

datetime_mock = Mock(wraps=datetime.datetime)
tz = pytz.timezone("Asia/Kolkata")
datetime_now = tz.localize(datetime.datetime(2020, 5, 21, 13, 0, 0, 0))
datetime_mock.now.return_value = datetime_now


class TestDateUtils(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestDateUtils, self).__init__(*args, **kwargs)

    @patch('datetime.datetime', new=datetime_mock)
    def test_get_timestamp_now_for_timezone(self):
        expected_timestamp_now = int(datetime_now.timestamp())
        timestamp_now = DateUtils.get_timestamp_now()
        self.assertEqual(first=timestamp_now, second=expected_timestamp_now)

    def test_convert_timestamp_to_utc_datetime_string(self):
        expected_result = '2020-05-21T07:30:00+00:00'
        result = DateUtils.convert_timestamp_to_utc_iso_string(1590046200)
        self.assertEqual(first=result, second=expected_result)

    @patch('datetime.datetime', new=datetime_mock)
    def test_get_date_range_in_utc_str(self):
        result = DateUtils.get_date_range_in_utc_str("Asia/Kolkata")
        expected_result = [('2020-05-14T18:30:00+00:00', '2020-05-15T18:29:59+00:00', 'May 15'),
                           ('2020-05-15T18:30:00+00:00', '2020-05-16T18:29:59+00:00', 'May 16'),
                           ('2020-05-16T18:30:00+00:00', '2020-05-17T18:29:59+00:00', 'May 17'),
                           ('2020-05-17T18:30:00+00:00', '2020-05-18T18:29:59+00:00', 'May 18'),
                           ('2020-05-18T18:30:00+00:00', '2020-05-19T18:29:59+00:00', 'May 19'),
                           ('2020-05-19T18:30:00+00:00', '2020-05-20T18:29:59+00:00', 'May 20'),
                           ('2020-05-20T18:30:00+00:00', '2020-05-21T07:30:00+00:00', 'May 21')]
        self.assertListEqual(list1=result, list2=expected_result)

    def test_convert_datetime_to_dashboard_formatted_string(self):
        timestampz = datetime.datetime(2020, 5, 27, 9, 15, 23,
                                       tzinfo=psycopg2.tz.FixedOffsetTimezone(offset=330, name=None))
        result = DateUtils.convert_datetime_to_experiment_dashboard_date_string(datetime_obj=timestampz)
        expected_result = '27-May-2020'
        self.assertEqual(first=result, second=expected_result)

    def test_convert_dashboard_date_string_to_iso_string(self):
        date_string = '2020-06-20'
        result = DateUtils.convert_dashboard_date_string_to_iso_string(date_string, "UTC")
        expected_result = '2020-06-20T00:00:00+00:00'
        self.assertEqual(first=result, second=expected_result)

    def test_convert_datetime_to_conversion_dashboard_date_string(self):
        datetime_obj = datetime.datetime(2020, 5, 27, 9, 15, 23,
                                         tzinfo=psycopg2.tz.FixedOffsetTimezone(offset=330, name=None))
        result = DateUtils.convert_datetime_to_conversion_dashboard_date_string(datetime_obj=datetime_obj,
                                                                                timezone_str="America/Grenada")
        expected_result = '2020-05-26'
        self.assertEqual(first=result, second=expected_result)

    def test_convert_conversion_datestring_to_iso_string(self):
        date_string = '2020-06-20T12-44-47'
        result = DateUtils.convert_conversion_datestring_to_iso_string(date_string, "Asia/Kolkata")
        expected_result = '2020-06-20T07:14:47+00:00'
        self.assertEqual(first=result, second=expected_result)

