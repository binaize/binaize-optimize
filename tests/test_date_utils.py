import datetime
from unittest import TestCase
from unittest.mock import Mock, patch

from utils.date_utils import DateUtils
import psycopg2


class TestDateUtils(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestDateUtils, self).__init__(*args, **kwargs)

    def test_get_timestamp_now_for_timezone(self):
        datetime_mock = Mock(wraps=datetime.datetime)
        datetime_now = datetime.datetime(2020, 5, 21)
        expected_timestamp_now = int(datetime_now.timestamp())
        datetime_mock.now.return_value = datetime_now
        with patch('datetime.datetime', new=datetime_mock):
            timestamp_now = DateUtils.get_timestamp_now()
        self.assertEqual(first=timestamp_now, second=expected_timestamp_now)

    def test_convert_timestamp_to_utc_datetime_string(self):
        expected_result = '2020-05-28 10:05:21'
        result = DateUtils.convert_timestamp_to_utc_datetime_string(1590660321)
        self.assertEqual(first=result, second=expected_result)

    def test_get_date_range_in_utc_str(self):
        datetime_mock = Mock(wraps=datetime.datetime)
        datetime_now = datetime.datetime(2020, 5, 21)
        datetime_mock.now.return_value = datetime_now
        with patch('datetime.datetime', new=datetime_mock):
            result = DateUtils.get_date_range_in_utc_str("Asia/Kolkata")
            expected_result = [('2020-05-14 18:30:00', '2020-05-15 18:29:59', 'May 15'),
                               ('2020-05-15 18:30:00', '2020-05-16 18:29:59', 'May 16'),
                               ('2020-05-16 18:30:00', '2020-05-17 18:29:59', 'May 17'),
                               ('2020-05-17 18:30:00', '2020-05-18 18:29:59', 'May 18'),
                               ('2020-05-18 18:30:00', '2020-05-19 18:29:59', 'May 19'),
                               ('2020-05-19 18:30:00', '2020-05-20 18:29:59', 'May 20'),
                               ('2020-05-20 18:30:00', '2020-05-20 18:30:00', 'May 21')]
        self.assertListEqual(list1=result, list2=expected_result)

    def test_timestampz_to_string(self):
        timestampz = datetime.datetime(2020, 5, 27, 9, 15, 23,
                                       tzinfo=psycopg2.tz.FixedOffsetTimezone(offset=330, name=None))
        result = DateUtils.timestampz_to_string(timestampz=timestampz)
        expected_result = '27-May-2020'
        self.assertEqual(first=result, second=expected_result)
