"""Tests for the Base Model LSA"""

from unittest import TestCase

from config import *
from utils.data_store.s3_data_store import S3DataStore


class TestS3DataStore(TestCase):
    """Tests for the Latent Semantic Analysis Model """

    def __init__(self, *args, **kwargs):
        super(TestS3DataStore, self).__init__(*args, **kwargs)
        self.s3_data_store = S3DataStore(access_key=AWS_ACCESS_KEY_ID,
                                         secret_key=AWS_SECRET_ACCESS_KEY)

    def test_01_create_bucket(self):
        response = self.s3_data_store.create_bucket(bucket_name="binaize-wow")
        self.assertEqual(first=str(response), second="s3.Bucket(name='binaize-wow')")

    def test_02_write_json_file(self):
        data = {"hi": 6}
        response = self.s3_data_store.write_json_file(bucket_name="binaize-wow", filename="binaize/file1.json",
                                                      contents=data)
        self.assertEqual(first=response["ResponseMetadata"]["HTTPStatusCode"], second=200)
        response = self.s3_data_store.write_json_file(bucket_name="binaize-wow", filename="binaize/file2.json",
                                                      contents=data)
        self.assertEqual(first=response["ResponseMetadata"]["HTTPStatusCode"], second=200)

    def test_03_read_json_file(self):
        data = {"hi": 6}
        response = self.s3_data_store.read_json_file(bucket_name="binaize-wow", filename="binaize/file1.json")
        self.assertDictEqual(d1=data, d2=response)

    def test_04_download_file(self):
        response = self.s3_data_store.download_file(bucket_name="binaize-wow", src="binaize/file1.json",
                                                    target="/tmp/file1.json")
        self.assertEqual(first=response, second=None)

    def test_05_upload_file(self):
        response = self.s3_data_store.upload_file(bucket_name="binaize-wow", src="/tmp/file1.json",
                                                  target="binaize/file3.json")
        self.assertEqual(first=response, second=None)

    def test_06_list_files(self):
        list = ['binaize/file1.json', 'binaize/file2.json', 'binaize/file3.json']
        response = self.s3_data_store.list_files(bucket_name="binaize-wow")
        self.assertListEqual(list1=list, list2=response)

    def test_07_delete_bucket(self):
        response = self.s3_data_store.delete_bucket(bucket_name="binaize-wow")
        self.assertEqual(first=response["ResponseMetadata"]["HTTPStatusCode"], second=204)
