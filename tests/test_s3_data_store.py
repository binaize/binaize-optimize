from unittest import TestCase

from moto import mock_s3

from utils.data_store.s3_data_store import S3DataStore


class TestS3DataStore(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestS3DataStore, self).__init__(*args, **kwargs)

    @mock_s3
    def setUp(self):
        self.s3_data_store = S3DataStore(access_key="AWS_ACCESS_KEY_ID",
                                         secret_key="AWS_SECRET_ACCESS_KEY")

    @mock_s3
    def test_create_bucket(self):
        response = self.s3_data_store.create_bucket(bucket_name="binaize-test-bucket")
        self.assertEqual(first=str(response), second="s3.Bucket(name='binaize-test-bucket')")

    @mock_s3
    def test_write_json_file(self):
        self.test_create_bucket()
        data = {"test": 6}
        response = self.s3_data_store.write_json_file(bucket_name="binaize-test-bucket",
                                                      filename="binaize/folder/file.json",
                                                      contents=data)
        self.assertEqual(first=response["ResponseMetadata"]["HTTPStatusCode"], second=200)

    @mock_s3
    def test_read_json_file(self):
        self.test_write_json_file()
        wrong_result = {"hi": 6}
        data = {"test": 6}
        response = self.s3_data_store.read_json_file(bucket_name="binaize-test-bucket",
                                                     filename="binaize/folder/file.json")
        self.assertDictEqual(d1=data, d2=response)
        self.assertNotEqual(first=wrong_result, second=response)

    @mock_s3
    def test_download_file(self):
        self.test_write_json_file()
        response = self.s3_data_store.download_file(bucket_name="binaize-test-bucket", src="binaize/folder/file.json",
                                                    target="/tmp/file.json")
        self.assertEqual(first=response, second=None)

    @mock_s3
    def test_upload_file(self):
        self.test_create_bucket()
        with open("/tmp/file1.json", "w") as fp:
            pass
        response = self.s3_data_store.upload_file(bucket_name="binaize-test-bucket", src="/tmp/file1.json",
                                                  target="binaize/folder/file1.json")
        self.assertEqual(first=response, second=None)
        with open("/tmp/file2.json", "w") as fp:
            pass
        response = self.s3_data_store.upload_file(bucket_name="binaize-test-bucket", src="/tmp/file2.json",
                                                  target="binaize/folder/file2.json")
        self.assertEqual(first=response, second=None)

    @mock_s3
    def test_list_files(self):
        self.test_upload_file()
        filenames = ["binaize/folder/file1.json", "binaize/folder/file2.json"]
        response = self.s3_data_store.list_files(bucket_name="binaize-test-bucket")
        self.assertListEqual(list1=filenames, list2=response)
        response = self.s3_data_store.list_files(bucket_name="binaize-test-bucket", prefix="binaize/folder")
        self.assertListEqual(list1=filenames, list2=response)
        response = self.s3_data_store.list_files(bucket_name="binaize-test-bucket", max_count=2)
        self.assertListEqual(list1=filenames, list2=response)
        response = self.s3_data_store.list_files(bucket_name="binaize-test-bucket", prefix="binaize/folder",
                                                 max_count=2)
        self.assertListEqual(list1=filenames, list2=response)

    @mock_s3
    def test_delete_bucket(self):
        self.test_upload_file()
        response = self.s3_data_store.delete_bucket(bucket_name="binaize-test-bucket")
        self.assertEqual(first=response["ResponseMetadata"]["HTTPStatusCode"], second=204)
