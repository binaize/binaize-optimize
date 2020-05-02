import json
import os

import boto3
import botocore

from utils.data_store.abstract_data_store import AbstractDataStore


class S3DataStore(AbstractDataStore):
    def __init__(self, src_bucket_name, access_key, secret_key):
        self.session = boto3.session.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self.s3_resource = self.session.resource('s3', config=botocore.client.Config(signature_version='s3v4'))
        self.bucket = self.s3_resource.Bucket(src_bucket_name)
        self.bucket_name = src_bucket_name

    def get_name(self):
        return "S3:" + self.bucket_name

    def list_files(self, prefix=None, max_count=None):
        """List all the files in the S3 bucket"""

        list_filenames = []
        if prefix is None:
            objects = self.bucket.objects.all()
            if max_count is None:
                list_filenames = [x.key for x in objects]
            else:
                counter = 0
                for obj in objects:
                    list_filenames.append(obj.key)
                    counter += 1
                    if counter == max_count:
                        break
        else:
            objects = self.bucket.objects.filter(Prefix=prefix)
            if max_count is None:
                list_filenames = [x.key for x in objects]
            else:
                counter = 0
                for obj in objects:
                    list_filenames.append(obj.key)
                    counter += 1
                    if counter == max_count:
                        break

        return list_filenames

    def read_json_file(self, filename):
        """Read JSON file from the S3 bucket"""

        obj = self.s3_resource.Object(self.bucket_name, filename).get()['Body'].read()
        utf_data = obj.decode("utf-8")
        return json.loads(utf_data)

    def write_json_file(self, filename, contents):
        """Write JSON file into S3 bucket"""
        self.s3_resource.Object(self.bucket_name, filename).put(Body=json.dumps(contents))
        return None

    def upload_file(self, src, target):
        """Upload file into data store"""
        self.bucket.upload_file(src, target)
        return None

    def download_file(self, src, target):
        """Download file from data store"""
        self.bucket.download_file(src, target)
        return None

    def upload_folder(self, src, target):
        for root, dirs, files in os.walk(src):
            for file in files:
                self.upload_file(os.path.join(root, file), os.path.join(target, file))

    def download_folder(self, src, target):
        """Download folder from data store"""
        files = self.list_files(prefix=src)
        print(files)
        for file in files:
            filename = file.replace(src, "")
            print(target + filename)
            self.bucket.download_file(file, target + filename)
        return None