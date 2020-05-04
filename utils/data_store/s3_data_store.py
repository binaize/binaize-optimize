import json

import boto3


class S3DataStore(object):
    def __init__(self, access_key, secret_key):
        self.session = boto3.session.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self.s3_resource = self.session.resource('s3')

    def create_bucket(self, bucket_name):
        x = self.s3_resource.create_bucket(Bucket=bucket_name)
        return x

    def delete_bucket(self, bucket_name):
        bucket = self.s3_resource.Bucket(bucket_name)
        for key in bucket.objects.all():
            key.delete()
        x = bucket.delete()
        return x

    def write_json_file(self, bucket_name, filename, contents):
        """Write JSON file into S3 bucket"""
        x = self.s3_resource.Object(bucket_name, filename).put(Body=json.dumps(contents))
        return x

    def read_json_file(self, bucket_name, filename):
        """Read JSON file from the S3 bucket"""
        obj = self.s3_resource.Object(bucket_name, filename).get()['Body'].read()
        utf_data = obj.decode("utf-8")
        file_content = json.loads(utf_data)
        return file_content

    def upload_file(self, bucket_name, src, target):
        """Upload file into data store"""
        bucket = self.s3_resource.Bucket(bucket_name)
        bucket.upload_file(src, target)
        return None

    def download_file(self, bucket_name, src, target):
        """Download file from data store"""
        bucket = self.s3_resource.Bucket(bucket_name)
        x = bucket.download_file(src, target)
        return x

    def list_files(self, bucket_name, prefix=None, max_count=None):
        """List all the files in the S3 bucket"""

        bucket = self.s3_resource.Bucket(bucket_name)
        list_filenames = []
        if prefix is None:
            objects = bucket.objects.all()
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
            objects = bucket.objects.filter(Prefix=prefix)
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
