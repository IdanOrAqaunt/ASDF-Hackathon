import io
from datetime import datetime
from typing import Callable, List

import boto3
import smart_open
from botocore.exceptions import ClientError
from pydantic import BaseModel

aws_access_key_id = 'AKIAR7K4OE4FA76YNLVL'
aws_secret_access_key = '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'


class Folder(BaseModel):
    name: str = None
    url: str = None


class File(BaseModel):
    name: str = None
    url: str = None
    size: int = None
    last_modified: datetime = None


class S3Accessor:
    def __init__(self, bucket_name: str):
        self.__s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret_access_key)
        self.__bucket_name = bucket_name

    def upload_file(self, file_name: str, file_content):
        object = self.__s3.Object(self.__bucket_name, file_name)
        object.put(Body=file_content)

    def get_folders(self, path: str = ''):
        items: List[str] = []
        bucket = self.__s3.Bucket(self.__bucket_name)

        # if not path.endswith('/'):
        #     path = path + '/'

        for object_summary in bucket.objects.filter(Prefix=path):
            split_key = list(filter(None, object_summary.key.split('/')))
            split_key.pop(len(split_key) - 1)

            if len(split_key) > 0:
                root_folder = Folder()
                root_folder.name = split_key[0]
                root_folder.url = split_key[0]
                items.append(root_folder)

                url = '/'.join(split_key)
                folder = Folder()
                folder.name = split_key[len(split_key) - 1]
                folder.url = url
                items.append(folder)

        unique_objects = list({object_.url: object_ for object_ in items}.values())
        return unique_objects

    def get_files(self, folder_url: str):
        items: List[str] = []
        my_bucket = self.__s3.Bucket(self.__bucket_name)

        if not folder_url.endswith('/'):
            folder_url = folder_url + '/'

        for object_summary in my_bucket.objects.filter(Prefix=folder_url):
            split_key = list(filter(None, object_summary.key.split('/')))
            if '.' in split_key[len(split_key) - 1]:
                f = File()
                f.name = split_key[len(split_key) - 1]
                f.url = object_summary.key
                f.size = object_summary.size
                f.last_modified = object_summary.last_modified
                items.append(f)

        return items

    def download_file_content(self, file_name: str):
        response = self.__s3.Object(self.__bucket_name, file_name).get()
        return response['Body'].read()

    def download_file_content_with_progress(self, file_name: str, on_progress: Callable[[float, float], None],
                                            unpack: bool = True):
        session = boto3.session.Session(region_name='eu-central-1')
        s3_client = session.client('s3', aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret_access_key,
                                   config=boto3.session.Config(signature_version='s3v4'))

        kwargs = {"Bucket": self.__bucket_name, "Key": file_name}
        object_size = s3_client.head_object(**kwargs)["ContentLength"]

        print(f'{file_name} object_size: {object_size}')

        so_far = 0

        def upload_progress(chunk):
            nonlocal so_far
            so_far = so_far + chunk
            on_progress(so_far, object_size)

        file_stream = io.BytesIO()
        s3_client.download_fileobj(
            Bucket=self.__bucket_name,
            Key=file_name,
            Fileobj=file_stream,
            Callback=lambda bytes_transferred: upload_progress(bytes_transferred)
        )

        file_stream.seek(0)
        return file_stream.read() if unpack else file_stream

    @staticmethod
    def get_schema(file_path: str):
        print("-- 1." + file_path)
        with smart_open.open(f's3://{aws_access_key_id}:{aws_secret_access_key}@si-input/{file_path}',
                             encoding='utf-8-sig') as lines:
            for line in lines:
                print("-- 2." + line)
                return repr(line.replace("'", "").replace('\n', ''))

    def create_presigned_url(self, object_name, expiration=10):
        session = boto3.session.Session(region_name='eu-central-1')
        s3_client = session.client('s3', aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret_access_key,
                                   config=boto3.session.Config(signature_version='s3v4'))
        try:
            response = s3_client.generate_presigned_url('get_object',
                                                        Params={'Bucket': self.__bucket_name,
                                                                'Key': object_name},
                                                        ExpiresIn=expiration)
        except ClientError as e:
            return None

        # The response contains the presigned URL
        return response

    def create_presigned_url_post(self, object_name, expiration=10):
        session = boto3.session.Session(region_name='eu-central-1')
        s3_client = session.client('s3', aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret_access_key,
                                   config=boto3.session.Config(signature_version='s3v4'))
        try:
            response = s3_client.generate_presigned_post(self.__bucket_name,
                                                         object_name,
                                                         Fields=None,
                                                         Conditions=None,
                                                         ExpiresIn=expiration)
        except ClientError as e:
            return None

        # The response contains the presigned URL
        return response
