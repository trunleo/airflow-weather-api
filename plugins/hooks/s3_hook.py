import boto3
from airflow.hooks.base_hook import BaseHook
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


class S3Hook(BaseHook):

    def __init__(self, aws_conn_id: str = "aws_default"):
        super().__init__()
        self.aws_conn_id = aws_conn_id
        self.s3_client = self.get_s3_client()

    def get_s3_client(self):
        try:
            session = boto3.Session()
            return session.client("s3")
        except (NoCredentialsError, PartialCredentialsError) as e:
            self.log.error(f"Error creating S3 client: {e}")
            return None
    def upload_file(self, file_name: str, bucket: str, object_name: str = None):
        if object_name is None:
            object_name = file_name
        try:
            self.s3_client.upload_file(file_name, bucket, object_name)
            self.log.info(f"File {file_name} uploaded to {bucket}/{object_name}")
        except Exception as e:
            self.log.error(f"Error uploading file: {e}")
            
    def download_file(self, bucket: str, object_name: str, file_name: str):
        try:
            self.s3_client.download_file(bucket, object_name, file_name)
            self.log.info(f"File {object_name} downloaded from {bucket} to {file_name}")
        except Exception as e:
            self.log.error(f"Error downloading file: {e}")