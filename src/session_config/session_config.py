"""
Configuration file for multi access for DynamoDB, S3 services
"""


import os

import boto3
from dotenv import load_dotenv


class SessionConfig:
    def __init__(self, session, s3_client, dynamo_client) -> None:
        """"""
        self.session = session
        self.s3_client = s3_client
        self.dynamo_client = dynamo_client

    def __get_enviroment(self):
        """Get data from enviroment"""
        load_dotenv()
        self._aws_access_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self._aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self._region = os.environ.get("AWS_DEFAULT_REGION")

    def _create_session(self):
        """Create sessions for dynamo and s3"""
        self.session = boto3.Session(
            aws_access_key_id=self._aws_access_id,
            aws_secret_access_key=self._aws_secret_key,
            region_name=self._region)

    def create_client(self):
        """Create clients for DB's"""
        self.__get_enviroment()
        self._create_session()
        s3_client = self.session.client("s3")
        dynamo_client = self.session.client("dynamodb")
        return s3_client, dynamo_client
