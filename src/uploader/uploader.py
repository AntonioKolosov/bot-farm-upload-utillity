from dotenv import load_dotenv
import boto3
import os


class AWS:
    def __init__(self):
        load_dotenv()
        self.__session: boto3.Session = boto3.Session(
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("AWS_DEFAULT_REGION"),
        )

    @property
    def session(self):
        return self.__session
