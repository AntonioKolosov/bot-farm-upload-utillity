from dotenv import load_dotenv
import boto3
import os


class AWS:
    def __init__(self):
        load_dotenv()
        self.__session: boto3.Session = self.__create_boto3_session()

    @staticmethod
    def __create_boto3_session():
        # Проверка наличия переменных окружения
        required_env_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION"]
        missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
        if missing_vars:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

        # Базовая валидация переменных окружения
        # Просто проверяем, что они не пустые, но можно подумать перед деплоем над более строгой валидацией.
        for var in required_env_vars:
            if not os.environ.get(var).strip():
                raise ValueError(f"Environment variable {var} cannot be empty.")

        try:
            session = boto3.Session(
                aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                region_name=os.environ["AWS_DEFAULT_REGION"],
            )
            return session
        except Exception as e:
            raise RuntimeError(f"Failed to create AWS session: {e}")

    @property
    def session(self):
        return self.__session
