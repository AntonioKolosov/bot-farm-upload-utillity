import os
from src.uploader.uploader import AWS
from src.descriptor.descriptor import Descriptor


class S3uploader(AWS):
    def __init__(self, descr: Descriptor):
        super().__init__()
        self.__descr = descr
        self.__path = None
        self.__client = self.session.client("s3")

    def make(self):
        """Create path"""
        # Get current dir
        current_directory: str = os.getcwd()
        # Join directory path
        self.__path = os.path.join(
            current_directory,
            self.__descr.output_root,
            self.__descr.bot_name,
            self.__descr.md_folder,
            self.__descr.cn_folder
        )

    def upload(self):
        """Upload files"""
        if self.__path is not None:
            try:
                for file in os.listdir(self.__path):
                    file_path = os.path.join(self.__path, file)
                    bucket_name = "bot-farm-storage"
                    upload_file_key = file
                    self.__client.upload_file(file_path,
                                                bucket_name,
                                                upload_file_key)
            except Exception as e:
                print(f"Error to uploading files to S3: {e}")
