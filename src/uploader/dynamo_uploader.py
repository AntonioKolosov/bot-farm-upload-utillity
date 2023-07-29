import os
import json
import time
from pathlib import Path
from src.descriptor.descriptor import Descriptor
from src.uploader.uploader import AWS
from src.logger.logger import tb_log
from src.uploader.topic import Topic


class DynamoDB(AWS):
    def __init__(self, descr: Descriptor):
        super().__init__()
        self.__descr = descr
        self.__md_path = None
        self.client = self.session.client('dynamodb')

    def make(self) -> None:
        """Create path"""
        # Get current dir
        current_directory: str = os.getcwd()
        # Join directory path
        self.__md_path = os.path.join(
            current_directory,
            self.__descr.output_root,
            self.__descr.bot_name,
            self.__descr.md_folder,
        )

    def get(self, table_name: str, hash_key: any, sort_key: any) -> Topic:
        data = self.client.get_item(
            TableName=table_name,
            Key={
                "bot_name": hash_key,
                "topic_name": sort_key
            }
        )
        formatted_data = Topic.convert_received_data(data)
        topic = Topic(**formatted_data)
        return topic

    def delete_item(self, table_name: str, hash_key: any, sort_key: any) -> None:
        self.client.delete_item(
            TableName=table_name,
            Key={
                "bot_name": {
                    "S": hash_key
                },
                "topic_name": {
                    "S": sort_key
                }
            }
        )

    def get_all_items(self, table_name: str) -> list[Topic]:
        response = self.client.scan(TableName=table_name)  # ожидается, что response["Item"] - топики в
        formatted_response = [Topic.convert_received_data(i) for i in response["Items"]]
        topics = [Topic(**i) for i in formatted_response]  # формате списка из словарей.
        return topics

    def save(self, table_name: str, topic: Topic) -> None:
        self.client.put_item(
            TableName=table_name,
            Item=topic.get_dict()
        )

    def get_bot_metadata(self, bot_name: str) -> None:
        """
        Fetch bot metadata from JSON files and upload them to the 'datatopics' DynamoDB table.

        Args:
            bot_name (str): The name of the bot.

        If the bot's folder is not found or contains no JSON files, the function prints an appropriate message.
        """

        if not self.__check_table_exists("datatopics"):
            self.create_table("datatopics",
                              ["bot_name", "topic_name"],
                              [("bot_name", "S"), ("topic_name", "S")])

            # Wait for the table to be active before proceeding
            for _ in range(15):
                table_status = self.__check_table_status("datatopics")
                if table_status != "ACTIVE":
                    print(f"Wait for the table to be active before proceeding."
                          f" Current table status is {table_status} ...")
                    time.sleep(5)
                else:
                    break

        self.make()

        try:
            print(self.__md_path)
            topics = os.listdir(self.__md_path)
        except FileNotFoundError as e:
            err = f"Директория бота {bot_name} не найдена: \n{e}"
            tb_log.log_info(err)
            raise FileNotFoundError(f"Директория бота {bot_name} не найдена: \n{e}")

        if topics:
            for file in topics:
                if file.endswith(".json"):
                    path_to_file = Path.joinpath(
                        Path(self.__md_path), file
                    )
                    topic_name = file.replace(".json", "")
                    topic = Topic.from_file(self.client, path_to_file, bot_name, topic_name)
                    self.save("datatopics", topic)
        else:
            print(f"Топики в {bot_name} не найдены")
            tb_log.log_info(f"Топики в {bot_name} не найдены")

    def __check_table_exists(self, table_name: str) -> bool:
        """
        Check if a DynamoDB table exists.
        """
        tables_list = self.client.list_tables()
        print(tables_list["TableNames"])
        return table_name in tables_list["TableNames"]

    def __check_table_status(self, table_name: str) -> str:
        """
        The method returns the table's status as a string (e.g., "CREATING", "ACTIVE", "DELETING", etc.),
        or returns None if the table with the given name does not exist in the DynamoDB service.
        """
        response = self.client.describe_table(TableName=table_name)
        return response.get("Table", {}).get("TableStatus", None)

    def create_table(self, table_name: str,
                     key_schema: list[str],
                     attributes: list[tuple[str, str]]) -> None:
        """
        Create a DynamoDB table with the given name, key schema, and attributes.

        Args:
            table_name (str): The name of the table to be created.
            key_schema (list[str]): A list containing the names of the primary key attributes.
            attributes (list[tuple[str, str]]): A list of tuples representing the attribute names and types.

        Raises:
            AttributeError: If the key schema contains more than two elements or an element is not present in the
            attributes.
        """
        if len(key_schema) > 2:
            raise AttributeError("KeySchema must include up to 2 elements.")

        for el in key_schema:
            if el not in map(lambda x: x[0], attributes):
                raise AttributeError("KeySchema must be one of the arguments")

        try:
            response = self.client.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        "AttributeName": key_schema[0],
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": key_schema[1],
                        "KeyType": "RANGE"
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': definition[0],
                        'AttributeType': definition[1]
                    } for definition in attributes
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1
                }
            )
            res = f"Table {table_name} was successfully created: \n{response}"
            tb_log.log_info(res)
        except Exception as e:
            tb_log.log_info(f"Error while creating table: {e}")
            raise e
