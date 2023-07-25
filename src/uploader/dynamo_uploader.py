import os
import json
import time
from pathlib import Path
from src.descriptor.descriptor import Descriptor
from src.uploader.uploader import AWS
from src.logger.logger import tb_log


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

    def put_item(self, table_name: str, data: dict) -> None:
        self.client.put_item(
            TableName=table_name,
            Item=data
        )

    @staticmethod
    def __convert_list(data: list[any]) -> list[dict]:
        data_res = []
        for d in data:
            if isinstance(d, int):
                data_res.append({"N": str(d)})
            elif isinstance(d, str):
                data_res.append({"S": str(d)})
            elif isinstance(d, set):
                data_res.append({"SS": list(d)})
            elif isinstance(d, bytes):
                data_res.append({"B": bytes(d)})
            elif isinstance(d, dict):
                data_res.append({"M": DynamoDB.__convert_data(d)})
            elif isinstance(d, list):
                data_res.append({"L": DynamoDB.__convert_list(d)})
        return data_res

    @staticmethod
    def __convert_data(data: dict[any]) -> dict[any]:
        result = {}
        for k, v in data.items():
            if isinstance(v, int):
                result[k] = {"N": str(v)}
            elif isinstance(v, str):
                result[k] = {"S": v}
            elif isinstance(v, set):
                result[k] = {"SS": list(v)}
            elif isinstance(v, bytes):
                result[k] = {"B": bytes(v)}
            elif isinstance(v, dict):
                result[k] = {"M": DynamoDB.__convert_data(v)}
            elif isinstance(v, list):
                result[k] = {"L": DynamoDB.__convert_list(v)}
        return result

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
                    # print(self.__md_path, file)
                    # разобраться с Path.joinpath
                    # path_to_file = Path.joinpath(self.__md_path, file)
                    path_to_file = f"{self.__md_path}/{file}"
                    with open(path_to_file,
                              encoding="UTF-8", mode="r") as the_file:
                        data = json.load(the_file)
                        data["bot_name"] = bot_name
                        data["topic_name"] = file.replace(".json", "")
                        data = self.__convert_data(data)
                        self.put_item("datatopics", data)
        else:
            print(f"Топики в {bot_name} не найдены")
            tb_log.log_info(f"Топики в {bot_name} не найдены")

    def __check_table_exists(self, table_name: str) -> bool:
        """
        Check if a DynamoDB table exists.
        """
        tables_list = self.client.list_tables()
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
