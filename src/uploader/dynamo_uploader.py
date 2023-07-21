# from src.uploader.uploader import AWS
# import os
# import json
# from src.descriptor.descriptor import Descriptor


# class DynamoDB(AWS):
#     def __init__(self, descr: Descriptor):
#         super().__init__()
#         self.__descr = descr
#         self.__md_path = None
#         self.client = self.session.client('dynamodb')

#     def make(self):
#         """Create path"""
#         # Get current dir
#         current_directory: str = os.getcwd()
#         # Join directory path
#         self.__md_path = os.path.join(
#             current_directory,
#             self.__descr.output_root,
#             self.__descr.bot_name,
#             self.__descr.md_folder,
#         )

#     def put_item(self, table_name: str, data: dict):
#         self.client.put_item(
#             TableName=table_name,
#             Item=data
#         )

#     @staticmethod
#     def convert_list(data) -> list:
#         data_res = []
#         for d in data:
#             if isinstance(d, int):
#                 data_res.append({"N": str(d)})
#             elif isinstance(d, str):
#                 data_res.append({"N": str(d)})
#             else:
#                 data_res.append({"N": str(d)})
#         return data_res

#     def get_bot_metadata(self, bot_name: str) -> None:
#         if not self.check_table_exists("datatopics"):
#             self.create_table("datatopics",
#                               ["bot_name", "topic_name"],
#                               [("bot_name", "S"), ("topic_name", "S")])

#         self.make()
#         try:
#             topics = os.listdir(self.__md_path)
#         except FileNotFoundError as e:
#             print(f"Директория бота {bot_name} не найдена.")
#             return
#         if topics:
#             for file in topics:
#                 if file.endswith(".json"):
#                     with open(f"{self.__md_path}/{file}",
#                               encoding="UTF-8", mode="r") as the_file:
#                         data = json.load(the_file)
#                         data["bot_name"] = bot_name
#                         data["topic_name"] = file.replace(".json", "")
#                         data_res = {}
#                         for k, v in data.items():
#                             if isinstance(v, int):
#                                 data_res[k] = {"N": str(v)} 
#                             elif isinstance(v, str):
#                                 data_res[k] = {"S": v}
#                             else:
#                                 data_res[k] = {"L": self.convert_list(v)}
#                         print(data_res)
#                         self.put_item("datatopics", data_res)
#         else:
#             print(f"Топики в {bot_name} не найдены")

#     # Забирает метаданные для всех ботов. 
#     # def get_metadata(self, path: str, bot_name: str | None):
#     #     if bot_name is not None:
#     #         self.get_bot_metadata(path, bot_name)
# # 
#     #     bot_dirs = os.listdir(path)  # директории с ботами по указанному пути
#     #     if not self.check_table_exists("datatopics"):
#     #         self.create_table("datatopics",
#     #                           ["bot_name", "topic_name"],
#     #                           [("bot_name", "S"), ("topic_name", "S")])
# # 
#     #     for bot in bot_dirs:
#     #         self.get_bot_metadata(path, bot)

#     def check_table_exists(self, table_name: str) -> bool:
#         tables_list = self.client.list_tables()
#         return table_name in tables_list["TableNames"]

#     def create_table(self, table_name: str,
#                      key_schema: list[str],
#                      attributes: list[tuple[str, str]]):
#         """
#         """
#         if len(key_schema) > 2:
#             raise AttributeError("KeySchema must include up to 2 elements.")

#         for el in key_schema:
#             if el not in map(lambda x: x[0], attributes):
#                 raise AttributeError("KeySchema must be one of the arguments")

#         try:
#             response = self.client.create_table(
#                 TableName=table_name,
#                 KeySchema=[
#                     {
#                         "AttributeName": key_schema[0],
#                         "KeyType": "HASH"
#                     },
#                     {
#                         "AttributeName": key_schema[1],
#                         "KeyType": "RANGE"
#                     }
#                 ],
#                 AttributeDefinitions=[
#                     {
#                         'AttributeName': definition[0],
#                         'AttributeType': definition[1]
#                     } for definition in attributes
#                 ],
#                 ProvisionedThroughput={
#                     "ReadCapacityUnits": 1,
#                     "WriteCapacityUnits": 1
#                 })
#             res = f"Table {table_name} was successfully created: \n{response}"
#             print(res)
#             # tb_log.log_info(res)
#         except Exception as e:
#             print("Error while creating table: ", e)
#             # tb_log.log_info(f"Error while creating table: {e}")
