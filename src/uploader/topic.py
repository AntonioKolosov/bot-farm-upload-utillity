from typing import Self
from pathlib import Path
import json


class Topic:
    bot_name: str
    topic_name: str

    def __init__(self, bot_name: str, topic_name: str, **kwargs) -> None:
        self.bot_name = bot_name
        self.topic_name = topic_name  # обязательные аргументы

        for k, v in kwargs.items():
            self.__setattr__(k, v)

    def get_dict(self):
        data = self.__dict__
        formatted_data = Topic.__convert_data_for_upload(data)
        return formatted_data
    
    @classmethod
    def from_file(cls, client, path_to_file: Path, bot_name: str, topic_name: str) -> Self:
        with open(path_to_file, encoding="UTF-8", mode="r") as file:
            data = json.load(file)
            topic = cls(
                bot_name=bot_name,
                topic_name=topic_name,
                **data
            )
            return topic

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
                data_res.append({"M": Topic.__convert_data_for_upload(d)})
            elif isinstance(d, list):
                data_res.append({"L": Topic.__convert_list(d)})
        return data_res
    
    @staticmethod
    def __convert_list_from_client(data: list[dict[str, any]]) -> list[any]:
        data_res = []
        for d in data:
            data_type = list(d.keys())[0]
            if data_type == "M":
                data_res.append(Topic.convert_received_data(d["M"]))
            elif data_type == "L":
                data_res.append(Topic.__convert_list_from_client(d["L"]))
            else:
                data_res.append(d[str(data_type)])
        return data_res

    @staticmethod
    def __convert_data_for_upload(data: dict[any]) -> dict[any]:
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
                result[k] = {"M": Topic.__convert_data_for_upload(v)}
            elif isinstance(v, list):
                result[k] = {"L": Topic.__convert_list(v)}
        return result
    
    @staticmethod
    def convert_received_data(data: dict[str, dict[any]]) -> dict[any]:
        result = {}
        for k, v in data.items():
            if v.get("M", None):
                result[k] = Topic.convert_received_data(v["M"])
            elif v.get("L", None):
                result[k] = Topic.__convert_list_from_client(v["L"])
            else:
                result[k] = v[list(v.keys())[0]]
        return result
