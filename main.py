"""
Main flow
python main.py -botname=name1"
"""


import os
from argparse import ArgumentParser
from pathlib import Path
from typing import Tuple
from src.descriptor.descriptor import Descriptor
from src.uploader.dynamo_uploader import DynamoDB
from src.uploader.s3_uploader import S3uploader


def args_parse(parser: ArgumentParser) -> Tuple[str | None, str | None]:
    parser.add_argument("-type", type=str, required=False,
                        help="D = dymamo only, S = S3 only, blank - all.")
    parser.add_argument("-botname", type=str, required=False, help="bot name")
    the_type = parser.parse_args().type
    bot_name = parser.parse_args().botname
    return the_type, bot_name


def main():
    """Main flow"""
    the_type, bot_name = args_parse(ArgumentParser())
    dscr = Descriptor(bot_name)

    if the_type != "S":
        db = DynamoDB(dscr)
        db.get_bot_metadata(bot_name)
        topics = db.get_all_items("datatopics")
        for topic in topics:
            print(topic.topic_name, topic.bot_name)
        db.delete_item("datatopics", "bot-name1", "name1_help.json")
    if the_type != "D":
        S3uploader(dscr).upload()
  

if __name__ == "__main__":
    main()
