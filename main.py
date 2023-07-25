"""
Main flow
python main.py -botname=name1"
"""


import os
from pathlib import Path

from src.uploader.dynamo_uploader import DynamoDB
from src.uploader.s3_uploader import S3uploader
# from src.uploader.dynamo_uploader import DynamoDB
import argparse

from src.descriptor.descriptor import Descriptor


def main():
    """Main flow"""
    parser = argparse.ArgumentParser()
    parser.add_argument("-type", type=str, required=False,
                        help="D = dymamo only, S = S3 only, blank - all.")
    parser.add_argument("-botname", type=str, required=False, help="bot name")
    the_type = parser.parse_args().type
    bot_name = parser.parse_args().botname
    print(the_type, bot_name)

    dscr = Descriptor(bot_name)

    if the_type is None:
        DynamoDB(dscr).get_bot_metadata(bot_name)
        S3uploader(dscr).upload()
  

if __name__ == "__main__":
    main()
