import argparse
import json

from google.cloud import logging
from elasticsearch import Elasticsearch

es = Elasticsearch("http://34.101.246.247:9200")

def list_entries(logger_name):
    logging_client = logging.Client()
    logger = logging_client.logger(logger_name)
    filter_str="ETL Status"

    print("Listing entries for logger {}:".format(logger.name))

    for entry in logger.list_entries(filter_=filter_str):
        timestamp = entry.timestamp.isoformat()
        # print("* {}".format(timestamp), "\n")
        # print("* {}".format(entry.payload), "\n")

        doc = {
            'timestamp': timestamp,
            'summary': entry.payload
        }
        res = es.index(index = 'dwh-siloam', body = doc)
        print(" data insert to elastic .")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("logger_name", help="Logger name", default="example_log")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("list", help=list_entries.__doc__)

    args = parser.parse_args()

    if args.command == "list":
        list_entries(args.logger_name)