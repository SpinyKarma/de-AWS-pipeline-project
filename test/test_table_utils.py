from moto import mock_s3
import boto3
from datetime import datetime as dt
from pprint import pprint

from src.table_utils.get_tables import get_tables

SEPERATOR = '_'


def get_ingestion_bucket_name():
    '''Gets the name of the bucket to store raw data in using the prefix.'''
    prefix = 'terrific-totes-ingestion-bucket'
    buckets = boto3.client("s3").list_buckets().get("Buckets")
    for bucket in buckets:
        if prefix in bucket["Name"]:
            name = bucket["Name"]
            break
    return name


def test_get_tables_returns_list():
    tables = get_tables(
        get_bucket_name=get_ingestion_bucket_name,
        seperator=SEPERATOR)

    assert isinstance(tables, list)


@mock_s3
def test_get_tables_returns_list_of_tables():
    # snagged from test_timestamp.py
    # create s3 bucket

    def get_bucket_name():
        return 'test'

    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket=get_bucket_name())

    '''
        Please do not change this without updating
        expected_result accordingly.
    '''
    timestamps = [
        dt(1980, 1, 2).isoformat(),
        dt(1980, 3, 1).isoformat(),
        dt(1980, 1, 1).isoformat(),
        dt(1970, 1, 1).isoformat(),
        dt(2000, 1, 1).isoformat(),
        dt(1964, 1, 1).isoformat()
    ]

    assert timestamps[-1] < timestamps[0]

    filename = 'test-table.csv'

    '''
        The most recent tables are first.
    '''
    expected_result = [
        f'2000-01-01T00:00:00{SEPERATOR}{filename}',
        f'1980-03-01T00:00:00{SEPERATOR}{filename}',
        f'1980-01-02T00:00:00{SEPERATOR}{filename}',
        f'1980-01-01T00:00:00{SEPERATOR}{filename}',
        f'1970-01-01T00:00:00{SEPERATOR}{filename}',
        f'1964-01-01T00:00:00{SEPERATOR}{filename}'
    ]

    filenames = [
        f'{timestamp}{SEPERATOR}{filename}' for timestamp in timestamps]

    # put fake files in bucket
    for filename in filenames:
        s3_client.put_object(
            Body="",
            Bucket=get_bucket_name(),
            Key=filename,
            ContentType='application/text',
        )

    tables = get_tables(get_bucket_name, SEPERATOR)
    assert tables == expected_result
