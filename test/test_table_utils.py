from moto import mock_s3
import boto3
from datetime import datetime as dt

from src.lambda_transformation.utils.get_tables import (
    get_tables,
    get_most_recent_table,
    TableNotFoundError,
    EmptyBucketError
)
import pytest

SEPERATOR = '/'


def test_get_tables_returns_list():
    """
    Test whether the function 'get_tables' returns a list.
    """

    tables = get_tables()
    assert isinstance(tables, list)


@mock_s3
def test_get_tables_returns_list_of_tables():
    """
    Test whether the function 'get_tables' returns a list of table names,
    sorted in descending order by timestamp, when retrieving tables from
    the S3 bucket.
    """

    s3_client = boto3.client('s3', region_name='us-east-1')

    bucket_name = 'terrific-totes-ingestion-bucket20230725102602583400000001'
    s3_client.create_bucket(
        Bucket=bucket_name)

    # Please do not change this without updating expected_result accordingly.
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

    # The most recent tables are first.
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

    # Put fake files in bucket
    for filename in filenames:
        s3_client.put_object(
            Body="",
            Bucket=bucket_name,
            Key=filename,
            ContentType='application/text',
        )

    tables = get_tables()
    assert tables == expected_result


@mock_s3
def test_get_most_recent_table():
    """
    Test whether the function 'get_most_recent_table' returns the most
    recent table name from the S3 bucket when given a table name.
    """

    s3_client = boto3.client('s3', region_name='us-east-1')
    bucket_name = 'terrific-totes-ingestion-bucket20230725102602583400000001'
    s3_client.create_bucket(Bucket=bucket_name)
    tablename = 'test-table.csv'

    # Please do not change this
    files = [
        f'2000-01-01T00:00:00{SEPERATOR}{tablename}',
        f'1980-03-01T00:00:00{SEPERATOR}{tablename}',
        f'1980-01-02T00:00:00{SEPERATOR}{tablename}',
        f'1980-01-01T00:00:00{SEPERATOR}{tablename}',
        f'1970-01-01T00:00:00{SEPERATOR}{tablename}',
        f'1964-01-01T00:00:00{SEPERATOR}{tablename}'
    ]
    # Put fake files in bucket
    for filename in files:
        s3_client.put_object(
            Body="",
            Bucket=bucket_name,
            Key=filename,
            ContentType='application/text',
        )

    assert get_most_recent_table(tablename) == files[0]


@mock_s3
def test_get_tables_throws_empty_bucket_error():
    """
    Test whether the function 'get_tables' raises an EmptyBucketError when
    the S3 bucket is empty.
    """

    bucket_name = 'terrific-totes-ingestion-bucket20230725102602583400000001'
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket=bucket_name)

    with pytest.raises(EmptyBucketError):
        get_tables()


@mock_s3
def test_get_most_recent_table_throws_table_not_found_error():
    """
    Test whether the function 'get_most_recent_table' raises a
    TableNotFoundError when the given table name does not exist in the
    S3 bucket.
    """

    bucket_name = 'terrific-totes-ingestion-bucket20230725102602583400000001'
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket=bucket_name)

    s3_client.put_object(
        Body="",
        Bucket=bucket_name,
        Key=f'2000-01-01T00:00:00{SEPERATOR}table.csv',
        ContentType='application/text',
    )

    with pytest.raises(TableNotFoundError):
        get_most_recent_table('does not exist')
