import boto3
from datetime import datetime as dt
import pandas as pd

SEPERATOR = '/'


class TableNotFoundError (Exception):
    pass


class EmptyBucketError (Exception):
    pass


class BucketNotFoundError (Exception):
    pass


def get_ingestion_bucket_name():
    '''
    Returns:
        The timestamped ingestion bucket name containing our raw CSV
    '''
    ingestion_bucket_name = 'terrific-totes-ingestion-bucket'
    ingestion_bucket_ts = get_timestamped_bucket_name(ingestion_bucket_name)
    return ingestion_bucket_ts


def get_parquet_bucket_name():
    '''
    Returns:
        The timestamped name of the bucket containing our parquet data
    '''
    processed_bucket_name = 'terrific-totes-processed-bucket'
    processed_bucket_ts = get_timestamped_bucket_name(processed_bucket_name)
    return processed_bucket_ts


def get_tables():
    ''' Used to collect a sorted list of tables, with the newest
        entries first.

    Args:
        get_ingestion_bucket_name - A function which returns the name of
        the ingestion bucket.

    Returns:
        tables: A list of table names - sorted by timestamp.
    '''

    bucket_name = get_ingestion_bucket_name()
    s3 = boto3.client('s3')

    try:
        list_tables_response = s3.list_objects(
            Bucket=bucket_name
        )
    except s3.exceptions.NoSuchBucket:
        raise BucketNotFoundError(bucket_name)

    # The bucket is empty because it has no contents
    if 'Contents' not in list_tables_response:
        raise EmptyBucketError(list_tables_response['Name'])

    # The names of the table
    tables = [table['Key'] for table in list_tables_response['Contents']]

    def get_key_timestamp(key):
        split = key.split(SEPERATOR)
        timestamp_str = split[0]
        return dt.fromisoformat(timestamp_str).timestamp()

    """
        Note: Reverse ordering is set to true as this puts the
        most recent tables first.
    """
    tables.sort(key=get_key_timestamp, reverse=True)
    return tables


def get_most_recent_table(table_name):
    '''
        Used to get the S3 name most recently updated version of a table,
        if for example we have 4 versions of staff.csv, this
        will return the newest one.

    Args:
        table_name - The name of the CSV file

    Returns:
        The most recent table name

    Throws:
        TableNotFoundError - when the table does not exist
    '''

    try:
        tables = get_tables()
    except EmptyBucketError:
        raise TableNotFoundError(table_name)

    for table in tables:
        if table.endswith(table_name):
            return table

    raise TableNotFoundError(table_name)


def read_table(table_name):
    '''
        Will return the contents of the most recent table
        packaged in a dictionary

    Args:
        table_name - the name of the table

    Returns
        A dictionary of form:
        -   name: The name of the table (timestamped)
        -   body: The CSV contents of the table as a string
    '''
    s3_client = boto3.client('s3')
    ingestion_bucket = get_ingestion_bucket_name()
    key = get_most_recent_table(table_name)

    response = s3_client.get_object(
        Bucket=ingestion_bucket,
        Key=key
    )

    dataframe = pd.read_csv(response['Body'])
    timestamp = dt.fromisoformat(key.split(SEPERATOR)[0])

    return {
        'Name': table_name,
        'Timestamp': timestamp,
        'Key': key,
        'Body': dataframe
    }


def get_timestamped_bucket_name(bucket_name):
    '''
    As our bucket names are time-stamped, we use this to find
    the correct bucket without specifying exactly when it was created.

    Args:
        bucket_name - the non-timestamped name of the bucket:
            example: terrific-totes-ingestion-bucket

    Returns:
        The timestamped bucket name, for example:
            terrific-totes-ingestion-bucket20230725102602583400000001

    Throws:
        BucketNotFoundError - when the bucket does not exist
    '''

    for bucket in boto3.client("s3").list_buckets().get("Buckets"):
        if bucket["Name"].startswith(bucket_name):
            return bucket["Name"]

    # The bucket does not exist.
    raise BucketNotFoundError(bucket_name)
