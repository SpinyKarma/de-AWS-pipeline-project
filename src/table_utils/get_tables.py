import boto3
import datetime as dt

SEPERATOR = '/'


class TableNotFoundError (Exception):
    pass


class EmptyBucketError (Exception):
    pass


def get_tables(get_bucket_name):
    ''' Used to collect a sorted list of tables, with the newest
        entries first.

    Args:
        get_ingestion_bucket_name - A function which returns the name of
        the ingestion bucket.

    Returns:
        tables: A list of table names - sorted by timestamp.
    '''

    list_tables_response = boto3.client('s3').list_objects(
        Bucket=get_bucket_name()
    )

    if 'Contents' not in list_tables_response:
        raise EmptyBucketError(list_tables_response['Name'])

    tables = [table['Key'] for table in list_tables_response['Contents']]

    def get_key_timestamp(key):
        split = key.split(SEPERATOR)
        timestamp_str = split[0]
        return dt.datetime.fromisoformat(timestamp_str).timestamp()

    """
        Note: Reverse ordering is set to true as this puts the
        most recent tables first.
    """
    tables.sort(key=get_key_timestamp, reverse=True)
    return tables


def get_most_recent_table(get_bucket_name, table_name):
    '''
        Used to get the most recent table name

    Args:
        get_bucket_name - A function which returns the bucket name
        table_name - The name of the CSV file

    Returns:
        The most recent table name

    Throws:
        TableNotFoundError - when the table does not exist
    '''

    try:
        tables = get_tables(get_bucket_name)
    except EmptyBucketError:
        raise TableNotFoundError(table_name)

    for table in tables:
        if table.endswith(table_name):
            return table

    raise TableNotFoundError(table_name)
