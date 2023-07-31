import boto3
import datetime as dt


def get_tables(get_bucket_name, seperator='_'):
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
    tables = [table['Key'] for table in list_tables_response['Contents']]

    def get_key_timestamp(key):
        split = key.split(seperator)
        timestamp_str = split[0]
        return dt.datetime.fromisoformat(timestamp_str).timestamp()

    """
        Note: Reverse ordering is set to true as this puts the
        most recent tables first.
    """
    tables.sort(key=get_key_timestamp, reverse=True)
    return tables
