import boto3
import logging
import pyarrow.parquet as pq
from pyarrow import fs
import pg8000.native as pg
import json


def loading_lambda_handler(event, context):
    s3_boto = boto3.client('s3', region_name='eu-west-2')
    # Ensure that parquet bucket exists
    try:
        parquet_bucket = get_parquet_bucket_name()
    except MissingBucketError as err:
        logging.critical(f"Missing Bucket Error : {err.message}")
        raise err
    # find if cache.txt exists, else create it. If does, then read in as a list of timestamps.

    # get parquet timestamps
    parquet_timestamps = list_timestamps(s3_boto, parquet_bucket)
    # compare cache list with timestamps in parquet buckect. Create new list of timestamps that are in parquet, but not in cache.
    # for each timestamp in new list
    # grab list of files under this timestamp
    # for each file under TS, go through each line, determine whether it's an insert or an update (by seeing if PK exists on Database)
    # If insert, INSERT row. If update, UPDATE row.
    # Add timestamp to cache.
    # Write cache to bucket
    try:
        cache_txt = s3_boto.get_object(Bucket=parquet_bucket, Key='cache.txt')['Body'].read().decode('utf-8').split(\n)
    except s3_boto.exceptions.NoSuchKey as e:
        s3_boto.put_object(Bucket=parquet_bucket, Key='cache.txt', Body='')
        cache_txt = []
    diff_list = [item for item in parquet_timestamps if item not in cache_txt]
    s3_py = fs.S3FileSystem(region='eu-west-2')
    for timestamp in diff_list:
        folder_contents = s3_py.get_file_info(fs.FileSelector(
            parquet_bucket + '/' + timestamp, recursive=False))
        for file in folder_contents:
            list_table = read_parquet(s3_py, file)
            table = file.base_name.split('/')[1][: -8]
            headers = list_table[0]
            list_table = list_table[1:]
            with connect() as db:
                res = db.run(f'SELECT * FROM {pg.identifier(table)};')
                pk = [row[0] for row in res]
                for row in list_table:
                    if row[0] not in pk:
                        headersstr = ', '.join(
                            [pg.identifier(item) for item in headers])
                        datastr = ', '.join([pg.literal(item) for item in row])
                        query = f'INSERT INTO {table} ({headersstr})'
                        query += f'VALUES ({datastr});'
                    else:
                        data = ', '.join([pg.identifier(
                            headers[i]) + ' = ' + pg.literal(row[i]) for i in range(len(headers))])
                        query = f'UPDATE TABLE {table} SET {data}'
                        query += f'WHERE {pg.identifier(headers[0])} = {pg.literal(row[0])};'
                    db.run(query)
        cache_txt.append(timestamp)      
    s3_boto.put_object(Bucket=parquet_bucket, Key='cache.txt', Body='\n'.join(cache_txt))


def read_parquet(s3, file):
    '''Takes a pyarrow FileInfo object that points to a parquet file on s3 and
       returns the parquet file contents as a list of lists, one for each row
       including column names.
       '''
    fh = s3.open_input_file(file.path)
    list_of_dicts = pq.read_table(fh).to_pylist()
    list_table = [
        list(list_of_dicts[0].keys())[1:]
    ] + [
        list(item.values())[1:] for item in list_of_dicts
    ]
    return list_table


def list_timestamps(s3, bucket_name):
    '''Lists out all prefixes that exist in the passed bucket.'''
    timestamps = s3.list_objects_v2(
        Bucket=bucket_name, Prefix="", Delimiter="/"
    ).get('CommonPrefixes', [])
    timestamps = [item['Prefix'] for item in timestamps]
    return timestamps


def get_parquet_bucket_name():
    '''Gets the name of the bucket of processed data using the prefix.'''
    prefix = 'terrific-totes-processed-bucket'
    buckets = boto3.client("s3").list_buckets().get("Buckets")
    for bucket in buckets:
        if prefix in bucket["Name"]:
            name = bucket["Name"]
            return name
    raise MissingBucketError(
        "parquet_bucket",
        "The processed data bucket was not found"
    )


def get_credentials():
    """Loads a set of DB credentials using a secret stored in AWS secrets.

        Args:
            secet_name: The name of the secret to extract credentials from,
            defaults to Ingestion_credentials. Also takes Warehouse_credentials

        Returns:
            credentials: A dictionary containing:
            - username
            - password
            - hostname
            - port
            Additionally contains:
            - db, if secret_name = Ingestion_credentials
            - schema, if secret_name = Warehouse_credentials

        Raises:
            InvalidCredentialsError if the keys of the dictionary are invalid.
    """

    secretsmanager = boto3.client('secretsmanager')
    credentials_response = secretsmanager.get_secret_value(
        SecretId='Warehouse_credentials'
    )
    credentials = json.loads(credentials_response['SecretString'])
    return credentials


def connect():
    """Will return a connection to the DB, to be used with context manager."""

    credentials = get_credentials()

    return pg.Connection(
        user=credentials['username'],
        password=credentials['password'],
        host=credentials['hostname'],
        database='postgres',
        port=credentials['port']
    )


class MissingBucketError(Exception):
    '''An error for when the prerequisite buckets do not exist.'''

    def __init__(self, source="", message=""):
        self.source = source
        self.message = message


if __name__ == "__main__":
    s3 = boto3.client('s3', region_name='eu-west-2')
    # response = s3.get_object(Bucket=get_parquet_bucket_name(), Key='cache.txt')
    # with connect() as db:
    # res = db.run(f'SELECT * FROM {pg.identifier("dim_date")};')
    s3_py = fs.S3FileSystem(region='eu-west-2')
    timestamp = list_timestamps(s3, get_parquet_bucket_name())[1]
    folder_contents = s3_py.get_file_info(fs.FileSelector(
        get_parquet_bucket_name() + '/' + timestamp, recursive=False))
    for file in folder_contents:
        list_table = read_parquet(s3_py, file)
        table = file.base_name[: -8]
        print(list_table)
        print(table)
