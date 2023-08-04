import boto3
import logging
import pyarrow.parquet as pq
from pyarrow import fs
import pg8000.native as pg
import json
from pprint import pprint


def loading_lambda_handler(event, context):

    # define boto3 s3 client
    s3_boto = boto3.client('s3', region_name='eu-west-2')

    # Ensure that parquet bucket exists
    try:
        parquet_bucket = get_parquet_bucket_name()
    except MissingBucketError as err:
        logging.critical(f"Missing Bucket Error : {err.message}")
        raise err

    # get list of timestamp folders in parquet bucket
    parquet_timestamps = list_timestamps(s3_boto, parquet_bucket)

    # find if cache.txt exists, else create it, cache.txt is a stored list of
    # timestamps that have been inserted into the tables already
    try:
        cache_txt = s3_boto.get_object(
            Bucket=parquet_bucket,
            Key='cache.txt'
        )['Body'].read().decode('utf-8').split("\n")
        if cache_txt == [""]:
            cache_txt = []
    except s3_boto.exceptions.NoSuchKey:
        s3_boto.put_object(Bucket=parquet_bucket, Key='cache.txt', Body='')
        cache_txt = []

    # compare cache list with timestamps in parquet bucket.
    # create a new list of timestamps that are in parquet, but not in cache.
    diff_list = [item for item in parquet_timestamps if item not in cache_txt]

    # define the pyarrow s3 client
    s3_py = fs.S3FileSystem(region='eu-west-2')

    # check if dim date is populated, if not then populate
    with connect() as db:
        res = db.run("SELECT * FROM dim_date LIMIT 1;")
        if res == []:
            file = s3_py.get_file_info(parquet_bucket+"/dim_date.parquet")
            insert_data(s3_py, file)

    # for each timestamp in new list:
    for timestamp in diff_list:

        # grab list of files under this timestamp
        folder_contents = s3_py.get_file_info(
            fs.FileSelector(parquet_bucket + '/' + timestamp, recursive=False)
        )

        # for each file under the timestamp:
        for file in folder_contents:

            # insert the files data into the appropriate table
            insert_data(s3_py, file)

        # Add timestamp to cache now that all data has been niserted
        cache_txt.append(timestamp)

    # Write cache to bucket so future calls to lambda don't insert old data
    # over new data
    s3_boto.put_object(
        Bucket=parquet_bucket, Key='cache.txt', Body='\n'.join(cache_txt)
    )


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


def insert_data(s3_py, file):
    ''' Takes a FileInfo pyarrow object that points to a parquet file in s3,
        reads the parquet data and writes to the appropriate table using
        update or insert as needed.'''
    # Reads the parquet file
    list_table = read_parquet(s3_py, file)
    # Reads the table name from the file's name
    table = file.base_name[:-8]
    # Separates the column headings from the data
    headers = list_table[0]
    list_table = list_table[1:]

    with connect() as db:
        # Queries to see what data exists in the yable already
        res = db.run(f'SELECT * FROM {pg.identifier(table)};')
        # Creates a list of primary keys that exist in the table
        pk = [row[0] for row in res]
        rows_to_insert = []
        # For each row in the data set:
        for row in list_table:
            # If the row's primary key exists in the table: run an update query
            if row[0] in pk:
                data = ', '.join(
                    [pg.identifier(headers[i]) + ' = ' + pg.literal(row[i])
                     for i in range(len(headers))]
                )
                query = f'UPDATE {pg.identifier(table)} SET {data} WHERE '
                query += f'{pg.identifier(headers[0])} = {pg.literal(row[0])};'
                db.run(query)
            # Else: add to a list so inserts can all be done at once
            else:
                rows_to_insert.append(row)
        # If there are rows to insert: build a query string to insert all
        if rows_to_insert != []:
            headersstr = ', '.join(
                [pg.identifier(item) for item in headers])
            datastr = ", ".join(
                ["("+', '.join([pg.literal(item) for item in row])+")"
                    for row in rows_to_insert])
            query = f'INSERT INTO {pg.identifier(table)} ({headersstr}) '
            query += f'VALUES {datastr};'
            db.run(query)


def get_credentials():
    """Loads a set of DB credentials using a secret stored in AWS secrets."""

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
    s3_boto = boto3.client('s3', region_name='eu-west-2')
    parquet_bucket = get_parquet_bucket_name()
    with connect() as db:
        res = db.run("SELECT * FROM dim_date LIMIT 100;")
        pprint(res)
