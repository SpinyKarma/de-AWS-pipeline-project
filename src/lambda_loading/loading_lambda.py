import boto3
import logging
import pyarrow.parquet as pq
from pyarrow import fs
import pg8000.native as pg
import json


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def loading_lambda_handler(event, context):
    """ Reads parquet files from an s3 bucket and inserts the data contained
        within them into the data warehouse, using a cache file to ensure the
        same entry is not inserted more than once.
    """

    # Define a boto3 s3 client.
    s3_boto = boto3.client('s3', region_name='eu-west-2')

    # Ensure that the parquet bucket exists.
    try:
        parquet_bucket = get_parquet_bucket_name()
        logger.info(f"Processed bucket established as {parquet_bucket}.")
    except MissingBucketError as err:
        logger.critical(f"Missing Bucket Error : {err.message}")
        raise err

    # Try to access cache.txt file in bucket and read if it exists.
    try:
        cache_txt = s3_boto.get_object(
            Bucket=parquet_bucket,
            Key='cache.txt'
        )['Body'].read().decode('utf-8').split("\n")
        if cache_txt == [""]:
            cache_txt = []
        logger.info("cache.txt found, reading.")

    # If it does not exist then create an empty one.
    except s3_boto.exceptions.NoSuchKey:
        logger.info("cache.txt not found, creating.")
        s3_boto.put_object(Bucket=parquet_bucket, Key='cache.txt', Body='')
        cache_txt = []

    # Create a list of timestamp folders that exist in the parquet bucket.
    parquet_timestamps = list_timestamps(s3_boto, parquet_bucket)

    # Compare the cache list with the timestamps that exist in the bucket.
    diff_list = [item for item in parquet_timestamps if item not in cache_txt]

    # If there is no diffeence: there is no new data to insert.
    if diff_list == []:
        logger.info("No new data to insert into warehouse.")

    # Define a pyarrow s3 client.
    s3_py = fs.S3FileSystem(region='eu-west-2')

    with connect() as db:

        # Query dim_date table.
        res = db.run("SELECT * FROM dim_date LIMIT 1;")

        # If empty response then dim_date has yet to be populated, so populate.
        if res == []:
            logger.info("dim_date not populated in waregouse, populating.")
            file = s3_py.get_file_info(parquet_bucket+"/dim_date.parquet")
            insert_data(s3_py, file)

    # For each timestamp that has yet to be processed:
    for timestamp in diff_list:
        logger.info(f"Populating with data from {timestamp}.")

        # List all files that belong to this timestamp.
        folder_contents = s3_py.get_file_info(
            fs.FileSelector(parquet_bucket + '/' + timestamp, recursive=False)
        )

        # For each parquet file under this timestamp:
        for file in folder_contents:

            # Insert the file's data into the appropriate table.
            insert_data(s3_py, file)

        # Add the timestamp to cache now that all data has been inserted.
        log_str = f"All data from {timestamp} inserted into "
        log_str += "warehouse, appending to cache list."
        logger.info(log_str)
        cache_txt.append(timestamp)

    # Write updated cache to bucket.
    if diff_list != []:
        logger.info("Writing out updated cache file.")
        s3_boto.put_object(
            Bucket=parquet_bucket, Key='cache.txt', Body='\n'.join(cache_txt)
        )


def read_parquet(s3, file):
    """ Takes a pyarrow FileInfo object that points to a parquet file on s3 and
        returns the parquet file contents as a list of lists, one for each row
        including column names.
        """
    fh = s3.open_input_file(file.path)
    list_of_dicts = pq.read_table(fh).to_pylist()
    list_table = [
        list(list_of_dicts[0].keys())[1:]
    ] + [
        list(item.values())[1:] for item in list_of_dicts
    ]
    return list_table


def list_timestamps(s3, bucket_name):
    """ Lists out all prefixes that exist in the passed bucket."""
    timestamps = s3.list_objects_v2(
        Bucket=bucket_name, Prefix="", Delimiter="/"
    ).get('CommonPrefixes', [])
    timestamps = [item['Prefix'] for item in timestamps]
    return timestamps


def get_parquet_bucket_name():
    """ Gets the name of the bucket of processed data using the prefix."""
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
    """ Takes a pyarrow FileInfo object that points to a parquet file on s3,
        reads the parquet data and writes to the appropriate table using
        UPDATE or INSERT as needed.

        Args:
            s3_py: A pyarrow s3 client.

            file: A pyarrow FileInfo object that points to the parquet file
            that will have its data inserted into the data warehouse.

        Returns:
            None.

        Sid-effects:
            The data warehouse will be UPDATEd/INSERTed apprpriately with the
            parquet data.
    """

    # Reads the parquet file.
    list_table = read_parquet(s3_py, file)

    # Reads the table name from the file's name.
    table = file.base_name[:-8]
    logger.info(f"Populating {table}.")

    # Separates the column headings from the data.
    headers = list_table[0]
    list_table = list_table[1:]

    with connect() as db:

        # Query to see what data exists in the table already.
        res = db.run(f'SELECT * FROM {pg.identifier(table)};')

        # Creates a list of primary keys that exist in the table
        if table != "fact_sales_order":
            pk = [row[0] for row in res]

        # fact_sales_order has its pk inserted into the second column.
        else:
            pk = [row[1] for row in res]

        rows_to_insert = []
        logged_update = False

        # For each row of data in the data set:
        for row in list_table:

            # If the rows primary key exists in the table: run an update query.
            if row[0] in pk:
                if not logged_update:
                    logger.info("Running UPDATE queries.")
                    logged_update = True

                data = ', '.join(
                    [pg.identifier(headers[i]) + ' = ' + pg.literal(row[i])
                     for i in range(len(headers))]
                )
                query = f'UPDATE {pg.identifier(table)} SET {data} WHERE '
                query += f'{pg.identifier(headers[0])} = {pg.literal(row[0])};'
                db.run(query)

            # Else: add to a list so inserts can all be done at once.
            else:
                rows_to_insert.append(row)

        # If there are rows to insert: build a query string to insert all.
        if rows_to_insert != []:
            logger.info("Compiling all INSERT data into one query.")

            # Concatenate the column headings for an INSERT query
            headersstr = ', '.join(
                [pg.identifier(item) for item in headers]
            )

            # Concatenate all of the row data to be inserted to an INSERT
            # query format.
            datastr = ", ".join(
                ["("+', '.join(
                    [pg.literal(item) for item in row]
                )+")" for row in rows_to_insert]
            )

            # Build the INSERT query.
            query = f'INSERT INTO {pg.identifier(table)} ({headersstr}) '
            query += f'VALUES {datastr};'
            logger.info("Running INSERT query.")

            # Run the insert query.
            db.run(query)


def get_credentials():
    """ Loads a set of DB credentials using a secret stored in AWS secrets."""

    secretsmanager = boto3.client('secretsmanager')
    credentials_response = secretsmanager.get_secret_value(
        SecretId='Warehouse_credentials'
    )
    credentials = json.loads(credentials_response['SecretString'])
    return credentials


def connect():
    """ Will return a connection to the DB, to be used with context manager."""

    credentials = get_credentials()

    return pg.Connection(
        user=credentials['username'],
        password=credentials['password'],
        host=credentials['hostname'],
        database='postgres',
        port=credentials['port']
    )


class MissingBucketError(Exception):
    """ An error for when the prerequisite buckets do not exist."""

    def __init__(self, source="", message=""):
        self.source = source
        self.message = message
