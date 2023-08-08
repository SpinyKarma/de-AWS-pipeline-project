import boto3
import logging
import json
import pg8000.native as pg8000
import csv
from datetime import datetime as dt
# from pprint import pprint

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def ingestion_lambda_handler(event, context):
    ''' Will query the ingestion database at regular intervals and save the
        results to csvs in an s3 bucket to await processing, using the s3
        prefix system to only query that has been added or updated since the
        last query.
    '''
    try:
        # List of tables to query
        # will need updating if project is expended to payment and/or
        # transaction schemas
        table_names = [
            'address',
            'counterparty',
            'currency',
            'department',
            'design',
            'sales_order',
            'staff',
        ]

        # The name of th s3 bucket that the csvs will be saved to
        bucket_name = get_ingestion_bucket_name()
        logger.info(f"Ingestion bucket established as {bucket_name}.")

        # Uses the timestamp prefixes in s3 to determine when the most recent
        # ingestion was
        last = get_last_ingestion_timestamp(bucket_name)
        logger.info(
            f"Most recent timestamp saved to bucket is {last.isoformat()}"
        )

        # Uses the most recent ingestion timestamp to query the database for
        # recent additions
        csvs_to_update = extract_table_to_csv(table_names, last)

        # Writes the returned query data to the s3 bucket in csv form under a
        # new timestamp prefix
        csv_to_s3(bucket_name, csvs_to_update)

    except TableIngestionError as error:
        logger.error(f'Table Ingestion Error: {error.message}')
        raise error

    except InvalidCredentialsError as error:
        logger.error(f'Invalid Credentials Error: {error}')
        raise error

    except NonTimestampedCSVError as error:
        err_str = 'A CSV is in the bucket without a timestamp, '
        err_str += f'remove all non-timestamped CSVs: {error}'
        logger.error(err_str)
        raise error

    except Exception as error:
        logger.error(f'An unexpected error occured: {error}')
        raise error


def get_ingestion_bucket_name():
    '''Gets the name of the bucket to store raw data in using the prefix.'''
    prefix = 'terrific-totes-ingestion-bucket'
    buckets = boto3.client("s3").list_buckets().get("Buckets")
    for bucket in buckets:
        if prefix in bucket["Name"]:
            name = bucket["Name"]
            break
    return name


class TableIngestionError(Exception):
    def __init__(self, message=""):
        self.message = message


class NonTimestampedCSVError(Exception):
    pass


class InvalidCredentialsError (Exception):
    pass


def get_credentials(secret_name='Ingestion_credentials'):
    """Loads a set of DB credentials using a secret stored in AWS secrets.

        Args:
            secet_name: The name of the secret to extract credentials from,
            defaults to Ingestion_credentials. Also takes Warehouse_credentials
            for establishing a connection to the data warehouse instead.

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
        SecretId=secret_name
    )
    credentials = json.loads(credentials_response['SecretString'])
    required_keys = {
        "Ingestion_credentials": [
            'hostname', 'port', "db", 'username', 'password'
        ],
        "Warehouse_credentials": [
            'hostname', 'port', "schema", 'username', 'password'
        ]
    }

    if sorted(list(credentials.keys())) != sorted(required_keys[secret_name]):
        raise InvalidCredentialsError(credentials)

    return credentials


def connect(db="ingestion"):
    """ Will return a connection to the ingestion database. If passed the
        argument "warehouse", will instead establish a connection to the data
        warehouse. To be used with context manager. """
    if db == "warehouse":
        credentials = get_credentials("Warehouse_credentials")

        return pg8000.Connection(
            user=credentials['username'],
            password=credentials['password'],
            host=credentials['hostname'],
            database='postgres',
            port=credentials['port']
        )
    else:
        credentials = get_credentials()

        return pg8000.Connection(
            user=credentials['username'],
            password=credentials['password'],
            host=credentials['hostname'],
            database=credentials['db'],
            port=credentials['port']
        )


class CsvBuilder:
    """For creating CSV without writing to a file."""

    def __init__(self):
        self.rows = []

    def write(self, row):
        self.rows.append(row)

    def as_txt(self):
        return ''.join(self.rows)


def get_last_ingestion_timestamp(Bucket):
    ''' Extracts the timestamp of the most recently added csv in the passed
        bucket.

    Args:
        Bucket: Name of the bucket to pull csvs from.

    Returns:
        last_timestamp: The timestamp from the most recently added
        csv, defaults to 1st Jan 1970 if no csvs in file.
    '''
    s3_client = boto3.client("s3")
    try:
        res = s3_client.list_objects_v2(Bucket=Bucket)['Contents']
    except KeyError:
        return dt(1970, 1, 1)
    names = [item['Key'] for item in res]
    sorted_names = sorted(names, reverse=True)
    last_timestamp = sorted_names[0].split("/")[0]
    print(last_timestamp)
    try:
        last_timestamp = dt.fromisoformat(last_timestamp)
    except ValueError:
        raise NonTimestampedCSVError
    return last_timestamp


def extract_table_to_csv(table_list, last_timestamp):
    ''' Runs a query on the given tables filtered by the given timestamp to
        return only newly added data.

    Args:
        table_list: The list of table names to query.

        last_timestamp: A datetime.datetime object, all entries with a
        last_updated key more recent than this will be pulled from the table.

    Returns:
        updated_tables: A dict of each table name that had a return from the
        postgres query as keys and the return formatted to a csv as the value.
    '''
    updated_tables = {}
    for table_name in table_list:
        f_tablename = pg8000.identifier(table_name)
        f_timestamp = pg8000.literal(last_timestamp.isoformat())
        with connect() as db:
            try:
                query_str = f'SELECT * FROM {f_tablename} WHERE '
                query_str += f'last_updated > {f_timestamp};'
                result = db.run(query_str)
            except Exception:
                raise TableIngestionError(f"Error querying {table_name} table")
            if result == []:
                logger.info(
                    f"No new data in {table_name} since last ingestion."
                )
            else:
                logger.info(f"Compiling new data from {table_name} to csv.")
            column_names = [column['name'] for column in db.columns]
            rows = [dict(zip(column_names, row)) for row in result]
            csv_builder = CsvBuilder()
            csv_writer = csv.DictWriter(
                csv_builder, fieldnames=column_names)
            if rows != []:
                csv_writer.writeheader()
                csv_writer.writerows(rows)
                csv_text = csv_builder.as_txt()
                updated_tables[table_name] = csv_text
    return updated_tables


def csv_to_s3(Bucket, updated_table_list):
    ''' Extracts new csv data from each table and saves to timestamped files in
        the bucket.

    Args:
        Bucket: Name of the bucket to add csvs to.

        updated_table_list: A dict of the table names to perform the function
        on, paired with the csv data to write.

    Returns:
        None

    Side-effects:
        Uploads the tmiestamped csvs with new data to S3.
    '''
    current_timestamp = dt.now()
    s3_client = boto3.client("s3")
    for table in updated_table_list.keys():
        key = f'{current_timestamp.isoformat()}/{table}.csv'
        logger.info(f'Writing "{key}" to bucket.')
        output_csv = updated_table_list[table]
        s3_client.put_object(
            Body=output_csv,
            Bucket=Bucket,
            Key=key,
            ContentType='application/text'
        )
