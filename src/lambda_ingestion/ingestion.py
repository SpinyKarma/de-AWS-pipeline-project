import json
import pg8000.native as pg8000
import boto3
import csv
from datetime import datetime as dt
from pprint import pprint
from io import StringIO
import csv
current_timestamp = dt.now()

csv_names = [
    'staff',
    'counterparty',
    'sales_order',
    'address',
    'payment',
    'purchase_order',
    'payment_type',
    'transaction',
]


def get_ingestion_bucket_name():
    name = 'terrific-totes-ingestion-bucket'
    name += '20230725102602583400000001'
    return name


INGESTION_BUCKET_NAME = get_ingestion_bucket_name()


class TableIngestionError(Exception):
    pass


class InvalidCredentialsError (Exception):
    pass


def get_credentials():
    """Loads our DB credentials using AWS secrets.

        Returns:
            a credentials dictionary containing:
            - username
            - password
            - hostname
            - db
            - port

        Raises:
            InvalidCredentialsError if the keys of the dictionary are invalid.
    """

    secretsmanager = boto3.client('secretsmanager')
    secret_name = 'Ingestion_credentials'
    credentials_response = secretsmanager.get_secret_value(
        SecretId=secret_name
    )
    credentials = json.loads(credentials_response['SecretString'])
    required_keys = ['hostname', 'port', 'db', 'username', 'password']

    if list(credentials.keys()) != required_keys:
        raise InvalidCredentialsError(credentials)

    return credentials


def connect():
    """Will return a connection to the DB."""
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


def get_ingestion_timestamps(Bucket, table_list):
    '''Extracts the most recent last_updated of each csv in the passed bucket.

    Args:
        Bucket: Name of the bucket to pull metadata from.

        table_list: A list of the table names to perform the function on.

    Returns:
        last_ingestion_timestamps: A dict with the name of the object as the
        key and the extracted timestamp as the value.
    '''
    last_ingestion_timestamps = {}
    s3_client = boto3.client("s3")
    for file in table_list:
        key = file + ".csv"
        try:
            response = s3_client.get_object(
                Bucket=Bucket,
                Key=key
            )
            csv_body = response.get('Body')
            content_str = csv_body.read().decode()
            f = StringIO(content_str)
            reader = list(csv.reader(f, delimiter=','))
            timestamp_index = reader[0].index("last_updated")
            last_updated = reader[-1][timestamp_index]
            last_ingestion_timestamps[file] = dt.fromisoformat(last_updated)
        except s3_client.exceptions.NoSuchKey:
            last_ingestion_timestamps[file] = dt(1970, 1, 1)
    return last_ingestion_timestamps


def extract_table_to_csv(table_name, timestamp):
    '''Runs a query on the given table filtered by the given timestamp.

    Args:
        table_name: The name of the table to query.

        timestamp: A datetime.datetime object, all entries with a last_updated
        key more recent than this will be pulled from the table.

    Returns:
        csv_text: A csv formatted string, if the csv file does not exist
        in the s3 bucket then this includes headers, otherwise it does not
        include headers.
    '''

    with connect() as db:
        query_str = f'SELECT * FROM {pg8000.identifier(table_name)} WHERE '
        query_str += f'last_updated > {pg8000.literal(timestamp.isoformat())};'
        result = db.run(query_str)
        column_names = [column['name'] for column in db.columns]
        rows = [dict(zip(column_names, row)) for row in result]
        csv_builder = CsvBuilder()
        csv_writer = csv.DictWriter(csv_builder, fieldnames=column_names)
        if timestamp.year == 1970:
            csv_writer.writeheader()
        csv_writer.writerows(rows)
        csv_text = csv_builder.as_txt()
        return csv_text


def postgres_to_csv(Bucket, table_list):
    '''Extracts new csv data from each table and appends to files in bucket.

    Queries each table in the database and writes the contents to a csv file
    in the S3 bucket. If the csv file already exists and there is new data in
    the tables, then appends the new data to the csvs.

    Args:
        Bucket: Name of the bucket to modify the csvs of.

        table_list: A list of the table names to perform the function on.

    Returns:
        None

    Side-effects:
        Updates the S3 csvs with new data.
    '''
    s3_client = boto3.client("s3")
    timestamp_dict = get_ingestion_timestamps(Bucket, table_list)

    for table in timestamp_dict:
        timestamp = timestamp_dict[table]
        output_csv = extract_table_to_csv(table, timestamp)
        if timestamp.year == 1970:
            s3_client.put_object(
                Body=output_csv,
                Bucket=Bucket,
                Key=f'{table}.csv',
                ContentType='application/text'
            )
        else:
            response = s3_client.get_object(Bucket=Bucket, Key=f'{table}.csv')
            csv_body = response.get('Body')
            content_str = csv_body.read().decode()
            new_body = content_str+output_csv
            s3_client.put_object(
                Body=new_body,
                Bucket=Bucket,
                Key=f'{table}.csv',
                ContentType='application/text'
            )


if __name__ == "__main__":
    extract_table_to_csv("staff", dt(1970, 1, 1))
