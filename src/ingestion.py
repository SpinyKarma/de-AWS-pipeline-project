import json
import pg8000.native as pg8000
import boto3
import csv
from datetime import datetime as dt
current_timestamp = dt.now()


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
    """
        Loads our DB credentials using AWS secrets
        Returns:
            a credentials dictionary containing:
            - username
            - password
            - hostname
            - db
            - port
        Throws:
            InvalidCredentialsError if the keys of the dictionary are invalid
    """

    secretsmanager = boto3.client('secretsmanager', region_name='eu-west-2')
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
    """
        Will return a connection to the DB
    """
    credentials = get_credentials()

    return pg8000.Connection(
        user=credentials['username'],
        password=credentials['password'],
        host=credentials['hostname'],
        database=credentials['db'],
        port=credentials['port']
    )


class CsvBuilder:
    """
        For creating CSV without writing to a file
    """

    def __init__(self):
        self.rows = []

    def write(self, row):
        self.rows.append(row)

    def as_txt(self):
        return ''.join(self.rows)


def get_last_ingestion_timestamps():
    last_ingestion_timestamps = {}
    # list objects in s3 bucket
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=INGESTION_BUCKET_NAME)
    # iterate thru objects and get metatdata
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
    # retrieve last ingestion timestamp metadata
            last_ingestion_timestamp = obj.get(
                'Metadata', {}).get('LastIngestionTimestamp')
            if last_ingestion_timestamp:
                last_ingestion_timestamp[key[:-4]] = last_ingestion_timestamp
    return last_ingestion_timestamps


def set_last_ingestion_timestamps(last_ingestion_timestamps):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=INGESTION_BUCKET_NAME)
    # iterate thru objects and updata metatdata
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
    # get the table name witout '.csv' extension
            table_name = key[:-4]
            # check if table exists in last_ingestion_timestamps
            if table_name in last_ingestion_timestamps:
                # update metadata for s3 object
                s3_client.copy_object(
                    Bucket=INGESTION_BUCKET_NAME,
                    CopySource={'Bucket': INGESTION_BUCKET_NAME, 'Key': key},
                    Key=key,
                    Metadata={
                        'LastIngestionTimestamp': current_timestamp
                        .isoformat()},
                    MetadataDirective='REPLACE'
                )


def extract_table_to_csv(table_name, last_run_timestamp):
    try:
        """
            Grab all the data from the database
        """
        with connect() as db:
            result = db.run(f'SELECT * FROM {pg8000.identifier(table_name)} ;')
            column_names = [column['name'] for column in db.columns]
            rows = [dict(zip(column_names, row)) for row in result]

        """
            Convert that data to a CSV form, and write it to the disk
        """
        csv_builder = CsvBuilder()
        csv_writer = csv.DictWriter(csv_builder, fieldnames=column_names)
        csv_writer.writeheader()
        csv_writer.writerows(rows)
        return csv_builder.as_txt()

    except Exception as e:
        print(f"Error extracting data from {table_name}: {e}")


def postgres_to_csv(last_ingestion_timestamps):
    table_names = [
        'staff',
        'counterparty',
        'sales_order',
        'address',
        'payment',
        'purchase_order',
        'payment_type',
        'transaction',
    ]

    table_name_to_csv = {}

    for table_name in table_names:
        last_ingestion_timestamp = last_ingestion_timestamps.get(table_name)
        print(f'Ingesting {table_name}...')
        csv = extract_table_to_csv(table_name, last_ingestion_timestamp)
        if csv:
            table_name_to_csv[table_name] = csv
            print(f'Ingestion of {table_name} is complete')
        else:
            raise TableIngestionError(table_name)

    print('OK')
    return table_name_to_csv


def ingest(s3_client):
    last_ingestion_timestamps = get_last_ingestion_timestamps()
    table_csv = postgres_to_csv(last_ingestion_timestamps)
    for table_name in table_csv.keys():
        csv_data = table_csv[table_name]
        s3_client.put_object(
            Body=csv_data,
            Bucket=INGESTION_BUCKET_NAME,
            Key=f'{table_name}.csv',
            ContentType='application/text'
        )
    last_ingestion_timestamps = {
        table_name: current_timestamp.isoformat()
        for table_name in table_csv.keys()}
    set_last_ingestion_timestamps(last_ingestion_timestamps)


if __name__ == '__main__':
    s3_client = boto3.client('s3')
    ingest(s3_client)
