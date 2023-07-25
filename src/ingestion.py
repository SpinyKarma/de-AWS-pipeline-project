import json
import pg8000.native as pg8000
import boto3
import csv


INGESTION_BUCKET_NAME = 'raw-csv-data-bucket'


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


def extract_table_to_csv(table_name):
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


def postgres_to_csv():
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
        print(f'Ingesting {table_name}...')
        csv = extract_table_to_csv(table_name)
        if csv:
            table_name_to_csv[table_name] = csv
            print(f'Ingestion of {table_name} is complete')
        else:
            raise TableIngestionError(table_name)

    print('OK')
    return table_name_to_csv


def ingest(s3_client):
    table_csv = postgres_to_csv()

    for table_name in table_csv.keys():
        csv_data = table_csv[table_name]
        s3_client.put_object(
            Body=csv_data,
            Bucket=INGESTION_BUCKET_NAME,
            Key=f'{table_name}.csv',
            ContentType='application/text'
        )


if __name__ == '__main__':
    s3_client = boto3.client('s3')
    ingest()
    # print(postgres_to_csv())
