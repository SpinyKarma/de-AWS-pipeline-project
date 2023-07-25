import json
import pg8000.native as pg8000
import boto3
import csv
from pprint import pprint

CSV_RESULT_FOLDER = 'tmp'


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

    for table_name in table_names:
        csv = extract_table_to_csv(table_name)
        if csv:
            # TODO: RETURN DICT OF TABLE NAME TO CSV DATA!
            print(csv)
        else:
            # TODO: RAISE EXCEPTION
            print(f"Failed to extract data for {table_name}")


if __name__ == '__main__':
    with connect() as db:
        result = db.run('SELECT * FROM staff;')
        pprint([column['name'] for column in db.columns])
