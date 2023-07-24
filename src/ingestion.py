import json
import pg8000.native as pg8000
import boto3
import csv


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
        Will return a connection to the ingestion database
    """

    credentials = get_credentials()

    return pg8000.Connection(
        user=credentials['username'],
        password=credentials['password'],
        host=credentials['hostname'],
        database=credentials['db'],
        port=credentials['port']
    )


if __name__ == '__main__':

    def extract_table_to_csv(table_name):
        try:
            with connect() as db:
                result = db.run(f'SELECT * FROM {table_name};')
                column_names = [column['name'] for column in db.columns]
                rows = [dict(zip(column_names, row)) for row in result]
                csv_file_path = f'{table_name}_data.csv'
            with open(csv_file_path, 'w', newline='') as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=column_names)
                csv_writer.writeheader()
                csv_writer.writerows(rows)
                return f'{table_name}_data.csv'
        except Exception as e:
            print(f"Error extracting data from {table_name}: {e}")
            return None

    def postgres_to_csv():
        table_names = [
            'staff',
            'counterparty',
            'sales_order',
            'address',
            'payment',
            'purchase_order',
            'payment_type',
            'transaction'
                    ]
        for table_name in table_names:
            csv_file_path = extract_table_to_csv(table_name)
            if csv_file_path:
                print(f'''Data extracted for {table_name}
                and saved to {csv_file_path}''')
            else:
                print(f"Failed to extract data for {table_name}")

