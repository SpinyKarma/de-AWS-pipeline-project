import json
import pg8000.native as pg8000
from pprint import pprint
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
    
    def postgres_to_csv(): 
        with connect() as db:
            staffdata = db.run('SELECT * FROM staff;')
            column_names = [column['name'] for column in db.columns]
            rows = [dict(zip(column_names, row)) for row in staffdata]
            csv_file_path = 'staff_data.csv'
        with open(csv_file_path, 'w', newline='') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=column_names)
            csv_writer.writeheader()
            csv_writer.writerows(rows)
        
        with connect() as db:
            staffdata = db.run('SELECT * FROM counterparty;')
            column_names = [column['name'] for column in db.columns]
            rows = [dict(zip(column_names, row)) for row in staffdata]
            csv_file_path = 'counterparty_data.csv'
        with open(csv_file_path, 'w', newline='') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=column_names)
            csv_writer.writeheader()
            csv_writer.writerows(rows)
    
        with connect() as db:
            staffdata = db.run('SELECT * FROM counterparty;')
            column_names = [column['name'] for column in db.columns]
            rows = [dict(zip(column_names, row)) for row in staffdata]
            csv_file_path = 'counterparty_data.csv'
        with open(csv_file_path, 'w', newline='') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=column_names)
            csv_writer.writeheader()
            csv_writer.writerows(rows)

        with connect() as db:
            staffdata = db.run('SELECT * FROM counterparty;')
            column_names = [column['name'] for column in db.columns]
            rows = [dict(zip(column_names, row)) for row in staffdata]
            csv_file_path = 'counterparty_data.csv'
        with open(csv_file_path, 'w', newline='') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=column_names)
            csv_writer.writeheader()
            csv_writer.writerows(rows)
        
        with connect() as db:
            staffdata = db.run('SELECT * FROM counterparty;')
            column_names = [column['name'] for column in db.columns]
            rows = [dict(zip(column_names, row)) for row in staffdata]
            csv_file_path = 'counterparty_data.csv'
        with open(csv_file_path, 'w', newline='') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=column_names)
            csv_writer.writeheader()
            csv_writer.writerows(rows)
    
    
    
    
    
    
    
    postgres_to_csv()
            
        
        
        
     


 
