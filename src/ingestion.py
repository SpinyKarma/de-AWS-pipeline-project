import json
import pg8000.native as pg8000
from pprint import pprint
import boto3


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
    """

    secretsmanager = boto3.client('secretsmanager')
    secret_name = 'Ingestion_credentials'
    credentials_response = secretsmanager.get_secret_value(
        SecretId=secret_name
    )
    credentials = json.loads(credentials_response['SecretString'])
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
    with connect() as db:
        result = db.run('SELECT * FROM staff;')
        pprint([column['name'] for column in db.columns])
