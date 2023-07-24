import pg8000
import boto3
import json


secretsmanager = boto3.client('secretsmanager')


def get_credentials():
    """
        Will return a dictionary containing
        our credentials (as defined in converter.py)
    """

    secret_name = 'ingestion_credentials'
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
    return pg8000.connect(
        user=credentials['username'],
        password=credentials['password'],
        host=credentials['hostname'],
        database=credentials['db'],
        port=credentials['port']
    )
