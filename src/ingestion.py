import converter
import json
import pg8000.native as pg8000
from pprint import pprint
# import boto3
boto3 = None


"""
def get_credentials():
        Will return a dictionary containing
        our credentials (as defined in converter.py)
    secretsmanager = boto3.client('secretsmanager')
    secret_name = 'Ingestion_credentials'
    credentials_response = secretsmanager.get_secret_value(
        SecretId=secret_name
    )
    credentials = json.loads(credentials_response['SecretString'])
    return credentials
"""

hostname = 'nc-data-eng-totesys-production'
hostname += '.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'


def connect():
    """
        Will return a connection to the ingestion database
    """

#    credentials = get_credentials()
    credentials = converter.convert_ingestion_credentials(
        hostname,
        port='5432',
        db='totesys',
        username='project_user_5',
        password='FiA0ooxIw4ojnmcJmc8VwrWm'
    )

    credentials = json.loads(credentials)

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
