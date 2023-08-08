import pytest
import src.lambda_ingestion.ingestion_lambda as i
from moto import mock_secretsmanager
import boto3


def test_ingestion_bucket_name():
    """
    Test whether the function 'get_ingestion_bucket_name' returns the
    correct name of the ingestion bucket with the timestamp appended.
    """

    name = i.get_ingestion_bucket_name()
    correct_name = 'terrific-totes-ingestion-bucket'
    correct_name += '20230725102602583400000001'
    assert name == correct_name


@mock_secretsmanager
def test_get_credentials_throws_Exception_on_no_credentials():
    """
    Test whether the function 'get_credentials' raises an Exception when
    no credentials are found in the AWS Secrets Manager.
    """

    with pytest.raises(Exception):
        i.get_credentials()


@mock_secretsmanager
def test_get_credentials_throws_InvalidCredentialsError():
    '''
        Test whether the function raises InvalidCredentialsError when invalid
        database credentials are encountered in the retrieved secret,
        to verify the function handles invalid database credentials content
        from the Secrets Manger by raising a InvalidCredentialsError.
    '''

    client = boto3.client('secretsmanager', region_name='eu-west-2')
    client.create_secret(Name='Ingestion_credentials',
                         SecretString='''
                        {
                            "hostname": "bad",
                            "port": "1234",
                            "db": "bad",
                            "username": "bad"
                        }
                        '''
                         )

    with pytest.raises(i.InvalidCredentialsError):
        i.get_credentials()


@mock_secretsmanager
def test_get_credentials_throws_JSONDecodeError():
    '''
        Test whether the function raises a JSONDecodeError when invalid
        JSON is encountered in the retrieved secret, to verify the function
        handles invalid JSON content from the Secrets Manger by raising a
        JSONDecodeError.
    '''

    client = boto3.client('secretsmanager', region_name="eu-west-2")
    client.create_secret(Name='Ingestion_credentials',
                         SecretString='''
                        {
                           bad json
                        }
                        ''')

    with pytest.raises(i.json.decoder.JSONDecodeError):
        i.get_credentials()


@mock_secretsmanager
def test_get_credentials_returns_dict():
    '''
        Test whether the function 'get_credentials' returns a dictionary
        of credentials when valid credentials are obtained from the
        Secrets Manager, to verify the function correctly retrieves
        and formats valid credentials.
    '''

    client = boto3.client('secretsmanager', region_name='eu-west-2')
    client.create_secret(Name='Ingestion_credentials',
                         SecretString='''
                        {
                            "hostname": "example",
                            "port": "1234",
                            "db": "example",
                            "username": "example",
                            "password": "example"
                        }
                        '''
                         )

    credentials = i.get_credentials()
    assert isinstance(credentials, dict)


def test_connect_returns_connection():
    '''
        Test whether the function 'connect' returns a connection object,
        to verify the function can establish a connection to the database
        and return a valid connection object.
    '''

    assert isinstance(i.connect(), i.pg8000.Connection)


def test_csv_builder():
    '''
        Test the functionality of the 'CsvBuilder' class for building CSV
        content to ensure that it correctly builds and stores CSV content
        as expected.
    '''

    builder = i.CsvBuilder()
    builder.write('first\n')
    builder.write('second\n')
    builder.write('third\n')
    assert builder.as_txt() == 'first\nsecond\nthird\n'
