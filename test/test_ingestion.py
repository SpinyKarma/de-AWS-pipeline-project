import pytest
from src.ingestion import *
from moto import mock_s3, mock_secretsmanager
import boto3


def test_ingestion_bucket_name():
    name = get_ingestion_bucket_name()
    correct_name = 'terrific-totes-ingestion-bucket'
    correct_name += '20230725102602583400000001'
    assert name == correct_name


@mock_secretsmanager
def test_get_credentials_throws_Exception_on_no_credentials():
    with pytest.raises(Exception):
        get_credentials()


@mock_secretsmanager
def test_get_credentials_throws_InvalidCredentialsError():
    client = boto3.client('secretsmanager')
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

    with pytest.raises(InvalidCredentialsError):
        get_credentials()


@mock_secretsmanager
def get_credentials_throws_JSONDecodeError():
    client = boto3.client('secretsmanager')
    client.create_secret(Name='Ingestion_credentials',
                         SecretString='''
                        {
                           bad json
                        }
                        ''')

    with pytest.raises(json.decoder.JSONDecodeError):
        get_credentials()


@mock_secretsmanager
def test_get_credentials_returns_dict():
    client = boto3.client('secretsmanager')
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

    credentials = get_credentials()
    assert isinstance(credentials, dict)
