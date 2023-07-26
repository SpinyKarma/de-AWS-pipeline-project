import pytest
import src.ingestion as i
from moto import mock_secretsmanager
import boto3


def test_ingestion_bucket_name():
    name = i.get_ingestion_bucket_name()
    correct_name = 'terrific-totes-ingestion-bucket'
    correct_name += '20230725102602583400000001'
    assert name == correct_name


@mock_secretsmanager
def test_get_credentials_throws_Exception_on_no_credentials():
    with pytest.raises(Exception):
        i.get_credentials()


@mock_secretsmanager
def test_get_credentials_throws_InvalidCredentialsError():
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
def get_credentials_throws_JSONDecodeError():
    client = boto3.client('secretsmanager')
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
    assert isinstance(i.connect(), i.pg8000.Connection)


def test_csv_builder():
    builder = i.CsvBuilder()
    builder.write('first\n')
    builder.write('second\n')
    builder.write('third\n')
    assert builder.as_txt() == 'first\nsecond\nthird\n'


"""
@mock_s3
def test_ingest_writes_to_bucket():
    client = boto3.client('s3')
    client.create_bucket(
        Bucket=i.get_ingestion_bucket_name(),
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )

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

    keys = [f'{tablename}.csv' for tablename in table_names]

    i.ingest(client)

    for key in keys:
        print(client.get_object(Key=key))


    test_ingest_writes_to_bucket()
"""
