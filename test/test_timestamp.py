from datetime import datetime as dt
import boto3
from moto import mock_s3
from src.lambda_ingestion.ingestion import (
    get_ingestion_timestamps,
    extract_table_to_csv,
    postgres_to_csv
)
from unittest.mock import Mock, patch
# from pprint import pprint
# function returning a sample csv string for testing
current_timestamp = dt.now().isoformat()


def generate_csv_string():
    value = "transaction_id,transaction_type,last_updated\n"
    value += f"434,SALE,{current_timestamp}\n"
    value += f"435,PURCHASE,{current_timestamp}\n"
    value += f"436,SALE,{current_timestamp}\n"
    value += f"437,PURCHASE,{current_timestamp}\n"
    return value


@mock_s3
def test_timestamp_functionality():
    # create s3 bucket
    bucket_name = "test_ingestion_bucket"
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket=bucket_name)

    # grab csv string
    csv_data = generate_csv_string()
    # get current timestamp

    s3_client.put_object(
        Body=csv_data,
        Bucket=bucket_name,
        Key='test_table.csv',
        ContentType='application/text',
    )
    list = get_ingestion_timestamps(bucket_name, ['test_table'])
    assert list['test_table'].isoformat() == current_timestamp


@patch('src.lambda_ingestion.ingestion.connect')
def test_extract_table_to_csv_concats_query_result_to_csv(mock_connection):
    mock_db = Mock()
    mock_connection.return_value.__enter__.return_value = mock_db
    mock_db.run.return_value = [
        ["434", "SALE", current_timestamp],
        ["435", "PURCHASE", current_timestamp],
        ["436", "SALE", current_timestamp],
        ["437", "PURCHASE", current_timestamp],
    ]
    mock_db.columns = [{'name': "transaction_id"}, {
        'name': "transaction_type"}, {'name': "last_updated"}]
    res = extract_table_to_csv("test_table", dt(1970, 1, 1))
    expected = "transaction_id,transaction_type,last_updated\r\n"
    expected += f"434,SALE,{current_timestamp}\r\n"
    expected += f"435,PURCHASE,{current_timestamp}\r\n"
    expected += f"436,SALE,{current_timestamp}\r\n"
    expected += f"437,PURCHASE,{current_timestamp}\r\n"
    assert res == expected


@mock_s3
@patch('src.lambda_ingestion.ingestion.connect')
def test_csv_data_has_no_headers_if_csv_already_in_s3(mock_connection):
    expected = "transaction_id,transaction_type,last_updated\r\n"
    expected += f"434,SALE,{current_timestamp}\r\n"
    expected += f"435,PURCHASE,{current_timestamp}\r\n"
    expected += f"436,SALE,{current_timestamp}\r\n"
    expected += f"437,PURCHASE,{current_timestamp}\r\n"
    fakecsv = expected
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket='test')
    s3_client.put_object(
        Body=fakecsv,
        Bucket='test',
        Key='fake.csv',
        ContentType='application/text',
    )
    mock_db = Mock()
    mock_connection.return_value.__enter__.return_value = mock_db
    new_timestamp = dt.now().isoformat()
    mock_db.run.return_value = [
        ["438", "SALE", new_timestamp],
    ]
    mock_db.columns = [{'name': "transaction_id"}, {
        'name': "transaction_type"}, {'name': "last_updated"}]
    res = extract_table_to_csv(
        "test_table", dt.fromisoformat(current_timestamp))
    expected = f"438,SALE,{new_timestamp}\r\n"
    assert res == expected


@mock_s3
@patch('src.lambda_ingestion.ingestion.connect')
def test_new_csvdata_appended_to_s3_csv(mock_connection):
    expected = "transaction_id,transaction_type,last_updated\r\n"
    expected += f"434,SALE,{current_timestamp}\r\n"
    expected += f"435,PURCHASE,{current_timestamp}\r\n"
    expected += f"436,SALE,{current_timestamp}\r\n"
    expected += f"437,PURCHASE,{current_timestamp}\r\n"
    fakecsv = expected
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket='test')
    s3_client.put_object(
        Body=fakecsv,
        Bucket='test',
        Key='fake.csv',
        ContentType='application/text',
    )
    mock_db = Mock()
    mock_connection.return_value.__enter__.return_value = mock_db
    new_timestamp = dt.now().isoformat()
    mock_db.run.return_value = [
        ["438", "SALE", new_timestamp],
    ]
    mock_db.columns = [{'name': "transaction_id"}, {
        'name': "transaction_type"}, {'name': "last_updated"}]
    postgres_to_csv('test', ['fake'])
    expected += f"438,SALE,{new_timestamp}\r\n"
    response = s3_client.get_object(Bucket='test', Key='fake.csv')
    returndata = response.get('Body')
    content_str = returndata.read().decode()
    assert content_str == expected
