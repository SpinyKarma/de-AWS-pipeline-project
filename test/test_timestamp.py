from datetime import datetime as dt
import boto3
import pytest
from moto import mock_s3
from src.lambda_ingestion.ingestion_lambda import (
    get_last_ingestion_timestamp,
    extract_table_to_csv,
    csv_to_s3,
    NonTimestampedCSVError
)
from unittest.mock import Mock, patch
# from pprint import pprint
# function returning a sample csv string for testing
current_timestamp = dt.now().isoformat()


def generate_csv_string(timestamp=current_timestamp):
    value = "transaction_id,transaction_type,last_updated\r\n"
    value += f"434,SALE,{timestamp}\r\n"
    value += f"435,PURCHASE,{timestamp}\r\n"
    value += f"436,SALE,{timestamp}\r\n"
    value += f"437,PURCHASE,{timestamp}\r\n"
    return value


@mock_s3
def test_returns_1970_1_1_when_no_objects_in_bucket():
    # create s3 bucket
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket="test")
    output = get_last_ingestion_timestamp("test")
    assert output == dt(1970, 1, 1)


@mock_s3
def test_gets_most_recent_timestamp_from_all_file_names():
    # create s3 bucket
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket="test")
    timestamp_1 = dt(1970, 1, 1).isoformat()
    timestamp_2 = dt(1980, 1, 2).isoformat()
    timestamp_3 = dt(1980, 3, 1).isoformat()
    timestamp_4 = dt(1980, 1, 1).isoformat()
    # put fake files in bucket
    s3_client.put_object(
        Body="",
        Bucket="test",
        Key=f'{timestamp_1}/test-table.csv',
        ContentType='application/text',
    )
    s3_client.put_object(
        Body="",
        Bucket="test",
        Key=f'{timestamp_2}/test-table.csv',
        ContentType='application/text',
    )
    s3_client.put_object(
        Body="",
        Bucket="test",
        Key=f'{timestamp_3}/test-table.csv',
        ContentType='application/text',
    )
    s3_client.put_object(
        Body="",
        Bucket="test",
        Key=f'{timestamp_4}/test-table.csv',
        ContentType='application/text',
    )
    output = get_last_ingestion_timestamp("test")
    assert output == dt.fromisoformat(timestamp_3)


@mock_s3
def test_raises_NonTimeStampedCSVError_when_file_without_timestamp_exists():
    # create s3 bucket
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket="test")
    s3_client.put_object(
        Body="",
        Bucket="test",
        Key='test_table.csv',
        ContentType='application/text',
    )
    with pytest.raises(NonTimestampedCSVError):
        get_last_ingestion_timestamp("test")


@patch('src.lambda_ingestion.ingestion_lambda.connect')
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
    res = extract_table_to_csv(["test_table"], dt(1970, 1, 1))
    expected = "transaction_id,transaction_type,last_updated\r\n"
    expected += f"434,SALE,{current_timestamp}\r\n"
    expected += f"435,PURCHASE,{current_timestamp}\r\n"
    expected += f"436,SALE,{current_timestamp}\r\n"
    expected += f"437,PURCHASE,{current_timestamp}\r\n"
    assert list(res.keys()) == ["test_table"]
    assert res["test_table"] == expected


@mock_s3
@patch('src.lambda_ingestion.ingestion_lambda.connect')
def test_return_empty_dict_when_no_return_from_SQL(mock_connection):
    mock_db = Mock()
    mock_connection.return_value.__enter__.return_value = mock_db
    new_timestamp = dt.now()
    mock_db.run.return_value = []
    mock_db.columns = [{'name': "transaction_id"}, {
        'name': "transaction_type"}, {'name': "last_updated"}]
    res = extract_table_to_csv("test_table", new_timestamp)
    assert res == {}


@mock_s3
def test_adds_new_csv_to_s3_when_passed_new_data():
    fake_csv = generate_csv_string()
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket='test')
    csv_to_s3('test', {"fake": fake_csv})
    res = s3_client.list_objects_v2(Bucket="test")
    object_list = [obj['Key'] for obj in res['Contents']]
    new_csv = "/fake.csv"
    present = False
    for item in object_list:
        if new_csv in item:
            present = True
            new_csv = item
    assert present
    new_csv_contents = s3_client.get_object(Bucket="test", Key=new_csv)['Body']
    new_csv_contents = new_csv_contents.read().decode()
    assert new_csv_contents == fake_csv


@mock_s3
def test_does_not_add_new_csv_when_not_passed_new_data():
    old_timestamp = dt(1970, 1, 1).isoformat()
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket='test')
    s3_client.put_object(
        Body=generate_csv_string(),
        Bucket='test',
        Key=f'{old_timestamp}/fake.csv',
        ContentType='application/text',
    )
    csv_to_s3('test', {})
    res = s3_client.list_objects_v2(Bucket="test")
    object_list = [obj['Key'] for obj in res.get('Contents')]
    assert object_list == [f'{old_timestamp}/fake.csv']
