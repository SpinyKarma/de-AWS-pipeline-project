from datetime import datetime as dt
import boto3
from moto import mock_s3
from src.lambda_ingestion.ingestion import (
    get_ingestion_timestamps,
    # extract_table_to_csv
)
# from unittest.mock import Mock
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
    list = get_ingestion_timestamps(s3_client, bucket_name, ['test_table'])
    assert list['test_table'].isoformat() == current_timestamp

# Mocks don't support context manager so don't know how to test
# def test_extract_table_to_csv_concats_query_result_to_csv():
#     mock_connection = Mock()
#     mock_db = Mock()
#     mock_connection.return_value = mock_db
#     mock_db.run.return_value = [
#         ["transaction_id", "transaction_type", "last_updated"],
#         ["434", "SALE", current_timestamp],
#         ["435", "PURCHASE", current_timestamp],
#         ["436", "SALE", current_timestamp],
#         ["437", "PURCHASE", current_timestamp],
#     ]
#     res = extract_table_to_csv("test_table", dt(1970, 1, 1), mock_connection)
#     expected = "transaction_id, transaction_type, last_updated\r\n"
#     expected += f"434, SALE, {current_timestamp}\r\n"
#     expected += f"435, PURCHASE, {current_timestamp}\r\n"
#     expected += f"436, SALE, {current_timestamp}\r\n"
#     expected += f"437, PURCHASE, {current_timestamp}\r\n"
#     assert res == expected
