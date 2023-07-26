from datetime import datetime as dt
import boto3
from moto import mock_s3 #, ingest
from src.ingestion import get_last_ingestion_timestamps, set_last_ingestion_timestamps
# function returning a sample csv string for testing


def generate_csv_string():
    value = "transaction_id,transaction_type\n"
    value += "434,SALE,435,PURCHASE,436,SALE,437,PURCHASE\n"
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
    current_timestamp = dt.now().isoformat()
    s3_client.put_object(
        Body=csv_data,
        Bucket=bucket_name,
        Key='test_table.csv',
        ContentType='application/text',
        Metadata={'LastIngestionTimestamp': current_timestamp}
    )
    # retrieve last ingestion timestamps again
    updated_last_ingestion_timestamps = get_last_ingestion_timestamps(
        s3_client=s3_client, Bucket=bucket_name)
    # check if the timestamp
    # for the test table was updated with new current timestamp
    assert 'test_table' in updated_last_ingestion_timestamps
    assert isinstance(updated_last_ingestion_timestamps['test_table'], dt)

@mock_s3
def test_timestamp_set():
    bucket_name = "test_ingestion_bucket"
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_object(
        Body='csv_data',
        Bucket=bucket_name,
        Key='test_table.csv',
        ContentType='application/text',
    )
    set_last_ingestion_timestamps(get_last_ingestion_timestamps(s3_client=s3_client, Bucket=bucket_name), s3_client=s3_client, Bucket=bucket_name)
    response=s3_client.get_object(Bucket=bucket_name, Key='test_table.csv')
    metadata=response.get('Metadata')
    #assert metadata == False
    assert metadata['lastingestiontimestamp'] == '2023-07-26T12:03:34.650508'
    