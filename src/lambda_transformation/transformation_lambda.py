import boto3
import logging
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import csv
import time


def get_ingestion_bucket_name():
    name = 'terrific-totes-ingestion-bucket'
    name += '20230725102602583400000001'
    return name


def get_processed_bucket_name():
    return 'terrific-totes-processed-bucket20230725102602584600000002'


def get_available_buckets(s3):
    """
        Will return a list of our available buckets
    """
    buckets = s3.list_buckets()['Buckets']
    available_buckets = [res['Name']for res in buckets]
    return available_buckets


def is_ingestion_bucket_available(s3):
    """
        Will return a boolean indicating that our
        CSV containing bucket is available
    """
    return get_ingestion_bucket_name() in get_available_buckets(s3)


def is_processed_bucket_available(s3):
    """
        Will return a boolean indicating that our
        Parquet containing bucket is available
    """
    return get_processed_bucket_name() in get_available_buckets(s3)


def get_csv_names(s3):
    contents = s3.list_objects_v2(
        Bucket=get_ingestion_bucket_name(),
    )['Contents']
    names = [object['Key'] for object in contents]
    names = [name for name in names if name.endswith('.csv')]
    return names


def response_to_data_frame(response):
    """
        This funtion will convert get_object response for a CSV file
        into a Pandas DataFrame
    """

    body_reader = response['Body']
    body = body_reader.read().decode('utf-8').splitlines()
    csv_reader = csv.DictReader(body)
    rows = []

    for data in csv_reader:
        rows.append(data)

    return pd.DataFrame.from_dict(rows)


def process_to_parquet(thread_index, csv_name):
    """
        This function will process the csv into a parquet format
        and put the parquet into our processed data bucket.

        This function is asynchronous and so out-of-order results will happen
    """

    logging.info(f'CSV to Parquet conversion begun for {csv_name}')
    spreadsheet_name = csv_name[0:-4] + '.parquet'
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=get_ingestion_bucket_name(), Key=csv_name)

    data_frame = response_to_data_frame(response)
    parquet_data = data_frame.to_parquet(engine='pyarrow')

    """
        Put our parquet into the processed bucket
    """
    s3.put_object(
        Bucket=get_processed_bucket_name(),
        Key=spreadsheet_name,
        Body=parquet_data
    )
    logging.info(f'CSV to Parquet conversion finished for {csv_name}')


def transformation_lambda_handler(event, context):
    s3 = boto3.client('s3', region_name='eu-west-2')

    """
        Ensure that our buckets exists
    """

    if not is_processed_bucket_available(s3):
        logging.critical(
            f'''
            error: the processed data bucket'
            {get_processed_bucket_name()}
            does not exist!
            ''')
        return

    if not is_ingestion_bucket_available(s3):
        logging.critical(
            f'''
            error: the ingestion bucket '
            {get_ingestion_bucket_name()}
            ' does not exist!
            ''')
        return

    """
        The buckets exist, so let's
        grab our processed data:
    """
    csv_names = get_csv_names(s3)
    max_workers = len(csv_names)

    times = []
    times.append(time.perf_counter())

    """
        Execute the data processing in parralel
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_to_parquet, range(max_workers), csv_names)

    times.append(time.perf_counter())
    logging.info('Work has finished')
    logging.debug('Seconds:', times[1] - times[0])
