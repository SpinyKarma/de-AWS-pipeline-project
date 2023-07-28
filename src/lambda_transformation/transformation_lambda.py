import boto3


# For demonstration purposes
import time
import random
# --

import logging
from concurrent.futures import ThreadPoolExecutor


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
    return ingestion.get_ingestion_bucket_name() in get_available_buckets(s3)


def is_processed_bucket_available(s3):
    """
        Will return a boolean indicating that our
        Parquet containing bucket is available
    """
    return get_processed_bucket_name() in get_available_buckets(s3)


def get_csv_names(s3):
    contents = s3.list_objects_v2(
        Bucket=ingestion.get_ingestion_bucket_name(),
    )['Contents']
    names = [object['Key'] for object in contents]
    names = [name for name in names if name.endswith('.csv')]
    return names


def process_to_parquet(thread_index, csv_name):
    """
        This function will process the csv into a parquet
        and put the parquet into our processed data bucket.
        This function is asynchronous and so out-of-order results
        may happen.
    """
    print(f'I am processing thread: {csv_name}')

    # For demonstration purposes: pretend that work is in progress
    time.sleep(random.uniform(0, 5))
    print(f'{csv_name} has completed processing')


def transformation_lambda_handler(event, context):
    s3 = boto3.client('s3', region_name='eu-west-2')

    """
        Ensure that our ingestion bucket exists
    """
    if not is_ingestion_bucket_available(s3):
        logging.error(
            f'''
            error: the ingestion bucket '
            {get_ingestion_bucket_name()}
            ' does not exist!
            ''')
        return

    if not is_processed_bucket_available(s3):
        logging.error(
            f'''
            error: the processed data bucket'
            {get_processed_bucket_name()}
            does not exist!
            ''')
        return

    """
        Grab our processed data:
    """
    csv_names = get_csv_names(s3)

    """
        TODO: Use threading to perform parquet formatting
    """

    print(csv_names)

    max_workers = 5
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_to_parquet, range(max_workers), csv_names)

    print('Work has finished')
