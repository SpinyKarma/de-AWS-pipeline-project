import boto3
import logging
# from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import csv
# import time
# from pprint import pprint
from utils.dim_counter_party import counter_party_address_to_dim_counterparty as cpatdc
from utils.dim_currency import currency_to_dim_currency as ctdc
from utils.dim_date import generate_dim_date as gdd
from utils.dim_design import design_to_dim_design as dtdd
from utils.dim_fact_sales_order import sales_order_to_fact_sales_order as sotfso
from utils.dim_location import address_to_dim_location as atdl
from utils.dim_staff import create_dim_staff_csv as sdtds


def transformation_lambda_handler(event, context):
    s3 = boto3.client('s3', region_name='eu-west-2')

    # will need updating if project is expended to payment and/or
    # transaction schemas
    table_names = [
        'address.csv',
        'counterparty.csv',
        'currency.csv',
        'department.csv',
        'design.csv',
        'sales_order.csv',
        'staff.csv',
    ]
    """
        Ensure that our buckets exists
    """
    try:
        raw_bucket = get_ingestion_bucket_name()
        parquet_bucket = get_parquet_bucket_name()
    except MissingBucketError as err:
        logging.critical(f"Missing Bucket Error : {err.message}")
        raise err

    if not dim_date_exists(s3, parquet_bucket):
        dim_date = gdd()
        parquet_name, parquet_body = process_to_parquet(dim_date)
        s3.put_object(Bucket = parquet_bucket, Key = parquet_name, Body=parquet_body)

    """
        The buckets exist, so let's
        grab our processed data:
    """
    # grab all timestamps that exist in ingestion bucket but not in parquet
    raw_timestamps = list_timestamps(s3, raw_bucket)
    parquet_timestamps = list_timestamps(s3, parquet_bucket)

    # if these are equal then all raw data has already been processed,
    # so do nothing, else:
    if raw_timestamps != parquet_timestamps:
        # lists are sorted oldest to newest so exclude len(parquet)
        # num of items from top of raw
        prefixes_to_process = raw_timestamps[len(parquet_bucket):]
        keys_to_process = [get_keys_by_prefix(
            s3, raw_bucket, timestamp) for timestamp in prefixes_to_process]
        processed_csvs = []
        for csv_group in keys_to_process:
            output_block = apply_transformations_to_group(s3, csv_group, table_names)
            processed_csvs.extend([output_block[key] for key in list(output_block)])
        for csv_dict in processed_csvs:
            parquet_name, parquet_body = process_to_parquet(csv_dict)
            s3.put_object(Bucket = parquet_bucket, Key = parquet_name, Body=parquet_body)


        # for timestamp in prefixes_to_process:
        #     keys_to_process.extend(
        #         get_keys_by_prefix(
        #             s3, raw_bucket, timestamp
        #         )
        #     )
        # for key in keys_to_process:

    #     csv_names = get_csv_names(s3)
    #     max_workers = len(csv_names)

    #     times = []
    #     times.append(time.perf_counter())

    """
        Execute the data processing in parralel
    """
    # with ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     executor.map(process_to_parquet, range(max_workers), csv_names)

    # print('Work has finished')


class MissingBucketError(Exception):
    def __init__(self, source="", message=""):
        self.source = source
        self.message = message

def dim_date_exists(s3, bucket_name):
    res = s3.list_objects_v2(Bucket=bucket_name).get('Contents')
    if not res:
        return False
    keys = [obj['Key'] for obj in res]
    return "dim_date.parquet" in keys


def get_ingestion_bucket_name():
    '''Gets the name of the bucket of raw data using the prefix.'''
    prefix = 'terrific-totes-ingestion-bucket'
    buckets = boto3.client("s3").list_buckets().get("Buckets")
    for bucket in buckets:
        if prefix in bucket["Name"]:
            name = bucket["Name"]
            return name
    raise MissingBucketError(
        "ingestion_bucket",
        "The raw data bucket was not found"
    )


def get_parquet_bucket_name():
    '''Gets the name of the bucket of processed data using the prefix.'''
    prefix = 'terrific-totes-processed-bucket'
    buckets = boto3.client("s3").list_buckets().get("Buckets")
    for bucket in buckets:
        if prefix in bucket["Name"]:
            name = bucket["Name"]
            return name
    raise MissingBucketError(
        "parquet_bucket",
        "The processed data bucket was not found"
    )


def list_timestamps(s3, bucket_name):
    timestamps = s3.list_objects_v2(
        Bucket=bucket_name, Prefix="", Delimiter="/"
    ).get('CommonPrefixes', [])
    timestamps = [item['Prefix'] for item in timestamps]
    return timestamps


def get_keys_by_prefix(s3, bucket_name, prefix):
    csvs = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=prefix, Delimiter="/"
    ).get('Contents', [])
    keys = [obj['Key'] for obj in csvs]
    return keys


def s3_obj_to_dict(s3, bucket_name, key):
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    body = response_to_data_frame(obj)
    timestamp = key.split("/")[0]
    formatted_obj = {
        'Key': key,
        'Body': body,
        'Timestamp': timestamp
    }
    return formatted_obj


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

def apply_transformations_to_group(s3, csv_group, table_names):
    output_block = {}
    process_block = {key.split("/")[1][:-4]: s3_obj_to_dict(s3, raw_bucket, key)
                    for key in csv_group if key.split("/")[1] in table_names}
    if process_block.get("address"):
        output_block["dim_location"] = atdl(process_block['address'])
        if process_block.get("counterparty"):
            output_block["dim_counterparty"] = cpatdc(process_block['counterparty'], process_block['address'])
    if process_block.get('currency'):
        output_block["dim_currency"] = ctdc(process_block['currency'])
    if process_block.get('department') and process_block.get('staff'):
        output_block["dim_staff"] = sdtds(process_block['staff'], process_block['department'])
    if process_block.get('design'):
        output_block["dim_design"] = dtdd(process_block['design'])
    if process_block.get('sales_order'):
        output_block["fact_sales_order"] = sotfso(process_block['sales_order'])

    return output_block

def process_to_parquet(csv_dict):
    """
        This function will process the csv into a parquet format
        and put the parquet into our processed data bucket.

        This function is asynchronous and so out-of-order results will happen
    """
    # for csv_name in names:
    # if 'currency' in csv_name:
    # create_currency_parquet(csv_name, ingestion_bucket, parquet_bucket)
    # elif 'design' in csv_name:
    # create_design_parquet(csv_name, ingestion_bucket, parquet_bucket)

    parquet_name = csv_dict['Key'][:-4] + '.parquet'
    parquet_body = csv_dict['Body'].to_parquet(engine='pyarrow')
    return parquet_name, parquet_body
    # logging.info(f'CSV to Parquet conversion finished for {csv_name}')


# if __name__ == "__main__":
#     s3 = boto3.client('s3', region_name='eu-west-2')
#     table_names = [
#         'address.csv',
#         'counterparty.csv',
#         'currency.csv',
#         'department.csv',
#         'design.csv',
#         'sales_order.csv',
#         'staff.csv',
#     ]
#     raw_bucket = get_ingestion_bucket_name()
    # li = list_timestamps(
    #     s3, get_ingestion_bucket_name())
    # # print(li, "\n")
    # li = get_keys_by_prefix(
    #     s3, get_ingestion_bucket_name(), '2023-07-31T16:00:44.942687/')
    # # print(li, "\n")
    # li = s3_obj_to_dict(s3, get_ingestion_bucket_name(), li[0])
    # # print(li, "\n")
    # prefixes_to_process = [
    #     '2023-07-31T12:24:11.422525/', '2023-07-31T16:00:44.942687/']
    # keys_to_process = [get_keys_by_prefix(
    #     s3, get_ingestion_bucket_name(), timestamp) for timestamp in prefixes_to_process]
    # # pprint(list(keys_to_process))
    # processed_timestamps = []
    # for csv_group in keys_to_process:
    #     timestamp = csv_group[0].split("/")[0]
    #     output_block = apply_transformations_to_group(s3, csv_group, table_names)
    #     if output_block != {}:
    #         processed_timestamps.extend([output_block[key] for key in list(output_block)])
    # pprint(processed_timestamps)