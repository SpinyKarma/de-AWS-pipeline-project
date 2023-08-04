import boto3
import logging
# from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import csv
# import time
# from pprint import pprint
from utils.dim_counter_party import (
    counter_party_address_to_dim_counterparty as cpatdc
)
from utils.dim_currency import currency_to_dim_currency as ctdc
from utils.dim_date import generate_dim_date as gdd
from utils.dim_design import design_to_dim_design as dtdd
from utils.dim_fact_sales_order import (
    sales_order_to_fact_sales_order as sotfso
)
from utils.dim_location import address_to_dim_location as atdl
from utils.dim_staff import staff_department_to_dim_staff as sdtds


def transformation_lambda_handler(event, context):
    s3 = boto3.client('s3', region_name='eu-west-2')

    # List of all tables names that are needed for transformation
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

    # Ensure that our buckets exists
    try:
        raw_bucket = get_ingestion_bucket_name()
        parquet_bucket = get_parquet_bucket_name()
    except MissingBucketError as err:
        logging.critical(f"Missing Bucket Error : {err.message}")
        raise err

    # Create the dim date parquet file if it does not exist,
    # this happens here as dim date does not need updating once created
    if not dim_date_exists(s3, parquet_bucket):
        dim_date = gdd()
        csv_name, csv_body = back_to_csv(dim_date)
        s3.put_object(Bucket=parquet_bucket,
                      Key=csv_name, Body=csv_body)

    # Grab all timestamp prefixes from each bucket
    raw_timestamps = list_timestamps(s3, raw_bucket)
    parquet_timestamps = list_timestamps(s3, parquet_bucket)

    # Grab all timestamps that exist in raw but not parquet
    prefixes_to_process = [
        item for item in raw_timestamps if item not in parquet_timestamps]

    # If this is zero then all raw data has already been processed,
    # so do nothing, else:
    if len(prefixes_to_process) > 0:
        # A list of sublists, one for each timestamp, each containing all the
        # keys under that timestamp
        keys_to_process = [get_keys_by_prefix(
            s3, raw_bucket, timestamp) for timestamp in prefixes_to_process]

        processed_csvs = []
        # For each timestamp group in keys_to_process:
        for csv_group in keys_to_process:
            # Apply the relevant transformations to each group based on the
            # table name contained in the key
            output_block = apply_transformations_to_group(
                s3, csv_group, table_names)

            # Append all output dicts from output_block to processed_csvs
            processed_csvs.extend([output_block[key]
                                  for key in list(output_block)])
        # For each csv that has been through the transformation process:
        for csv_dict in processed_csvs:
            # Convert the body and key to parquet format
            csv_name, csv_body = back_to_csv(csv_dict)
            # Save the resulting parquet file to the correct s3 bucket
            s3.put_object(Bucket=parquet_bucket,
                          Key=csv_name, Body=csv_body)

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
    '''An error for when the prerequisite buckets do not exist.'''

    def __init__(self, source="", message=""):
        self.source = source
        self.message = message


def dim_date_exists(s3, bucket_name):
    '''Bool for whether dim_date exists in the passed bucket.'''
    res = s3.list_objects_v2(Bucket=bucket_name).get('Contents')
    if not res:
        return False
    keys = [obj['Key'] for obj in res]
    return "dim_date.parquet" in keys or "dim_date.csv" in keys


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
    '''Lists out all prefixes that exist in the passed bucket.'''
    timestamps = s3.list_objects_v2(
        Bucket=bucket_name, Prefix="", Delimiter="/"
    ).get('CommonPrefixes', [])
    timestamps = [item['Prefix'] for item in timestamps]
    return timestamps


def get_keys_by_prefix(s3, bucket_name, prefix):
    '''Gets keys from the passed bucket that have the passed prefix.'''
    csvs = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=prefix, Delimiter="/"
    ).get('Contents', [])
    keys = [obj['Key'] for obj in csvs]
    return keys


def s3_obj_to_dict(s3, bucket_name, key):
    '''Uses the passed key and bucket name to grab an object and convert it to
       a useable format.
    '''
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
    """This funtion will convert get_object response for a CSV file
       into a Pandas DataFrame.

        Args:
            response: The response from a boto3 s3 get_object function

        Returns:
            dataframe: The body of the response converted to a pandas DataFrame
    """

    body_reader = response['Body']
    body = body_reader.read().decode('utf-8').splitlines()
    csv_reader = csv.DictReader(body)
    rows = []

    for data in csv_reader:
        rows.append(data)
    dataframe = pd.DataFrame.from_dict(rows)
    return dataframe


def apply_transformations_to_group(s3, csv_group, table_names):
    '''Applies the correct transformation to each csv dict in the passed group
       and outputs them on a new dict.

        Args:
            s3: A boto3 s3 client

            csv_group: A list of csv dicts that each share the same timestamp.

            table_names: A list of table names that need transformations
            applied to them.

        Returns:
            output_block: A dict of transformed csv_dicts, each on the key of
            the appropriate transformed table name.
    '''
    output_block = {}
    process_block = {key.split("/")[1][:-4]: s3_obj_to_dict(
        s3,
        get_ingestion_bucket_name(),
        key
    )
        for key in csv_group if key.split("/")[1] in table_names}
    if process_block.get("address"):
        output_block["dim_location"] = atdl(process_block['address'])
        if process_block.get("counterparty"):
            output_block["dim_counterparty"] = cpatdc(
                process_block['counterparty'], process_block['address'])
    if process_block.get('currency'):
        output_block["dim_currency"] = ctdc(process_block['currency'])
    if process_block.get('department') and process_block.get('staff'):
        output_block["dim_staff"] = sdtds(
            process_block['staff'], process_block['department'])
    if process_block.get('design'):
        output_block["dim_design"] = dtdd(process_block['design'])
    if process_block.get('sales_order'):
        output_block["fact_sales_order"] = sotfso(process_block['sales_order'])

    return output_block


def back_to_csv(csv_dict):
    """This function will process a csv dict into csv string body."""

    csv_body = csv_dict['Body'].to_csv()
    return csv_dict['Key'], csv_body


# if __name__ == "__main__":
#     s3 = boto3.client('s3', region_name='eu-west-2')
#     dim_date = gdd()
#     csv_name, csv_body = back_to_csv(dim_date)
#     s3.put_object(Bucket=get_parquet_bucket_name(),
#                   Key=csv_name, Body=csv_body)
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
#     li = list_timestamps(
#         s3, get_ingestion_bucket_name())
#     # print(li, "\n")
#     li = get_keys_by_prefix(
#         s3, get_ingestion_bucket_name(), '2023-07-31T16:00:44.942687/')
#     # print(li, "\n")
#     li = s3_obj_to_dict(s3, get_ingestion_bucket_name(), li[0])
#     # print(li, "\n")
#     prefixes_to_process = [
#         '2023-07-31T12:24:11.422525/', '2023-07-31T16:00:44.942687/']
#     keys_to_process = [get_keys_by_prefix(
#         s3,
#         get_ingestion_bucket_name(),
#         timestamp
#     ) for timestamp in prefixes_to_process]
#     # pprint(list(keys_to_process))
#     processed_timestamps = []
#     for csv_group in keys_to_process:
#         timestamp = csv_group[0].split("/")[0]
#         output_block = apply_transformations_to_group(s3,
#                                                       csv_group,
#                                                       table_names
#                                                       )
#         if output_block != {}:
#             processed_timestamps.extend(
#                 [output_block[key] for key in list(output_block)]
#             )
#     pprint(processed_timestamps)
