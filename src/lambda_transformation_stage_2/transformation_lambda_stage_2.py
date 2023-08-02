import boto3
import logging
# from concurrent.futures import ThreadPoolExecutor
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pyarrow import fs
# from pprint import pprint


def transformation_lambda_handler_stage_2(event, context):
    '''Stage 2 of the transformation process.

    Takes the warehouse schema csvs that have temporarily been saved to the
    processed bucket, finishes the transform stage by converting them to
    parquets, writing them to the same prefix in the same bucket and then
    deleting the csv.
    '''
    s3 = fs.S3FileSystem(region='eu-west-2')

    try:
        parquet_bucket = get_parquet_bucket_name()
    except MissingBucketError as err:
        logging.critical(f"Missing Bucket Error : {err.message}")
        raise err

    # List all contents in a bucket, recursively

    bucket_contents = s3.get_file_info(fs.FileSelector(
        parquet_bucket(), recursive=True))

    # Filter out all entries that are not files and do not end with .csv

    bucket_csvs = [
        file for file in bucket_contents
        if file.is_file and file.extension == "csv"]

    # For each csv file:
    for file in bucket_csvs:
        # Open a path to s3 to read the file
        fh = s3.open_input_stream(file.path)
        # Read the csv contents
        reader = pv.open_csv(fh)
        # Convert to parquet and write to the same bucket and folder,
        # changing .csv to .parquet
        pq.ParquetWriter(
            f"s3://{file.path[:-4]}.parquet",
            schema=reader.schema
        )
        # Delete the interim csv now that it has been fully processed
        s3.delete_file(file.path)


class MissingBucketError(Exception):
    '''An error for when the prerequisite buckets do not exist.'''

    def __init__(self, source="", message=""):
        self.source = source
        self.message = message


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


# if __name__ == "__main__":
#     s3 = fs.S3FileSystem(region='eu-west-2')

#     bucket_contents = s3.get_file_info(fs.FileSelector(
#         get_parquet_bucket_name(), recursive=True))

#     bucket_csvs = [
#         file for file in bucket_contents if
#         file.is_file and file.extension == "csv"]
#     for file in bucket_csvs:
#         fh = s3.open_input_stream(file.path)
#         reader = pv.open_csv(fh)
#         pq.ParquetWriter(
#             f"s3://{file.path[:-4]}.parquet", schema=reader.schema)
#         s3.delete_file(file.path)
