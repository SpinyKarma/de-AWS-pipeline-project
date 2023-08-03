import boto3
import logging
# from concurrent.futures import ThreadPoolExecutor
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pyarrow import fs
from pprint import pprint


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

    bucket_contents = s3.get_file_info(fs.FileSelector(
        parquet_bucket, recursive=False))

    folder_names = [file.base_name for file in bucket_contents]

    # ignore dim date parquet
    if folder_names[-1] == "dim_date.parquet":
        folder_names = folder_names[:-1]
    # process dim date csv
    elif folder_names[-1] == "dim_date.csv":
        file = bucket_contents[-1]
        process_file_to_parquet(s3, file)
        folder_names = folder_names[:-1]

    for folder in folder_names:
        folder_contents = s3.get_file_info(fs.FileSelector(
            parquet_bucket+"/"+folder, recursive=False))
        bucket_csvs = [
            file for file in folder_contents
            if file.is_file and file.extension == "csv"]
        for file in bucket_csvs:
            process_file_to_parquet(s3, file)


class MissingBucketError(Exception):
    '''An error for when the prerequisite buckets do not exist.'''

    def __init__(self, source="", message=""):
        self.source = source
        self.message = message


def process_file_to_parquet(s3, file):
    fh = s3.open_input_stream(file.path)
    # Read the csv contents
    reader = pv.open_csv(fh)
    # Convert to parquet and write to the same bucket and folder,
    # changing .csv to .parquet
    writer = pq.ParquetWriter(
        f"s3://{file.path[:-4]}.parquet", schema=reader.schema, use_dictionary=False
    )
    writer.write_table(reader.read_all())

    writer.close()
    reader.close()
    # # Delete the interim csv now that it has been fully processed
    s3.delete_file(file.path)


def read_parquet(s3, file):
    '''Takes a pyarrow FileInfo object that points to a parquet file on s3 and
       returns the parquet file contents as a list of lists, one for each row
       including column names.
       '''
    fh = s3.open_input_file(file.path)
    dataframe = pq.read_table(fh).to_pandas()
    str_table = dataframe.iloc[:, 1:].to_string(index=False)
    list_table = [row.split() for row in str_table.split("\n")]
    return list_table


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
#     # transformation_lambda_handler_stage_2("banana", "apple")
#     s3 = fs.S3FileSystem(region='eu-west-2')
#     parquet_bucket = get_parquet_bucket_name()
#     bucket_contents = s3.get_file_info(fs.FileSelector(
#         parquet_bucket, recursive=False))
#     date_csv_file = [
#         file for file in bucket_contents if file.extension == "parquet"][0]
#     read_parquet(s3, date_csv_file)
#     bucket_csvs = [
#         file for file in bucket_contents if
#         file.is_file and file.extension == "csv"]
#     for file in bucket_csvs:
#         fh = s3.open_input_stream(file.path)
#         reader = pv.open_csv(fh)
#         pq.ParquetWriter(
#             f"s3://{file.path[:-4]}.parquet", schema=reader.schema)
#         s3.delete_file(file.path)
