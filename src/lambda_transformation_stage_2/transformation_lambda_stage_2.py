import boto3
import logging
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pyarrow import fs

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def transformation_lambda_handler_stage_2(event, context):
    """ Stage 2 of the transformation process.

        Takes the warehouse schema csvs that have temporarily been saved to the
        processed bucket, finishes the transform stage by converting them to
        parquets, writing them to the same prefix in the same bucket and then
        deleting the csvs.
    """
    s3 = fs.S3FileSystem(region='eu-west-2')

    try:
        parquet_bucket = get_parquet_bucket_name()
        logger.info(f"Processed bucket established as {parquet_bucket}.")
    except MissingBucketError as err:
        logger.critical(f"Missing Bucket Error : {err.message}")
        raise err

    bucket_contents = s3.get_file_info(fs.FileSelector(
        parquet_bucket, recursive=False))

    folder_names = [file.base_name for file in bucket_contents]

    # Ignore dim_date.parquet.
    if folder_names[-1] == "dim_date.parquet":
        folder_names = folder_names[:-1]

    # Process dim_date.csv to dim_date.parquet, then ignore.
    elif folder_names[-1] == "dim_date.csv":
        file = bucket_contents[-1]
        process_file_to_parquet(s3, file)
        folder_names = folder_names[:-1]

    # Ignore cache.txt.
    if folder_names[-1] == "cache.txt":
        folder_names = folder_names[:-1]

    files_to_convert = False

    # For each folder in the processed bucket:
    for folder in folder_names:

        # Get a FileInfo object for each itme in the folder.
        folder_contents = s3.get_file_info(fs.FileSelector(
            parquet_bucket+"/"+folder, recursive=False))

        # Filter out any files that are not csvs.
        folder_csvs = [
            file for file in folder_contents
            if file.is_file and file.extension == "csv"]

        if folder_csvs != []:
            files_to_convert = True
        for file in folder_csvs:
            process_file_to_parquet(s3, file)
    if not files_to_convert:
        logger.info("No csvs to convert to to parquet.")


class MissingBucketError(Exception):
    """ An error for when the prerequisite buckets do not exist."""

    def __init__(self, source="", message=""):
        self.source = source
        self.message = message


def process_file_to_parquet(s3, file):
    fh = s3.open_input_stream(file.path)

    # Read the csv contents.
    reader = pv.open_csv(fh)

    # Convert to parquet and write to the same bucket and folder,
    # changing .csv to .parquet.
    logger.info(f"Writing to {file.base_name[:-4]}.parquet.")
    writer = pq.ParquetWriter(
        f"s3://{file.path[:-4]}.parquet",
        schema=reader.schema,
        use_dictionary=False
    )
    writer.write_table(reader.read_all())

    writer.close()
    reader.close()

    # Delete the interim csv now that it has been fully processed.
    logger.info(f"Deleting {file.base_name}.")
    s3.delete_file(file.path)


def read_parquet(s3, file):
    """ Takes a pyarrow FileInfo object that points to a parquet file on s3 and
        returns the parquet file contents as a list of lists, one for each row
        including column names.
    """
    fh = s3.open_input_file(file.path)
    dataframe = pq.read_table(fh).to_pandas()
    str_table = dataframe.iloc[:, 1:].to_string(index=False)
    list_table = [row.split() for row in str_table.split("\n")]
    return list_table


def get_parquet_bucket_name():
    """ Gets the name of the bucket of processed data using the prefix."""
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
