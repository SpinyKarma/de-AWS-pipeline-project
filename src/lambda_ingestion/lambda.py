import boto3
import ingestion as i
import logging

def ingestion_lambda_handler(event,context):
    try:
        s3=boto3.client('s3')
        i.ingest(s3)
    except i.TableIngestionError as error:
        logging.error(f'Table Ingestion Error: {error}')
    except i.InvalidCredentialsError as error:
        logging.error(f'Invalid Credentials Error: {error}')
    except Exception as e:
        logging.error(f'An unexpected error occured: {e}')

