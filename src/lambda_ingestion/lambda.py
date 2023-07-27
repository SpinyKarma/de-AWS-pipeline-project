import boto3
import ingestion as i
import logging


def ingestion_lambda_handler(event, context):
    '''
    '''
    try:
        table_names = [
            'staff',
            'counterparty',
            'sales_order',
            'address',
            'payment',
            'purchase_order',
            'payment_type',
            'transaction',
        ]
        buckey_name = i.get_ingestion_bucket_name()
        i.postgres_to_csv(buckey_name, table_names)
    except i.TableIngestionError as error:
        logging.error(f'Table Ingestion Error: {error}')
    except i.InvalidCredentialsError as error:
        logging.error(f'Invalid Credentials Error: {error}')
    except Exception as e:
        logging.error(f'An unexpected error occured: {e}')
