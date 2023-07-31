import boto3
import pandas as pd
import ccy

s3 = boto3.client('s3')
def create_currency_parquet(key, ingestion_bucket, parquet_bucket):
    response = s3.get_object(Bucket=ingestion_bucket, Key=key)
    df = pd.read_csv(response['Body'])
    df = df.drop(columns=['last_updated','created_at'])
    df['currency_name'] = df['currency_code'].apply(
        lambda x: ccy.currency(x).name)
    new_currency = df.to_parquet()
    key_parts = key.split('/')
    timestamp = '/'.join(key_parts[:-1])
    s3.put_object(Bucket=parquet_bucket, Key=f'{timestamp}/dim_currency.parquet', Body=new_currency)
