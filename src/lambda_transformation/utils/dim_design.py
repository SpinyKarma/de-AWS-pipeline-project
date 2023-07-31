import boto3
import pandas as pd
from datetime import datetime as dt

current_timestamp = dt.now()

s3 = boto3.client('s3')
def create_design_parquet(key, ingestion_bucket, parquet_bucket):
    response = s3.get_object(Bucket=ingestion_bucket, Key=key)
    df = pd.read_csv(response['Body'])
    df = df.drop(columns=['last_updated', 'created_at'])
    new_design = df.to_parquet(engine='pyarrow')
    key_parts = key.split('/')
    timestamp = '/'.join(key_parts[:-1])
    s3.put_object(Bucket=parquet_bucket, Key=f'{timestamp}/dim_design.parquet', Body=new_design)



    
    
    
    




