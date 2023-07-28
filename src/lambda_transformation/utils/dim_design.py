import boto3
import pandas as pd

s3 = boto3.client('s3')
def create_design_parquet(key, ingestion_bucket, parquet_bucket):
    response = s3.get_object(Bucket=ingestion_bucket, Key=key)
    df = pd.read_csv(response['Body'])
    df = pd.drop(columns=['last_updated', 'created_at'])
    new_design = df.to_parquet()
    s3.put_object(Bucket=parquet_bucket, Key='dim_design.parquet', Body=new_design)



