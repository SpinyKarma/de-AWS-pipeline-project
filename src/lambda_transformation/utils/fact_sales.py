import boto3
import pandas as pd 
s3=boto3.client('s3')
def generate_fact_sales_order(key,ingestion_bucket,parquet_bucket):
    response=s3.get_object(Bucket=ingestion_bucket,Key=key)
    df=pd.read_csv(response['Body'])
   