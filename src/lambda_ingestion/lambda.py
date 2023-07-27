import boto3
import ingestion 

def lambda_handler(event,context):
    s3=boto3.client('s3')
    ingestion.ingest(s3)
    

    
