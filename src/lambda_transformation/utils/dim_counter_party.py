import boto3
import pandas as pd
from src.table_utils.get_tables import (get_most_recent_table)
from pprint import pprint
from src.lambda_ingestion.ingestion_lambda import (get_last_ingestion_timestamp)
from datetime import datetime as dt
current_timestamp = dt.now()

def get_current_timestamp():
    return current_timestamp

s3 = boto3.client('s3')

def generate_counter_party(key, ingestion_bucket, parquet_bucket):
    firstResponse= s3.get_object(Bucket=ingestion_bucket, Key=key[0])
    address = pd.read_csv(firstResponse['Body'])
    # print(address)
    secondResponse= s3.get_object(Bucket=ingestion_bucket, Key=key[1])
    counterparty = pd.read_csv(secondResponse['Body'])
    # print(counterparty)
    
    # merge the two files above in order to create a dim_counterparty.csv 
    m_data=pd.merge(counterparty,address,left_on='legal_address_id', right_on='address_id', how='left')
    
    

    # m_data=pd.merge(counterparty,address,on=['legal_address_id','address_id'], how='left')

    dim_counterparty = m_data[[
                                'counterparty_id', 
                                'counterparty_legal_name',
                                'address_line_1', 
                                'address_line_2', 
                                'district', 
                                'city',
                                'postal_code', 
                                'country',
                                'phone'
                            ]].copy()
    
    # pprint(dim_counterparty)

    dim_counterparty.rename(
        columns={
            'counterparty_id':'counterparty_id', 
            'counterparty_legal_name':'counterparty_legal_name',
            'address_line_1':'counterparty_legal_address_line_1', 
            'address_line_2':'counterparty_legal_address_line_2', 
            'district':'counterparty_legal_district', 
            'city':'counterparty_legal_city',
            'postal_code':'counterparty_legal_postal_code', 
            'country':'counterparty_legal_country',
            'phone':'counterparty_legal_phone_number'
            },
            inplace=True
        )
    
    # pprint(dim_counterparty)

    # timestamp = get_last_ingestion_timestamp(ingestion_bucket)
    # print(timestamp)

    new_counterparty = dim_counterparty.to_parquet()
    pprint(new_counterparty)

    key_parts = key[1].split('/')
    timestamp = '/'.join(key_parts[:-1])
    s3.put_object(Bucket=parquet_bucket, Key=f'{timestamp}/dim_counterparty.parquet',Body=new_counterparty)

def get_bucket_name():
    return 'terrific-totes-ingestion-bucket20230725102602583400000001'

keyList = [
    get_most_recent_table(get_bucket_name, 'address.csv'),
    get_most_recent_table(get_bucket_name, 'counterparty.csv')
    ]
    
generate_counter_party(
    keyList,'terrific-totes-ingestion-bucket20230725102602583400000001','terrific-totes-processed-bucket20230725102602584600000002'
    )

# pprint(get_most_recent_table(get_bucket_name, 'counterparty.csv'))

# pprint(get_most_recent_table(get_bucket_name, 'counterparty.csv'))