import boto3
import pandas as pd

s3 = boto3.client('s3')
def generate_counter_party(key,ingestion_bucket,parquet_bucket):
    response= s3.get_object(Bucket=ingestion_bucket, Key=key)
    # read the address.csv and counterparty.csv from the bucket
    address = pd.read_csv(response['address.csv'])
    print(address)
    counterparty = pd.read_csv(response['counterparty.csv'])
    # merge the two files above in order to create a dim_counterparty.csv 
    m_data=pd.merge(counterparty,address,left_on='legal_address_id', right_on='address_id', how='left')
    # m_data=pd.merge(counterparty,address,on=['legal_address_id','address_id'], how='left')

    dim_counterparty = m_data[['counterparty_id', 
                               'name',
                                'address_line_1', 
                                'address_line_2', 
                                'district', 
                                'city',
                                'postal_code', 
                                'country',
                                'phone_number']]
    
    dim_counterparty.rename(
        columns={
            'counterparty_id':'counterparty_id', 
            'name':'counterparty_legal_name',
            'address_line_1':'counterparty_legal_address_line_1', 
            'address_line_2':'counterparty_legal_address_line_2', 
            'district':'counterparty_legal_district', 
            'city':'counterparty_legal_city',
            'postal_code':'counterparty_legal_postal_code', 
            'country':'counterparty_legal_country',
            'phone_number':'counterparty_legal_phone_number'})
    
    s3.put_object(Bucket=parquet_bucket,key='dim_counterparty.csv')

generate_counter_party('dim_counterparty.csv','ingestion_bucket','parquet_bucket')