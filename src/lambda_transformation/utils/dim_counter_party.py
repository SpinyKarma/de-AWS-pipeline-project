import boto3
import pandas as pd
from src.table_utils.get_tables import (get_most_recent_table)
from pprint import pprint
from src.lambda_ingestion.ingestion_lambda import (get_last_ingestion_timestamp)
from datetime import datetime as dt

# s3 = boto3.client('s3')

def counter_party_address_to_dim_counter_party(counterparty_dict, address_dict):
    
    key=address_dict['Key']
    address=address_dict['Body']
    counterparty=counterparty_dict['Body']    
#merge the two files above in order to create a dim_counterparty.csv 
    m_data=pd.merge(counterparty,address,left_on='legal_address_id', right_on='address_id', how='left')
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

# rename dim_counterparty columns name 
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
    
    dim_counterparty=dim_counterparty.where(pd.notnull(dim_counterparty),None)  
   # concatenate the key with the dim_counterparty
    new_key=key.split('/')[0]+"/dim_counterparty.csv"
   # make dict and return it 
    dim_counterparty_dict={"Key": new_key,"Body": dim_counterparty}  
    return dim_counterparty_dict
