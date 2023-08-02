import pytest
import src.lambda_transformation.utils.dim_counter_party as util
import src.table_utils.get_tables as tables
import pandas as pd


def test_that_the_function_return_a_dict_with_desired_keys():
    address = tables.read_table('address.csv')
    counterparty = tables.read_table('counterparty.csv')
    result=util.counter_party_address_to_dim_counterparty(counterparty,address)
    assert isinstance(result, dict)

def test_that_body_for_both_address_and_counterparty_are_DataFrame_Dict(): 
    address_dict = {
        'Key': 'address.csv',
        'Body': pd.DataFrame({ 
           'address_id':[1111],
           'address_line_1':['20 larch road'],
           'address_line_2':['west yourkshire'],
           'district':['paddock'],
           'city':['Huddersfield'],
           'postal_code':['HD1 3PD'],
           'country':['United kingdom'],
           'phone':['00441484555333'],
           'created_at':['2023-08-02 13:10:09.556000'],
           'last_update':['2023-08-02 13:10:09.556000']
       })
       
    }
    counterparty_dict={
        'Key':'address.csv',
        'Body':pd.DataFrame({ 
            'counterparty_id' :[8],
            'counterparty_legal_name':['xxxx'],
            'legal_address_id' :[1111],
            'commercial_contact':['00441484555333'],
            'delivery_contact':['07822234567'],
            'created_at' :['2023-08-02 13:10:09.556000'],
            'last_updated':['2023-08-02 13:10:09.556000']
        })
       }
    result = util.counter_party_address_to_dim_counterparty(counterparty_dict,address_dict)
    assert isinstance(result['Body'],pd.DataFrame)
    

def test_dim_counterparty_dict_contains_all_the_expected_keys():
    address_dict={
       'Key':'address.csv',
       'Body':pd.DataFrame({  
           'address_id':[1111],
           'address_line_1':['20 larch road'],
           'address_line_2':['west yourkshire'],
           'district':['paddock'],
           'city':['Huddersfield'],
           'postal_code':['HD1 3PD'],
           'country':['United kingdom'],
           'phone':['00441484555333'],
           'created_at':['2023-08-02 13:10:09.556000'],
           'last_update':['2023-08-02 13:10:09.556000']
           
       })
       
    }
    counterparty_dict={
        'Key':'address.csv',
        'Body':pd.DataFrame({  
            'counterparty_id' :[8],
            'counterparty_legal_name':['xxxx'],
            'legal_address_id' :[1111],
            'commercial_contact':['00441484555333'],
            'delivery_contact':['07822234567'],
            'created_at' :['2023-08-02 13:10:09.556000'],
            'last_updated':['2023-08-02 13:10:09.556000']

        })
       }
    with pytest.raises(KeyError):
        util.counter_party_address_to_dim_counterparty(counterparty_dict,address_dict)


if __name__ == '__main__':
    address = tables.read_table('address.csv')
    counterparty = tables.read_table('counterparty.csv')
    result = util.counter_party_address_to_dim_counterparty(counterparty,address) 
    print(result)