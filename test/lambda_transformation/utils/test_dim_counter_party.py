import pytest
import src.lambda_transformation.utils.dim_counter_party as util
import src.table_utils.get_tables as tables
import pandas as pd 

def test_that_the_function_return_a_dict():
    address = tables.read_table('address.csv')
    counterparty = tables.read_table('counterparty.csv')
    result=util.counter_party_address_to_dim_counterparty(counterparty,address)
    assert isinstance(result, dict)

def test_that_body_for_both_address_and_counterparty_are_DataFrame_Dict(): 
    address = tables.read_table('address.csv')
    counterparty = tables.read_table('counterparty.csv')
    result=util.counter_party_address_to_dim_counterparty(counterparty,address)
    
    assert isinstance(result,dict)
    # assertin('Key',result)
    # assertin('Body',result)
    

def test_dim_counterparty_dict_contains_all_the_expected_keys():
   
    pass


# if __name__ == '__main__':
#     address = tables.read_table('address.csv')
#     counterparty = tables.read_table('counterparty.csv')
#     result = util.counter_party_address_to_dim_counterparty(counterparty,address) 
#     print(result)