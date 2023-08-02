import pytest
import src.lambda_transformation.utils.dim_counter_party as util
import src.lambda_transformation.utils.get_tables as tables
import pandas as pd


test_address = {
    'Key': 'address.csv',
    'Body': pd.DataFrame({
        'address_id': [1111],
        'address_line_1': ['20 larch road'],
        'address_line_2': ['west yourkshire'],
        'district': ['paddock'],
        'city': ['Huddersfield'],
        'postal_code': ['HD1 3PD'],
        'country': ['United kingdom'],
        'phone': ['00441484555333'],
        'created_at': ['2023-08-02 13:10:09.556000'],
        'last_update': ['2023-08-02 13:10:09.556000']
    })
}
test_counterparty = {
    'Key': 'counterparty.csv',
    'Body': pd.DataFrame({
        'counterparty_id': [8],
        'counterparty_legal_name': ['xxxx'],
        'legal_address_id': [1111],
        'commercial_contact': ['00441484555333'],
        'delivery_contact': ['07822234567'],
        'created_at': ['2023-08-02 13:10:09.556000'],
        'last_updated': ['2023-08-02 13:10:09.556000']
    })
}


def test_that_the_function_return_a_dict_with_desired_keys():
    result = util.counter_party_address_to_dim_counterparty(
        test_counterparty, test_address)
    assert isinstance(result, dict)


def test_timestamp_carried_over():
    output = util.counter_party_address_to_dim_counterparty(
        test_counterparty, test_address)
    timestamp = test_address['Key'].split('/')[0]
    expected_new_key = f'{timestamp}/dim_counterparty.csv'
    assert output['Key'] == expected_new_key


def test_that_the_result_body_of_the_dim_counterparty_is_DataFrame_Dict():
    result = util.counter_party_address_to_dim_counterparty(
        test_counterparty, test_address)
    assert isinstance(result['Body'], pd.DataFrame)


def test_address_and_counterparty_join_into_dim_counterparty_dict_when_columns_missing_from_address_csv():
    # example of testing address dict with missing column
    # this is the missing column from the address.csv 'phone':['00441484555333']
    address_dict_missing_column = {
        'Key': 'address.csv',
        'Body': pd.DataFrame({
            'address_id': [1111],
            'address_line_1': ['20 larch road'],
            'address_line_2': ['west yourkshire'],
            'district': ['paddock'],
            'city': ['Huddersfield'],
            'postal_code': ['HD1 3PD'],
            'country': ['United kingdom'],
            'created_at': ['2023-08-02 13:10:09.556000'],
            'last_update': ['2023-08-02 13:10:09.556000']

        })
    }
    with pytest.raises(KeyError):
        util.counter_party_address_to_dim_counterparty(
            test_counterparty, address_dict_missing_column)


if __name__ == '__main__':
    address = tables.read_table('address.csv')
    counterparty = tables.read_table('counterparty.csv')
    result = util.counter_party_address_to_dim_counterparty(
        counterparty, address)
    print(result)


# test_address = tables.read_table('address.csv')
# test_counterparty = tables.read_table('counterparty.csv')
