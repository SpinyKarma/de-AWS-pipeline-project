import pandas as pd
from src.lambda_transformation.utils.dim_currency import (
    currency_to_dim_currency as dcy
)

test_input_dict = {
    'Key': '2023-07-31T12:24:11.422525/currency.csv',
    'Body': pd.DataFrame({
        'currency_id': [1, 2, 3],
        'currency_code': ['GBP', 'USD', 'EUR'],
    })
}


def test_timestamp_carried_over():
    """
    Test whether the function 'currency_to_dim_currency' carries over the 
    timestamp correctly from the input dictionary to the output dictionary.
    """
    output = dcy(test_input_dict)
    timestamp = test_input_dict['Key'].split('/')[0]
    expected_new_key = f'{timestamp}/dim_currency.csv'
    assert output['Key'] == expected_new_key


def test_output_body_is_pandas_dataframe():
    """
    Test whether the 'Body' value in the output dictionary of the function 'currency_to_dim_currency' is a
    Pandas DataFrame.
    """
    output = dcy(test_input_dict)
    assert isinstance(output['Body'], pd.DataFrame)


def test_output_body_has_expected_transformation_applied():
    """
    Test whether the function 'currency_to_dim_currency' correctly transforms the input 
    DataFrame and returns the expected output.
    """
    output = dcy(test_input_dict)
    test_output = {
        'Key': '2023-07-31T12:24:11.422525/currency.csv',
        'Body': pd.DataFrame({
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'EUR'],
            'currency_name': ['British Pound', 'US Dollar', 'Euro']
        })
    }
    assert output['Body'].equals(test_output['Body'])
