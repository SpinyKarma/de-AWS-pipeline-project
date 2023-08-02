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
    output = dcy(test_input_dict)
    timestamp = test_input_dict['Key'].split('/')[0]
    expected_new_key = f'{timestamp}/dim_currency.csv'
    assert output['Key'] == expected_new_key


def test_output_body_is_pandas_dataframe():
    output = dcy(test_input_dict)
    assert isinstance(output['Body'], pd.DataFrame)


def test_output_body_has_expected_transformation_appgitlied():
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
