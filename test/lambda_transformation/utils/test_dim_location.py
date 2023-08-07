from src.lambda_transformation.utils.dim_location import (
    address_to_dim_location as atdl
)
from datetime import datetime as dt
import pandas as pd

timestamp = dt(2020, 1, 1).isoformat()

fake_address_dict = {
    'Key': f'{timestamp}/address.csv',
    'Body': pd.DataFrame({
        'address_id': [1],
        'address_line_1': ['Someplace'],
        'address_line_2': [""],
        'district': ["Somewhere"],
        'city': ["London"],
        'postal_code': ["AB3 4EF"],
        'country': ["banana"],
        'phone': [123456789],
        'created_at': [dt(2019, 1, 1).isoformat()],
        'last_updated': [dt(2019, 1, 1).isoformat()]
    })
}

expected_dim_location_dict = {
    'Key': f'{timestamp}/dim_location.csv',
    'Body': pd.DataFrame({
        'location_id': [1],
        'address_line_1': ['Someplace'],
        'address_line_2': [""],
        'district': ["Somewhere"],
        'city': ["London"],
        'postal_code': ["AB3 4EF"],
        'country': ["banana"],
        'phone': [123456789]
    })
}


def test_body_of_return_is_correct_format():
    """ Test whether the 'Body' value in the output dictionary of the function
        'address_to_dim_location' has the correct column format.
    """
    output = atdl(fake_address_dict)
    output_cols = list(output['Body'].columns)
    expected_cols = list(expected_dim_location_dict['Body'].columns)
    assert output_cols == expected_cols


def test_timestamp_is_carried_over_from_input_to_output():
    """ Test whether the function 'address_to_dim_location' carries
        over the timestamp correctly from the input dictionary to the output
        dictionary.
    """
    output = atdl(fake_address_dict)
    output_timestamp = output['Key'].split("/")[0]
    expected_timestamp = expected_dim_location_dict['Key'].split("/")[0]
    assert output_timestamp == expected_timestamp


def test_overall_formatting_success():
    """ Test whether the function 'address_to_dim_location' correctly formats
        the input address dictionary and returns the expected output for
        'dim_location'.
    """
    output = atdl(fake_address_dict)['Body']
    expected = expected_dim_location_dict['Body']
    out_cols = output.columns
    exp_cols = expected.columns
    assert list(out_cols) == list(exp_cols)
    for col in exp_cols:
        assert list(output.get(col)) == list(expected.get(col))
