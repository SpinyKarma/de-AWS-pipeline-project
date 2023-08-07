import pandas as pd
from src.lambda_transformation.utils.dim_design import design_to_dim_design

test_input_dict = {
    'Key': '2023-07-31T12:24:11.422525/design.csv',
    'Body': pd.DataFrame({
        'design_id': [8, 51, 50, 14],
        'design_name': ['Wooden', 'Bronze', 'Granite', 'Granite'],
        'file_location': ['/usr', '/private', '/private/var', '/private/var'],
        'file_name': ['wooden-20220717-npgz.json',
                      'bronze-20221024-4dds.json',
                      'granite-20220205-3vfw.json',
                      'granite-20210406-uwqg.json']
    })
}


def test_timestamp_carried_over():
    """ Test whether the function 'design_to_dim_design' carries over the
        timestamp correctly from the input dictionary to the output dictionary.
    """
    output = design_to_dim_design(test_input_dict)
    timestamp = test_input_dict['Key'].split('/')[0]
    expected_new_key = f'{timestamp}/dim_design.csv'
    assert output['Key'] == expected_new_key


def test_output_body_is_pandas_dataframe():
    """ Test whether the 'Body' value in the output dictionary of the function
        'design_to_dim_design' is a Pandas DataFrame.
    """
    output = design_to_dim_design(test_input_dict)
    assert isinstance(output['Body'], pd.DataFrame)


def test_output_body_has_expected_transformation_applied():
    """ Test whether the function 'design_to_dim_design' correctly transforms
        the input DataFrame and returns the expected output.
    """
    output = design_to_dim_design(test_input_dict)
    assert output['Body'].equals(test_input_dict['Body'])
