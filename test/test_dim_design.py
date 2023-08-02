import pandas as pd
import pytest
from src.lambda_transformation.utils.dim_design import design_to_dim_design

def test_timestamp_carried_over():
    test_input_dict =  {
        'Key': '2023-07-31T12:24:11.422525/design.csv',
        'Body': pd.DataFrame({
            'design_id': [8, 51, 50, 14],
            'design_name':['Wooden', 'Bronze', 'Granite', 'Granite'],
            'file_location':['/usr', '/private', '/private/var', '/private/var'],
            'file_name':['wooden-20220717-npgz.json', 'bronze-20221024-4dds.json', 'granite-20220205-3vfw.json', '	granite-20210406-uwqg.json']
            })
    }
    output = design_to_dim_design(test_input_dict)
    timestamp = test_input_dict['Key'].split('/')[0]
    expected_new_key = f'{timestamp}/dim_design.csv'
    print(type(output), '<<<')
    assert isinstance(output, pd.DataFrame)
    assert output['Key'] == expected_new_key
  
