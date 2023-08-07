from src.lambda_transformation.utils.dim_staff import (
    staff_department_to_dim_staff as sdtds
)
from datetime import datetime as dt
from pandas import DataFrame

old_timestamp = dt(2019, 5, 7)
timestamp = dt(2020, 1, 1).isoformat()
fake_staff_dict = {
    'Key': f'{timestamp}/staff.csv',
    'Timestamp': timestamp,
    'Body': DataFrame({
        'staff_id': [1],
        'first_name': ['apple'],
        'last_name': ['banana'],
        'department_id': [5],
        'email_address': ["a.b@c.com"],
        'created_at': [old_timestamp],
        'last_updated': [old_timestamp]
    })
}
fake_department_dict = {
    'Key': f'{timestamp}/department.csv',
    'Timestamp': timestamp,
    'Body': DataFrame({
        'department_id': [5],
        'department_name': ['orange'],
        'location': ['somewhere'],
        'manager': ["someone"],
        'created_at': [old_timestamp],
        'last_updated': [old_timestamp]
    })
}
expected_dim_staff_dict = {
    'Key': f'{timestamp}/dim_staff.csv',
    'Timestamp': timestamp,
    'Body': DataFrame({
        'staff_id': [1],
        'first_name': ['apple'],
        'last_name': ['banana'],
        'department_name': ["orange"],
        'location': ['somewhere'],
        'email_address': ['a.b@c.com']
    })
}


def test_timestamp_is_preserved():
    """
    Test whether the 'Timestamp' value in the output dictionary is preserved
    correctly when converting staff and department data to 'dim_staff'.
    """

    output = sdtds(fake_staff_dict, fake_department_dict)
    assert output['Key'].split("/")[0] == timestamp
    assert output['Timestamp'] == timestamp


def test_body_is_a_pd_dataframe():
    """
    Test whether the 'Body' value in the output dictionary is
    a Pandas DataFrame when converting staff and department
    data to 'dim_staff'.
    """

    output = sdtds(fake_staff_dict, fake_department_dict)
    assert type(output['Body']) is DataFrame


def test_body_is_as_expected():
    """
    Test whether the 'Body' value in the output dictionary matches
    the expected DataFrame when converting staff and department
    data to 'dim_staff'. The test checks if the columns and
    corresponding data in the output and expected DataFrames
    are identical.
    """

    output = sdtds(fake_staff_dict, fake_department_dict)['Body']
    expected = expected_dim_staff_dict['Body']
    out_cols = output.columns
    exp_cols = expected.columns
    assert list(out_cols) == list(exp_cols)
    for col in exp_cols:
        assert list(output.get(col)) == list(expected.get(col))
