import src.lambda_transformation.utils.dim_staff as util
import src.lambda_transformation.utils.get_tables as tables
from pandas import DataFrame

# Test based on assumption that broke lambda, see comments in dim_staff.py
# def test_timestamp_is_preserved():
#     try:
#         staff = tables.read_table('staff.csv')
#         depts = tables.read_table('department.csv')
#         result = util.staff_department_to_dim_staff(staff, depts)
#         # Get the most recent timestamp
#         recent_timestamp = [
#             staff['Timestamp'],
#             depts['Timestamp']
#         ]
#         recent_timestamp.sort(reverse=True)
#         recent_timestamp = recent_timestamp[0]
#         result_timestamp = result['Timestamp']

#         '''
#             Is the timestamp the most recent?
#         '''
#         assert recent_timestamp == result_timestamp

#         '''
#             Is the key prefixed with the timestamp in isoformat?
#         '''
#         assert result['Key'].startswith(recent_timestamp.isoformat())
#     except tables.TableNotFoundError:
#         pass


def test_body_is_a_pd_dataframe():
    try:
        result: DataFrame = util.staff_department_to_dim_staff(
            tables.read_table('staff.csv'),
            tables.read_table('department.csv')
        )
        assert type(result['Body']) is DataFrame
    except tables.TableNotFoundError:
        pass


def test_body_is_as_expected():
    try:
        staff = tables.read_table('staff.csv')
        depts = tables.read_table('department.csv')
        result = util.staff_department_to_dim_staff(staff, depts)

        body = result['Body']

        '''
            As according to the schema
        '''

        expected_columns = [
            'staff_id',
            'first_name',
            'last_name',
            'department_name',
            'location',
            'email_address'
        ]

        for column in expected_columns:
            assert column in body.columns
    except tables.TableNotFoundError:
        pass
