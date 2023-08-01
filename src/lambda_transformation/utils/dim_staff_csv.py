import src.table_utils.get_tables as util
from pprint import pprint
import pandas as pd
import io
from datetime import datetime as dt


def create_dim_staff_csv(staff_dict, department_dict):
    '''
        Will return a KB dictionary in the form:
            'Key' - dim_staff.csv prefixed with a timestamp
            'Body' - our merged data frame according to the schema

    Args:
        staff_dict - A KB dictionary
        department_dict - A KB dictionary

    Notes:
        The timestamp is the most recent timestamp from the two arguments.
    '''

    staff_key, staff_df = staff_dict['Key'], staff_dict['Body']
    dept_key, dept_df = department_dict['Key'], department_dict['Body']

    '''
        Find the most recent timestamp
    '''
    staff_timestamp = dt.fromisoformat(staff_key.split('/')[0])
    dept_timestamp = dt.fromisoformat(dept_key.split('/')[0])
    timestamps = [staff_timestamp, dept_timestamp]
    timestamps.sort(reverse=True)
    timestamp = timestamps[0].isoformat()

    '''
        Create dim_staff by merging the dataframes
    '''
    dim_staff = staff_df.merge(dept_df, on='department_id', how='left')
    dim_staff = dim_staff[[
        'staff_id',
        'first_name',
        'last_name',
        'email_address',
        'department_name',
        'location',
    ]]

    return {'Key': f'{timestamp}/dim_staff.csv', 'Body': dim_staff}


if __name__ == "__main__":
    staff = util.get_table_contents('staff.csv')
    staff_wrapper = io.StringIO(staff['body'])
    staff_df = pd.read_csv(staff_wrapper)

    dept = util.get_table_contents('department.csv')
    dept_wrapper = io.StringIO(dept['body'])
    dept_df = pd.read_csv(dept_wrapper)

    current_timestamp = dt.now().isoformat()

    staff_dict = {'Key': f'{current_timestamp}/staff.csv', 'Body': staff_df}
    department_dict = {
        'Key': f'{current_timestamp}/department.csv', 'Body': dept_df}

    result = create_dim_staff_csv(
        staff_dict=staff_dict,
        department_dict=department_dict
    )

    pprint(result)
