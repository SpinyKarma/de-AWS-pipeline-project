
def staff_department_to_dim_staff(staff_dict, department_dict):
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

    staff_df, dept_df = staff_dict['Body'], department_dict['Body']

    '''
        Find the most recent timestamp
    '''
    timestamp = [staff_dict['Timestamp'], department_dict['Timestamp']]
    timestamp.sort(reverse=True)
    timestamp = timestamp[0]

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
    return {'Key': f'{timestamp}/dim_staff.csv',
            'Body': dim_staff,
            'Timestamp': timestamp}
