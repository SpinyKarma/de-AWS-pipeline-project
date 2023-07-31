from src.table_utils.get_tables import (
    get_most_recent_table, get_table_contents)

from pprint import pprint
import csv


def csv_to_dict(csv_text):
    reader = csv.DictReader(csv_text)
    id_fieldname = reader.fieldnames[0]
    rest_fields = reader.fieldnames[1:]
    print(rest_fields)

    dictionary = {}
    for row in reader:
        rest_data = {field: row[field] for field in rest_fields}
        dictionary[row[id_fieldname]] = rest_data

    return dictionary


def create_dim_staff_csv():
    '''
        This function will generate the dim_staff csv.

    Returns:
        A dictionary in the form
            'name' which is {timestamp}/dim_staff.csv
            'body' which is our table as a string
    '''

    # Our required tables
    requirements = [
        'staff.csv',
        'department.csv'
    ]

    contents = {}

    for table in requirements:
        timestamped_table = get_most_recent_table(table)
        table_contents = get_table_contents(timestamped_table)
        contents[table] = csv_to_dict(table_contents['body'])

    pprint(contents)
