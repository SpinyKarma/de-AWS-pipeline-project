from src.table_utils.get_tables import (
    get_most_recent_table, get_table_contents)

import csv
from datetime import datetime as dt


class CsvBuilder:
    """For creating CSV without writing to a file."""

    def __init__(self):
        self.rows = []

    def write(self, row):
        self.rows.append(row)

    def as_txt(self):
        return ''.join(self.rows)


def csv_to_dict(csv_text):
    '''
        Converts a CSV in text-form to a dictionary of records,
        with the primary key being the first field, which should be the ID
        of the data.

        Example: staff_id : { staff data ...}, and so on
    '''
    reader = csv.DictReader(csv_text)
    id_fieldname = reader.fieldnames[0]
    rest_fields = reader.fieldnames[1:]

    dictionary = {}
    for row in reader:
        rest_data = {field: row[field] for field in rest_fields}
        dictionary[row[id_fieldname]] = rest_data

    return dictionary


def table_to_dict(table):
    timestamped_table = get_most_recent_table(table)
    table_contents = get_table_contents(timestamped_table)
    return csv_to_dict(table_contents['body'])


def create_dim_staff_csv():
    '''
        This function will generate the dim_staff csv.

    Returns:
        A dictionary in the form
            'name' which is {timestamp}/dim_staff.csv
            'body' which is our table as a string
    '''

    '''
        Create a timestamped name as according to the directive
    '''
    name = f'{dt.now().isoformat()}/dim_staff.csv'

    '''
        Load our tables
    '''
    staff = table_to_dict('staff.csv')
    departments = table_to_dict('department.csv')

    '''
        We store our data in this dictionary,
        this is a dictionary of dictionaries in the form:

        ...
        (staff_id) : {
            fields...
        },
        ...

        Meaning that it should be accessed like:
        dim_staff[staff_id],
    '''
    dim_staff = {}

    '''
        Create dim_staff by merging the staff and department tables
    '''
    for id in staff.keys():
        dim_staff[id] = staff[id]
        dept_id = dim_staff[id]['department_id']
        dept = departments[dept_id]
        dim_staff[id]['department_name'] = dept['department_name']
        dim_staff[id]['location'] = dept['location']

        '''
            These fields are not included in the schema
        '''
        del dim_staff[id]['department_id']
        del dim_staff[id]['created_at']
        del dim_staff[id]['last_updated']

    '''
        Keep track of our fields for our CSV
    '''
    dim_staff_csv_fields = ['staff_id']
    for field in dim_staff.keys():
        dim_staff_csv_fields.append(field)

    '''
        Create the rows of our data, dim_staff_csv will be
        an array of records
    '''
    dim_staff_csv = []
    for id in dim_staff.keys():
        staff = dim_staff[id]
        row = {}
        row['staff_id'] = id
        for prop in staff.keys():
            row[prop] = staff[prop]
        dim_staff_csv.append(row)

    '''
        Build the string according to the directive
    '''
    csv_builder = CsvBuilder()
    fields = [field for field in dim_staff_csv[0].keys()]
    csv_writer = csv.writer(csv_builder)
    csv_writer.writerow(fields)
    rows = [[row[key] for key in fields] for row in dim_staff_csv]
    csv_writer.writerows(rows)

    # name is timestamped
    return {'name': name,  'body': csv_builder.as_txt()}
