import pandas as pd
from pprint import pprint
import boto3
import csv


def get_ingestion_bucket_name():
    name = 'terrific-totes-ingestion-bucket'
    name += '20230725102602583400000001'
    return name


def response_to_data_frame(response):
    """
        This funtion will convert get_object response for a CSV file
        into a Pandas DataFrame
    """

    body_reader = response['Body']
    body = body_reader.read().decode('utf-8').splitlines()
    csv_reader = csv.DictReader(body)
    rows = []

    for data in csv_reader:
        rows.append(data)

    return pd.DataFrame.from_dict(rows)


def read_csv_as_dataframes():
    s3 = boto3.client('s3')

    objects = s3.list_objects(Bucket=get_ingestion_bucket_name())
    pprint(objects)

    staff_response = s3.get_object(
        Bucket=get_ingestion_bucket_name(),
        Key='staff.csv')

    departments_response = s3.get_object(
        Bucket=get_ingestion_bucket_name(),
        Key='department.csv')

    return {
        'staff': response_to_data_frame(staff_response),
        'department': response_to_data_frame(departments_response)
    }


def print_dicts(staff_dict, department_dict):
    pprint('STAFF DICT', staff_dict)
    pprint('DEPARTMENT DICT', department_dict)


def gen_dim_staff(
    staff_dataframe: pd.DataFrame,
    department_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Description:
        This function shall generate our dim_staff table as a dataframe.

    Args:
        staff_dataframe: The dataframe representing
        the staff table from ingestion

        department_dataframe: The dataframe representing
        the departments table from ingestion

    Returns:
        TODO
    """

    staff_dict = staff_dataframe.to_dict('records')
    department_dict = department_dataframe.to_dict('records')
    print_dicts(staff_dict, department_dict)


# Testing:
dataframes = read_csv_as_dataframes()

gen_dim_staff(staff_dataframe=dataframes['staff'],
              department_dataframe=dataframes['department'])
