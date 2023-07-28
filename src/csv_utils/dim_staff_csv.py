import pandas as pd
from pprint import pprint
import boto3


def get_ingestion_bucket_name():
    name = 'terrific-totes-ingestion-bucket'
    name += '20230725102602583400000001'
    return name


def read_csv():
    s3 = boto3.client('s3')

    staff_obj = s3.get_object(
        Bucket=get_ingestion_bucket_name(),
        Key='staff.csv')

    departments_obj = s3.get_object(
        Bucket=get_ingestion_bucket_name(),
        Key='department.csv')

    staff_csv = staff_obj['Body'].read()
    departments_csv = staff_obj['Body'].read()

    print(staff_csv)


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

    staff = staff_dataframe.to_dict()
    department = department_dataframe.to_dict()

    pprint(staff)
    pprint(department)


read_csv()
