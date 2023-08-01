import boto3
import pandas as pd
from datetime import datetime as dt
from src.lambda_transformation.transformation_lambda import get_ingestion_bucket_name
from pprint import pprint


def address_to_dim_location(address_dict):
    '''Takes all addresses from address csv and remaps to dim_location schema.

    Args:
        address_dict: a dict with two key/val pairs:
            "Key": the key of the address csv
            "Body": a pandas dataframe of the csv contents.

    Returns:
        dim_location_dict: a dict with two key/val pairs:
            "Key": the key of the dim_location file
            "Body": a pandas dataframe of the dim_location contents.
    '''
    key = address_dict['Key']
    address = address_dict['Body']
    dim_location = address[['address_id',
                            'address_line_1',
                            'address_line_2',
                            'district',
                            'city',
                            'postal_code',
                            'country',
                            'phone']]

    # pandas rename not working for whatever reason, so just assigned all
    dim_location.rename(
        columns={
            'address_id': 'location_id'},
        inplace=True)

    # Replace the "NaN"s with "None"s
    dim_location = dim_location.where(pd.notnull(dim_location), None)

    # Put everything together and return
    new_key = key.split("/")[0]+"/dim_location.csv"
    dim_location_dict = {"Key": new_key, "Body": dim_location}
    return dim_location_dict


if __name__ == "__main__":
    current_timestamp = dt.now().isoformat()
    key = '2023-07-31T12:24:11.422525/address.csv'
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=get_ingestion_bucket_name(), Key=key)
    body = pd.read_csv(response['Body'])
    dict = {"Key": key, "Body": body}
    # print(dict[key])
    out = address_to_dim_location(dict)["Body"]
    pprint(out)
