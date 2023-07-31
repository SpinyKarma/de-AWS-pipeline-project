# import pandas as pd
# from datetime import datetime as dt
# from src.lambda_transformation.transformation_lambda import get_ingestion_bucket_name


def address_to_dim_location(address_dict):
    '''Takes all addresses from address csv and remaps to dim_location schema.

    Args:
        address_dict: a dict with the key of the address csv as the key and
        a pandas dataframe of the csv contents as the value.

    Returns:
        dim_location_dict: a dict with the key of the dim_location file as its
        key and a pandas dataframe of the contents as the value.
    '''
    key = list(address_dict.keys())[0]
    address = address_dict[key]
    dim_location = address[['address_id',
                            'address_line_1',
                            'address_line_2',
                            'district',
                            'city',
                            'postal_code',
                            'country',
                            'phone']]
    # pandas rename not working for whatever reason, so just assigned all
    dim_location.columns = ['location_id',
                            'address_line_1',
                            'address_line_2',
                            'district',
                            'city',
                            'postal_code',
                            'country',
                            'phone']
    dim_location_dict = {key: dim_location}
    return dim_location_dict


# if __name__ == "__main__":
#     current_timestamp = dt.now().isoformat()
#     key = '2023-07-31T12:24:11.422525/address.csv'
#     s3 = boto3.client('s3')
#     response = s3.get_object(Bucket=get_ingestion_bucket_name(), Key=key)
#     body = pd.read_csv(response['Body'])
#     dict = {key: body}
#     # print(dict[key])
#     out = address_to_dim_location(dict)[key]
#     pprint(out)
