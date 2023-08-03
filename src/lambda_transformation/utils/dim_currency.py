import ccy
import pandas as pd


def currency_to_dim_currency(currency_dict):
    '''Takes all currencies from currency csv and remaps to dim_currency schema.

    Args:
        currency_dict: a dict with two key/val pairs:
            "Key": the key of the currency csv
            "Body": a panda dataframe of the csv contents.

    Returns:
        dim_currency_dict: a dict with two key/val pairs:
            "Key": the key of the dim_currency file
            "Body": a pandas dataframe of the dim_currency contents.
    '''
    key = currency_dict['Key']
    currency = currency_dict['Body']
    currency_codes = currency['currency_code']
    currency_names = []
    # map currency name to id, using ccy module
    for currency_code in currency_codes:
        try:
            currency_name = ccy.currency(currency_code).name
        except ccy.UnknownCurrency:
            currency_name = None
        currency_names.append(currency_name)

    dim_currency = currency[['currency_id',
                             'currency_code']].copy()
    
    dim_currency['currency_name'] = currency_names
    dim_currency_df = pd.DataFrame(dim_currency)
    new_key = key.split('/')[0]+'/dim_currency.csv'
    dim_currency_dict = {'Key': new_key, 'Body': dim_currency_df}
    return dim_currency_dict
