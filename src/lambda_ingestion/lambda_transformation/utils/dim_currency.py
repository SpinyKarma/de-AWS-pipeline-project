import ccy


def currency_to_dim_currency(currency_dict):
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
                             'currency_code']]
    dim_currency['currency_name'] = currency_names
    new_key = key.split('/')[0]+'/dim_currency.csv'
    dim_currency_dict = {'Key': new_key, 'Body': dim_currency}
    return dim_currency_dict
