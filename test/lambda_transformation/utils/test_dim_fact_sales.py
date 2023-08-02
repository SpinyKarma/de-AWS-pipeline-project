import pytest
import pandas as pd
import src.lambda_transformation.utils.dim_fact_sales_order as dim_sales
import src.lambda_transformation.utils.get_tables as tables


def test_sales_order_to_fact_sales_order():
    '''
        Test to see if the function works as intended.
    '''
    sales_order_dict = {
        'Key': 'sales_order.csv',
        'Body': pd.DataFrame({
            'sales_order_id': [3539],
            'created_at': ['2023-08-02 09:10:09.786000'],
            'last_updated': ['2023-08-02 09:10:09.786000'],
            'design_id': [85],
            'staff_id': [13],
            'counterparty_id': [8],
            'units_sold': [33289],
            'unit_price': [3.25],
            'currency_id': [11],
            'agreed_payment_date': ['2023-08-06'],
            'agreed_delivery_date': ['2023-08-04'],
            'agreed_delivery_location_id': [19]
        })
    }

    result = dim_sales.sales_order_to_fact_sales_order(sales_order_dict)

    expected_key = 'sales_order.csv/fact_sales_order.csv'
    assert result['Key'] == expected_key
    assert isinstance(result['Body'], pd.DataFrame)


def test_sales_order_to_fact_sales_order_missing_columns():
    '''
        Test to see if it raises KeyError if missing required Body values.
    '''
    sales_order_dict = {
        'Key': 'sales_order.csv',
        'Body': pd.DataFrame({
            'sales_order_id': [3539],
            'created_at': ['2023-08-02 09:10:09.786000'],
            'last_updated': ['2023-08-02 09:10:09.786000'],
            'design_id': [85],
            'staff_id': [13],
            'counterparty_id': [8],
            'units_sold': [33289],
            'unit_price': [3.25],
            'currency_id': [11],
            'agreed_payment_date': ['2023-08-06'],
            'agreed_delivery_date': ['2023-08-04']
        })
    }

    with pytest.raises(KeyError):
        dim_sales.sales_order_to_fact_sales_order(sales_order_dict)


def test_sales_order_to_fact_sales_order_empty_dataframe():
    '''
        Tests to see key error is raised if ran with empty dataframe.
    '''
    sales_order_dict = {
        'Key': 'sales_order.csv',
        'Body': pd.DataFrame()
    }

    with pytest.raises(KeyError):
        dim_sales.sales_order_to_fact_sales_order(sales_order_dict)


if __name__ == '__main__':
    sales_data = tables.read_table('sales_order.csv')
    result = dim_sales.sales_order_to_fact_sales_order(sales_data)
    print(result)
