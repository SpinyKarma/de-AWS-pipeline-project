import pandas as pd


def sales_order_to_fact_sales_order(sales_order_dict):
    '''Takes all sales orders from sales_order csv and remaps to
       fact_sales_order schema.

    Args:
        sales_order_dict: a dict with two key/val pairs:
            "Key": the key of the sales_order csv
            "Body": a panda dataframe of the csv contents.

    Returns:
        fact_sales_order_dict: a dict with two key/val pairs:
            "Key": the key of the fact_sales_order file
            "Body": a pandas dataframe of the fact_sales_order contents.
    '''

    key = sales_order_dict['Key']
    sales = sales_order_dict['Body']
    fact_sales_order = sales[['sales_order_id',
                             'created_at',
                              'last_updated',
                              'design_id',
                              'staff_id',
                              'counterparty_id',
                              'units_sold',
                              'unit_price',
                              'currency_id',
                              'agreed_payment_date',
                              'agreed_delivery_date',
                              'agreed_delivery_location_id']]

    created_date = fact_sales_order.created_at.apply(
        lambda x: x.split(' ')[0]
    )
    created_time = fact_sales_order.created_at.apply(
        lambda x: x.split(' ')[1]
    )
    last_updated_date = fact_sales_order.created_at.apply(
        lambda x: x.split(' ')[0]
    )
    last_updated_time = fact_sales_order.created_at.apply(
        lambda x: x.split(' ')[1]
    )
    adl_id = fact_sales_order.agreed_delivery_location_id

    fact_sales_order = pd.DataFrame({
        'sales_order_id': fact_sales_order.sales_order_id,
        'created_date': created_date,
        'created_time': created_time,
        'last_updated_date': last_updated_date,
        'last_updated_time': last_updated_time,
        'sales_staff_id': fact_sales_order.staff_id,
        'counterparty_id': fact_sales_order.counterparty_id,
        'units_sold': fact_sales_order.units_sold,
        'unit_price': fact_sales_order.unit_price,
        'currency_id': fact_sales_order.currency_id,
        'design_id': fact_sales_order.design_id,
        'agreed_payment_date': fact_sales_order.agreed_payment_date,
        'agreed_delivery_date': fact_sales_order.agreed_delivery_date,
        'agreed_delivery_location_id': adl_id
    })

    new_key = key.split('/')[0]+"/fact_sales_order.csv"
    fact_sales_order_dict = {"Key": new_key, "Body": fact_sales_order}
    return fact_sales_order_dict
