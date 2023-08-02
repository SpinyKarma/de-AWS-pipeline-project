import pytest
import pandas as pd
import src.lambda_transformation.utils.dim_fact_sales_order as sales
import src.table_utils.get_tables as tables

def test_sales_order_to_fact_sales_order_missing_columns():
    pass

if __name__ == '__main__':
    sales = tables.read_table('sales_order.csv')
    result = sales.sales_order_to_fact_sales_order(sales)
    print(result)