from src.lambda_transformation.utils.dim_date import generate_dim_date as gdd
from datetime import datetime as dt
import pandas as pd


def test_output_has_required_columns():
    output = gdd()
    output_cols = list(output['Body'].columns)
    expected_dim_date_cols = [
        "date_id",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "quarter"
    ]
    assert output_cols == expected_dim_date_cols


def test_first_and_last_two_rows_have_proper_output_assume_rest_follow_suit():
    output = gdd()['Body']
    output_row_0 = output.loc[0, :].values.flatten().tolist()
    output_row_1 = output.loc[1, :].values.flatten().tolist()
    output_row_minus_1 = output.loc[7304, :].values.flatten().tolist()
    output_row_minus_2 = output.loc[7303, :].values.flatten().tolist()
    expected_row_0 = [
        dt(2020, 1, 1),
        2020,
        1,
        1,
        2,
        "Wednesday",
        "January",
        1
    ]
    expected_row_1 = [
        dt(2020, 1, 2),
        2020,
        1,
        2,
        3,
        "Thursday",
        "January",
        1
    ]
    expected_row_minus_1 = [
        dt(2039, 12, 31),
        2039,
        12,
        31,
        5,
        "Saturday",
        "December",
        4
    ]
    expected_row_minus_2 = [
        dt(2039, 12, 30),
        2039,
        12,
        30,
        4,
        "Friday",
        "December",
        4
    ]
    assert output_row_0 == expected_row_0
    assert output_row_1 == expected_row_1
    assert output_row_minus_1 == expected_row_minus_1
    assert output_row_minus_2 == expected_row_minus_2
