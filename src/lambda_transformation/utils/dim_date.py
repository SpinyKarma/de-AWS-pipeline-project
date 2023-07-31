import pandas as pd


def generate_dim_date():
    '''Generates a series of dates and expands them to the dim_date schema.

    Args:
        None

    Returns:
        dim_date_dict: a dict with the key of the dim_date file as its
        key and a pandas dataframe of the contents as the value.
    '''

    # Generate the range of dates to populate
    dates = pd.DataFrame(
        {"date_id": pd.date_range("2020-01-01", "2030-12-31")})

    dates['year'] = dates.date_id.dt.year
    dates['month'] = dates.date_id.dt.month
    dates['day'] = dates.date_id.dt.day
    dates['day_of_week'] = dates.date_id.dt.day_of_week
    dates['day_name'] = dates.date_id.dt.day_name()
    dates['month_name'] = dates.date_id.dt.month_name()
    dates['quarter'] = dates.date_id.dt.quarter

    dim_date_dict = {"dim_date.csv": dates}
    return dim_date_dict


# if __name__ == "__main__":
#     out = generate_dim_date()
#     print(out["dim_date.csv"])
