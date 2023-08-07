import pandas as pd


def design_to_dim_design(design_dict):
    '''Takes all designs from design csv and remaps to dim_design schema.

    Args:
        design_dict: a dict with two key/val pairs:
            "Key": the key of the design csv
            "Body": a panda dataframe of the csv contents.

    Returns:
        dim_design_dict: a dict with two key/val pairs:
            "Key": the key of the dim_design file
            "Body": a pandas dataframe of the dim_design contents.
    '''
    key = design_dict['Key']
    design = design_dict['Body']
    new_key = key.split('/')[0]+'/dim_design.csv'
    dim_design = design[['design_id',
                         'design_name',
                         'file_location',
                         'file_name']]

    dim_design_df = pd.DataFrame(dim_design)
    # make dict of key and body then return it
    dim_design_dict = {'Key': new_key, 'Body': dim_design_df}
    return dim_design_dict
