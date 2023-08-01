


def design_to_dim_design(design_dict):
    key = design_dict['Key']
    design = design_dict['Body']
    new_key = key.split('/')[0]+'/dim_design.csv'
    dim_design = design[['design_id',
                         'design_name',
                         'file_location',
                         'file_name']]
    
    dim_design_dict = {'Key': new_key, 'Body': dim_design}
    return dim_design_dict




    
    
    
    




