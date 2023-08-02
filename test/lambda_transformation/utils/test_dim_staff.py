import src.lambda_transformation.utils.dim_staff as util
import src.table_utils.get_tables as tables


def create_template_dictionary():
    pass


def test_timestamp():
    pass


if __name__ == '__main__':
    staff = tables.get_table_contents('staff.csv')
    depts = tables.get_table_contents('department.csv')

    print(staff)
