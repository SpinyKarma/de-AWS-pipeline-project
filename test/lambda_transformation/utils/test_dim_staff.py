import src.lambda_transformation.utils.dim_staff as util
import src.table_utils.get_tables as tables


def test_timestamp_is_preserved():
    staff = tables.read_table('staff.csv')
    depts = tables.read_table('department.csv')
    result = util.create_dim_staff_csv(staff, depts)

    # Get the most recent timestamp
    recent_timestamp = [
        staff['Timestamp'],
        depts['Timestamp']
    ]
    recent_timestamp.sort(reverse=True)
    recent_timestamp = recent_timestamp[0]

    result_timestamp = result['Timestamp']

    assert recent_timestamp == result_timestamp


test_timestamp_is_preserved()
