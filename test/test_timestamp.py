
@mock_s3
@patch('src.lambda_ingestion.ingestion.connect')
def test_new_csvdata_appended_to_s3_csv(mock_connection):
    expected = "transaction_id,transaction_type,last_updated\r\n"
    expected += f"434,SALE,{current_timestamp}\r\n"
    expected += f"435,PURCHASE,{current_timestamp}\r\n"
    expected += f"436,SALE,{current_timestamp}\r\n"
    expected += f"437,PURCHASE,{current_timestamp}\r\n"
    fakecsv = expected
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket='test')
    s3_client.put_object(
        Body=fakecsv,
        Bucket='test',
        Key='fake.csv',
        ContentType='application/text',
    )
    mock_db = Mock()
    mock_connection.return_value.__enter__.return_value = mock_db
    new_timestamp = dt.now().isoformat()
    mock_db.run.return_value = [
        ["438", "SALE", new_timestamp],
    ]
    mock_db.columns = [{'name': "transaction_id"}, {
        'name': "transaction_type"}, {'name': "last_updated"}]
    postgres_to_csv('test', ['fake'])
    expected += f"438,SALE,{new_timestamp}\r\n"
    response = s3_client.get_object(Bucket='test', Key='fake.csv')
    returndata = response.get('Body')
    content_str = returndata.read().decode()
    assert content_str == expected