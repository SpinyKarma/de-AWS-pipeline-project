from ingestion_lambda import connect
from pprint import pprint

with connect() as db:
    res = db.run(
        "SELECT * FROM information_schema.tables WHERE table_schema is ;")
    cols = [item['name'] for item in db.columns]
    pprint(cols)
    print(len(res))
