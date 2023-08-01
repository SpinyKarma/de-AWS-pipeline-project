from ingestion_lambda import connect
import pg8000.native as pg
from pprint import pprint

with connect() as db:
    res = db.run(
        f'SELECT * FROM project_team_5;')
    cols = [item['name'] for item in db.columns]
    pprint(res)
    print(len(res))
