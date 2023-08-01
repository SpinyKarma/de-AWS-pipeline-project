from ingestion_lambda import connect

with connect() as db:
    res = db.run("SELECT * FROM project_user_5;")
    print(res)
