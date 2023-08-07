# Python Source Directory

Here we carpenmentalize our lambda functions.

`convertor.py` is a utility for creating a JSON representation of database credentials.

| Lambda Name (s)       | Description                                                                                       |
| ----------------- |---------------------------------------------------------------------------------------------------|
| Ingestion         | Pulls the latest data from the totesys database into an S3 bucket of raw data, to be processed later. |
| Transformation (stage 1 and 2)    | Processes the ingested data into dimensional tables, populating an S3 bucket of transformed data .|
| Loading           | Deploys the transformed data to our data-warehouse        |