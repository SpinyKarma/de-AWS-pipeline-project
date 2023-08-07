# Python Source Directory

Here we carpenmentalize our lambda functions.

| Lambda Name       | Description                                                                                       |
| ----------------- |---------------------------------------------------------------------------------------------------|
| Ingestion         | Pulls the latest data from the totesys database into an S3 bucket of raw data, to be processed later. |
| Transformation    | Processes the ingested data into dimensional tables, populating an S3 bucket of transformed data .|
| Transformation II | TO BE FILLED IN        |
| Loading           | Deploys the transformed data to our data-warehouse        |
