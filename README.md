## Data-Dynamics ETL Platform


A data engineering application built in python that pipes structured data from a database into a data warehouse. The primary data source for the project is a moderately complex (but not very large) database called totesys which is meant to simulate the back end data of a commercial application. Data is inserted and updated into this database several times a day. After extraction, the process transforms the data to parquet files formatted for easy insertion to a data warehouse, using a second S3 bucket and finally uses the data to populate a star-schema data warehouse.

Authors:

https://github.com/jampen

https://github.com/SpinyKarma

https://github.com/JK-A2023

https://github.com/abumohamedfanan

https://github.com/robbfox

## Deployment

To deploy this project:


Clone:



```bash
  gh repo clone jampen/data-dynamics-de-project
  cd data-dynamics-de-project
```
Install requirements:

```bash
make create-environment
```
Move to Terraform folder and start:
```bash
cd terraform
terraform init 
```
Action terraform:
```bash
terraform plan
terraform apply
```

## Tech Stack

**Languages** python postgresql

**Data Tools** pg8000 boto3 pyarrow pandas

**Cloud Platform** Amazon Web Services


## Data Pipelines

Three main processes:

**Ingestion**   
    Uses AWS Eventbridge to action ```pg8000``` to read from the Postgres database every three minutes, and ```boto3``` to transfer data into an s3 bucket as .csv files

**Transform**   
    Takes the data from the .csv files as ```pandas``` dataframes, formats the data according to warehouse table-specifications, and adds this as .parquet files to a second S3 bucket

**Load**   
    Reads the .parquet files, and inserts the data into a star-schema data warehouse.
