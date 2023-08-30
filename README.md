# NYC_taxi_data

### Brief Description
This project focuses on creating a simple data pipeline that will take data from a website, load into a data lake, and then transformed and loaded into a data warehouse. The data is extracted from a [github repo](https://github.com/DataTalksClub/nyc-tlc-data/), loaded into a GCS bucket, and transformed to generate analytics and insights from New York City Taxi data. Here's a brief overview of what each file does:

- EL_from_web_to_gcs.py : Extracts the data as a zipped csv file that is ziped with .gzip, uncompress the file and upload to the GCS bucket as a csv file.
- fhv_taxi_etl.py : Extracts the FHV(for-hire-vehicle) file from the GCS bucket, Transform and clean it, Load into BigQuery
- green_taxi_etl.py : Extracts the green taxi service file from the GCS bucket, Transform and clean it, Load into BigQuery
- yellow_taxi_etl.py : Extracts the yellow taxi service file from the GCS bucket, Transform and clean it, Load into BigQuery
- taxi_zone_extract.py : Extracts the taxi zone lookup file from the github repo, Unzip and Load into GCS bucket, Transform and clean it, Load into BigQuery
- NYC_Queries.sql : Sql queries that are used to gain some insights from the data.


## Programming/Query Language
Python

SQL

## Libraries
os

google

tempfile

pandas

requests

gzip




## Ideas
with more time, i would implement the following into the project
- move the project into a Docker image.
- Set up a Kubernetes cluster to automate the deployment, scaling, and management of the Docker container:
 Use a cloud provider (e.g., Amazon EKS, Google Kubernetes Engine)
