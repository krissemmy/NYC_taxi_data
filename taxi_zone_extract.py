from google.cloud import storage
from google.cloud import bigquery
import requests
import pandas as pd
import numpy as np
import tempfile


url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
bucket_name = "my-practce-bucket"
file_name = "taxi_zone_lookup.csv"
dataset_name = "alt"
project_id = "logical-craft-384210"

bq_client = bigquery.Client()
schema = [
    bigquery.SchemaField("location_id", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("borough", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("zone", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("service_zone", "STRING", mode="NULLABLE")
]

table_id = f"{project_id}.{dataset_name}.taxi_zone"
table = bigquery.Table(table_id, schema=schema)
table = bq_client.create_table(table)  # Make an API request.
print( "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


with tempfile.TemporaryDirectory() as tempdir:   
    # Download the CSV file
    r = requests.get(url)

    file_path = f"{tempdir}/{file_name}"
    open(file_path ,"wb").write(r.content)
    
    #upload the files to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_path)
    print(f"{file_name} file successfully uploaded to Google Cloud Storage.")

    df = pd.read_csv(file_path)
    print(df.head())

    df = df.rename(index=str, columns={'LocationID' : 'location_id',\
            'Borough' : 'borough', \
            'Zone' : 'zone', \
            'service zone' : 'service_zone', \
            })

    df.drop_duplicates(inplace=True)
    print(f"After dropping duplicates, the dataframe is left with {df.shape[0]} rows")


    # Set up a job configuration
    job_config = bigquery.LoadJobConfig(autodetect=False)
    
    # Submit the job
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)  

    # Wait for the job to complete and then show the job results
    job.result()  
    
    # Read back the properties of the table
    table = bq_client.get_table(table_id)  
    print("Table:", table.table_id, "has", table.num_rows, "rows and", len(table.schema), "columns")
    print("JOB SUCCESSFUL")
