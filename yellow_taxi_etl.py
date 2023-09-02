from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import numpy as np
import tempfile
import time


bq_client = bigquery.Client()
service = "yellow"
dataset_name = "alt"
project_id = "logical-craft-384210"
bucket_name = 'my-practce-bucket'


def Yellow():
    """
    ETL process for loading Yellow taxi trip data into BigQuery.
    Downloads CSV files from Google Cloud Storage, performs transformations, and loads data into BigQuery table.
    """
    
    # Define the schema for the BigQuery table
    schema = [
        bigquery.SchemaField("vendor_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("pickup_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("passengers", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("distance", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("rate_code_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("store_and_fwd_flag", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_location_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_location_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("payment_type", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("fare", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("extra", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("mta_tax", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("tip", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("tolls_amount", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("improvement_surcharge", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("total_amount", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("congestion_surcharge", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("airport_fee", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("season", "STRING", mode="NULLABLE")
    ]

    # Create the BigQuery table
    table_id = f"{project_id}.{dataset_name}.{service}_tripdata"
    table = bigquery.Table(table_id, schema=schema)
    table = bq_client.create_table(table)  # Make an API request.
    print( "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    # Initialize Google Cloud Storage client and bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Loop through each month and process the corresponding CSV file
    for i in ['01','02','03','04','05','06','07','08','09','10','11','12']:
        time.sleep(7)
        with tempfile.NamedTemporaryFile("w") as tempdir:
            destination_uri = tempdir.name
            file_name = f'{service}_tripdata_2019-{i}.csv'

            # Extract the file from Google Cloud Storage
            blob = bucket.blob(file_name)
            blob.download_to_filename(destination_uri)
            print(f"{i} {service} file successfully downloaded from Google Cloud Storage.")
            time.sleep(5)

            # Read the CSV file into a Pandas DataFrame
            df = pd.read_csv(destination_uri, low_memory=False)
            print(f"{i} csv file read into dataframe")

            # Replacing missing rows in the rate_code_id column with the most common rate code ID
            df['RatecodeID'] = df['RatecodeID'].fillna(df['RatecodeID'].mode()[0])

            # Split the dataframe into 5 dataframes
            dfs = np.array_split(df, 5)

            # Apply transformations and load each dataframe
            for j, df_part in enumerate(dfs):
                print(f"Processing dataframe {j+1}")

                # We will do some data cleaning 
                df_part = df_part.rename(index=str, columns={'VendorID' : 'vendor_id',\
                        'tpep_pickup_datetime' : 'pickup_time', \
                        'tpep_dropoff_datetime' : 'dropoff_time', \
                        'passenger_count' : 'passengers', \
                        'RatecodeID' : 'rate_code_id', \
                        'PULocationID' : 'pickup_location_id', \
                        'DOLocationID' : 'dropoff_location_id', \
                        'trip_distance' : 'distance', \
                        'fare_amount' : 'fare', \
                        'tip_amount' : 'tip'})

                # Remove duplicates - first step is to drop generic duplicates 
                df_part = df_part.drop_duplicates()

                # Dropping all rows with zero in the distance, fare, total_amount and improvement charge column
                df_part = df_part[df_part['distance'] > 0]
                df_part = df_part[df_part['fare'] > 0]
                
                
                # change to python string for conversion
                df_part['pickup_time'] = df_part['pickup_time'].astype('str')
                df_part['dropoff_time'] = df_part['dropoff_time'].astype('str')

                # Categorize trips into seasons
                conditions = [
                    (df_part['pickup_time'].between('2018-12-01 00:00:00', '2019-02-28 23:59:59')) &
                    (df_part['dropoff_time'].between('2018-12-01 00:00:00', '2019-02-28 23:59:59')),
                    (df_part['pickup_time'].between('2019-03-01 00:00:00', '2019-05-31 23:59:59')) &
                    (df_part['dropoff_time'].between('2019-03-01 00:00:00', '2019-05-31 23:59:59')),
                    (df_part['pickup_time'].between('2019-06-01 00:00:00', '2019-08-31 23:59:59')) &
                    (df_part['dropoff_time'].between('2019-06-01 00:00:00', '2019-08-31 23:59:59')),
                    (df_part['pickup_time'].between('2019-09-01 00:00:00', '2019-11-30 23:59:59')) &
                    (df_part['dropoff_time'].between('2019-09-01 00:00:00', '2019-11-30 23:59:59')),
                    (df_part['pickup_time'].between('2019-12-01 00:00:00', '2019-12-31 23:59:59')) &
                    (df_part['dropoff_time'].between('2019-12-01 00:00:00', '2019-12-31 23:59:59'))
                ]
                values = ['winter','spring','summer', 'autumn', 'winter_0']
                df_part['season'] = np.select(conditions, values)
                df_part['season'] = df_part['season'].replace(['winter_0'], 'winter')

                # change to pandas date time for conversion
                df_part['pickup_time'] = pd.to_datetime(df_part['pickup_time'])
                df_part['dropoff_time'] = pd.to_datetime(df_part['dropoff_time'])


                # Set up a job configuration
                table_id = f"{project_id}.{dataset_name}.{service}_tripdata"
                job_config = bigquery.LoadJobConfig(autodetect=False)

                # Submit the job
                job = bq_client.load_table_from_dataframe(df_part, table_id, job_config=job_config)  

                # Wait for the job to complete and then show the job results
                job.result()  
                
                # Read back the properties of the table
                table = bq_client.get_table(table_id)  
                print("Table:", table.table_id, "has", table.num_rows, "rows and", len(table.schema), "columns")
                print(f"Dataframe {j+1} loaded")
            print("JOB SUCCESSFUL")


Yellow()




