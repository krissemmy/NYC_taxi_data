from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import numpy as np
import tempfile


service = "fhv"
dataset_name = "alt"
project_id = "logical-craft-384210"
bucket_name = 'my-practce-bucket'
bq_client = bigquery.Client()


def FHV():
    """
    ETL process for loading for hire vehicle(fhv) taxi trip data into BigQuery.
    Downloads CSV files from Google Cloud Storage, performs transformations, and loads data into BigQuery table.
    """

    # Define the schema for the BigQuery table
    schema = [
        bigquery.SchemaField("dispatching_base_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_time", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_time", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_location_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_location_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("affiliated_base_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("season", "STRING", mode="NULLABLE"),
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
        with tempfile.NamedTemporaryFile("w") as tempdir:
            destination_uri = tempdir.name

            file_name = f'{service}_tripdata_2019-{i}.csv'


            # extract the file from Google Cloud Storage
            blob = bucket.blob(file_name)
            blob.download_to_filename(destination_uri)

            # Read the CSV file into a Pandas DataFrame
            df = pd.read_csv(destination_uri)
            print(f"{i} parquet file read into dataframe")

            # Split the dataframe into 5 dataframes
            dfs = np.array_split(df, 5)

            # Apply transformations and load each dataframe
            for i, df_part in enumerate(dfs):
                print(f"Processing dataframe {i+1}")
                
                # Data cleaning
                df_part = df_part.rename(columns={'dispatching_base_num': 'dispatching_base_id',
                                                'pickup_datetime': 'pickup_time',
                                                'dropOff_datetime': 'dropoff_time',
                                                'PUlocationID': 'pickup_location_id',
                                                'DOlocationID': 'dropoff_location_id',
                                                'SR_Flag': 'special_request_flag',
                                                'Affiliated_base_number': 'affiliated_base_id'})

                df_part = df_part.drop_duplicates()
                df_part = df_part.drop(['special_request_flag'], axis=1)

                # change to python string for further exploration and conversion
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
                values = ['winter', 'spring', 'summer', 'autumn', 'winter']
                df_part['season'] = np.select(conditions, values, default='Unknown')
                
                # Load dataframe into BigQuery
                table_id = f"{project_id}.{dataset_name}.{service}_tripdata"
                job_config = bigquery.LoadJobConfig(autodetect=False)
                job = bq_client.load_table_from_dataframe(df_part, table_id, job_config=job_config)
                job.result()
                print(f"Dataframe {i+1} loaded")

            # Read back the properties of the table
            table = bq_client.get_table(table_id)
            print("Table:", table.table_id, "has", table.num_rows, "rows and", len(table.schema), "columns")
            print("JOB SUCCESSFUL")



FHV()
