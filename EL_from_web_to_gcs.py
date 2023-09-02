from google.cloud import storage
import tempfile
import requests
import gzip


bucket_name = 'my-practce-bucket'

# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz

def web_to_gcs(year, service):
    """
    Download CSV files for a specific year and service and upload them to Google Cloud Storage bucket.

    Args:
        year (str): The year for which the files need to be downloaded.
        service (str): The service name for which the files need to be downloaded.

    Returns:
        None
    """

    for i in range(1,13):

        # Format the month with leading zero if necessary
        month = '0'+str(i)
        month = month[-2:]

        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"


        with tempfile.TemporaryDirectory() as tempdir:
            req_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{service}_tripdata_{year}-{month}.csv.gz"

            r = requests.get(req_url)

            file_path = f"{tempdir}/{file_name}"
            with open(file_path ,"wb") as file:
                file.write(r.content) 

            output_file = f"{service}_tripdata_{year}-{month}.csv"
        
            with gzip.open(file_path, 'rb') as f_in:
                with tempfile.NamedTemporaryFile(dir=tempfile.gettempdir(), delete=False) as temp_file:
                    temp_file.write(f_in.read())
                    temp_file_path = temp_file.name


                    #upload the files to GCS
                    storage_client = storage.Client()
                    bucket = storage_client.bucket(bucket_name)
                    
                    blob = bucket.blob(output_file)
                    blob.upload_from_filename(temp_file_path)
                    print(f"{i} {service} file successfully uploaded to Google Cloud Storage.")
    print("Success!")



# Specify the year and service
year = '2019'
service = ["green", "yellow", "fhv"]

# Call the function to download and upload files
for i in service:
    web_to_gcs(year, i)
