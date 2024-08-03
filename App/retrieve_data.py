import os
import cdsapi
import boto3
import io
from dotenv import load_dotenv

# Load environment variables (only needed if running locally with a .env file)
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = "geltonas.tech"

dataset = "sis-agroproductivity-indicators"
request = {
    'product_family': ['crop_productivity_indicators'],
    'variable': ['total_above_ground_production'],
    'crop_type': ['maize'],
    'year': '2023',
    'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
    'day': ['10', '20', '28', '30', '31'],
    'growing_season': ['1st_season_per_campaign'],
    'harvest_year': '2023'
}

client = cdsapi.Client()

# In-memory buffer
buffer = io.BytesIO()

try:
    # Retrieve and download data directly into the buffer
    client.retrieve(dataset, request).download(buffer)
    buffer.seek(0)  # Move to the start of the buffer

    # Upload the buffer to S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    s3_key = 'App/Data/data.zip'
    s3_client.upload_fileobj(buffer, BUCKET_NAME, s3_key)
    print(f"File uploaded to S3 bucket {BUCKET_NAME} with key {s3_key}")

except Exception as e:
    print(f"Error: {e}")
