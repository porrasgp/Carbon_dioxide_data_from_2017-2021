import boto3
import os
import tempfile
from dotenv import load_dotenv
import cdsapi

#Load environment variables (only needed if running locally with a .env file)
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

# Antes de llamar a la API de Copernicus
from cdsapi import Client
client = Client()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = "us-east-1"
BUCKET_NAME = "geltonas.tech"

# Ensure AWS credentials and region are correctly set
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_REGION:
    raise ValueError("AWS credentials or region are not set properly.")

client = cdsapi.Client()

# Define years to process
years = [
    '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', 
    '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
    '2018', '2019', '2020', '2021', '2022'
]

# Define the variables to retrieve
variables = {
    'mid_tropospheric_columns_of_atmospheric_carbon_dioxide': 'MidTropospheric_CO2',
    'column_average_dry_air_mole_fraction_of_atmospheric_carbon_dioxide': 'XCO2'
}

sensor_and_algorithm_list = ['airs_nlis', 'iasi_metop_a_nlis', 
                'iasi_metop_b_nlis', 'iasi_metop_c_nlis', 
                'sciamachy_wfmd', 'sciamachy_besd', 
                'tanso_fts_ocfp', 'tanso_fts_srmp', 
                'tanso2_fts_srmp','merged_emma', 
                'merged_obs4mips'
            ]
# Process each variable and year in batches
for var, var_name in variables.items():
        request = {
            'processing_level': ['level_2', 'level_3'],
            'variable': [var],
            'sensor_and_algorithm': sensor_and_algorithm_list,
            'year': [years],
            'version': ['latest'],
            'data_format': 'zip'
        }

        # Temporary File method
        try:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file_path = temp_file.name
                print(f"Retrieving data for {var_name} in {years}...")

                # Retrieve data and save it to the temporary file
                response = client.retrieve("satellite-carbon-dioxide", request)
                response.download(temp_file_path)
                
                # Check if file is empty
                if os.path.getsize(temp_file_path) > 0:
                    # Upload the temporary file to S3
                    s3_client = boto3.client(
                        's3',
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name=AWS_REGION
                    )

                    s3_key = f"{year}/{var_name}.zip"
                    s3_client.upload_file(temp_file_path, BUCKET_NAME, s3_key)
                    print(f"File uploaded to S3 bucket {BUCKET_NAME} with key {s3_key}")
                else:
                    print(f"No data retrieved for {var_name} in {year}. No folder created.")

        except Exception as e:
            print(f"Error processing {var_name} for {years}: {e}")

        finally:
            # Clean up the temporary file
            try:
                os.remove(temp_file_path)
            except:
                pass
