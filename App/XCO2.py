import boto3
import os
import tempfile
import certifi
import ssl
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import cdsapi

# Configuración SSL
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
ssl_context = ssl.create_default_context(cafile=certifi.where())

# Carga de variables de entorno
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

# Configuración AWS
AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": "us-east-1"
}
BUCKET_NAME = "geltonas.tech"

# Verificación de credenciales
if not all(AWS_CONFIG.values()):
    raise ValueError("Error en credenciales AWS")

# Configuración de datasets (Basado en la imagen)
DATASET_CONFIG = {
    "XCO2": {
        "products": {
            "LVL2": [
                {"name": "TANSO-FTS_OCFP", "api_name": "tanso_fts_ocfp", "years": range(2017, 2022)},
                {"name": "TANSO2-FTS_SRFP", "api_name": "tanso2_fts_srmp", "years": range(2019, 2022)}
            ],
            "MERGED": [
                {"name": "MERGED_EMMA", "api_name": "merged_emma", "years": range(2017, 2022)},
                {"name": "MERGED_OBS4MIPS", "api_name": "merged_obs4mips", "years": range(2003, 2023)}
            ]
        }
    },
    "MidTropospheric_CO2": {
        "sensors": [
            {"name": "IASI_Metop-A_NLIS", "api_name": "iasi_metop_a_nlis", "years": range(2017, 2022)},
            {"name": "IASI_Metop-B_NLIS", "api_name": "iasi_metop_b_nlis", "years": range(2017, 2022)},
            {"name": "IASI_Metop-C_NLIS", "api_name": "iasi_metop_c_nlis", "years": range(2019, 2022)}
        ]
    }
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_cds_client():
    return cdsapi.Client(
        url='https://cds.climate.copernicus.eu/api/v2',
        verify=ssl_context
    )

def process_dataset():
    client = get_cds_client()
    s3 = boto3.client('s3', **AWS_CONFIG)

    # Procesar XCO2
    for product_type, products in DATASET_CONFIG["XCO2"]["products"].items():
        for product in products:
            for year in product["years"]:
                try:
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        request = {
                            'variable': 'column_average_dry_air_mole_fraction_of_atmospheric_carbon_dioxide',
                            'sensor_and_algorithm': product["api_name"],
                            'year': str(year),
                            'month': 'all',
                            'version': 'latest',
                            'format': 'zip'
                        }

                        client.retrieve('satellite-carbon-dioxide', request, tmp_file.name)
                        
                        if os.path.getsize(tmp_file.name) > 0:
                            s3_key = f"climate/XCO2/{product_type}/{product['name']}/{year}.zip"
                            s3.upload_file(tmp_file.name, BUCKET_NAME, s3_key)
                            print(f"✅ {product['name']} {year} subido")
                        else:
                            print(f"⚠️ Archivo vacío: {product['name']} {year}")

                except Exception as e:
                    print(f"❌ Error en {product['name']} {year}: {str(e)}")
                finally:
                    if os.path.exists(tmp_file.name):
                        os.remove(tmp_file.name)

    # Procesar MidTropospheric CO2
    for sensor in DATASET_CONFIG["MidTropospheric_CO2"]["sensors"]:
        for year in sensor["years"]:
            try:
                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                    request = {
                        'variable': 'mid_tropospheric_columns_of_atmospheric_carbon_dioxide',
                        'sensor_and_algorithm': sensor["api_name"],
                        'year': str(year),
                        'month': 'all',
                        'version': 'latest',
                        'format': 'zip'
                    }

                    client.retrieve('satellite-carbon-dioxide', request, tmp_file.name)
                    
                    if os.path.getsize(tmp_file.name) > 0:
                        s3_key = f"climate/MidTropospheric_CO2/{sensor['name']}/{year}.zip"
                        s3.upload_file(tmp_file.name, BUCKET_NAME, s3_key)
                        print(f"✅ {sensor['name']} {year} subido")
                    else:
                        print(f"⚠️ Archivo vacío: {sensor['name']} {year}")

            except Exception as e:
                print(f"❌ Error en {sensor['name']} {year}: {str(e)}")
            finally:
                if os.path.exists(tmp_file.name):
                    os.remove(tmp_file.name)

if __name__ == "__main__":
    process_dataset()
