import boto3
import os
import tempfile
import time
import certifi
import ssl
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import cdsapi

# Configuración SSL
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
ssl_context = ssl.create_default_context(cafile=certifi.where())

# Cargar variables de entorno
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

# Configuración AWS
AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": "us-east-1"
}
BUCKET_NAME = "geltonas.tech"

# Configuración CDS
DATASET = "satellite-carbon-dioxide"
VARIABLES = {
    "MidTropospheric_CO2": {
        "sensors": ["iasi_metop_a_nlis", "iasi_metop_b_nlis", "iasi_metop_c_nlis"],
        "params": {
            "processing_level": "level_2",
            "variable": "mid_tropospheric_columns_of_atmospheric_carbon_dioxide",
            "version": "10.1",
            "month": ["01","02","03","04","05","06","07","08","09","10","11","12"]
        },
        "years": {
            "iasi_metop_a_nlis": range(2017, 2022),
            "iasi_metop_b_nlis": range(2017, 2022),
            "iasi_metop_c_nlis": range(2019, 2022)
        }
    },
    "XCO2": {
        "sensors": ["tanso_fts_ocfp", "tanso_fts_srfp", "tanso2_fts_srfp", "merged_emma"],
        "params": {
            "processing_level": "level_2",
            "variable": "column_average_dry_air_mole_fraction_of_atmospheric_carbon_dioxide",
            "version": "7.3",
            "month": ["01","02","03","04","05","06","07","08","09","10","11","12"]
        },
        "years": {
            "tanso_fts_ocfp": range(2017, 2022),
            "tanso_fts_srfp": range(2017, 2022),
            "tanso2_fts_srfp": range(2019, 2022),
            "merged_emma": range(2017, 2022)
        }
    }
}

client = cdsapi.Client()

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60))
def wait_for_job_completion(request):
    """Maneja reintentos inteligentes con backoff exponencial"""
    try:
        return client.retrieve(DATASET, request)
    except Exception as e:
        if "still running" in str(e).lower():
            print(f"🔄 Job en progreso. Reintentando en 60s...")
            time.sleep(60)
            raise
        raise

def upload_to_s3(temp_file_path, bucket, s3_key):
    """Sube archivo a S3 con verificación de tamaño"""
    s3 = boto3.client('s3', **AWS_CONFIG)
    
    if os.path.getsize(temp_file_path) > 1024:  # 1KB mínimo
        s3.upload_file(temp_file_path, bucket, s3_key)
        print(f"✅ Subido a s3://{bucket}/{s3_key}")
    else:
        print(f"⚠️ Archivo vacío o corrupto: {temp_file_path}")

def process_data():
    for var_name, var_config in VARIABLES.items():
        for sensor in var_config["sensors"]:
            for year in var_config["years"][sensor]:
                request = {
                    **var_config["params"],
                    "sensor_and_algorithm": sensor,
                    "year": str(year),
                    "format": "zip"
                }
                
                try:
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        print(f"🚀 Iniciando {sensor} {year}...")
                        
                        # Paso 1: Iniciar proceso
                        result = wait_for_job_completion(request)
                        
                        # Paso 2: Descargar datos
                        result.download(tmp_file.name)
                        print(f"📥 Descarga completada: {tmp_file.name}")
                        
                        # Paso 3: Subir a S3
                        s3_key = f"climate-data/{var_name}/{sensor}/{year}.zip"
                        upload_to_s3(tmp_file.name, BUCKET_NAME, s3_key)
                        
                except Exception as e:
                    print(f"❌ Error crítico en {sensor} {year}: {str(e)}")
                finally:
                    if os.path.exists(tmp_file.name):
                        os.remove(tmp_file.name)

if __name__ == "__main__":
    process_data()
