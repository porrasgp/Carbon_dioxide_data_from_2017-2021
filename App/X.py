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

# Configuración CDS para MidTropospheric_CO2
DATASET = "satellite-carbon-dioxide"
VAR_NAME = "MidTropospheric_CO2"
SENSOR = "iasi_metop_a_nlis"
YEAR = "2021"  # Año de prueba

# Parámetros asociados a MidTropospheric_CO2
PARAMS = {
    "processing_level": "level_2",
    "variable": "mid_tropospheric_columns_of_atmospheric_carbon_dioxide",
    "version": "10.1",
    "month": ["01","02","03","04","05","06","07","08","09","10","11","12"],
    "day": ["01", "02", "03","04", "05", "06","07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18",
        "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"]
}

client = cdsapi.Client()

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60))
def wait_for_job_completion(request):
    """Maneja reintentos inteligentes con backoff exponencial."""
    try:
        return client.retrieve(DATASET, request)
    except Exception as e:
        if "still running" in str(e).lower():
            print("🔄 Job en progreso. Reintentando en 60s...")
            time.sleep(60)
            raise
        raise

def upload_to_s3(temp_file_path, bucket, s3_key):
    """Sube archivo a S3 con verificación de tamaño."""
    s3 = boto3.client('s3', **AWS_CONFIG)
    if os.path.getsize(temp_file_path) > 1024:  # Verifica que el archivo tenga al menos 1KB
        s3.upload_file(temp_file_path, bucket, s3_key)
        print(f"✅ Subido a s3://{bucket}/{s3_key}")
    else:
        print(f"⚠️ Archivo vacío o corrupto: {temp_file_path}")

def process_data():
    # Construir la solicitud para el sensor y año de prueba
    request = {
        **PARAMS,
        "sensor_and_algorithm": SENSOR,
        "year": YEAR,
        "format": "zip"
    }
    
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            print(f"🚀 Iniciando descarga para {SENSOR} en {YEAR}...")
            
            # Paso 1: Iniciar proceso con CDS
            result = wait_for_job_completion(request)
            
            # Paso 2: Descargar datos al archivo temporal
            result.download(tmp_file.name)
            print(f"📥 Descarga completada: {tmp_file.name}")
            
            # Paso 3: Subir el archivo a S3
            s3_key = f"climate-data/{VAR_NAME}/{SENSOR}/{YEAR}.zip"
            upload_to_s3(tmp_file.name, BUCKET_NAME, s3_key)
            
    except Exception as e:
        print(f"❌ Error crítico en {SENSOR} {YEAR}: {str(e)}")
    finally:
        if os.path.exists(tmp_file.name):
            os.remove(tmp_file.name)
            print(f"🗑️ Archivo temporal eliminado: {tmp_file.name}")

if __name__ == "__main__":
    process_data()
