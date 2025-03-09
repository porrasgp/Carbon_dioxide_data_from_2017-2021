import boto3
import os
import tempfile
import time
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import cdsapi
import certifi
import ssl

# Configuración SSL global
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
ssl_context = ssl.create_default_context(cafile=certifi.where())

# Cargar variables de entorno
load_dotenv()

# Configuración AWS
AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": "us-east-1"
}
BUCKET_NAME = "geltonas.tech"

# Validar credenciales
if not all(AWS_CONFIG.values()):
    raise ValueError("🚨 Faltan credenciales AWS en el archivo .env")

# Configuración CDS
DATASET = "satellite-carbon-dioxide"
SENSOR_CONFIG = {
    "iasi_metop_a_nlis": {
        "variable": "mid_tropospheric_columns_of_atmospheric_carbon_dioxide",
        "years": ["2017"],
        "version": "10_1"
    }
}

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=30, max=120))
def download_data(client, request):
    """Maneja la descarga con reintentos inteligentes"""
    try:
        return client.retrieve(DATASET, request)
    except Exception as e:
        if "still running" in str(e).lower():
            print("🔄 El proceso sigue activo, reintentando en 60 segundos...")
            time.sleep(60)
            raise
        raise

def upload_to_s3(temp_file_path, s3_key):
    """Sube el archivo a S3 con verificación"""
    s3 = boto3.client('s3', **AWS_CONFIG)
    
    if os.path.getsize(temp_file_path) > 1024:  # 1KB mínimo
        s3.upload_file(temp_file_path, BUCKET_NAME, s3_key)
        print(f"✅ Subido a s3://{BUCKET_NAME}/{s3_key}")
    else:
        print(f"⚠️ Archivo vacío: {temp_file_path}")

def process_year(year):
    """Procesa un año específico"""
    client = cdsapi.Client()
    
    request = {
        "processing_level": ["level_2"],
        "variable": "co2" ,
        "sensor_and_algorithm": "iasi_metop_a_nlis",
        "year": ["2017","2018","2019","2020","2021",],
        "month": ["01"],
        "day": ["01", "02", "03","04", "05", "06","07", "08", "09","10", "11", "12",
        "13", "14", "15","16", "17", "18","19", "20", "21","22", "23", "24","25", "26", "27",
        "28", "29", "30","31"],
        "version": ["10_1"],
        "format": "zip"
    }

  with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        try:
            print(f"\n📅 Procesando año {year}...")
            
            # Paso 1: Iniciar descarga
            result = download_data(client, request)
            
            # Paso 2: Descargar datos
            result.download(tmp_file.name)
            print(f"✅ Datos de {year} descargados")
            
            # Paso 3: Subir a S3 con estructura de carpetas
            s3_key = f"climate-data/iasi_metop_a/{year}/datos_completos.zip"
            upload_to_s3(tmp_file.name, s3_key)
            print(f"📂 Carpeta creada en S3: s3://{BUCKET_NAME}/{s3_key}")
            
        except Exception as e:
            print(f"❌ Error en {year}: {str(e)}")
            raise
        finally:
            if os.path.exists(tmp_file.name):
                os.remove(tmp_file.name)

if __name__ == "__main__":
    print("⚡ Iniciando proceso de organización por años")
    for year in SENSOR_CONFIG["iasi_metop_a_nlis"]["years"]:
        process_year(year)
    print("\n🎉 Estructura en S3 creada exitosamente!")
    print("   s3://geltonas.tech/climate-data/iasi_metop_a/")
    print("   ├── 2017/datos_completos.zip")
    print("   ├── 2018/datos_completos.zip")
    print("   └── ...")
