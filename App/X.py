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

# Validar credenciales AWS
if not all(AWS_CONFIG.values()):
    raise ValueError("🚨 Faltan credenciales AWS en el archivo .env")

# Configuración CDS y request template
DATASET = "satellite-carbon-dioxide"
request_template = {
        "processing_level": "level_2",
        "variable": "co2",
        "sensor_and_algorithm": "iasi_metop_a_nlis",
        "year":  ["2017","2018","2019","2020","2021"],
        "month": ["01"],
        "day": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
                "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
                "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"],
        "version": "10_1",
        "format": "zip"
    }
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=30, max=120))
def download_data(client, request):
    """Maneja la descarga con reintentos inteligentes."""
    try:
        return client.retrieve(DATASET, request)
    except Exception as e:
        if "still running" in str(e).lower():
            print("🔄 El proceso sigue activo, reintentando en 60 segundos...")
            time.sleep(60)
            raise
        raise

def upload_to_s3(temp_file_path, s3_key):
    """Sube el archivo a S3 con verificación."""
    s3 = boto3.client('s3', **AWS_CONFIG)
    if os.path.getsize(temp_file_path) > 1024:  # Verifica que el archivo tenga al menos 1KB
        s3.upload_file(temp_file_path, BUCKET_NAME, s3_key)
        print(f"✅ Subido a s3://{BUCKET_NAME}/{s3_key}")
    else:
        print(f"⚠️ Archivo vacío: {temp_file_path}")

def process_year(year, template):
    """Procesa la descarga y subida para un año específico utilizando el template."""
    client = cdsapi.Client()
    # Copia el template y reemplaza el parámetro "year" por el año actual (en lista)
    request = template.copy()
    request["year"] = [year]
    
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        try:
            print(f"\n📅 Procesando año {year}...")
            
            # Paso 1: Iniciar descarga desde CDS
            result = download_data(client, request)
            
            # Paso 2: Descargar datos en un archivo temporal
            result.download(tmp_file.name)
            print(f"✅ Datos de {year} descargados en {tmp_file.name}")
            
            # Paso 3: Subir el archivo a S3 en la ruta deseada
            s3_key = f"climate-data/iasi_metop_a/{year}/datos_completos.zip"
            upload_to_s3(tmp_file.name, s3_key)
            print(f"📂 Archivo subido a S3: s3://{BUCKET_NAME}/{s3_key}")
            
        except Exception as e:
            print(f"❌ Error en el procesamiento del año {year}: {str(e)}")
            raise
        finally:
            if os.path.exists(tmp_file.name):
                os.remove(tmp_file.name)
                print(f"🗑️ Archivo temporal eliminado: {tmp_file.name}")

if __name__ == "__main__":
    print("⚡ Iniciando proceso de organización por años")
    for year in request_template["year"]:
        process_year(year, request_template)
    print("\n🎉 Estructura en S3 creada exitosamente!")
    print("   s3://geltonas.tech/climate-data/iasi_metop_a/")
    print("   ├── 2017/datos_completos.zip")
    print("   ├── 2018/datos_completos.zip")
    print("   ├── 2019/datos_completos.zip")
    print("   ├── 2020/datos_completos.zip")
    print("   └── 2021/datos_completos.zip")
