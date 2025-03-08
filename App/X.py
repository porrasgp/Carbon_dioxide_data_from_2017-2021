import boto3
import os
import tempfile
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import cdsapi

# Configuración inicial
load_dotenv()  # Carga las variables de AWS desde .env

# Credenciales AWS (NO las hardcodees)
AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": "us-east-1"
}
BUCKET_NAME = "geltonas.tech"

# Verificar credenciales
if not all(AWS_CONFIG.values()):
    raise ValueError("❌ Faltan credenciales AWS en el .env")

# Configuración CDS
CDS_REQUEST = {
    "dataset": "satellite-carbon-dioxide",
    "variable": "mid_tropospheric_columns_of_atmospheric_carbon_dioxide",
    "processing_level": "level_2",
    "sensor_and_algorithm": "iasi_metop_a_nlis",
    "year": "2019",
    "month": "01",
    "day": ["01", "02", "03"],
    "version": "10_1",
    "format": "zip"
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def download_and_upload():
    # Configurar clientes
    s3 = boto3.client('s3', **AWS_CONFIG)
    cds_client = cdsapi.Client()

    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            # 1. Descargar de Copernicus
            print("⬇️ Descargando datos...")
            cds_client.retrieve(CDS_REQUEST["dataset"], CDS_REQUEST, tmp_file.name)
            
            # 2. Subir a S3
            s3_key = (
                f"climate-data/{CDS_REQUEST['sensor_and_algorithm']}/"
                f"{CDS_REQUEST['year']}/{CDS_REQUEST['month']}.zip"
            )
            
            print(f"📤 Subiendo a S3: s3://{BUCKET_NAME}/{s3_key}")
            s3.upload_file(tmp_file.name, BUCKET_NAME, s3_key)
            
            print("✅ ¡Operación completada con éxito!")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise
    finally:
        # Limpiar archivo temporal
        if os.path.exists(tmp_file.name):
            os.remove(tmp_file.name)

if __name__ == "__main__":
    download_and_upload()
