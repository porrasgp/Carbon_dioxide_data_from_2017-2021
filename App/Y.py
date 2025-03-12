import boto3
import os
import tempfile
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import cdsapi
import certifi
import ssl

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

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

# Inicializar cliente S3
s3_client = boto3.client('s3', **AWS_CONFIG)

# Definir DATASET (ajustar según el dataset deseado)
DATASET = os.getenv("CDS_DATASET", "satellite-carbon-dioxide")

# Configuración centralizada de sensores
SENSORS_CONFIG = {
    "IASI_Metop-A_NLIS": {
        "variable": "co2",
        "sensor": "iasi_metop_a_nlis",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "10_1",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],  # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]        # "01" a "31"
    },
    "IASI_Metop-B_NLIS": {
        "variable": "co2",
        "sensor": "iasi_metop_b_nlis",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "10_1",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],     # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]        # "01" a "31"
    },
    "IASI_Metop-C_NLIS": {
        "variable": "co2",
        "sensor": "iasi_metop_c_nlis",
        "years": ["2019", "2020", "2021"],
        "version": "10_1",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],  # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]        # "01" a "31"
    },
    "TANSO-FTS_OCFP": {
        "variable": "xco2",
        "sensor": "tanso_fts_ocfp",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "7_3",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],  # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]        # "01" a "31"
    },
    "TANSO-FTS_SRFP": {
        "variable": "xco2",
        "sensor": "tanso_fts_srfp",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "2_3_8",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],  # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]        # "01" a "31"
    },
    "TANSO2-FTS_SRFP": {
        "variable": "xco2",
        "sensor": "tanso2_fts_srfp",
        "years": ["2019", "2020", "2021"],
        "version": "2_1_0",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],  # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]        # "01" a "31"
    },
    "MERGED_EMMA": {
        "variable": "xco2",
        "sensor": "merged_emma",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "4_5",
        "level": "level_3",
        "months": ["{:02d}".format(m) for m in range(1, 13)],  # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]        # "01" a "31"
    },
    "MERGED_OBS4MIPS": {
        "variable": "xco2",
        "sensor": "merged_obs4mips",
        "years": [str(y) for y in range(2003, 2022)],  # Desde 2003 hasta 2021
        "version": "4_5",
        "level": "level_3",
        "months": ["{:02d}".format(m) for m in range(1, 13)],  # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]        # "01" a "31"
    }
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def download_data(client, request):
    """
    Descarga datos utilizando la API de CDS.
    Se aplica reintento en caso de fallo.
    """
    logger.info(f"Iniciando descarga para la solicitud: {request}")
    result = client.retrieve(request["dataset"], request)
    return result

def upload_to_s3(file_path, s3_key):
    """
    Sube el archivo ubicado en file_path a S3 utilizando s3_key como ruta.
    """
    try:
        s3_client.upload_file(file_path, BUCKET_NAME, s3_key)
        logger.info(f"Archivo subido exitosamente a s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        logger.error(f"Error al subir archivo a S3: {e}")
        raise

def process_sensor(sensor_name, config):
    """
    Procesa un sensor específico utilizando su configuración.
    Itera por cada año, descarga los datos y los sube a S3.
    """
    client = cdsapi.Client()  # Instancia del cliente de CDS
    
    for year in config["years"]:
        # Construir la solicitud para el sensor y año en curso
        request = {
            "dataset": DATASET,
            "processing_level": config["level"],
            "variable": config["variable"],
            "sensor_and_algorithm": config["sensor"],
            "year": year,
            "month": config["months"],
            "day": config["days"],
            "version": config["version"],
            "format": "zip"
        }
        
        # Crear archivo temporal para almacenar la descarga
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_file:
            tmp_file_path = tmp_file.name
        
        try:
            logger.info(f"🛰️ Procesando {sensor_name} - {year}")
            result = download_data(client, request)
            result.download(tmp_file_path)
            
            s3_key = f"climate-data/{sensor_name}/{year}/data.zip"
            upload_to_s3(tmp_file_path, s3_key)
        except Exception as e:
            logger.error(f"⛔ Error crítico en {sensor_name} {year}: {e}")
        finally:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
                logger.info(f"Archivo temporal {tmp_file_path} eliminado.")

def main():
    logger.info("🚀 Iniciando pipeline unificado de CDS a S3")
    for sensor_name, config in SENSORS_CONFIG.items():
        logger.info(f"\n{'═'*50}\n🔧 Procesando sensor: {sensor_name}\n{'═'*50}")
        process_sensor(sensor_name, config)
    
    logger.info("✅ Todos los sensores procesados exitosamente!")
    logger.info(f"Estructura final en S3: s3://{BUCKET_NAME}/climate-data/")

if __name__ == "__main__":
    main()
