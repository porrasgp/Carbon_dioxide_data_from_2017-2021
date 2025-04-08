import os
import tempfile
import ssl
import certifi
import concurrent.futures
import boto3
from dotenv import load_dotenv
import cdsapi
from tenacity import retry, stop_after_attempt, wait_exponential

# Configuración SSL para entorno problemático
os.environ["REQUESTS_CA_BUNDLE"] = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cacert.pem')

# Configuración SSL global
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

# Validación de credenciales AWS
if not all(AWS_CONFIG.values()):
    raise ValueError("🚨 Credenciales AWS incompletas - Verifica .env o secrets de GitHub")

# Lista completa de solicitudes validadas
REQUESTS = [
    # Mid-tropospheric CO2 (IASI)
    {
        "dataset": "satellite-carbon-dioxide",
        "variable": "mid_tropospheric_columns_of_atmospheric_carbon_dioxide",
        "processing_level": "level_2",
        "sensor_and_algorithm": "iasi_metop_a_nlis",
        "year": ["2017","2018","2019","2020","2021"],
        "month": ["01","02","03","04","05","06","07","08","09","10","11","12"],
        "day": ["01","02","03","04","05","06","07","08","09","10","11","12","13",
                "14","15","16","17","18","19","20","21","22","23","24","25","26",
                "27","28","29","30","31"],
        "version": "10_1",
        "format": "zip"
    },
    {
        "dataset": "satellite-carbon-dioxide",
        "variable": "mid_tropospheric_columns_of_atmospheric_carbon_dioxide",
        "processing_level": "level_2",
        "sensor_and_algorithm": "iasi_metop_b_nlis",
        "year": ["2017","2018","2019","2020","2021"],
        "month": ["01","02","03","04","05","06","07","08","09","10","11","12"],
        "day": ["01","02","03","04","05","06","07","08","09","10","11","12","13",
                "14","15","16","17","18","19","20","21","22","23","24","25","26",
                "27","28","29","30","31"],
        "version": "10_1",
        "format": "zip"
    },
    {
        "dataset": "satellite-carbon-dioxide",
        "variable": "mid_tropospheric_columns_of_atmospheric_carbon_dioxide",
        "processing_level": "level_2",
        "sensor_and_algorithm": "iasi_metop_c_nlis",
        "year": ["2019","2020","2021"],
        "month": ["01","02","03","04","05","06","07","08","09","10","11","12"],
        "day": ["01","02","03","04","05","06","07","08","09","10","11","12","13",
                "14","15","16","17","18","19","20","21","22","23","24","25","26",
                "27","28","29","30","31"],
        "version": "10_1",
        "format": "zip"
    },

    # XCO2 Level 2
    {
        "dataset": "satellite-carbon-dioxide",
        "variable": "column_average_dry_air_mole_fraction_of_atmospheric_carbon_dioxide",
        "processing_level": "level_2",
        "sensor_and_algorithm": "tanso_fts_ocfp",
        "year": ["2017","2018","2019","2020","2021"],
        "month": ["01","02","03","04","05","06","07","08","09","10","11","12"],
        "day": ["01","02","03","04","05","06","07","08","09","10","11","12","13",
                "14","15","16","17","18","19","20","21","22","23","24","25","26",
                "27","28","29","30","31"],
        "version": "7_3",
        "format": "zip"
    },
    {
        "dataset": "satellite-carbon-dioxide",
        "variable": "column_average_dry_air_mole_fraction_of_atmospheric_carbon_dioxide",
        "processing_level": "level_2",
        "sensor_and_algorithm": "tanso_fts_srfp",
        "year": ["2017","2018","2019","2020","2021"],
        "month": ["01","02","03","04","05","06","07","08","09","10","11","12"],
        "day": ["01","02","03","04","05","06","07","08","09","10","11","12","13",
                "14","15","16","17","18","19","20","21","22","23","24","25","26",
                "27","28","29","30","31"],
        "version": "2_3_8",
        "format": "zip"
    },
    {
        "dataset": "satellite-carbon-dioxide",
        "variable": "column_average_dry_air_mole_fraction_of_atmospheric_carbon_dioxide",
        "processing_level": "level_2",
        "sensor_and_algorithm": "tanso2_fts_srfp",
        "year": ["2019","2020","2021"],
        "month": ["01","02","03","04","05","06","07","08","09","10","11","12"],
        "day": ["01","02","03","04","05","06","07","08","09","10","11","12","13",
                "14","15","16","17","18","19","20","21","22","23","24","25","26",
                "27","28","29","30","31"],
        "version": "2_1_0",
        "format": "zip"
    },

    # Productos combinados
    {
        "dataset": "satellite-carbon-dioxide",
        "variable": "column_average_dry_air_mole_fraction_of_atmospheric_carbon_dioxide",
        "processing_level": "level_3",
        "sensor_and_algorithm": "merged_obs4mips",
        "year": ["2003","2004","2005","2006","2007","2008","2009","2010",
                 "2011","2012","2013","2014","2015","2016","2017","2018",
                 "2019","2020","2021"],
        "month": "all",
        "version": "4_5",
        "format": "zip"
    },
    {
        "dataset": "satellite-carbon-dioxide",
        "variable": "column_average_dry_air_mole_fraction_of_atmospheric_carbon_dioxide",
        "processing_level": "level_2",
        "sensor_and_algorithm": "merged_emma",
        "year": ["2017","2018","2019","2020","2021"],
        "month": ["01","02","03","04","05","06","07","08","09","10","11","12"],
        "day": ["01","02","03","04","05","06","07","08","09","10","11","12","13",
                "14","15","16","17","18","19","20","21","22","23","24","25","26",
                "27","28","29","30","31"],
        "version": "4_5",
        "format": "zip"
    }
]

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def download_with_retry(request):
    # Se crea una copia para no modificar el original
    req_copy = request.copy()
    # Si el nivel es 3, se elimina el parámetro 'day'
    if req_copy['processing_level'] == "level_3":
        req_copy.pop('day', None)
    
    # Se define un nombre de archivo basado en los parámetros de la solicitud
    filename = f"{req_copy['sensor_and_algorithm']}_l{req_copy['processing_level'][-1]}_v{req_copy['version']}.zip"
    tmp_dir = tempfile.gettempdir()
    file_path = os.path.join(tmp_dir, filename)
    
    print(f"▶️ Iniciando descarga: {filename}")
    client = cdsapi.Client()
    client.retrieve(req_copy['dataset'], req_copy).download(file_path)
    print(f"✔️ Descarga completada: {filename}")
    return file_path, filename

def upload_to_s3(file_path, key):
    """Sube el archivo a S3 utilizando boto3."""
    s3 = boto3.client('s3', **AWS_CONFIG)
    try:
        print(f"⬆️ Subiendo {key} a S3...")
        s3.upload_file(file_path, BUCKET_NAME, key)
        print(f"✔️ Subida completada: {key}")
    except Exception as e:
        print(f"✖️ Error al subir {key}: {str(e)}")
        raise

def process_request(request):
    """Descarga el archivo, lo sube a S3 y elimina el archivo temporal."""
    file_path, filename = download_with_retry(request)
    upload_to_s3(file_path, filename)
    # Eliminamos el archivo temporal tras la subida
    os.remove(file_path)
    print(f"🗑️ Eliminado archivo temporal: {filename}")

if __name__ == "__main__":
    # Se paraleliza la descarga y subida usando ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_request, req) for req in REQUESTS]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error en el procesamiento de una solicitud: {str(e)}")
    print("🎉 Todas las descargas y cargas a S3 completadas!")
