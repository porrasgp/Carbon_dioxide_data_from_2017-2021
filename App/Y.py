import os
import tempfile
import logging
import zipfile
import xarray as xr
import pandas as pd
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import cdsapi
import certifi
import ssl
import boto3

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
        "months": ["{:02d}".format(m) for m in range(1, 13)],  # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]    # "01" a "31"
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

def nc_to_excel(nc_file_path, output_excel_path):
    """
    Abre el archivo .nc y lo convierte a un archivo Excel.
    """
    try:
        ds = xr.open_dataset(nc_file_path)
        # Convertir a DataFrame; se resetean los índices para aplanar las dimensiones
        df = ds.to_dataframe().reset_index()
        df.to_excel(output_excel_path, index=False)
        logger.info(f"Archivo Excel generado: {output_excel_path}")
    except Exception as e:
        logger.error(f"Error al convertir {nc_file_path} a Excel: {e}")

def get_date_range(config, year):
    """
    Retorna el rango de fechas (start_date, end_date) para un año dado,
    basándose en la configuración de meses y días.
    """
    if config["months"] == "all":
        start_month, end_month = "01", "12"
    else:
        start_month = config["months"][0]
        end_month = config["months"][-1]
    
    if config["days"] in [None, "all"]:
        start_day, end_day = "01", "31"
    else:
        start_day = config["days"][0]
        end_day = config["days"][-1]
    
    start_date = f"{year}-{start_month}-{start_day}"
    end_date = f"{year}-{end_month}-{end_day}"
    return start_date, end_date

def process_nc_files(zip_file_path, sensor_name, year, config):
    """
    Extrae archivos .nc desde el ZIP, los convierte a Excel y los guarda en la carpeta
    'processed_data' con nombres que incluyen el sensor y el rango de fechas.
    """
    start_date, end_date = get_date_range(config, year)
    processed_dir = "processed_data"
    os.makedirs(processed_dir, exist_ok=True)
    
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        # Filtrar archivos que terminen en .nc
        nc_files = [f for f in zip_ref.namelist() if f.endswith('.nc')]
        if not nc_files:
            logger.warning(f"No se encontraron archivos .nc en el ZIP para {sensor_name} {year}")
            return
        
        for idx, nc_filename in enumerate(nc_files, start=1):
            # Crear archivo temporal para el contenido .nc
            with tempfile.NamedTemporaryFile(delete=False, suffix=".nc") as tmp_nc_file:
                tmp_nc_path = tmp_nc_file.name
                with zip_ref.open(nc_filename) as source:
                    tmp_nc_file.write(source.read())
            
            # Determinar el nombre del archivo Excel
            if len(nc_files) == 1:
                excel_filename = f"{sensor_name}_{start_date}_{end_date}.xlsx"
            else:
                excel_filename = f"{sensor_name}_{start_date}_{end_date}_part{idx}.xlsx"
            excel_output_path = os.path.join(processed_dir, excel_filename)
            
            # Convertir el archivo .nc a Excel
            nc_to_excel(tmp_nc_path, excel_output_path)
            
            # Eliminar el archivo temporal .nc
            os.remove(tmp_nc_path)
            logger.info(f"Archivo temporal {tmp_nc_path} eliminado.")

def process_sensor(sensor_name, config):
    """
    Procesa un sensor específico utilizando su configuración.
    Para cada año, descarga los datos, extrae los archivos .nc del ZIP y los convierte a Excel.
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
        
        # Crear archivo temporal para almacenar la descarga ZIP (no se guarda de forma permanente)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_file:
            tmp_zip_path = tmp_file.name
        
        try:
            logger.info(f"🛰️ Procesando {sensor_name} - {year}")
            result = download_data(client, request)
            result.download(tmp_zip_path)
            
            # Procesar el ZIP para extraer y convertir los archivos .nc a Excel
            process_nc_files(tmp_zip_path, sensor_name, year, config)
            
        except Exception as e:
            logger.error(f"⛔ Error crítico en {sensor_name} {year}: {e}")
        finally:
            if os.path.exists(tmp_zip_path):
                os.remove(tmp_zip_path)
                logger.info(f"Archivo temporal {tmp_zip_path} eliminado.")

def main():
    logger.info("🚀 Iniciando pipeline de CDS a Excel")
    for sensor_name, config in SENSORS_CONFIG.items():
        logger.info(f"\n{'═'*50}\n🔧 Procesando sensor: {sensor_name}\n{'═'*50}")
        process_sensor(sensor_name, config)
    
    logger.info("✅ Todos los sensores procesados exitosamente!")
    logger.info("Archivos Excel generados se encuentran en la carpeta: processed_data")

if __name__ == "__main__":
    main()

