import boto3
import os
import tempfile
from dotenv import load_dotenv
import cdsapi
import certifi
import ssl
from tenacity import retry, stop_after_attempt, wait_exponential

# Configuración inicial de seguridad SSL
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
ssl_context = ssl.create_default_context(cafile=certifi.where())

# Carga de variables de entorno
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

# Configuración de AWS
AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": "us-east-1"
}
BUCKET_NAME = "geltonas.tech"

# Verificación de credenciales
if not all(AWS_CONFIG.values()):
    raise ValueError("Faltan credenciales de AWS")

# Configuración del cliente CDS con reintentos y seguridad
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_cds_client():
    return cdsapi.Client(
        url='https://cds.climate.copernicus.eu/api/v2',  # Endpoint oficial
        verify=ssl_context
    )

# Parámetros de descarga
YEARS = [str(y) for y in range(2002, 2023)]
SENSORS = {
    'MidTropospheric_CO2': ['airs_nlis', 'iasi_metop_a_nlis', 'iasi_metop_b_nlis', 'iasi_metop_c_nlis'],
    'XCO2': [
        'sciamachy_wfmd', 'sciamachy_besd', 'tanso_fts_ocfp', 
        'tanso_fts_srmp', 'tanso2_fts_srmp', 'merged_emma', 
        'merged_obs4mips'
    ]
}

def process_data():
    client = get_cds_client()
    s3 = boto3.client('s3', **AWS_CONFIG)

    for variable, sensors in SENSORS.items():
        for year in YEARS:
            for sensor in sensors:
                try:
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        print(f"Descargando {variable} - {sensor} - {year}")
                        
                        # Construcción de la solicitud
                        request = {
                            'variable': variable,
                            'processing_level': 'level_3' if 'merged' in sensor else 'level_2',
                            'sensor_and_algorithm': sensor,
                            'year': year,
                            'month': 'all',
                            'version': 'latest',
                            'format': 'zip'
                        }

                        # Descarga y subida
                        client.retrieve('satellite-carbon-dioxide', request, tmp_file.name)
                        
                        if os.path.getsize(tmp_file.name) > 0:
                            s3_key = f"climate/{variable}/{sensor}/{year}.zip"
                            s3.upload_file(tmp_file.name, BUCKET_NAME, s3_key)
                            print(f"✅ Subido: {s3_key}")
                        else:
                            print(f"⚠️ Archivo vacío: {variable}-{sensor}-{year}")

                except Exception as e:
                    print(f"❌ Error en {variable}-{sensor}-{year}: {str(e)}")
                finally:
                    if os.path.exists(tmp_file.name):
                        os.remove(tmp_file.name)

if __name__ == "__main__":
    process_data()
