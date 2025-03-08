import cdsapi
from tenacity import retry, stop_after_attempt, wait_exponential

# Configuración común para todos los requests
COMMON_CONFIG = {
    "dataset": "satellite-carbon-dioxide",
    "processing_level": "level_2",  # String en lugar de lista
    "month": ["01","02","03","04","05","06","07","08","09","10","11","12"],
    "format": "zip"  # Parámetro requerido
}

# Configuración específica para cada sensor
SENSOR_CONFIG = {
    # Mid-tropospheric CO2
    "iasi_metop_a_nlis": {
        "variable": "mid_tropospheric_columns_of_atmospheric_carbon_dioxide",
        "years": ["2017","2018","2019","2020","2021"],
        "version": "10.1"  # Versión con punto
    },
    "iasi_metop_b_nlis": {
        "variable": "mid_tropospheric_columns_of_atmospheric_carbon_dioxide",
        "years": ["2017","2018","2019","2020","2021"],
        "version": "10.1"
    },
    "iasi_metop_c_nlis": {
        "variable": "mid_tropospheric_columns_of_atmospheric_carbon_dioxide",
        "years": ["2019","2020","2021"],
        "version": "10.1"
    },
    
    # XCO2
    "tanso_fts_ocfp": {
        "variable": "column_average_dry_air_mole_fraction_of_atmospheric_carbon_dioxide",
        "years": ["2017","2018","2019","2020","2021"],
        "version": "7.3"  # Versión corregida
    },
    "tanso_fts_srfp": {
        "variable": "column_average_dry_air_mole_fraction_of_atmospheric_carbon_dioxide",
        "years": ["2017","2018","2019","2020","2021"],
        "version": "2.3.8"  # Versión con puntos
    },
    "tanso2_fts_srfp": {
        "variable": "column_average_dry_air_mole_fraction_of_atmospheric_carbon_dioxide",
        "years": ["2019","2020","2021"],
        "version": "2.1.0"
    },
    "merged_emma": {
        "variable": "column_average_dry_air_mole_fraction_of_atmospheric_carbon_dioxide",
        "years": ["2017","2018","2019","2020","2021"],
        "version": "4.5"
    }
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_client():
    return cdsapi.Client()

def process_request(sensor, config):
    client = get_client()
    try:
        request = {
            **COMMON_CONFIG,
            "variable": config["variable"],
            "sensor_and_algorithm": sensor,
            "year": config["years"],
            "version": config["version"]
        }
        
        print(f"⬇️ Descargando {sensor}...")
        result = client.retrieve(COMMON_CONFIG["dataset"], request)
        result.download(f"{sensor}_{config['version']}.zip")
        print(f"✅ Descarga completada: {sensor}")
        
    except Exception as e:
        print(f"❌ Error en {sensor}: {str(e)}")
        raise

# Ejecutar todas las descargas
for sensor, config in SENSOR_CONFIG.items():
    process_request(sensor, config)
