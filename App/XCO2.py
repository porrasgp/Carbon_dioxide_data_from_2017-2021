import cdsapi
import os
from tenacity import retry, stop_after_attempt, wait_exponential

# Configuración SSL para entornos problemáticos
os.environ["REQUESTS_CA_BUNDLE"] = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cacert.pem')

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
    client = cdsapi.Client()
    try:
        filename = f"{request['sensor_and_algorithm']}_l{request['processing_level'][-1]}_v{request['version']}.zip"
        print(f"▶️ Iniciando descarga: {filename}")
        
        # Eliminar día si es nivel 3
        if request['processing_level'] == "level_3":
            request.pop('day', None)
            
        client.retrieve(request['dataset'], request).download(filename)
        print(f"✔️ Descarga completada: {filename}")
        
    except Exception as e:
        print(f"✖️ Error crítico en {request['sensor_and_algorithm']}: {str(e)}")
        raise

if __name__ == "__main__":
    for req in REQUESTS:
        download_with_retry(req)
    print("🎉 Todas las descargas completadas!")
