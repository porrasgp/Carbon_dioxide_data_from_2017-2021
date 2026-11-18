"""
XCO2.py  -  Versión "lista explícita de solicitudes"
=====================================================

¿QUÉ HACE ESTE SCRIPT?
    Es un mini pipeline ETL (Extract -> Transform -> Load) de tres pasos:

        [1] EXTRAER   Pide datos de CO2 satelital al Copernicus Climate Data
                      Store (CDS) usando la librería `cdsapi`.
        [2] GUARDAR   Los datos llegan como un .zip a una carpeta temporal.
        [3] CARGAR    El .zip se sube a un bucket de AWS S3 con `boto3` y el
                      archivo temporal se borra para no llenar el disco.

    Diagrama:

        CDS (Copernicus)  --cdsapi-->  /tmp/archivo.zip  --boto3-->  S3 (AWS)
                                                  |
                                                  +--> os.remove()  (limpieza)

¿CÓMO SE CONECTA CON EL RESTO DEL REPOSITORIO?
    * App/.cdsapirc        -> credenciales de CDS. `cdsapi.Client()` lo busca
                              solo en  ~/.cdsapirc  (el workflow lo copia ahí).
    * .env  /  Secrets     -> credenciales de AWS (ver más abajo).
    * .github/workflows/   -> automatiza la ejecución en cada `git push`.
      trigger.yml            (OJO: el workflow actual ejecuta Y.py, no este).
    * App/requirements.txt -> librerías que hay que instalar.
    * App/Y.py             -> versión más moderna y ordenada de este mismo
                              proceso (usa logging y una config por sensor).

CÓMO EJECUTARLO EN TU COMPUTADOR:
    1) pip install -r App/requirements.txt
    2) Crea ~/.cdsapirc con tu propia key de CDS.
    3) Crea un archivo .env junto a este script con:
           AWS_ACCESS_KEY_ID=tu_id
           AWS_SECRET_ACCESS_KEY=tu_secreto
    4) python XCO2.py
"""

# ---------------------------------------------------------------------------
# 1. IMPORTS  (cada librería tiene un rol claro en el pipeline)
# ---------------------------------------------------------------------------
import os                          # leer variables de entorno y armar rutas de archivos
import tempfile                    # obtener la carpeta temporal del sistema (/tmp, %TEMP%)
import ssl                         # certificados HTTPS
import certifi                     # lista actualizada de certificados de confianza
import concurrent.futures          # ejecutar varias descargas en paralelo (hilos)
import boto3                       # cliente oficial de AWS: lo usamos para S3
from dotenv import load_dotenv     # lee el archivo .env y lo vuelve variables de entorno
import cdsapi                      # cliente oficial del Climate Data Store (Copernicus)
from tenacity import retry, stop_after_attempt, wait_exponential  # reintentos automáticos

# ---------------------------------------------------------------------------
# 2. CONFIGURACIÓN SSL
#    Algunos entornos (servidores, Windows corporativo) no encuentran los
#    certificados HTTPS y las descargas fallan. `certifi` trae un paquete
#    de certificados confiable y se lo indicamos a `requests` (que usa cdsapi).
# ---------------------------------------------------------------------------
# NOTA PARA CLASE: la siguiente línea apunta a un 'cacert.pem' local, pero la
# línea de abajo la SOBRESCRIBE de inmediato con certifi. En la práctica solo
# la segunda tiene efecto; se deja como ejemplo de configuración anterior.
os.environ["REQUESTS_CA_BUNDLE"] = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cacert.pem')

# Esta es la que realmente se usa: certificados de certifi.
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
# (ssl_context se crea pero este script no lo usa directamente; sirve de ejemplo.)
ssl_context = ssl.create_default_context(cafile=certifi.where())

# ---------------------------------------------------------------------------
# 3. VARIABLES DE ENTORNO  (cómo manejar secretos sin ponerlos en el código)
#    Regla de oro: NUNCA escribir claves dentro del .py ni subirlas a GitHub.
#      - En tu PC:            se leen del archivo .env  (que va en .gitignore)
#      - En GitHub Actions:   vienen de "Secrets" (Settings > Secrets), y el
#                             workflow las pasa en la sección `env:`.
#    GitHub Actions define siempre GITHUB_ACTIONS="true"; así sabemos dónde
#    estamos y solo cargamos .env cuando corremos en local.
# ---------------------------------------------------------------------------
if not os.getenv("GITHUB_ACTIONS"):
    load_dotenv()

# Diccionario con los parámetros de conexión a AWS. Las claves del diccionario
# tienen exactamente los nombres que espera boto3.client(...), por eso más
# adelante lo pasamos con `**AWS_CONFIG` (desempaquetado de diccionario).
AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": "us-east-1"
}
BUCKET_NAME = "geltonas.tech"   # bucket de S3 donde quedarán los .zip

# Validación temprana ("fail fast"): si falta alguna credencial, detenemos el
# programa YA con un mensaje claro, en vez de descargar horas de datos y fallar
# al final cuando intentemos subirlos. `all(...)` es False si algún valor es None.
if not all(AWS_CONFIG.values()):
    raise ValueError("🚨 Credenciales AWS incompletas - Verifica .env o secrets de GitHub")

# ---------------------------------------------------------------------------
# 4. LISTA DE SOLICITUDES (REQUESTS)
#    Cada diccionario es una "orden de pedido" para la API de Copernicus.
#    Significado de los campos:
#       dataset               -> el catálogo: "satellite-carbon-dioxide"
#       variable              -> qué medimos:
#                                 * ...dry_air_mole_fraction...  = XCO2 (columna total)
#                                 * mid_tropospheric_columns...  = CO2 en la troposfera media
#       processing_level      -> level_2 = datos por órbita del satélite
#                                level_3 = datos ya promediados en una grilla 5°x5°
#       sensor_and_algorithm  -> qué instrumento y qué algoritmo de procesamiento
#                                (IASI/Metop, TANSO-FTS/GOSAT, TANSO2/GOSAT-2, merged...)
#       year / month / day    -> rango de fechas a descargar
#       version               -> versión del algoritmo (distintas versiones, distintos datos)
#       format                -> "zip" : un archivo comprimido con muchos NetCDF dentro
#
#    Los parámetros deben coincidir EXACTAMENTE con el formulario de CDS; si un
#    valor no existe, la API responde con error 400.
# ---------------------------------------------------------------------------
REQUESTS = [
    # ---- Grupo A: CO2 en la troposfera media (instrumento IASI, 3 satélites Metop) ----
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
        # Metop-C se lanzó en 2018, por eso sus datos empiezan en 2019.
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

    # ---- Grupo B: XCO2 nivel 2 (satélites japoneses GOSAT y GOSAT-2, sensor TANSO) ----
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
        # GOSAT-2 (tanso2) empezó a medir en 2019.
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

    # ---- Grupo C: productos combinados (varios satélites fusionados) ----
    {
        # Nivel 3 = ya viene promediado por mes en una grilla; por eso aquí
        # usamos month="all" y NO existe el campo "day".
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

# ---------------------------------------------------------------------------
# 5. PASO "EXTRAER": descargar un archivo desde CDS
# ---------------------------------------------------------------------------
# @retry es un DECORADOR: envuelve la función y, si lanza una excepción, la
# vuelve a llamar automáticamente.
#   - stop_after_attempt(3)   -> máximo 3 intentos
#   - wait_exponential(...)   -> espera creciente entre intentos (4s, luego hasta 10s)
# Las redes fallan a veces (timeouts, servidor ocupado): reintentar evita que
# todo el pipeline se caiga por un error pasajero.
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def download_with_retry(request):
    """Descarga UNA solicitud de CDS a la carpeta temporal.

    Recibe : un diccionario de REQUESTS.
    Devuelve: (ruta_local_del_zip, nombre_del_zip)
    """
    # .copy() para NO modificar el diccionario original de la lista REQUESTS
    # (si lo modificáramos, un reintento partiría de datos ya alterados).
    req_copy = request.copy()

    # Nivel 3 viene mensual, no diario: si enviáramos "day" la API lo rechazaría.
    if req_copy['processing_level'] == "level_3":
        req_copy.pop('day', None)   # pop(..., None) no falla si la clave no existe

    # Nombre de archivo único basado en los parámetros. Ejemplo:
    #   iasi_metop_a_nlis_l2_v10_1.zip
    # processing_level[-1] toma el último carácter: "level_2" -> "2".
    filename = f"{req_copy['sensor_and_algorithm']}_l{req_copy['processing_level'][-1]}_v{req_copy['version']}.zip"

    # tempfile.gettempdir() funciona en Linux, Mac y Windows sin cambiar código.
    tmp_dir = tempfile.gettempdir()
    file_path = os.path.join(tmp_dir, filename)

    print(f"▶️ Iniciando descarga: {filename}")

    # cdsapi.Client() lee la URL y la key desde ~/.cdsapirc (ese es el vínculo
    # con el archivo App/.cdsapirc de este repositorio).
    # .retrieve() envía el pedido (CDS lo pone en cola y lo prepara) y
    # .download() baja el resultado cuando está listo.
    client = cdsapi.Client()
    client.retrieve(req_copy['dataset'], req_copy).download(file_path)

    print(f"✔️ Descarga completada: {filename}")
    return file_path, filename

# ---------------------------------------------------------------------------
# 6. PASO "CARGAR": subir un archivo a AWS S3
# ---------------------------------------------------------------------------
def upload_to_s3(file_path, key):
    """Sube `file_path` al bucket BUCKET_NAME con el nombre (clave) `key`.

    En S3 no hay carpetas reales: la "key" es el nombre completo del objeto.
    """
    # **AWS_CONFIG desempaqueta el diccionario como argumentos con nombre:
    #   boto3.client('s3', aws_access_key_id=..., aws_secret_access_key=..., region_name=...)
    s3 = boto3.client('s3', **AWS_CONFIG)
    try:
        print(f"⬆️ Subiendo {key} a S3...")
        s3.upload_file(file_path, BUCKET_NAME, key)   # (archivo local, bucket, nombre en S3)
        print(f"✔️ Subida completada: {key}")
    except Exception as e:
        print(f"✖️ Error al subir {key}: {str(e)}")
        raise   # volvemos a lanzar el error para que el llamador sepa que falló

# ---------------------------------------------------------------------------
# 7. PIPELINE COMPLETO DE UNA SOLICITUD  (une los pasos 5 y 6)
# ---------------------------------------------------------------------------
def process_request(request):
    """Descarga -> sube a S3 -> borra el temporal. Se ejecuta una vez por solicitud."""
    file_path, filename = download_with_retry(request)   # EXTRAER
    upload_to_s3(file_path, filename)                    # CARGAR
    # Limpieza: estos zips pueden pesar GB; si no los borramos, el disco
    # (y más aún el runner de GitHub Actions) se llenaría.
    os.remove(file_path)
    print(f"🗑️ Eliminado archivo temporal: {filename}")

# ---------------------------------------------------------------------------
# 8. PUNTO DE ENTRADA
#    `if __name__ == "__main__"` es True solo cuando ejecutas `python XCO2.py`.
#    Si otro archivo hace `import XCO2`, esta parte NO corre automáticamente.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # PARALELISMO: bajar y subir archivos son tareas de "espera" (red), así que
    # con hilos podemos tener hasta 5 solicitudes en marcha a la vez en lugar de
    # hacerlas una por una. max_workers=5 evita saturar a CDS y la memoria.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # submit() programa cada solicitud y devuelve un "future" (promesa de resultado).
        futures = [executor.submit(process_request, req) for req in REQUESTS]

        # as_completed() entrega cada future apenas termina, sin importar el orden.
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()   # si el hilo lanzó una excepción, aquí se relanza
            except Exception as e:
                # Un fallo en una solicitud NO detiene a las demás.
                print(f"Error en el procesamiento de una solicitud: {str(e)}")
    print("🎉 Todas las descargas y cargas a S3 completadas!")
