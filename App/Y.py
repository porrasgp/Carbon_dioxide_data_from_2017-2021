"""
Y.py  -  Pipeline "unificado": configuración por sensor + logging
=================================================================

¿QUÉ HACE ESTE SCRIPT?
    Hace lo mismo que XCO2.py (CDS -> archivo temporal -> S3), pero mejor
    organizado. En lugar de una lista larga de pedidos repetidos, define UNA
    configuración por sensor (SENSORS_CONFIG) y el código ARMA los pedidos
    recorriendo cada sensor y cada año.

    Flujo completo:

        main()
          └─ por cada sensor de SENSORS_CONFIG ........ process_sensor()
               └─ por cada año del sensor
                    1. arma el diccionario `request`
                    2. download_data()  -> pide y descarga a un .zip temporal
                    3. upload_to_s3()   -> sube a s3://BUCKET/climate-data/<sensor>/<año>/data.zip
                    4. borra el .zip temporal (bloque finally)

¿CÓMO SE CONECTA CON EL RESTO DEL REPOSITORIO?
    * .github/workflows/trigger.yml  -> ESTE es el archivo que ejecuta el workflow
                                         (`python Y.py`) en cada `git push`.
    * App/.cdsapirc                  -> credenciales de CDS; el workflow lo copia a
                                         ~/.cdsapirc y `cdsapi.Client()` lo lee solo.
    * GitHub Secrets                 -> AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY
                                         llegan como variables de entorno.
    * App/requirements.txt           -> dependencias a instalar.
    * App/XCO2.py                    -> versión anterior del mismo proceso.

DIFERENCIAS CLAVE CON XCO2.py (buen punto de discusión en clase):
    1. Config por sensor + bucle  -> menos código repetido, más fácil de ampliar.
    2. `logging` en vez de `print` -> mensajes con fecha, hora y nivel (INFO/ERROR).
    3. Una solicitud por AÑO       -> archivos más pequeños y reanudables.
    4. Secuencial (sin hilos)      -> más lento, pero más simple y amable con CDS.
    5. Estructura de carpetas en S3 ordenada por sensor y año.

CÓMO EJECUTARLO EN LOCAL:
    1) pip install -r App/requirements.txt
    2) Crea ~/.cdsapirc con tu key de CDS.
    3) Crea un .env con AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY.
    4) python Y.py
"""

# ---------------------------------------------------------------------------
# 1. IMPORTS
# ---------------------------------------------------------------------------
import boto3                     # cliente de AWS (S3)
import os                        # variables de entorno y rutas
import tempfile                  # archivos temporales
import logging                   # sistema de mensajes profesional (mejor que print)
from tenacity import retry, stop_after_attempt, wait_exponential   # reintentos
from dotenv import load_dotenv   # lee el archivo .env
import cdsapi                    # cliente del Climate Data Store
import certifi                   # certificados HTTPS actualizados
import ssl

# ---------------------------------------------------------------------------
# 2. LOGGING
#    Un logger registra mensajes con un NIVEL de importancia:
#      DEBUG < INFO < WARNING < ERROR < CRITICAL
#    Con level=INFO se muestran INFO, WARNING, ERROR y CRITICAL.
#    En GitHub Actions estos mensajes aparecen en la pestaña "Actions" y son
#    la mejor herramienta para saber qué pasó cuando algo falla.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",   # ej: 2025-01-31 10:15:00 [INFO] texto
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)   # __name__ = nombre del módulo, útil para identificar el origen

# ---------------------------------------------------------------------------
# 3. CONFIGURACIÓN SSL
#    Le indicamos a `requests` (que usa cdsapi por debajo) que confíe en los
#    certificados de certifi. Evita errores "SSL: CERTIFICATE_VERIFY_FAILED".
#    (ssl_context queda creado como ejemplo; este script no lo usa directamente.)
# ---------------------------------------------------------------------------
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
ssl_context = ssl.create_default_context(cafile=certifi.where())

# ---------------------------------------------------------------------------
# 4. VARIABLES DE ENTORNO Y CREDENCIALES
#    load_dotenv() busca un archivo .env y lo carga. En GitHub Actions no
#    existe ese archivo (no pasa nada, simplemente no hace nada) y las
#    variables llegan desde los "Secrets" que define el workflow.
#    Regla de oro: las claves NUNCA van escritas en el código.
# ---------------------------------------------------------------------------
load_dotenv()

AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": "us-east-1"
}
BUCKET_NAME = "geltonas.tech"

# "Fail fast": si falta una credencial, paramos ahora con un mensaje claro en
# lugar de descargar datos para luego no poder subirlos.
if not all(AWS_CONFIG.values()):
    raise ValueError("🚨 Faltan credenciales AWS en el archivo .env")

# Cliente S3 global: se crea UNA vez y lo reutilizan todas las subidas.
# (Los nombres del diccionario coinciden con los parámetros de boto3, por eso
#  se pasa con ** = "desempaquetar diccionario como argumentos con nombre").
s3_client = boto3.client('s3', **AWS_CONFIG)

# Dataset del catálogo de Copernicus. os.getenv(nombre, valor_por_defecto):
# si existe la variable CDS_DATASET se usa; si no, se usa "satellite-carbon-dioxide".
DATASET = os.getenv("CDS_DATASET", "satellite-carbon-dioxide")

# ---------------------------------------------------------------------------
# 5. CONFIGURACIÓN CENTRALIZADA DE SENSORES  (el "corazón" de este script)
#    Para agregar un sensor nuevo basta con añadir un bloque aquí: el resto del
#    código NO se toca. Esta es la idea de "separar configuración y lógica".
#
#    Campos:
#       variable -> qué se pide a CDS ("co2" = CO2 troposfera media,
#                   "xco2" = columna total de CO2)
#       sensor   -> identificador "sensor_and_algorithm" de CDS
#       years    -> años disponibles para ese sensor
#       version  -> versión del algoritmo de procesamiento
#       level    -> level_2 (por órbita) / level_3 (grilla mensual)
#       months, days -> listas generadas con una "list comprehension"
#
#    Truco: "{:02d}".format(5) devuelve "05"  (CDS exige 2 dígitos con cero).
#           range(1, 13) produce 1..12  (el 13 queda fuera).
# ---------------------------------------------------------------------------
SENSORS_CONFIG = {
    # ---- IASI (a bordo de los satélites Metop A, B y C): CO2 en troposfera media ----
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
        # Metop-C solo tiene datos desde 2019.
        "variable": "co2",
        "sensor": "iasi_metop_c_nlis",
        "years": ["2019", "2020", "2021"],
        "version": "10_1",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],  # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]        # "01" a "31"
    },
    # ---- TANSO-FTS (satélite GOSAT) y TANSO-FTS2 (GOSAT-2): XCO2 ----
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
    # ---- Productos combinados (varios satélites fusionados) ----
    "MERGED_EMMA": {
        "variable": "xco2",
        "sensor": "merged_emma",
        "years": ["2017", "2018", "2019", "2020", "2021"],
        "version": "4_5",
        "level": "level_2",
        "months": ["{:02d}".format(m) for m in range(1, 13)],  # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]        # "01" a "31"
    },
    "MERGED_OBS4MIPS": {
        # Nivel 3 = grilla mensual de 5°x5°. Aquí el rango es 2017-2021;
        # el producto existe desde 2003 (ver XCO2.py si quieres toda la serie).
        "variable": "xco2",
        "sensor": "merged_obs4mips",
        "years": [str(y) for y in range(2017, 2022)],  # 2017 a 2021 (range excluye el 2022)
        "version": "4_5",
        "level": "level_3",
        "months": ["{:02d}".format(m) for m in range(1, 13)],  # "01" a "12"
        "days": ["{:02d}".format(d) for d in range(1, 32)]        # "01" a "31"
    }
}

# ---------------------------------------------------------------------------
# 6. PASO "EXTRAER": pedir y descargar datos a CDS
# ---------------------------------------------------------------------------
# @retry (decorador de tenacity): si la función lanza una excepción, la vuelve
# a ejecutar hasta 3 veces, esperando cada vez un poco más (backoff exponencial).
# Útil porque las APIs de internet fallan de forma intermitente.
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def download_data(client, request):
    """
    Descarga datos utilizando la API de CDS.
    Se aplica reintento en caso de fallo.

    OJO: `client.retrieve()` ENVÍA el pedido y espera a que CDS lo prepare
    (puede tardar minutos u horas en cola). Devuelve un objeto "resultado";
    el archivo se baja después con `result.download(ruta)` en process_sensor().
    """
    logger.info(f"Iniciando descarga para la solicitud: {request}")
    result = client.retrieve(request["dataset"], request)
    return result

# ---------------------------------------------------------------------------
# 7. PASO "CARGAR": subir un archivo a AWS S3
# ---------------------------------------------------------------------------
def upload_to_s3(file_path, s3_key):
    """
    Sube el archivo ubicado en file_path a S3 utilizando s3_key como ruta.

    En S3 no existen carpetas reales: "climate-data/IASI/2017/data.zip" es solo
    un nombre largo (key); la consola de AWS lo muestra como carpetas por las "/".
    """
    try:
        s3_client.upload_file(file_path, BUCKET_NAME, s3_key)   # (archivo local, bucket, key)
        logger.info(f"Archivo subido exitosamente a s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        logger.error(f"Error al subir archivo a S3: {e}")
        raise   # relanzamos: quien llama decide qué hacer con el error

# ---------------------------------------------------------------------------
# 8. PROCESAR UN SENSOR COMPLETO (aquí se unen los pasos 6 y 7)
# ---------------------------------------------------------------------------
def process_sensor(sensor_name, config):
    """
    Procesa un sensor específico utilizando su configuración.
    Itera por cada año, descarga los datos y los sube a S3.
    """
    client = cdsapi.Client()  # Lee ~/.cdsapirc (URL + key de Copernicus)

    for year in config["years"]:
        # Armamos el "pedido" combinando datos globales (DATASET) con la
        # configuración del sensor. Son los mismos campos que usa XCO2.py,
        # pero con UN solo año por solicitud.
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

        # Creamos un archivo temporal VACÍO solo para reservar un nombre único
        # y seguro. `delete=False` evita que Python lo borre al cerrar el `with`;
        # lo cerramos para que cdsapi pueda escribir en esa ruta (importante en Windows).
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_file:
            tmp_file_path = tmp_file.name

        # try / except / finally:
        #   try     -> intenta el trabajo
        #   except  -> si algo falla, lo registramos y seguimos con el siguiente año
        #   finally -> SIEMPRE se ejecuta (haya error o no): ideal para limpiar
        try:
            logger.info(f"🛰️ Procesando {sensor_name} - {year}")
            result = download_data(client, request)   # EXTRAER: CDS prepara el pedido
            result.download(tmp_file_path)            # ...y lo guardamos en el .zip temporal

            # Ruta final en S3, ordenada por sensor y año:
            #   climate-data/<sensor>/<año>/data.zip
            s3_key = f"climate-data/{sensor_name}/{year}/data.zip"
            upload_to_s3(tmp_file_path, s3_key)       # CARGAR
        except Exception as e:
            # Se captura el error para que UN año fallido no detenga todo el
            # pipeline. Ojo: por eso el workflow puede terminar en verde (✔)
            # aunque algunos años hayan fallado. Revisa siempre los logs.
            logger.error(f"⛔ Error crítico en {sensor_name} {year}: {e}")
        finally:
            # Limpieza del temporal para no llenar el disco.
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
                logger.info(f"Archivo temporal {tmp_file_path} eliminado.")

# ---------------------------------------------------------------------------
# 9. FUNCIÓN PRINCIPAL: recorre todos los sensores
# ---------------------------------------------------------------------------
def main():
    logger.info("🚀 Iniciando pipeline unificado de CDS a S3")
    # .items() entrega pares (clave, valor): (nombre_del_sensor, su_configuración)
    for sensor_name, config in SENSORS_CONFIG.items():
        logger.info(f"\n{'═'*50}\n🔧 Procesando sensor: {sensor_name}\n{'═'*50}")
        process_sensor(sensor_name, config)

    logger.info("✅ Todos los sensores procesados exitosamente!")
    logger.info(f"Estructura final en S3: s3://{BUCKET_NAME}/climate-data/")

# Punto de entrada: main() solo corre con `python Y.py`, no al hacer `import Y`.
if __name__ == "__main__":
    main()
