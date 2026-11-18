# 🌍 Satellite-Based Atmospheric CO₂ Dataset

Source Data: https://cds.climate.copernicus.eu/datasets/satellite-carbon-dioxide?tab=overview

This dataset provides observations of atmospheric carbon dioxide (CO₂) amounts obtained from **satellite instruments** 🛰️. CO₂ is a naturally occurring **greenhouse gas (GHG)** 🌱, but human activities (e.g., fossil fuel combustion 🏭🔥, deforestation 🪓) have increased its abundance to **~420 ppm** (vs. pre-industrial 280 ppm). The annual cycle (🌳🍃 seasonal uptake/release by vegetation) is most pronounced in the northern hemisphere.

---

## 🔬 Measurement Methodology
- Satellites measure **near-infrared/infrared radiation** 🔆 reflected/emitted by Earth.
- **Absorption signatures** 📉 of CO₂ (and other gases) are analyzed in radiance data to determine column-averaged CO₂ abundance.
- **Retrieval algorithms** 💻⚙️ (software) process these signatures into CO₂ concentrations. Different algorithms have unique strengths/weaknesses ⚖️.

---

## 📂 Data Products
### Two main types:
1. **Column-averaged mixing ratios (XCO₂)** 🌐  
   - Instruments: `SCIAMACHY/ENVISAT`, `TANSO-FTS/GOSAT`, `TANSO-FTS2/GOSAT2`, `OCO-2`  
   - Formats: **Level 2** (L2: orbit tracks 🛰️) and **Level 3** (L3: gridded 🌍🔳).  
2. **Mid-tropospheric CO₂ columns** 🏔️  
   - Instruments: `IASI/Metop`, `AIRS`  

---

## 📊 Data Specifications
### **Spatial Coverage**  
- Horizontal: ~70°N to 70°S 🌍  
- Resolutions:  
  - SCIAMACHY (L2): 30x60 km²  
  - TANSO (L2): 10 km diameter  
  - IASI (L2): 12 km diameter  
  - L3 Gridded: 5°x5° #️⃣  

### **Temporal Coverage** 📅  
| Instrument      | Period                     |
|-----------------|----------------------------|
| SCIAMACHY (L2)  | Oct 2002 - Apr 2012        |
| IASI (L2)       | Jul 2007 - Dec 2021        |
| TANSO (L2)      | Apr 2009 - Dec 2021        |
| L3 Products     | Aug 2003 - Mar 2021        |

### **File Format** 📁  
- **NetCDF4** (CF-1.6 conventions ✅)  
- Updated **yearly** 🔄 (1 year behind real-time).  

---

## 📈 Key Variables  
| Variable                          | Units | Description                                  |
|-----------------------------------|-------|----------------------------------------------|
| **XCO₂** 🌐                      | ppm   | Column-average dry-air mole fraction of CO₂ |
| **Mid-tropospheric CO₂** 🏔️     | ppm   | Mid-troposphere CO₂ mixing ratio            |

---

## 🛠️ Usage Notes  
- **Latest versions** ⬆️ are recommended (multiple algorithm/file versions exist).  
- **Documentation** 📄 details sensor-specific sampling frequencies and auxiliary variables (e.g., pressure, temperature, aerosols).  

---

## 📜 Attribution  
- Produced for **C3S** (Copernicus Climate Change Service) 🌍, except SCIAMACHY/AIRS L2 (developed under ESA’s **GHG-CCI** project).  

---

# 🎓 Guía didáctica: cómo funciona el código

Esta sección explica el **pipeline de datos** del repositorio, pensada para estudiantes.

## 1. La idea en una frase
> Pedimos datos de CO₂ a Copernicus, los guardamos un momento en un archivo temporal y los subimos a un bucket de AWS S3. Todo se automatiza con GitHub Actions.

Es un proceso **ETL** simplificado (Extract → Transform → Load) donde la parte "Extract" es la descarga y "Load" es la subida a S3:

```
 ┌────────────────────┐   cdsapi    ┌────────────────┐   boto3    ┌──────────────┐
 │ Copernicus CDS     │ ──────────▶ │ .zip temporal  │ ─────────▶ │ AWS S3       │
 │ (satélites de CO₂) │  descarga   │ (/tmp)         │  subida    │ climate-data │
 └────────────────────┘             └───────┬────────┘            └──────────────┘
                                             └──▶ se borra al terminar
```

## 2. Mapa de archivos: qué hace cada uno y cómo se conectan

| Archivo | Rol | Se conecta con... |
|---|---|---|
| `App/Y.py` | **Script principal.** Una configuración por sensor; el código recorre sensores y años. Usa `logging`. | Lo ejecuta el workflow. Lee `~/.cdsapirc` y las variables de AWS. |
| `App/XCO2.py` | Versión anterior: lista explícita de pedidos y descarga en **paralelo** con hilos. | Mismo flujo que `Y.py`. Útil para comparar estilos. |
| `App/.cdsapirc` | Credenciales de Copernicus (URL + key). | `cdsapi.Client()` lo busca en `~/.cdsapirc`. |
| `App/requirements.txt` | Librerías de Python necesarias. | `pip install -r App/requirements.txt` |
| `.github/workflows/trigger.yml` | Automatiza la ejecución en cada `push`. | Instala dependencias, copia `.cdsapirc`, pasa los *Secrets* de AWS y ejecuta `Y.py`. |
| `.env` (local, no se sube) | Credenciales de AWS en tu computador. | `python-dotenv` lo carga en `os.environ`. |

## 3. El camino completo, paso a paso

1. **Haces `git push`** → GitHub detecta `trigger.yml` y arranca un servidor Linux temporal.
2. El workflow **instala Python y las librerías** y copia `App/.cdsapirc` a `~/.cdsapirc`.
3. Ejecuta `python Y.py` con los **Secrets** de AWS como variables de entorno.
4. `Y.py` valida que existan las credenciales (*fail fast*) y crea el cliente de S3.
5. `main()` recorre `SENSORS_CONFIG`; por cada **sensor** y cada **año** se arma un pedido (`request`).
6. `download_data()` envía el pedido a CDS (con **reintentos** vía `tenacity`) y se descarga a un `.zip` temporal.
7. `upload_to_s3()` lo sube a `s3://geltonas.tech/climate-data/<sensor>/<año>/data.zip`.
8. El bloque `finally` **borra el temporal**, haya o no errores.

## 4. Conceptos de programación que se practican

| Concepto | Dónde verlo |
|---|---|
| Variables de entorno y secretos | `os.getenv`, `load_dotenv`, `secrets.*` del workflow |
| *Fail fast* (validar al inicio) | `if not all(AWS_CONFIG.values()): raise ValueError` |
| Separar configuración de lógica | `SENSORS_CONFIG` en `Y.py` |
| *List comprehension* | `["{:02d}".format(m) for m in range(1, 13)]` |
| Decoradores y reintentos | `@retry(...)` de `tenacity` |
| Manejo de errores | `try / except / finally` en `process_sensor()` |
| Logging profesional | `logging.basicConfig`, `logger.info/error` |
| Concurrencia con hilos | `ThreadPoolExecutor` en `XCO2.py` |
| Punto de entrada | `if __name__ == "__main__":` |
| CI/CD | `.github/workflows/trigger.yml` |

## 5. Cómo ejecutarlo en tu computador

```bash
git clone https://github.com/porrasgp/Carbon_dioxide_data_from_2017-2021.git
cd Carbon_dioxide_data_from_2017-2021
python -m venv venv && source venv/bin/activate     # Windows: venv\Scripts\activate
pip install -r App/requirements.txt
```

1. Crea una cuenta en <https://cds.climate.copernicus.eu>, acepta la licencia del dataset y copia tu *API key* a `~/.cdsapirc`:
   ```
   url: https://cds.climate.copernicus.eu/api
   key: TU_KEY_PERSONAL
   ```
2. Crea un archivo `.env` dentro de `App/`:
   ```
   AWS_ACCESS_KEY_ID=tu_id
   AWS_SECRET_ACCESS_KEY=tu_secreto
   ```
3. Ejecuta `cd App && python Y.py`.

> ⏱️ Los pedidos pueden tardar de minutos a horas: CDS los pone en cola antes de prepararlos.

## 6. 🔐 Seguridad: lo que NO se debe hacer
- **Nunca** subas claves a un repositorio público (ni `.cdsapirc` con key real, ni `.env`). Cualquiera puede leerlas y usarlas.
- Usa **GitHub Secrets** (*Settings → Secrets and variables → Actions*) y léelos con `${{ secrets.NOMBRE }}`.
- Si una clave se expuso, **revócala y genera una nueva**: borrarla del repo no basta, queda en el historial de git.

## 7. Ejercicios sugeridos para estudiantes
1. Agrega un sensor nuevo a `SENSORS_CONFIG` sin tocar el resto del código.
2. Cambia el workflow para que corra con `workflow_dispatch` (botón manual) en vez de en cada `push`.
3. Añade paralelismo a `Y.py` con `ThreadPoolExecutor`, como en `XCO2.py`.
4. Haz que el script termine con error (código ≠ 0) si algún año falló. Hoy los errores se capturan y el workflow sale en verde.
5. Compara las diferencias entre `XCO2.py` y `Y.py` en los parámetros de nivel 3 (`day`, `variable`) y comprueba cuál acepta la API.
