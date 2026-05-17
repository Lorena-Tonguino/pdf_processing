# VERSION MODIFICADA PARA CY
"""
Este script de validación compara los datos extraídos en la tabla Silver 
con el Ground Truth esperado para la carga 28861101.
"""

import argparse
import logging
from pyspark.sql import SparkSession, functions as F, types as T

# ------------------------------------------------------------------------------
# 🪵 Logger Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("CY_Validator")

# ------------------------------------------------------------------------------
# 0️⃣ Parse Arguments (Optimizado para soportar ejecución por botón RUN)
# ------------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="CY Extractor Validator")
parser.add_argument(
    "--source_table",
    required=False,  # Se cambia a False para que no falle al darle clic en RUN
    default="logistics.silver.load_confirmations_cy",  # Ruta por defecto del proyecto
    help="Spark table name for validation"
)
args, unknown = parser.parse_known_args()  # Evita conflictos con argumentos internos de Databricks
source_table = args.source_table

# ------------------------------------------------------------------------------
# 1️⃣ Spark Session
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("CY_Extractor_Validator").getOrCreate()

# ------------------------------------------------------------------------------
# 2️⃣ Target Data Extraction
# ------------------------------------------------------------------------------
load_id = "28861101"
logger.info(f"Fetching target record for loadConfirmationNumber={load_id} from {source_table}")

try:
    df_target = spark.table(source_table).filter(F.col("loadConfirmationNumber") == load_id)
    target_count = df_target.count()
except Exception as e:
    logger.error(f"Error reading table {source_table}: {str(e)}")
    raise e

if target_count == 0:
    logger.error(f"Target record with loadConfirmationNumber={load_id} not found in {source_table}")
    raise ValueError(f"Target record {load_id} missing.")

# ------------------------------------------------------------------------------
# 3️⃣ Ground Truth Record Definition (Alineado al Agente de Coyote)
# ------------------------------------------------------------------------------
truth_record = {
    "broker_name": "Coyote Logistics, LLC",
    "broker_phone": "+1 (423) 385 3805 x2246",       # Teléfono directo del agente operativo
    "broker_fax": "+1 (847) 810 4891",               # Fax directo del agente operativo
    "broker_address": "25 Northpoint Parkway",
    "broker_city": "Alpharetta",
    "broker_state": "GA",
    "broker_zipcode": "30005",
    "broker_email": "Tamaz.Bazgadze@coyote.com",     # Correo directo del agente operativo
    "loadConfirmationNumber": "28861101",
    "totalCarrierPay": "600.00",                     # String plano para homologación de tipos en Spark
    "carrier_name": "GTT Freight Corp",
    "carrier_mc": "3723304",                         # Mapeado al USDOT por diseño lógico de Coyote
    "carrier_address": "",
    "carrier_city": "",
    "carrier_state": "",
    "carrier_zipcode": "",
    "carrier_phone": "",
    "carrier_fax": "",
    "carrier_email": "gtt.expresscorp@gmail.com",
    
    # Orígenes (Pickups)
    "pickup_customer_1": "United Sugars",
    "pickup_address_1": "450 SONORA DRIVE GATE D",
    "pickup_city_1": "Clewiston",
    "pickup_state_1": "FL",
    "pickup_zipcode_1": "33440",
    "pickup_start_datetime_1": "2023-03-29T08:00:00",
    "pickup_end_datetime_1": "2023-03-29T08:00:00",
    
    "pickup_customer_2": "", "pickup_address_2": "", "pickup_city_2": "", "pickup_state_2": "", "pickup_zipcode_2": "", "pickup_start_datetime_2": "", "pickup_end_datetime_2": "",
    "pickup_customer_3": "", "pickup_address_3": "", "pickup_city_3": "", "pickup_state_3": "", "pickup_zipcode_3": "", "pickup_start_datetime_3": "", "pickup_end_datetime_3": "",
    
    # Destinos (Deliveries)
    "delivery_customer_1": "Batory Foods",
    "delivery_address_1": "885 DOUGLAS HILLS RD",
    "delivery_city_1": "Lithia Springs",
    "delivery_state_1": "GA",
    "delivery_zipcode_1": "30122-3626",
    "delivery_start_datetime_1": "2023-03-30T09:30:00",
    "delivery_end_datetime_1": "2023-03-30T09:30:00",
    
    "delivery_customer_2": "", "delivery_address_2": "", "delivery_city_2": "", "delivery_state_2": "", "delivery_zipcode_2": "", "delivery_start_datetime_2": "", "delivery_end_datetime_2": "",
    "delivery_customer_3": "", "delivery_address_3": "", "delivery_city_3": "", "delivery_state_3": "", "delivery_zipcode_3": "", "delivery_start_datetime_3": "", "delivery_end_datetime_3": ""
}

# ------------------------------------------------------------------------------
# 4️⃣ Schema Validation
# ------------------------------------------------------------------------------
schema = df_target.schema
target_cols = set(schema.fieldNames())
truth_cols = set(truth_record.keys())

missing_in_target = truth_cols - target_cols
extra_in_target = target_cols - truth_cols - {"processed_at", "source_file", "carrier_contact"}

if missing_in_target:
    logger.error(f"❌ Structural Error: Columns missing in target table: {missing_in_target}")
if extra_in_target:
    logger.warning(f"⚠️ Notice: Extra columns found in target table: {extra_in_target}")

# ------------------------------------------------------------------------------
# 5️⃣ Content Validation
# ------------------------------------------------------------------------------
results = []
target_row = df_target.limit(1).collect()[0]
target_values = target_row.asDict()

for col in truth_record.keys():
    if col in target_values:
        truth_val = truth_record.get(col)
        target_val = target_values.get(col)
        
        norm_truth = str(truth_val).strip().lower() if truth_val else ""
        norm_target = str(target_val).strip().lower() if target_val else ""
        
        status = "✅ Match" if norm_truth == norm_target else "❌ Mismatch"
        results.append((col, status, truth_val, target_val))
    else:
        results.append((col, "❌ Missing Col", truth_record.get(col), "None"))

# ------------------------------------------------------------------------------
# 6️⃣ Log Results
# ------------------------------------------------------------------------------
logger.info(f"Validation results for loadConfirmationNumber={load_id}:")
for field, status, truth, target in results:
    if status == "✅ Match":
        logger.info(f"{field:30} | {status:10} | truth='{truth}' | target='{target}'")
    else:
        logger.error(f"{field:30} | {status:10} | truth='{truth}' | target='{target}'")

# ------------------------------------------------------------------------------
# 7️⃣ Fail Pipeline if Errors Detected
# ------------------------------------------------------------------------------
errors = [r for r in results if not r[1].startswith("✅")]
if errors or missing_in_target:
    logger.error(f"Validation failed. Total content field errors: {len(errors)}")
    raise ValueError("Validation failed for Coyote Logistics Delta table.")
else:
    logger.info("✅ SUCCESS: All schema fields and data contents match perfectly with the Ground Truth.")