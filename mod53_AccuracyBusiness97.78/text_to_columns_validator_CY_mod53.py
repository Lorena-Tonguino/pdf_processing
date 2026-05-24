# VERSION MODIFICADA PARA CY — VALIDACIÓN AUTOMÁTICA MULTI-DOCUMENTO v5

import argparse
import logging
from pyspark.sql import SparkSession, functions as F

# ─────────────────────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("CY_Validator")


# ─────────────────────────────────────────────────────────────
# Argumentos CLI
# ─────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="CY Extractor Validator")
parser.add_argument(
    "--source_table",
    required=False,
    default="logistics.silver.load_confirmations_cy",
    help="Tabla Spark a validar"
)
args, _ = parser.parse_known_args()
source_table = args.source_table


# ─────────────────────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("CY_Extractor_Validator").getOrCreate()


# ─────────────────────────────────────────────────────────────
# Normalización de rutas para source_file
#
# El extractor guarda la ruta como:
#   /Volumes/logistics/bronze/raw/cy/pdf/archivo.pdf
# El ground truth usa la misma ruta sin prefijo dbfs:.
# Esta función garantiza que ambos lados sean comparables
# independientemente de cómo Databricks exponga la ruta.
# ─────────────────────────────────────────────────────────────
def normalize_path(p: str) -> str:
    if not p:
        return ""
    return (
        p.replace("dbfs:", "")
         .replace("/txt_pymupdf4llm/", "/pdf/")
         .replace(".txt", ".pdf")
         .strip()
    )


# ─────────────────────────────────────────────────────────────
# Ground Truth — 3 escenarios de prueba
# ─────────────────────────────────────────────────────────────
truth_records = {

    # ── CASO BASE: 1 pickup + 1 delivery ──────────────────────
    "28861101": {
        "source_file": "/Volumes/logistics/bronze/raw/cy/pdf/1679676366011_CY FL-GA.pdf",
        "broker_name": "Coyote Logistics, LLC",
        "broker_phone": "+1 (423) 385 3805 x2246",
        "broker_fax": "+1 (847) 810 4891",
        "broker_address": "960 Northpoint Parkway Suite 150",
        "broker_city": "Alpharetta", "broker_state": "GA", "broker_zipcode": "30005",
        "broker_email": "Tamaz.Bazgadze@coyote.com",
        "loadConfirmationNumber": "28861101",
        "totalCarrierPay": "600.00",
        "carrier_name": "GTT Freight Corp",
        "carrier_mc": "3723304",
        "carrier_address": "", "carrier_city": "", "carrier_state": "",
        "carrier_zipcode": "", "carrier_phone": "", "carrier_fax": "",
        "carrier_email": "gtt.expresscorp@gmail.com",
        "pickup_customer_1": "United Sugars",
        "pickup_address_1": "450 SONORA DRIVE GATE D",
        "pickup_city_1": "Clewiston", "pickup_state_1": "FL", "pickup_zipcode_1": "33440",
        "pickup_start_datetime_1": "2023-03-29T08:00:00",
        "pickup_end_datetime_1":   "2023-03-29T13:00:00",
        "delivery_customer_1": "Batory Foods",
        "delivery_address_1": "885 DOUGLAS HILLS RD",
        "delivery_city_1": "Lithia Springs", "delivery_state_1": "GA",
        "delivery_zipcode_1": "30122-3626",
        "delivery_start_datetime_1": "2023-03-30T09:30:00",
        "delivery_end_datetime_1":   "2023-03-30T09:30:00",
        "delivery_customer_2": "", "delivery_address_2": "", "delivery_city_2": "",
        "delivery_state_2": "", "delivery_zipcode_2": "",
        "delivery_start_datetime_2": "", "delivery_end_datetime_2": "",
    },

    # ── CASO TELÉFONO AGENTE: 1 pickup + 1 delivery ───────────
    "30604868": {
        "source_file": "/Volumes/logistics/bronze/raw/cy/pdf/1704722951491_CY GA-FL.pdf",
        "broker_name": "Coyote Logistics, LLC",
        "broker_phone": "+1 (847) 235 8235 x90944",
        "broker_fax": "",
        "broker_address": "960 Northpoint Parkway Suite 150",
        "broker_city": "Alpharetta", "broker_state": "GA", "broker_zipcode": "30005",
        "broker_email": "India.Wymes@coyote.com",
        "loadConfirmationNumber": "30604868",
        "totalCarrierPay": "1348.79",
        "carrier_name": "BRIGHT STAR LOGISTIC SOLUTIONS LLC",
        "carrier_mc": "4083240",
        "carrier_address": "", "carrier_city": "", "carrier_state": "",
        "carrier_zipcode": "", "carrier_phone": "", "carrier_fax": "",
        "carrier_email": "dispatch70brightstarsolutions@gmail.com",
        "pickup_customer_1": "DSC-JM Smucker / Big Heart Pet DC",
        "pickup_address_1": "5000 BOHANNON DR Building A Building A",
        "pickup_city_1": "Fairburn", "pickup_state_1": "GA", "pickup_zipcode_1": "30213",
        "pickup_start_datetime_1": "2024-01-07T06:00:00",
        "pickup_end_datetime_1":   "2024-01-07T06:00:00",
        "delivery_customer_1": "Publix Super Market",
        "delivery_address_1": "6123 Sawyer Road",
        "delivery_city_1": "Sarasota", "delivery_state_1": "FL",
        "delivery_zipcode_1": "34238",
        "delivery_start_datetime_1": "2024-01-08T09:00:00",
        "delivery_end_datetime_1":   "2024-01-08T09:00:00",
        "delivery_customer_2": "", "delivery_address_2": "", "delivery_city_2": "",
        "delivery_state_2": "", "delivery_zipcode_2": "",
        "delivery_start_datetime_2": "", "delivery_end_datetime_2": "",
    },

    # ── CASO MULTI-PARADAS: 1 pickup + 2 deliveries ───────────
    "29671154": {
        "source_file": "/Volumes/logistics/bronze/raw/cy/pdf/1691512349942_CY GA-FL.pdf",
        "broker_name": "Coyote Logistics, LLC",
        "broker_phone": "+1 (773) 365 6256 x6256",
        "broker_fax": "+1 (773) 365 4256",
        "broker_address": "960 Northpoint Parkway Suite 150",
        "broker_city": "Alpharetta", "broker_state": "GA", "broker_zipcode": "30005",
        "broker_email": "Dan.Matkovic@coyote.com",
        "loadConfirmationNumber": "29671154",
        "totalCarrierPay": "1500.00",
        "carrier_name": "GTT Freight Corp",
        "carrier_mc": "3723304",
        "carrier_address": "", "carrier_city": "", "carrier_state": "",
        "carrier_zipcode": "", "carrier_phone": "", "carrier_fax": "",
        "carrier_email": "gtt.expresscorp@gmail.com",
        "pickup_customer_1": "CJ McDonough Main",
        "pickup_address_1": "220 GREENWOOD CT SUITE 230",
        "pickup_city_1": "McDonough", "pickup_state_1": "GA", "pickup_zipcode_1": "30253",
        "pickup_start_datetime_1": "2023-08-10T11:00:00",
        "pickup_end_datetime_1":   "2023-08-10T11:00:00",
        "delivery_customer_1": "Publix - Lakeland",
        "delivery_address_1": "2600 COUNTY LINE RD",
        "delivery_city_1": "Lakeland", "delivery_state_1": "FL",
        "delivery_zipcode_1": "33811",
        "delivery_start_datetime_1": "2023-08-11T05:00:00",
        "delivery_end_datetime_1":   "2023-08-11T05:00:00",
        "delivery_customer_2": "Publix Super Market",
        "delivery_address_2": "6123 Sawyer Road",
        "delivery_city_2": "Sarasota", "delivery_state_2": "FL",
        "delivery_zipcode_2": "34238",
        "delivery_start_datetime_2": "2023-08-11T09:30:00",
        "delivery_end_datetime_2":   "2023-08-11T09:30:00",
    },
}


# ─────────────────────────────────────────────────────────────
# Validación
# ─────────────────────────────────────────────────────────────
global_errors = 0

# ACCURACY GLOBAL
global_matches = 0
global_total = 0

# Accuracy global por categoría
global_category_stats = {
    "broker":   {"matches": 0, "total": 0},
    "carrier":  {"matches": 0, "total": 0},
    "pickup":   {"matches": 0, "total": 0},
    "delivery": {"matches": 0, "total": 0},
    "other":    {"matches": 0, "total": 0},
}


# BUSINESS ACCURACY
# Solo campos donde truth tiene valor real
business_matches = 0
business_total = 0

# BUSINESS ACCURACY POR CATEGORÍA
business_category_stats = {
    "broker":   {"matches": 0, "total": 0},
    "carrier":  {"matches": 0, "total": 0},
    "pickup":   {"matches": 0, "total": 0},
    "delivery": {"matches": 0, "total": 0},
    "other":    {"matches": 0, "total": 0},
}



for active_id in truth_records.keys():
    logger.info("=" * 70)
    logger.info(f"📍 VALIDANDO LOAD: {active_id}")
    logger.info("=" * 70)

    truth_record = truth_records[active_id]

    # ACCURACY POR LOAD
    load_matches = 0
    load_total = 0

    load_category_stats = {
        "broker":   {"matches": 0, "total": 0},
        "carrier":  {"matches": 0, "total": 0},
        "pickup":   {"matches": 0, "total": 0},
        "delivery": {"matches": 0, "total": 0},
        "other":    {"matches": 0, "total": 0},
    }


    # Leer tabla Silver
    try:
        df_target    = spark.table(source_table).filter(
            F.col("loadConfirmationNumber") == active_id
        )
        target_count = df_target.count()
    except Exception as e:
        logger.error(f"Error al leer {source_table}: {e}")
        global_errors += 1
        continue

    if target_count == 0:
        logger.error(
            f"❌ loadConfirmationNumber={active_id} no encontrado en {source_table}"
        )
        global_errors += 1
        continue

    # Validación de schema
    target_cols = set(df_target.schema.fieldNames())
    truth_cols  = set(truth_record.keys())

    SYSTEM_COLS = {"processed_at", "source_file", "carrier_contact",
                   "modificationTime", "path", "length"}
    missing_in_target = truth_cols - target_cols
    extra_in_target   = target_cols - truth_cols - SYSTEM_COLS

    if missing_in_target:
        logger.error(f"❌ Columnas ausentes en Silver: {missing_in_target}")
        global_errors += 1
    if extra_in_target:
        logger.warning(f"⚠️  Columnas adicionales en destino: {extra_in_target}")       
    

    # Validación de contenido campo a campo
    target_row    = df_target.limit(1).collect()[0]
    target_values = target_row.asDict()
    results       = []

    for field_name in truth_record.keys():
        # Detectar categoría del campo
        if field_name.startswith("broker_"):
            category = "broker"

        elif field_name.startswith("carrier_"):
            category = "carrier"

        elif field_name.startswith("pickup_"):
            category = "pickup"

        elif field_name.startswith("delivery_"):
            category = "delivery"

        else:
            category = "other"        

        if field_name not in target_values:
            results.append((field_name, "❌ Missing Col", truth_record[field_name], "None"))
            continue

        truth_val  = truth_record[field_name]
        target_val = target_values[field_name]

        # Normalización especial para source_file
        if field_name == "source_file":
            norm_truth  = normalize_path(str(truth_val)).lower()  if truth_val  else ""
            norm_target = normalize_path(str(target_val)).lower() if target_val else ""
        else:
            norm_truth  = str(truth_val).strip().lower()  if truth_val  else ""
            norm_target = str(target_val).strip().lower() if target_val else ""

        status = "✅ Match" if norm_truth == norm_target else "❌ Mismatch"
        results.append((field_name, status, truth_val, target_val))

        # =====================================================
        # ACUMULAR MÉTRICAS DE ACCURACY
        # =====================================================

        global_total += 1
        global_category_stats[category]["total"] += 1

        if status == "✅ Match":
            global_matches += 1
            global_category_stats[category]["matches"] += 1

        
        # =====================================================
        # BUSINESS ACCURACY
        # Solo evaluar campos con valor esperado real (no vacíos)
        # =====================================================

        if norm_truth != "":
            
            # Business accuracy global
            business_total += 1

            # Business accuracy por categoría
            business_category_stats[category]["total"] += 1

            if status == "✅ Match":

                business_matches += 1

                business_category_stats[category]["matches"] += 1

    # =====================================================
    # Imprimir resultados
    # =====================================================
    logger.info(f"Resultados para loadConfirmationNumber={active_id}:")
    for field_name, status, truth, target in results:
        if status == "✅ Match":
            logger.info(
                f"{field_name:35} | {status:10} | truth='{truth}' | target='{target}'"
            )
        else:
            logger.error(
                f"{field_name:35} | {status:10} | truth='{truth}' | target='{target}'"
            )
            global_errors += 1
        


# =========================================================
# ACCURACY GLOBAL
# =========================================================
if global_total > 0:
    global_accuracy = (global_matches / global_total) * 100
else:
    global_accuracy = 0

logger.info("=" * 70)
logger.info(f"🎯 ACCURACY GLOBAL: {global_accuracy:.2f}%")
logger.info(f"✅ Matches: {global_matches}")
logger.info(f"📦 Total campos evaluados: {global_total}")
logger.info("=" * 70)


# =========================================================
# ACCURACY POR CATEGORÍA
# =========================================================
logger.info("📊 ACCURACY POR CATEGORÍA")

for category, stats in global_category_stats.items():

    total_cat   = stats["total"]
    matches_cat = stats["matches"]

    if total_cat > 0:
        acc_cat = (matches_cat / total_cat) * 100
    else:
        acc_cat = 0

    logger.info(
        f"{category.upper():10} | "
        f"Accuracy={acc_cat:6.2f}% | "
        f"Matches={matches_cat:4} | "
        f"Total={total_cat:4}"
    )


# =========================================================
# BUSINESS ACCURACY
# =========================================================
if business_total > 0:
    business_accuracy = (business_matches / business_total) * 100
else:
    business_accuracy = 0

logger.info("=" * 70)
logger.info(f"💼 BUSINESS ACCURACY: {business_accuracy:.2f}%")
logger.info(f"✅ Business Matches: {business_matches}")
logger.info(f"📦 Campos negocio evaluados: {business_total}")
logger.info("=" * 70)

# =========================================================
# BUSINESS ACCURACY POR CATEGORÍA
# =========================================================
logger.info("📊 BUSINESS ACCURACY POR CATEGORÍA")

for category, stats in business_category_stats.items():

    total_cat   = stats["total"]
    matches_cat = stats["matches"]

    if total_cat > 0:
        acc_cat = (matches_cat / total_cat) * 100
    else:
        acc_cat = 0

    logger.info(
        f"{category.upper():10} | "
        f"Business Accuracy={acc_cat:6.2f}% | "
        f"Matches={matches_cat:4} | "
        f"Total={total_cat:4}"
    )

logger.info("=" * 70)

# ─────────────────────────────────────────────────────────────
# Resultado global
# ─────────────────────────────────────────────────────────────
if global_errors > 0:
    logger.error(
        f"❌ CONTROL DE CALIDAD RECHAZADO: {global_errors} inconsistencias detectadas."
    )
    raise ValueError("Validation failed for one or more Coyote Logistics records.")
else:
    logger.info("🎉 ✅ SUCCESS GLOBAL: Todos los escenarios coinciden al 100%.")
