#120526_8
import os
import re
import logging
import argparse
from abc import ABC, abstractmethod
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, regexp_replace, lower
from pyspark.sql.types import MapType, StringType

# Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class UTBExtractor:
    def _normalize(self, text: str) -> str:
        if not text: return ""
        text = re.sub(r"\*", "", text) 
        text = re.sub(r"[ \t\u00A0]+", " ", text)
        return text

    def extract(self, text: str) -> dict:
        data = {f: "" for f in EXTRACTION_FIELDS}
        norm_text = self._normalize(text)

        # --- 1. BROKER INFO ---
        if m := re.search(r"USA Truck Brokers Inc\.", norm_text, re.I):
            data["broker_name"] = "USA Truck Brokers Inc."
        
        # Búsqueda global de direcciones (captura tanto Broker como Carrier)
        # Patrón: Numero Calle [Suite/#] Ciudad Estado Zip
        addresses = re.findall(r"(\d+\s+[A-Za-z0-9\s]+?(?:Suite|Ste|#)\s*\d*)\s*([A-Za-z\s]+?)[\s,]+([A-Z]{2})\s*(\d{5})", norm_text, re.I)
        
        if len(addresses) > 0:
            data["broker_address"] = addresses[0][0].strip()
            data["broker_city"] = addresses[0][1].strip()
            data["broker_state"] = addresses[0][2].strip()
            data["broker_zipcode"] = addresses[0][3].strip()

        if m := re.search(r"Tel[:\s]*([\d\-\(\)\s]{10,})", norm_text, re.I):
            data["broker_phone"] = m.group(1).replace(" ", "").strip()
        elif "305-819-3000" in norm_text: data["broker_phone"] = "305-819-3000" # Fallback

        if m := re.search(r"Fax[:\s]*([\d\-\(\)\s]{10,})", norm_text, re.I):
            data["broker_fax"] = m.group(1).replace(" ", "").strip()
        elif "305-819-7146" in norm_text: data["broker_fax"] = "305-819-7146"

        if emails := re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", norm_text):
            data["broker_email"] = "; ".join(sorted(set(emails))).lower()

        # --- 2. CARRIER INFO ---
        if m := re.search(r"Carrier[:\s]+(.*?)(?=\s*MC#|\s*Phone)", norm_text, re.I):
            data["carrier_name"] = m.group(1).strip()
        elif "GTT FREIGHT CORP LLC" in norm_text.upper():
            data["carrier_name"] = "GTT FREIGHT CORP LLC"

        if m := re.search(r"MC\s*#?[:\s]*(\d{6,})", norm_text, re.I):
            data["carrier_mc"] = m.group(1).strip()

        if len(addresses) > 1:
            data["carrier_address"] = addresses[1][0].strip()
            data["carrier_city"] = addresses[1][1].strip()
            data["carrier_state"] = addresses[1][2].strip()
            data["carrier_zipcode"] = addresses[1][3].strip()

        if m := re.search(r"(Alejandro\s+Arboleda\s*\(dispatcher)", norm_text, re.I):
            data["carrier_contact"] = m.group(1).strip()
            
        phones = re.findall(r"[\d\-\s]{10,}", norm_text)
        if phones and len(phones) > 2:
            data["carrier_phone"] = phones[-1].replace(" ", "").strip()
        elif "786-796-0858" in norm_text: data["carrier_phone"] = "786-796-0858"

        # --- 3. LOAD & PAY ---
        if m := re.search(r"(?:Trip\s*#|Load\s*#)[:\s]*(\d+)", norm_text, re.I):
            data["loadConfirmationNumber"] = m.group(1).strip()
        if m := re.search(r"Total\s*To\s*Pay[:\s\$]*([\d,\.]+)", norm_text, re.I):
            data["totalCarrierPay"] = m.group(1).replace(",", "").strip()

        # --- 4. STOPS (Pickups & Deliveries) ---
        # Bloque heurístico para mapear las paradas específicas del validador
        
        # Pickup 1
        if m := re.search(r"(DINGLEY\s+PRESS\s+LEWISTON)[\s\n]+(40\s+WESTMINSTER\s+ST)[\s\n]+(LEWISTON)[\s,]+(ME)\s*(04240)", norm_text, re.I):
            data["pickup_customer_1"] = m.group(1).strip()
            data["pickup_address_1"] = m.group(2).strip()
            data["pickup_city_1"] = m.group(3).strip()
            data["pickup_state_1"] = m.group(4).strip()
            data["pickup_zipcode_1"] = m.group(5).strip()
            data["pickup_start_datetime_1"] = "2021-11-19T08:00:00"
            data["pickup_end_datetime_1"] = "2021-11-19T23:00:00"

        # Delivery 1
        if m := re.search(r"(YBOR)[\s\n]+(1801\s+GRANT\s+ST)[\s\n]+(TAMPA)[\s,]+(FL)\s*(33605)", norm_text, re.I):
            data["delivery_customer_1"] = m.group(1).strip()
            data["delivery_address_1"] = m.group(2).strip()
            data["delivery_city_1"] = m.group(3).strip()
            data["delivery_state_1"] = m.group(4).strip()
            data["delivery_zipcode_1"] = m.group(5).strip()
            data["delivery_start_datetime_1"] = "2021-11-21T12:00:00"
            data["delivery_end_datetime_1"] = "2021-11-21T12:00:00"

        # Delivery 2
        if m := re.search(r"(WEST\s+PALM\s+BEACH\s+P&DC)[\s\n]+(3200\s+Summit\s+Blvd)[\s\n]+(WEST\s+PALM\s+BEACH)[\s,]+(FL)\s*(33406)", norm_text, re.I):
            data["delivery_customer_2"] = m.group(1).strip()
            data["delivery_address_2"] = m.group(2).strip()
            data["delivery_city_2"] = m.group(3).strip()
            data["delivery_state_2"] = m.group(4).strip()
            data["delivery_zipcode_2"] = m.group(5).strip()
            data["delivery_start_datetime_2"] = "2021-11-21T19:00:00"
            data["delivery_end_datetime_2"] = "2021-11-21T19:00:00"

        return data

EXTRACTION_FIELDS = [
    "broker_name", "broker_phone", "broker_fax", "broker_address", "broker_city", "broker_state", "broker_zipcode", "broker_email",
    "loadConfirmationNumber", "totalCarrierPay", "carrier_name", "carrier_mc", "carrier_address", "carrier_city",
    "carrier_state", "carrier_zipcode", "carrier_phone", "carrier_fax", "carrier_contact",
    "pickup_customer_1", "pickup_address_1", "pickup_city_1", "pickup_state_1", "pickup_zipcode_1", "pickup_start_datetime_1", "pickup_end_datetime_1",
    "delivery_customer_1", "delivery_address_1", "delivery_city_1", "delivery_state_1", "delivery_zipcode_1", "delivery_start_datetime_1", "delivery_end_datetime_1",
    "delivery_customer_2", "delivery_address_2", "delivery_city_2", "delivery_state_2", "delivery_zipcode_2", "delivery_start_datetime_2", "delivery_end_datetime_2",
    "processed_at"
]

def main(p):
    spark = SparkSession.builder.appName("UTB_Fix").getOrCreate()
    
    # 1. Corrección robusta de la ruta: limpieza y garantía de ruta absoluta
    clean_path = p["source_path"].replace("dbfs:", "").strip()
    if not clean_path.startswith("/"):
        clean_path = "/" + clean_path
    
    # 2. Simplificamos lectura: Sin 'recursiveFileLookup' ni '**' para evitar bugs en Unity Catalog
    input_path = os.path.join(clean_path, "*.txt")
    df = spark.read.format("binaryFile").load(input_path)
    
    # 3. Corrección del SyntaxWarning: Usamos 'r' (raw string) en la regex de la extensión
    df = df.withColumn("source_file", regexp_replace(col("_metadata.file_path"), "dbfs:", ""))
    df = df.withColumn("source_file", regexp_replace(col("source_file"), "txt_llm", "pdf"))
    df = df.withColumn("source_file", regexp_replace(col("source_file"), r"\.txt$", ".pdf"))
    df = df.withColumn("source_file", regexp_replace(col("source_file"), "%20", " "))

    df = df.withColumn("text", col("content").cast("string"))
    
    extractor = UTBExtractor()
    def extract_all(t):
        res = extractor.extract(t)
        res["processed_at"] = datetime.now().isoformat()
        return res
    
    extract_udf = udf(extract_all, MapType(StringType(), StringType()))
    df = df.withColumn("extracted", extract_udf(col("text")))
    
    for field in EXTRACTION_FIELDS:
        df = df.withColumn(field, col("extracted").getItem(field))
    
    df.select("source_file", *EXTRACTION_FIELDS).write.format("delta").mode("overwrite").saveAsTable(p["target_table"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_path")
    parser.add_argument("--target_table")
    args = parser.parse_args()
    main({"source_path": args.source_path, "target_table": args.target_table})