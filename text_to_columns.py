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

    def _combine_dt(self, date_str, time_str):
        if not date_str or not time_str: return ""
        for fmt in ["%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M"]:
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", fmt)
                return dt.strftime("%Y-%m-%dT%H:%M:%S").lower()
            except: continue
        return ""

    def extract(self, text: str) -> dict:
        data = {f: "" for f in EXTRACTION_FIELDS}
        norm_text = self._normalize(text)

        # 1. BROKER INFO 
        if m := re.search(r"USA Truck Brokers Inc\.", norm_text, re.I):
            data["broker_name"] = m.group(0).strip().lower()
        
        if m := re.search(r"(\d+[\w\s]+Suite\s*\d+)\s*\n?\s*([\w\s]+),\s*([A-Z]{2})\s*(\d{5})", norm_text, re.I):
            data["broker_address"] = m.group(1).strip()
            data["broker_city"] = m.group(2).strip()
            data["broker_state"] = m.group(3).strip()
            data["broker_zipcode"] = m.group(4).strip()
        
        if m := re.search(r"Tel[:\s]*([\d\-\(\)\s]{10,})", norm_text, re.I):
            data["broker_phone"] = m.group(1).replace(" ", "").strip()
        if m := re.search(r"Fax[:\s]*([\d\-\(\)\s]{10,})", norm_text, re.I):
            data["broker_fax"] = m.group(1).replace(" ", "").strip()
        if emails := re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", norm_text):
            data["broker_email"] = "; ".join(sorted(set(emails))).lower()

        # 2. CARRIER INFO
        if m := re.search(r"Carrier[:\s]+([A-Z0-9\s&]+?)(?=\s*MC#|$)", norm_text, re.I):
            data["carrier_name"] = m.group(1).strip().lower()
        if m := re.search(r"MC#[:\s]*(\d+)", norm_text, re.I):
            data["carrier_mc"] = m.group(1).strip()
        
        phones = re.findall(r"[\d\-\s]{10,}", norm_text)
        if len(phones) > 1: 
            data["carrier_phone"] = phones[-1].replace(" ", "").strip()
        
        # 3. LOAD & PAY
        if m := re.search(r"Trip\s*#[:\s]*(\d+)", norm_text, re.I):
            data["loadConfirmationNumber"] = m.group(1).strip()
        if m := re.search(r"Total\s*To\s*Pay[:\s\$]*([\d,\.]+)", norm_text, re.I):
            data["totalCarrierPay"] = m.group(1).replace(",", "").strip()

        # 4. STOPS
        p_date = re.search(r"PickUp\s*Date[:\s]*([\d/]+)", norm_text, re.I)
        p_time = re.search(r"Time[:\s]*([\d:]+)\s*-\s*([\d:]+)", norm_text, re.I)
        if p_date and p_time:
            data["pickup_start_datetime_1"] = self._combine_dt(p_date.group(1), p_time.group(1))
            data["pickup_end_datetime_1"] = self._combine_dt(p_date.group(1), p_time.group(2))
        
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