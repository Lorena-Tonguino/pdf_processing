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

import re
from datetime import datetime

class UTBExtractor:
    def _normalize(self, text: str) -> str:
        if not text: return ""
        # 1. Eliminar asteriscos
        text = re.sub(r"\*", "", text)
        # 2. Colapsar TODO tipo de espacio en blanco (incluyendo saltos de línea y tabs) a un solo espacio
        # Esto soluciona los problemas de "espacios dobles" y "saltos de línea" en direcciones 
        text = re.sub(r"[\s\n\r\t\u00A0]+", " ", text)
        return text.strip()

    def _parse_datetime(self, date_str: str, time_str: str) -> str:
        """Convierte formatos MM/DD/YYYY y HH:MM:SS a ISO YYYY-MM-DDTHH:MM:SS """
        try:
            date_part = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", date_str).group(1)
            time_part = re.search(r"(\d{1,2}:\d{2}:\d{2})", time_str).group(1)
            dt = datetime.strptime(f"{date_part} {time_part}", "%m/%d/%Y %H:%M:%S")
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except:
            return ""

    def extract(self, text: str) -> dict:
        data = {f: "" for f in EXTRACTION_FIELDS}
        norm_text = self._normalize(text)

        # --- 1. LOAD INFO ---
        if m := re.search(r"Trip\s*#:\s*(\d+)", norm_text, re.I):
            data["loadConfirmationNumber"] = m.group(1).strip() # 301238 [cite: 1]

        # --- 2. BROKER INFO (Generalizado) ---
        # Nombre (Suele estar al inicio o cerca de las instrucciones)
        if "USA Truck Brokers Inc." in norm_text:
            data["broker_name"] = "USA Truck Brokers Inc."

        # Dirección, Teléfono y Fax (Buscando patrones cerca de la sección de facturación) 
        broker_addr_pattern = r"USA\s+Truck\s+Brokers\s+Inc\.\s+(.*?)\s+(.*?),\s+([A-Z]{2})\s+(\d{5})"
        if m := re.search(broker_addr_pattern, norm_text, re.I):
            data["broker_address"] = m.group(1).strip()
            data["broker_city"] = m.group(2).strip()
            data["broker_state"] = m.group(3).strip()
            data["broker_zipcode"] = m.group(4).strip()

        if m := re.search(r"Tel[:\s]*([\d\-\s]{10,})", norm_text, re.I):
            data["broker_phone"] = m.group(1).replace(" ", "").strip() # 305-819-3000 [cite: 4]
        if m := re.search(r"Fax[:\s]*([\d\-\s]{10,})", norm_text, re.I):
            data["broker_fax"] = m.group(1).replace(" ", "").strip() # 305-819-7146 [cite: 5]

        # Emails (Recopilar todos los encontrados) [cite: 3, 5, 18]
        emails = re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", norm_text)
        if emails:
            data["broker_email"] = "; ".join(sorted(set(emails))).lower()

        # --- 3. CARRIER INFO (Desde la tabla final)  ---
        if m := re.search(r"Carrier\s+([A-Z0-9\s]+?)\s+MC\s*#", norm_text, re.I):
            data["carrier_name"] = m.group(1).strip()
        
        if m := re.search(r"MC\s*#?\s*(\d+)", norm_text, re.I):
            data["carrier_mc"] = m.group(1).strip() # 1311415

        # Carrier Address (Usando \s* para manejar los múltiples espacios entre TX y 78215) 
        carrier_pattern = r"Address\s+(.*?)\s+City\s+(.*?),\s+([A-Z]{2})\s+(\d{5})"
        if m := re.search(carrier_pattern, norm_text, re.I):
            data["carrier_address"] = m.group(1).strip()
            data["carrier_city"] = m.group(2).strip()
            data["carrier_state"] = m.group(3).strip()
            data["carrier_zipcode"] = m.group(4).strip()

        if m := re.search(r"Phone\s+([\d\-\s]{10,})", norm_text, re.I):
            data["carrier_phone"] = m.group(1).replace(" ", "").strip() # 786-796-0858
        
        if m := re.search(r"Contact\s+(.*?)\s+(?:Service|Notes|Payment)", norm_text, re.I):
            data["carrier_contact"] = m.group(1).strip() # Alejandro Arboleda (dispatcher

        # --- 4. STOPS (Pickup & Deliveries) ---
        def extract_stop_data(prefix, stop_type_label, stop_num):
            # Tu patrón corregido: Maneja "Address # 1" y salta campos intermedios como "Contact:" 
            pattern = (
                rf"{stop_type_label}\s*#\s*{stop_num}\s+"
                rf"Customer:\s*(.*?)\s+"
                rf"{stop_type_label}\s*Date:\s*([\d\-/]+).*?"
                rf"Address\s*#\s*1:\s*(.*?)\s+"
                rf"{stop_type_label}\s*Time:\s*([\d:]+)\s*-\s*([\d:]+).*?"
                rf"City:\s*([^,:]+?),\s*([A-Z]{2})\s+.*?" 
                rf"Zip\s*Code:\s*(\d{{5}})"
            )
            m = re.search(pattern, norm_text, re.I)
            if m:
                data[f"{prefix}_customer_{stop_num}"] = m.group(1).strip()
                data[f"{prefix}_address_{stop_num}"] = m.group(3).strip()
                data[f"{prefix}_city_{stop_num}"] = m.group(6).strip()
                data[f"{prefix}_state_{stop_num}"] = m.group(7).strip()
                data[f"{prefix}_zipcode_{stop_num}"] = m.group(8).strip()
                
                data[f"{prefix}_start_datetime_{stop_num}"] = self._parse_datetime(m.group(2), m.group(4))
                data[f"{prefix}_end_datetime_{stop_num}"] = self._parse_datetime(m.group(2), m.group(5))

        extract_stop_data("pickup", "PickUp", 1)    # LEWISTON, ME 
        extract_stop_data("delivery", "Delivery", 1) # TAMPA, FL [cite: 3]
        extract_stop_data("delivery", "Delivery", 2) # WEST PALM BEACH, FL [cite: 3]

        # --- 5. FINANCIALS ---
        if m := re.search(r"Total\s*To\s*Pay[:\s\$]*([\d,\.]+)", norm_text, re.I):
            data["totalCarrierPay"] = m.group(1).replace(",", "").strip() # 3800.00 

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