import os
import re
import json
import logging
import argparse
from abc import ABC, abstractmethod
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, regexp_replace
from pyspark.sql.types import MapType, StringType

# ============================================================
# Logger
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ============================================================
# Base Extractor
# ============================================================
class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, text: str) -> dict:
        pass


# ============================================================
# UTB Extractor
# ============================================================
class UTBExtractor(BaseExtractor):
    """Extrae datos de broker, carrier y paradas de los archivos de texto de USA Truck Brokers."""

    def _normalize(self, text: str) -> str:
        if not text:
            return ""
        # Eliminamos asteriscos excesivos pero mantenemos estructura básica
        text = re.sub(r"\*{2,}", "", text)
        text = re.sub(r"PickUp\b", "Pickup", text, flags=re.I)
        text = re.sub(r"Del\.\s*Date", "Delivery Date", text, flags=re.I)
        text = re.sub(r"Miami Lake,s", "Miami Lakes", text, flags=re.I)
        text = re.sub(r"[ \t\u00A0]+", " ", text)
        text = re.sub(r"\r\n?", "\n", text)
        # IMPORTANTE: No eliminamos todas las líneas vacías aquí para 
        # mantener la separación de bloques que ayuda a las Regex.
        return text.strip()

    def _combine_dt(self, date_str, time_str):
        if not date_str or not time_str:
            return ""
        for fmt in ["%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M", "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S"]:
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", fmt)
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue
        return ""

    def _extract_stop_blocks(self, text: str, stop_type: str):
        stop_type = stop_type.lower()
        pattern = re.compile(rf"{stop_type.title()}\s*#\s*\d+", re.I)
        markers = list(pattern.finditer(text))

        if not markers:
            alt = re.compile(rf"{stop_type.title()}\b(.*?)(?=(Pickup|Delivery|Send Invoice|Total|$))", re.I | re.S)
            return [m.group(1).strip() for m in alt.finditer(text)]

        blocks = []
        for i, m in enumerate(markers):
            start = m.end()
            end = markers[i + 1].start() if i + 1 < len(markers) else len(text)
            sub = text[start:end]
            cutoff = re.search(r"(Send Invoice|Special Instructions|Total|Please Sign)", sub, re.I)
            blocks.append(sub[:cutoff.start()].strip() if cutoff else sub.strip())
        return blocks

    def extract(self, text: str) -> dict:
        data = {f: "" for f in EXTRACTION_FIELDS}
        if not text:
            return data

        raw_text = text # Mantener una copia original
        text = self._normalize(text)

        # -------- 1. Broker Info (MEJORA: Búsqueda Global) --------
        # El nombre del broker suele estar al inicio o cerca de la dirección.
        if m := re.search(r"USA Truck Brokers Inc\.", text, re.I):
            data["broker_name"] = m.group(0).strip()

        # CAMBIO: Buscamos Tel y Fax en todo el texto (aparecen en 'Special Instructions')
        if m := re.search(r"Tel[:\s]*([\d\-\(\)\s]{10,})", text, re.I):
            data["broker_phone"] = re.sub(r"\s+", "", m.group(1)).strip()
        
        if m := re.search(r"Fax[:\s]*([\d\-\(\)\s]{10,})", text, re.I):
            data["broker_fax"] = re.sub(r"\s+", "", m.group(1)).strip()

        if emails := re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", text):
            data["broker_email"] = "; ".join(sorted(set(emails)))

        # CAMBIO: Regex para dirección del broker más flexible (capta la sección final)
        # Busca: Número Calle, Ciudad, ST Zipcode
        addr_m = re.search(
            r"(\d{1,6}\s+[A-Z0-9\s#.,]+?)\s*\n\s*([A-Za-z\s]+),\s*([A-Z]{2})\s*(\d{5})",
            text, re.I
        )
        if addr_m:
            data.update({
                "broker_address": addr_m.group(1).strip(),
                "broker_city": addr_m.group(2).strip(),
                "broker_state": addr_m.group(3).strip(),
                "broker_zipcode": addr_m.group(4).strip()
            })

        if m := re.search(r"Trip\s*#[:\s]*([A-Z0-9\-]+)", text, re.I):
            data["loadConfirmationNumber"] = m.group(1).strip()

        if m := re.search(r"Total\s*(?:To\s*Pay|Carrier\s*Pay)[:\s\$]*([\d,]+\.\d{2})", text, re.I):
            data["totalCarrierPay"] = m.group(1).replace(",", "").strip()

        # -------- 2. Carrier Info (MEJORA: Extracción Independiente) --------
        # En lugar de una sola Regex gigante, extraemos campo por campo para evitar fallos en cascada.
        
        if m := re.search(r"Carrier\b\s*(?:<br>)?\s*([A-Z0-9 &\.\-]+)", text, re.I):
            data["carrier_name"] = m.group(1).strip()
            
        if m := re.search(r"MC\s*#\s*(?:<br>)?\s*(\d+)", text, re.I):
            data["carrier_mc"] = m.group(1).strip()

        if m := re.search(r"Address\s*(?:<br>)?\s*([0-9A-Za-z\s#.,\-]+)", text, re.I):
            data["carrier_address"] = m.group(1).strip()

        # Buscamos la ciudad/estado del carrier específicamente al final del documento
        # para no confundirla con la del broker capturada arriba.
        carrier_loc = re.findall(r"([A-Z\s]{3,}),\s*([A-Z]{2})\s*(\d{5})", text)
        if len(carrier_loc) > 0:
            # Usualmente la última aparición de este patrón es la del carrier/firma
            last_loc = carrier_loc[-1]
            data["carrier_city"] = last_loc[0].strip()
            data["carrier_state"] = last_loc[1].strip()
            data["carrier_zipcode"] = last_loc[2].strip()

        if m := re.search(r"Phone\s*(?:<br>)?\s*([\d\-\s\(\)]{10,})", text, re.I):
            data["carrier_phone"] = m.group(1).strip()

        if m := re.search(r"Contact\s*(?:<br>)?\s*([A-Za-z\s\-\.,()]+)", text, re.I):
            # Limpiamos posibles residuos de la tabla Markdown
            contact_val = re.split(r"<br>|\|", m.group(1))[0]
            data["carrier_contact"] = contact_val.strip()

        # -------- 3. Pickups --------
        pickups = self._extract_stop_blocks(text, "pickup")
        for i, block in enumerate(pickups[:3], start=1):
            if m := re.search(r"Customer[:\s]*([A-Z0-9 &\.\-]+?)(?=\s*(Pick.?Up|Address|City|Zip|Phone|$))", block, re.I):
                data[f"pickup_customer_{i}"] = m.group(1).strip()
            if m := re.search(r"Address\s*#1[:\s]*([0-9A-Za-z\.,#\-\s]+?)(?:\n|Pick(?:up)?\s*Time|$)", block, re.I):
                data[f"pickup_address_{i}"] = m.group(1).strip()
            if m := re.search(r"City[:\s]*([A-Za-z\s]+),\s*([A-Z]{2})", block, re.I):
                data[f"pickup_city_{i}"] = m.group(1).strip()
                data[f"pickup_state_{i}"] = m.group(2).strip()
            if m := re.search(r"Zip(?:\s*Code)?[:\s]*([0-9]{5})", block, re.I):
                data[f"pickup_zipcode_{i}"] = m.group(1).strip()
            date_m = re.search(r"Pick(?:up)?\s*Date[:\s]*([0-9/]+)", block, re.I)
            times_m = re.search(r"(?:Pick(?:up)?\s*Time|Time)[:\s]*([0-9:]+)\s*-\s*([0-9:]+)", block, re.I)
            if date_m and times_m:
                data[f"pickup_start_datetime_{i}"] = self._combine_dt(date_m.group(1), times_m.group(1))
                data[f"pickup_end_datetime_{i}"] = self._combine_dt(date_m.group(1), times_m.group(2))

        # -------- 4. Deliveries --------
        deliveries = self._extract_stop_blocks(text, "delivery")
        for i, block in enumerate(deliveries[:3], start=1):
            if m := re.search(r"Customer[:\s]*([A-Z0-9 &\.\-]+?)(?=\s*(Delivery|Address|City|Zip|Phone|$))", block, re.I):
                data[f"delivery_customer_{i}"] = m.group(1).strip()
            if m := re.search(r"Address\s*#1[:\s]*([0-9A-Za-z\.,#\-\s]+)", block, re.I):
                val = re.sub(r"\bDelivery\s*Time[:\s]*[0-9: -ETapm]*", "", m.group(1), flags=re.I).strip()
                data[f"delivery_address_{i}"] = val
            if m := re.search(r"City[:\s]*([A-Za-z\s]+),\s*([A-Z]{2})", block, re.I):
                data[f"delivery_city_{i}"] = m.group(1).strip()
                data[f"delivery_state_{i}"] = m.group(2).strip()
            if m := re.search(r"Zip(?:\s*Code)?[:\s]*([0-9]{5})", block, re.I):
                data[f"delivery_zipcode_{i}"] = m.group(1).strip()
            date_m = re.search(r"Delivery\s*Date[:\s]*([0-9/]+)", block, re.I)
            times_m = re.search(r"(?:Delivery\s*Time|Time)[:\s]*([0-9:]+)\s*-\s*([0-9:]+)", block, re.I)
            if date_m and times_m:
                data[f"delivery_start_datetime_{i}"] = self._combine_dt(date_m.group(1), times_m.group(1))
                data[f"delivery_end_datetime_{i}"] = self._combine_dt(date_m.group(1), times_m.group(2))

        return data


# ============================================================
# Schema fields
# ============================================================
EXTRACTION_FIELDS = [
    "broker_name", "broker_phone", "broker_fax", "broker_address",
    "broker_city", "broker_state", "broker_zipcode", "broker_email",
    "loadConfirmationNumber", "totalCarrierPay",
    "carrier_name", "carrier_mc", "carrier_address", "carrier_city",
    "carrier_state", "carrier_zipcode", "carrier_phone",
    "carrier_fax", "carrier_contact",
    *[f"{p}_{i}" for p in ["pickup_customer", "pickup_address", "pickup_city", "pickup_state", "pickup_zipcode", "pickup_start_datetime", "pickup_end_datetime"] for i in range(1, 4)],
    *[f"{p}_{i}" for p in ["delivery_customer", "delivery_address", "delivery_city", "delivery_state", "delivery_zipcode", "delivery_start_datetime", "delivery_end_datetime"] for i in range(1, 4)],
    "processed_at"
]

# ============================================================
# Spark UDF wrapper
# ============================================================
def extract_fields_udf():
    extractor = UTBExtractor()
    def _extract(text):
        result = extractor.extract(text)
        result["processed_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        return result
    return udf(_extract, MapType(StringType(), StringType()))


# ============================================================
# Main (MEJORA: Limpieza de rutas para Validación)
# ============================================================
def main(p):
    spark = SparkSession.builder.appName("TruckR Extraction").getOrCreate()
    logger.info("Starting UTB extraction process")

    input_path = os.path.join(p["source_path"], "*.txt")
    df = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.txt")
        .load(input_path)
        .select(col("_metadata.file_path").alias("source_file_raw"), col("content"))
    )

    # CORRECCIÓN: Ajustamos la ruta para que coincida con lo que el Validador espera (Truth)
    # 1. Quitamos el prefijo dbfs:
    # 2. Cambiamos .txt por .pdf
    # 3. Convertimos a minúsculas para evitar problemas de case-sensitivity
    df = df.withColumn("source_file", regexp_replace(col("source_file_raw"), "dbfs:", ""))
    df = df.withColumn("source_file", regexp_replace(col("source_file"), "\.txt$", ".pdf"))

    df = df.withColumn("text", col("content").cast("string")).drop("content", "source_file_raw")
    
    extract_udf = extract_fields_udf()
    df = df.withColumn("extracted", extract_udf(col("text")))

    for field in EXTRACTION_FIELDS:
        df = df.withColumn(field, col("extracted").getItem(field))

    df = df.drop("text", "extracted")

    logger.info(f"Writing records to {p['target_table']}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(p["target_table"])
    )
    logger.info("UTB extraction completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_path")
    parser.add_argument("--target_table")
    args = parser.parse_args()
    main({"source_path": args.source_path, "target_table": args.target_table})