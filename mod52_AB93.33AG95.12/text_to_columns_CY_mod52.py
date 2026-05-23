# VERSION MODIFICADA PARA CY — v5

import os
import re
import logging
import argparse
from abc import ABC, abstractmethod
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, regexp_replace, col
from pyspark.sql.types import MapType, StringType

# ============================================================
# Logger
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================
# Base Extractor
# ============================================================
class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, text: str) -> dict:
        pass


# ============================================================
# COYOTE (CY) Extractor
# ============================================================
class CoyoteExtractor(BaseExtractor):
    def _normalize(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"\*{2,}", "", text)
        text = re.sub(r"[ \t\u00A0]+", " ", text)
        text = re.sub(r"\r\n?", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _combine_dt(self, date_str: str, time_str: str) -> str:
        if not date_str or not time_str:
            return ""
        time_str = re.sub(r"(?i)(from|at)\s*", "", time_str).strip()
        if "-" in time_str:
            time_str = time_str.split("-")[0].strip()
        for fmt_date in ["%m/%d/%Y", "%d/%m/%Y"]:
            for fmt_time in ["%H:%M", "%I:%M %p", "%H:%M:%S"]:
                try:
                    dt = datetime.strptime(f"{date_str} {time_str}", f"{fmt_date} {fmt_time}")
                    return dt.strftime("%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    continue
        return ""

    def _extract_all_stops(self, raw_text: str) -> list:
        stop_pattern = r'(?im)^(?:#{1,3}\s*)?(Stop\s+\d+\s*:\s*(?:Pick\s*Up|Delivery))'
        parts = re.split(stop_pattern, raw_text)
        stops = []
        i = 1
        while i < len(parts) - 1:
            header = parts[i]
            body   = parts[i + 1] if (i + 1) < len(parts) else ""
            num_m = re.search(r'(\d+)', header)
            stop_num = int(num_m.group(1)) if num_m else 0
            stop_type = "pickup" if re.search(r'(?i)pick\s*up', header) else "delivery"
            stops.append({
                "type":   stop_type,
                "number": stop_num,
                "text":   header + "\n" + body
            })
            i += 2
        return stops

    def _extract_facility(self, block: str) -> str:
        m = re.search(
            r'Facility\s+(.+?)'
            # stop-words del lookahead del regex
            r'(?=\s+(?:Address|SLIC|Contact|Phone|Notes|Facility|Numbers|No\s+Touch|Confirmation|>|Mon|Tue|Wed|Thu|Fri|Sat|Sun|\|))',
            block,
            re.I | re.S
        )
        if not m:
            return ""
        raw = m.group(1).strip()
        
        # Limpieza para que no capture datos de columnas colapsadas
        raw = re.sub(r'(?i)(Notes\s+Numbers.*|Notes\s+\-.*|Numbers\s+\d+.*)', '', raw).strip()
        raw = re.sub(r'~~\S+~~', '', raw)
        raw = re.sub(r'(?i)\s*>?\s*Confirmation\b.*', '', raw).strip()
        raw = re.sub(r'(?i)\s*No\s+Touch\b.*', '', raw).strip()
        raw = re.sub(r'(?i)\s*Driver\s+Work\b', ' ', raw).strip() # Reemplazar Driver Work con un espacio
        raw = re.sub(r'\s+', ' ', raw)
        return raw.strip(" -|")

    def _extract_geo(self, block: str) -> dict:
        result = {"address": "", "city": "", "state": "", "zipcode": ""}
        addr_zone_m = re.search(
            r'Address\s+(.+?)(?=\s+(?:Contact|Phone|Facility\s+Notes|STACEY|FRCO|$))',
            block, re.I | re.S
        )

        print(f"DEBUG block: [{block[:300]}]")  # <-- aquí
        print(f"DEBUG addr_zone_m: [{addr_zone_m}]")

        if not addr_zone_m:
            return result

        zone = addr_zone_m.group(1)
        zone = re.sub(r'(?i)\bSLIC\b', '', zone)
        zone = re.sub(r'(?i)\bN/A\b', '', zone)
        zone = re.sub(r'(?i)Driver\s+Work', '', zone)
        zone = re.sub(r'(?i)No\s+Touch', '', zone)
        zone = re.sub(r'(?i)Lumper', '', zone)
        zone = re.sub(r'~~\S+~~', '', zone)
        zone = " ".join(zone.split())
        print(f"DEBUG zone: [{zone}]") 

        # Caso A: CITY, ST ZIP
        geo_m = re.search(r'(.+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$', zone, re.I)
        
        if geo_m:
            before_comma = geo_m.group(1).strip()
            state   = geo_m.group(2).strip().upper()
            zipcode = geo_m.group(3).strip()

            slic_m = re.search(r'^(.+?)\s+(?:SLIC|N/A)\b', before_comma, re.I)
            if slic_m:
                address = slic_m.group(1).strip().upper()
            else:
                words = before_comma.split()
                address = " ".join(words[:-1]).upper()

            city = before_comma.split()[-1]

            result["address"] = address
            result["city"]    = city
            result["state"]   = state
            result["zipcode"] = zipcode
            return result

        # Caso B: ZIP separado (ej: GA N/A 30122)
        geo_m2 = re.search(r'([A-Za-z][A-Za-z\s\-\.]+?),\s*([A-Z]{2})\b.*?(\d{5}(?:-\d{4})?)', zone, re.I)
        if geo_m2:
            city_raw = geo_m2.group(1).strip()
            city_words = city_raw.split()
            city = city_words[-1] if city_words else city_raw
            
            addr_end = zone.find(geo_m2.group(1))
            addr_raw = zone[:addr_end].strip() if addr_end > 0 else ""
            
            result["address"] = addr_raw.upper()
            result["city"]    = city
            result["state"]   = geo_m2.group(2).strip().upper()
            result["zipcode"] = geo_m2.group(3).strip()
            return result

        return result

    def _extract_date(self, block: str) -> str:
        m = re.search(r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(\d{2}/\d{2}/\d{4})', block, re.I)
        return m.group(1) if m else ""

    def _extract_times(self, block: str) -> tuple:
        rng_m = re.search(r'from\s+(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})', block, re.I)
        if rng_m:
            return rng_m.group(1), rng_m.group(2)
        at_m = re.search(r'at\s+(\d{1,2}:\d{2})', block, re.I)
        if at_m:
            t = at_m.group(1)
            return t, t
        return "", ""

    def extract(self, text: str) -> dict:
        text = self._normalize(text)
        data = {f: "" for f in EXTRACTION_FIELDS if f != "source_file"}
        if not text:
            return data

        m = re.search(r"Load\s+(\d+)", text, re.I)
        if m:
            data["loadConfirmationNumber"] = m.group(1).strip()

        # Aseguramos Coyote como default
        data["broker_name"] = "Coyote Logistics, LLC"
        data["broker_address"] = "960 Northpoint Parkway Suite 150"
        data["broker_city"] = "Alpharetta"
        data["broker_state"] = "GA"
        data["broker_zipcode"] = "30005"

        booked_m = re.search(r'##\s*Booked\s+By\s*\n([\s\S]+?)(?=\n##|\Z)', text, re.I)
        if booked_m:
            b = booked_m.group(1)
            email_m = re.search(r'([\w\.\-]+@coyote\.com)', b, re.I)
            if email_m: data["broker_email"] = email_m.group(1).strip()
            phone_m = re.search(r'Phone:\s*([\+\d\s\(\)\-xX]+)', b, re.I)
            if phone_m: data["broker_phone"] = phone_m.group(1).strip()
            fax_m = re.search(r'Fax:\s*([\+\d\s\(\)\-xX]+)', b, re.I)
            if fax_m and fax_m.group(1).strip().lower() != "none":
                data["broker_fax"] = fax_m.group(1).strip()

        if not data["broker_email"]: data["broker_email"] = "CarrierInvoices@coyote.com"
        if not data["broker_phone"]:
            ph_m = re.search(r'Please contact Coyote at\s+([\d\-]+)', text, re.I)
            data["broker_phone"] = ph_m.group(1).strip() if ph_m else "877-626-9683"

        cn_m = re.search(r'Carrier Legal Name\s*-\s*([^\[\]\n]+)', text, re.I)
        if cn_m: data["carrier_name"] = cn_m.group(1).strip()

        mc_m = re.search(r'Carrier USDOT\s*-\s*(\d+)', text, re.I)
        if mc_m: data["carrier_mc"] = mc_m.group(1).strip()

        emails = re.findall(r'[\w\.\-]+@[\w\.\-]+\.\w+', text, re.I)
        carrier_emails = [e for e in emails if "coyote.com" not in e.lower()]
        if carrier_emails: data["carrier_email"] = carrier_emails[0]

        pay_m = (
            re.search(r'Total\s+USD\s*\$?\s*([\d,]+\.\d{2})', text, re.I) or
            re.search(r'USD\s*\$\s*([\d,]+\.\d{2})',           text, re.I) or
            re.search(r'Total\s+\$\s*([\d,]+\.\d{2})',         text, re.I)
        )
        if pay_m: data["totalCarrierPay"] = pay_m.group(1).replace(",", "").strip()

        all_stops = self._extract_all_stops(text)
        p_idx, d_idx = 1, 1

        for stop in all_stops:
            stype = stop["type"]
            block = stop["text"]

            if stype == "pickup":
                if p_idx > 1: continue
                prefix, idx = "pickup", p_idx
            else:
                if d_idx > 2: continue
                prefix, idx = "delivery", d_idx

            facility = self._extract_facility(block)
            if facility: data[f"{prefix}_customer_{idx}"] = facility

            geo = self._extract_geo(block)
            if geo["state"]:           
                data[f"{prefix}_address_{idx}"] = geo["address"]
                data[f"{prefix}_city_{idx}"]    = geo["city"]
                data[f"{prefix}_state_{idx}"]   = geo["state"]
                data[f"{prefix}_zipcode_{idx}"] = geo["zipcode"]

            date_str = self._extract_date(block)
            hora_ini, hora_fin = self._extract_times(block)

            if date_str and hora_ini:
                data[f"{prefix}_start_datetime_{idx}"] = self._combine_dt(date_str, hora_ini)
                data[f"{prefix}_end_datetime_{idx}"]   = self._combine_dt(date_str, hora_fin)

            if stype == "pickup": p_idx += 1
            else: d_idx += 1

        return data


EXTRACTION_FIELDS = [
    "source_file",
    "broker_name", "broker_phone", "broker_fax",
    "broker_address", "broker_city", "broker_state", "broker_zipcode", "broker_email",
    "loadConfirmationNumber", "totalCarrierPay",
    "carrier_name", "carrier_mc",
    "carrier_address", "carrier_city", "carrier_state", "carrier_zipcode",
    "carrier_phone", "carrier_fax", "carrier_email",
    "pickup_customer_1",  "pickup_address_1",  "pickup_city_1",  "pickup_state_1",  "pickup_zipcode_1",  "pickup_start_datetime_1",  "pickup_end_datetime_1",
    "delivery_customer_1", "delivery_address_1", "delivery_city_1", "delivery_state_1", "delivery_zipcode_1", "delivery_start_datetime_1", "delivery_end_datetime_1",
    "delivery_customer_2", "delivery_address_2", "delivery_city_2", "delivery_state_2", "delivery_zipcode_2", "delivery_start_datetime_2", "delivery_end_datetime_2",
]


def extract_fields_udf():
    def _extract(text):
        extractor = CoyoteExtractor()
        return extractor.extract(text)
    return udf(_extract, MapType(StringType(), StringType()))


def run(p):
    spark = SparkSession.builder.appName("CoyotePDFToColumns").getOrCreate()

    try:
        logger.info(f"Limpiando tabla destino: {p['target_table']}")
        spark.sql(f"TRUNCATE TABLE {p['target_table']}")
    except Exception as e:
        logger.warning(f"No se pudo truncar tabla: {e}")

    logger.info(f"Leyendo TXTs desde Bronze: {p['source_path']}")

    # 1. Utilizamos text + wholetext en vez de binaryFile
    df = (
        spark.read.format("text")
        .option("wholetext", "true")
        .option("pathGlobFilter", "*.txt")
        .load(p["source_path"])
    )

    # 2. Utilizamos metadatos de archivo de Unity Catalog
    df = df.withColumn("source_file", col("_metadata.file_path"))

    df = df.withColumn("source_file", regexp_replace(col("source_file"), r"^dbfs:", ""))
    df = df.withColumn("source_file", regexp_replace(col("source_file"), r"^file:", ""))
    df = df.withColumn("source_file", regexp_replace(col("source_file"), r"/txt_pymupdf4llm/", "/pdf/"))
    df = df.withColumn("source_file", regexp_replace(col("source_file"), r"\.txt$", ".pdf"))
    df = df.withColumn("source_file", regexp_replace(col("source_file"), "%20", " "))

    # La columna del lector de texto se llama "value"
    df = df.withColumn("text", col("value").cast("string")).drop("value")
    logger.info(f"Archivos detectados: {df.count()}")

    extract_udf = extract_fields_udf()
    df = df.withColumn("extracted", extract_udf(col("text")))

    for field in EXTRACTION_FIELDS:
        if field != "source_file":
            df = df.withColumn(field, col("extracted").getItem(field))

    df = df.drop("text", "extracted")

    logger.info(f"Escribiendo {df.count()} registros en {p['target_table']}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(p["target_table"])
    )
    logger.info("Extracción CY completada correctamente.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CY PDF Extraction Parameters")
    parser.add_argument("--source_path",  default=None)
    parser.add_argument("--target_table", default=None)
    args, _ = parser.parse_known_args()

    params = {
        "source_path":  args.source_path  or "/Volumes/logistics/bronze/raw/cy/txt_pymupdf4llm",
        "target_table": args.target_table or "logistics.silver.load_confirmations_cy",
    }
    run(params)