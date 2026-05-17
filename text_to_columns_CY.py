#VERSION MODIFICADA PARA CY

"""
ruta por defecto para los txt: /Volumes/logistics/bronze/raw/cy/txt_pymupdf4llm
No usar run para hacer pruebas con archivos de texto alojados en otra carpeta 
(por ejemplo, una ruta local o un directorio de pruebas temporales) 
o si se quiere guardar el resultado en una tabla Silver diferente
Para estos casos, ejecutarlo mediante la terminal o la celda de comandos pasando los parámetros explícitos:
python text_to_columns_CY.py --source_path /Ruta/De/Prueba --target_table logistics.silver.mi_tabla_test
"""

import os
import re
import json
import logging
import argparse
from abc import ABC, abstractmethod
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
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
# COYOTE (CY) Extractor
# ============================================================
class CoyoteExtractor(BaseExtractor):
    """Extracts broker, carrier, pickup, and delivery data from Coyote Logistics load confirmation text files."""

    def _normalize(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"\*{2,}", "", text)
        text = re.sub(r"[ \t\u00A0]+", " ", text)
        text = re.sub(r"\r\n?", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _combine_dt(self, date_str, time_str):
        if not date_str or not time_str:
            return ""
        
        # Limpieza de ruidos comunes en texto de horas
        time_str = re.sub(r"(?i)from\s*", "", time_str).strip()
        time_str = re.sub(r"(?i)at\s*", "", time_str).strip()
        
        # Intentar parsear rango (ej: "08:00 - 13:00") tomando solo la hora de inicio
        if "-" in time_str:
            time_str = time_str.split("-")[0].strip()

        # Formatos posibles de fecha en Coyote (ej: 03/29/2023)
        for fmt_date in ["%m/%d/%Y", "%d/%m/%Y"]:
            for fmt_time in ["%H:%M", "%I:%M %p"]:
                try:
                    dt = datetime.strptime(f"{date_str} {time_str}", f"{fmt_date} {fmt_time}")
                    return dt.strftime("%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    continue
        return ""

    # Se elimina la función rígida _extract_stop_blocks por presentar pérdida de datos en cargas multi-stop.
    # Se introduce la función _extract_all_stops para segmentar dinámicamente cualquier cantidad de paradas.
    def _extract_all_stops(self, text: str) -> list:
        """Segmenta el texto completo en bloques independientes por cada parada detectada (Stop X)."""
        # Se buscan todos los patrones que inicien con la estructura 'Stop' seguida de un número y su tipo
        stop_matches = list(re.finditer(r"Stop\s+(\d+):\s*(Pick\s*Up|Delivery)", text, re.I))
        blocks = []
        
        for idx, match in enumerate(stop_matches):
            start_pos = match.start()
            # Se calcula el límite del bloque basándose en el inicio de la siguiente parada
            if idx + 1 < len(stop_matches):
                end_pos = stop_matches[idx + 1].start()
            else:
                # Si es la última parada, el bloque termina donde inicia la sección de cobros o firmas
                charges_match = re.search(r"Charges|Contact\s+None|Facility\s+Notes", text[start_pos:], re.I)
                end_pos = start_pos + charges_match.start() if charges_match else len(text)
                
            stop_num = match.group(1)
            # Se clasifica dinámicamente el tipo de parada para el mapeo de columnas posterior
            stop_type = "pickup" if "pick" in match.group(2).lower() else "delivery"
            stop_text = text[start_pos:end_pos].strip()
            
            blocks.append({
                "number": stop_num,
                "type": stop_type,
                "text": stop_text
            })
        return blocks


    def extract(self, text: str) -> dict:
        """
        Analiza el texto plano de la confirmación de carga de Coyote Logistics,
        priorizando la información del agente operativo para el llenado de los
        campos del Broker, garantizando granularidad en la capa Silver.
        """
        text = self._normalize(text)
        data = {f: "" for f in EXTRACTION_FIELDS}
        if not text:
            return data

        # ----------------------------------------------------------------------
        # 1. Identificación del Número de Confirmación de Carga
        # ----------------------------------------------------------------------
        # El algoritmo localiza la palabra clave 'Load' seguida de dígitos numéricos.
        m = re.search(r"Load\s+(\d+)", text, re.I)
        if m:
            data["loadConfirmationNumber"] = m.group(1).strip()

        # ----------------------------------------------------------------------
        # 2. Información del Broker (Orientada al Agente de Operaciones)
        # ----------------------------------------------------------------------
        # Se mantiene la entidad corporativa principal como base.
        data["broker_name"] = "Coyote Logistics, LLC"
        
        # Extracción analítica de la dirección de facturación corporativa (Encabezado).
        direc_match = re.search(r"(\d+)\s+Northpoint\s+Parkway\s+([A-Za-z\s]+),\s*([A-Z]{2})\s*(\d{5})", text, re.I)
        if direc_match:
            data["broker_address"] = f"{direc_match.group(1)} Northpoint Parkway"
            data["broker_city"] = direc_match.group(2).strip()
            data["broker_state"] = direc_match.group(3).strip()
            data["broker_zipcode"] = direc_match.group(4).strip()

        # ESTRATEGIA DE EXTRACCIÓN DIRIGIDA (AGENTE):
        # El algoritmo aísla el bloque 'Booked By' para capturar de forma exclusiva 
        # los datos de contacto directos del agente encargado de la negociación.
        booked_block_match = re.search(r"##\s*Booked\s+By\s*\n([\s\S]+?)(?=\n##|$)", text, re.I)
        if booked_block_match:
            booked_text = booked_block_match.group(1)
            
            # Captura del correo electrónico directo del agente (Ej: Tamaz.Bazgadze@coyote.com)
            # Reemplaza la coincidencia greedy general del encabezado izquierdo.
            email_agent = re.search(r"([\w\.-]+@coyote\.com)", booked_text, re.I)
            if email_agent:
                data["broker_email"] = email_agent.group(1).strip()
            
            # Captura del teléfono directo y extensión del agente (Ej: +1 (423) 385 3805 x2246)
            phone_agent = re.search(r"Phone:\s*([\+\d\s\(\)\-xX]+)", booked_text, re.I)
            if phone_agent:
                data["broker_phone"] = phone_agent.group(1).strip()
                
            # Captura del fax directo del agente si se encuentra disponible.
            fax_agent = re.search(r"Fax:\s*([\+\d\s\(\)\-xX]+)", booked_text, re.I)
            if fax_agent:
                data["broker_fax"] = fax_agent.group(1).strip()
        
        # Mecanismo de contingencia (Fallback) por seguridad estructural.
        # Si el bloque dinámico 'Booked By' fallara, recurre a los canales generales.
        if not data["broker_email"]:
            data["broker_email"] = "CarrierInvoices@coyote.com"
        if not data["broker_phone"]:
            if m := re.search(r"Please contact Coyote at\s+([\d-]+)", text, re.I):
                data["broker_phone"] = m.group(1).strip()
            else:
                data["broker_phone"] = "877-626-9683"

        # ----------------------------------------------------------------------
        # 3. Información del Transportista (Carrier)
        # ----------------------------------------------------------------------
        agreement_match = re.search(r"Carrier Legal Name\s*-\s*([A-Z0-9 &\.\-]+)", text, re.I)
        if agreement_match:
            data["carrier_name"] = agreement_match.group(1).strip()

        # El identificador USDOT se asigna a 'carrier_mc' siguiendo las pautas de homologación de la tabla.
        usdot_match = re.search(r"Carrier USDOT\s*-\s*(\d+)", text, re.I)
        if usdot_match:
            data["carrier_mc"] = usdot_match.group(1).strip()

        # Extracción del correo electrónico de despacho del transportista en la sección de firmas.
        carrier_email_match = re.search(r"Carrier\s+.*?Email\s+([\w\.-]+@[\w\.-]+)", text, re.I | re.S)
        if carrier_email_match:
            data["carrier_email"] = carrier_email_match.group(1).strip()

        # ----------------------------------------------------------------------
        # 4. Liquidación Económica (Total Pay)
        # ----------------------------------------------------------------------
        # Se prioriza la captura del elemento 'Total' financiero definitivo del documento.
        if m := re.search(r"Total\s+\$([\d,]+\.\d{2})", text, re.I):
            data["totalCarrierPay"] = m.group(1).replace(",", "").strip()
        elif m := re.search(r"Amount\s+\$([\d,]+\.\d{2})", text, re.I):
            data["totalCarrierPay"] = m.group(1).replace(",", "").strip()

        # ----------------------------------------------------------------------
        # 5. Procesamiento Dinámico de Paradas (Capa Medallion - Silver)
        # ----------------------------------------------------------------------
        all_stops = self._extract_all_stops(text)
        p_idx = 1
        d_idx = 1
        
        for stop in all_stops:
            block = stop["text"]
            
            if stop["type"] == "pickup":
                i = p_idx
                if p_idx > 3: continue  # Límite físico: máximo 3 paradas de origen.
                
                if m := re.search(r"Facility\s+([A-Z0-9\s&,\.\-]+?)(?=\s+Driver|\s+Address|\s+Contact|$)", block, re.I):
                    data[f"pickup_customer_{i}"] = m.group(1).strip()
                
                if m := re.search(r"Address\s+(.+?)\s+SLIC\s+([A-Za-z\s]+),\s*([A-Z]{2})(?:\s+N/A)?\s*(\d{5}(?:-\d{4})?)", block, re.I):
                    data[f"pickup_address_{i}"] = m.group(1).strip()
                    data[f"pickup_city_{i}"] = m.group(2).strip()
                    data[f"pickup_state_{i}"] = m.group(3).strip()
                    data[f"pickup_zipcode_{i}"] = m.group(4).strip()

                date_m = re.search(r"Scheduled For\s+([A-Za-z ]+)?(\d{2}/\d{2}/\d{4})", block, re.I)
                time_m = re.search(r"(?:from|at)\s*([\d: \-]+(?:AM|PM)?)", block, re.I)
                if date_m and time_m:
                    extracted_dt = self._combine_dt(date_m.group(2), time_m.group(1))
                    data[f"pickup_start_datetime_{i}"] = extracted_dt
                    data[f"pickup_end_datetime_{i}"] = extracted_dt
                p_idx += 1

            elif stop["type"] == "delivery":
                i = d_idx
                if d_idx > 3: continue  # Límite físico: máximo 3 paradas de destino.
                
                if m := re.search(r"Facility\s+([A-Z0-9\s&,\.\-]+?)(?=\s+Driver|\s+Address|\s+Contact|$)", block, re.I):
                    data[f"delivery_customer_{i}"] = m.group(1).strip()
                
                if m := re.search(r"Address\s+(.+?)\s+SLIC\s+([A-Za-z\s]+),\s*([A-Z]{2})(?:\s+N/A)?\s*(\d{5}(?:-\d{4})?)", block, re.I):
                    data[f"delivery_address_{i}"] = m.group(1).strip()
                    data[f"delivery_city_{i}"] = m.group(2).strip()
                    data[f"delivery_state_{i}"] = m.group(3).strip()
                    data[f"delivery_zipcode_{i}"] = m.group(4).strip()

                date_m = re.search(r"Scheduled For\s+([A-Za-z ]+)?(\d{2}/\d{2}/\d{4})", block, re.I)
                time_m = re.search(r"(?:from|at)\s*([\d: \-]+(?:AM|PM)?)", block, re.I)
                if date_m and time_m:
                    extracted_dt = self._combine_dt(date_m.group(2), time_m.group(1))
                    data[f"delivery_start_datetime_{i}"] = extracted_dt
                    data[f"delivery_end_datetime_{i}"] = extracted_dt
                d_idx += 1

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
    "carrier_fax", "carrier_email", # cambio carrier_contact por carrier_email
    *[f"{p}_{i}" for p in [
        "pickup_customer", "pickup_address", "pickup_city",
        "pickup_state", "pickup_zipcode",
        "pickup_start_datetime", "pickup_end_datetime"
    ] for i in range(1, 4)],
    *[f"{p}_{i}" for p in [
        "delivery_customer", "delivery_address", "delivery_city",
        "delivery_state", "delivery_zipcode",
        "delivery_start_datetime", "delivery_end_datetime"
    ] for i in range(1, 4)],
    "processed_at"
]

# ============================================================
# Spark UDF wrapper
# ============================================================
def extract_fields_udf():
    extractor = CoyoteExtractor()

    def _extract(text):
        result = extractor.extract(text)
        result["processed_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        return result

    return udf(_extract, MapType(StringType(), StringType()))


# ============================================================
# Main (parameterized)
# ============================================================
def main(p):
    spark = SparkSession.builder.appName("TruckR Extraction").getOrCreate()
    logger.info("Starting CY extraction process")

    input_path = os.path.join(p["source_path"], "*.txt")
    df = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.txt")
        .option("recursiveFileLookup", "false")
        .load(input_path)
        .select(col("_metadata.file_path").alias("source_file"), col("content"))
    )

    df = df.withColumn("text", col("content").cast("string")).drop("content")
    logger.info(f"Files detected: {df.count()}")

    extract_udf = extract_fields_udf()
    df = df.withColumn("extracted", extract_udf(col("text")))

    for field in EXTRACTION_FIELDS:
        df = df.withColumn(field, col("extracted").getItem(field))

    df = df.drop("text", "extracted")

    logger.info(f"Writing {df.count()} records to {p['target_table']}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(p["target_table"])
    )

    logger.info("CY extraction completed successfully.")


# ============================================================
# CLI entry
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CY PDF Extraction Parameters")
    parser.add_argument("--source_path")
    parser.add_argument("--target_table")
    
    args, unknown = parser.parse_known_args()

    # Rutas por defecto del Volume raw de la arquitectura Medallion
    source_path_final = args.source_path if args.source_path else "/Volumes/logistics/bronze/raw/cy/txt_pymupdf4llm"
    target_table_final = args.target_table if args.target_table else "logistics.silver.load_confirmations_cy"

    params = {
        "source_path": source_path_final, 
        "target_table": target_table_final
    }
    
    main(params)