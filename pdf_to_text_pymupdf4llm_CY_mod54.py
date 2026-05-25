# VERSION MODIFICADA PARA CY — pdf_to_text v2
"""
Convierte PDFs de Coyote Logistics a TXT (Markdown) usando pymupdf4llm.

CAMBIOS v2:
- Extracción página a página: reduce el colapso de columnas que ocurre
  cuando pymupdf4llm procesa el PDF completo de una sola vez.
- margins=(0, 0, 0, 0): evita que márgenes artificiales mezclen columnas.
- page_chunks=False: resultado como string único por página, más fácil
  de concatenar.
- Fallback: si una página falla se registra el error y se continúa con
  el resto del documento.
- show_progress=False: elimina salida ruidosa en producción.

NOTA sobre calidad del TXT:
El PDF de Coyote usa un layout de 2 columnas en las secciones de paradas
(Stop N). pymupdf4llm no puede reconstruir fielmento ese layout; las
columnas se aplanan en líneas horizontales densas. El extractor
text_to_columns_CY.py v2 está diseñado para tolerar ese formato.
"""

# Instalar librerías (Databricks — dejar que pip elija versiones compatibles)
#%pip install pymupdf pymupdf4llm
dbutils.library.restartPython()

import os
import time
import logging
import argparse
import fitz          # PyMuPDF
import pymupdf4llm

# ─────────────────────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

start_time = time.time()

# ─────────────────────────────────────────────────────────────
# Argumentos CLI
# ─────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Extraer PDFs a Markdown TXT")
parser.add_argument("--source_path", required=False, help="Carpeta raíz con PDFs")
parser.add_argument("--target_path", required=False, help="Carpeta destino para TXTs")
args, _ = parser.parse_known_args()

# Rutas por defecto para CY
input_root  = args.source_path or "/Volumes/logistics/bronze/raw/cy/pdf/"
output_root = args.target_path or "/Volumes/logistics/bronze/raw/cy/txt_pymupdf4llm/"

logger.info(f"Iniciando extracción de PDFs desde: {input_root}")
logger.info(f"Destino TXTs: {output_root}")


# ─────────────────────────────────────────────────────────────
# Función de procesamiento — extracción página a página
# ─────────────────────────────────────────────────────────────
def process_pdf(pdf_path: str) -> bool:
    """
    Convierte un PDF a Markdown TXT extrayendo página por página.
    Estrategia página a página: reduce el colapso horizontal de columnas
    que pymupdf4llm produce cuando procesa el documento completo.
    """
    try:
        doc = fitz.open(pdf_path)
        md_pages = []

        for page_num in range(len(doc)):
            try:
                page_md = pymupdf4llm.to_markdown(
                    doc,
                    pages=[page_num],
                    margins=(0, 0, 0, 0),   # sin márgenes artificiales
                    page_chunks=False,       # resultado como string
                    show_progress=False,
                )
                md_pages.append(page_md)
            except Exception as page_err:
                logger.warning(
                    f"⚠️  Página {page_num + 1} de '{pdf_path}' falló: {page_err}"
                )
                md_pages.append("")         # continuar con el resto

        doc.close()
        text = "\n\n".join(md_pages)

        # Calcular ruta de salida preservando estructura de subdirectorios
        relative_path   = os.path.relpath(pdf_path, input_root)
        relative_noext  = os.path.splitext(relative_path)[0]
        output_dir      = os.path.join(output_root, os.path.dirname(relative_path))
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(output_root, relative_noext + ".txt")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(text)

        logger.info(f"✅ {pdf_path} → {output_file}")
        return True

    except Exception as e:
        logger.error(f"❌ Error procesando '{pdf_path}': {e}")
        return False


# ─────────────────────────────────────────────────────────────
# Recorrer todos los PDFs bajo input_root
# ─────────────────────────────────────────────────────────────
processed_count = 0
failed_count    = 0

for root, _, files in os.walk(input_root):
    for file in files:
        if file.lower().endswith(".pdf"):
            pdf_path = os.path.join(root, file)
            if process_pdf(pdf_path):
                processed_count += 1
            else:
                failed_count += 1

logger.info(f"PDFs procesados: {processed_count} | Fallidos: {failed_count}")

if processed_count == 0:
    logger.error("No se procesó ningún PDF. Verificar ruta de entrada.")
    raise SystemExit(1)

total_duration = time.time() - start_time
logger.info(f"✅ COMPLETADO EN {total_duration:.2f} SEGUNDOS")
