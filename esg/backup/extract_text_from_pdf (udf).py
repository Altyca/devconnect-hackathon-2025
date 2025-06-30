# Databricks notebook source
# MAGIC %pip install PyMuPDF

# COMMAND ----------

import fitz  # PyMuPDF
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("string")
def extract_text_from_pdf(pdf_paths: pd.Series) -> pd.Series:
    """
    Extracts text from PDF files.

    This function takes a Pandas Series of PDF file paths, reads each PDF, and extracts the text content.
    The PDF file paths should be in the format 'dbfs:/Volumes/.../file.pdf'.

    Usage:
       binary_df = spark.read.format("binaryFile").load("dbfs:/Volumes/.../path/to/folder") # Load PDF files
       text_df = binary_df.withColumn("text", extract_text_from_pdf(binary_df.path))
       display(text_df)

    Args:
    pdf_paths (pd.Series): A Pandas Series containing paths to PDF files.

    Returns:
    pd.Series: A Pandas Series containing the extracted text from each PDF file.
    """
    def extract_text(pdf_path):
        pdf_path_no_prefix = pdf_path.removeprefix('dbfs:')
        document = fitz.open(pdf_path_no_prefix)
        text = ""
        for page_num in range(len(document)):
            page = document.load_page(page_num)
            text += page.get_text()
        return text

    return pdf_paths.apply(extract_text)

# COMMAND ----------

# register function as a udf to make it accessible in SQL
spark.udf.register("extract_text_from_pdf", extract_text_from_pdf)
