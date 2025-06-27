# Databricks notebook source
# MAGIC %md
# MAGIC # ESG Analysis: Initial PDF load

# COMMAND ----------

# MAGIC %md
# MAGIC Use this Notebook to initially load all ESG reports in PDF format and save them into a unity catalog table. The table will contain the document name, any file metadata and the text extracted from the pdf string.

# COMMAND ----------

# MAGIC %md
# MAGIC First, we install pymupdf to load the PDFs. You might want to experiment with different PDF readers depending on your file structure.

# COMMAND ----------

# MAGIC %pip install pymupdf

# COMMAND ----------

# MAGIC %md
# MAGIC Then, we extract a list of file names in the Volume. Please change the paths accordingly

# COMMAND ----------

# Input
pdf_volume = "/Volumes/workspace/esg/pdf_documents"

# Output
# TODO: change output catalog/table/schema
catalog = ''
schema = ''
table = ''

# COMMAND ----------

from pathlib import Path
# returns all file paths that has .pdf as extension in the specified directory
pdf_search = Path(pdf_volume).glob("*.pdf")
# convert the glob generator out put to list
# skip this if you are comfortable with generators and pathlib
pdf_files = pdf_files = [str(file.absolute()) for file in pdf_search]


# COMMAND ----------

# MAGIC %md
# MAGIC Finally, we load the PDFs and create a Dataframe that we save as a table.

# COMMAND ----------

import pandas as pd
import fitz  # PyMuPDF

extracted_files = []

for file_path in pdf_files:
    document = fitz.open(file_path)
    text = ''.join([page.get_text() for page in document])
    metadata = document.metadata

    # Create a dictionary for each file
    data = {
        'document_name': file_path.split('/')[-1],
        'metadata': metadata,
        'text': text
    }
    extracted_files.append(data)

# Create a DataFrame from the list of dictionaries
df_pandas = pd.DataFrame(extracted_files)
display(df_pandas)

# Convert pandas DataFrame to Spark DataFrame
df_spark = spark.createDataFrame(df_pandas)

# Save to a table with schema evolution enabled
df_spark.write.mode('overwrite') \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC Now it is your turn to build the Information Extraction Agent. Refer to the slides for further information.

# COMMAND ----------


