# Databricks notebook source
# MAGIC %run
# MAGIC /Users/leonardomares1@gmail.com/CODE_ELEVATE/Diário_de_Bordo/UTILS

# COMMAND ----------

url_file = "https://raw.githubusercontent.com/LeonardoMares/code_elevate/refs/heads/feature/code_assigment/scr_data/info_transportes.csv"

local_path = "/tmp/info_transportes.csv"

# COMMAND ----------

download_csv_with_curl(url_file, local_path)

# COMMAND ----------

df_info_tramsportes = spark.read.option("sep", ";") \
    .option("header", "true") \
        .option("inferSchema", "true") \
            .csv("file:/tmp/info_transportes.csv")

df_info_tramsportes.display()

# COMMAND ----------

nome_tbl = "info_transportes"
local_csv = "file:/tmp/info_transportes.csv"
delimitador = ";"
schema = StructType([
  StructField("DATA_INICIO", StringType(), True),
  StructField("DATA_FIM", StringType(), True),
  StructField("CATEGORIA", StringType(), True),
  StructField("LOCAL_INICIO", StringType(), True),
  StructField("LOCAL_FIM", StringType(), True),
  StructField("DISTANCIA", IntegerType(), True),
  StructField("PROPOSITO", StringType(), True)
    ])

# COMMAND ----------

salvar_csv_como_delta(schema, local_csv, nome_tbl, delimitador)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   info_transportes