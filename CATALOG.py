# Databricks notebook source
# MAGIC %run
# MAGIC /Users/leonardomares1@gmail.com/CODE_ELEVATE/Diário_de_Bordo/UTILS

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/info_corridas_do_dia', recurse=True)

# COMMAND ----------

nome_tabela = "info_corridas_do_dia"
particao = "DT_REFE"
colunas = [
    "DT_REFE date",
    "QT_CORR int",
    "QT_CORR_NEG int",
    "QT_CORR_PESS int",
    "VL_MAX_DIST int",
    "VL_MIN_DIST int",
    "VL_AVG_DIST decimal(10,2)",
    "QT_CORR_REUNI int",
    "QT_CORR_NAO_REUNI int"
]

# COMMAND ----------

criar_tabela_calculada(colunas, nome_tabela, particao)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC from
# MAGIC info_corridas_do_dia

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table info_corridas_do_dia