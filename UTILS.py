# Databricks notebook source

import subprocess
from pyspark.sql.types import *

#Reproduz comportamento antigo relacionado ao datetime
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# DBTITLE 1,DOWNLOAD_CSV
def download_csv_with_curl(url, local_path):

    subprocess.run(['curl', '-L', url, '-o', local_path], check=True)
    
    print(f"✅ Arquivo baixado para {local_path}")

# COMMAND ----------

# DBTITLE 1,CSV_TO_DELTA
def salvar_csv_como_delta(schema, local_path, nome_tbl, delimitador = ","):
   
    df = spark.read.format("csv")\
          .option("header", "true")\
            .schema(schema)\
              .option("delimiter", delimitador)\
                .load(local_path)
                
    df.write.format("delta")\
      .mode("overwrite")\
        .saveAsTable(nome_tbl)
  
    print(f"✅ Tabela Delta '{nome_tbl}' criada com sucesso!")

    return df

# COMMAND ----------

# DBTITLE 1,SILVER_TABLE
def criar_tabela_calculada(colunas, nome_tbl, particao):
    
    colunas_selecionadas = ','.join(colunas)
    query = f"""
    create table if not exists {nome_tbl}(
    {colunas_selecionadas}
    )
    using delta
    partitioned by ({particao})
    """
    spark.sql(query)

    print(f"✅ Tabela {nome_tbl}, criada com sucesso")
    
    return spark.table(nome_tbl)
    

# COMMAND ----------

# DBTITLE 1,TRATANDO DATA
def tratar_data(colunas, from_tbl , nome_tempview):
    colunas_selecionadas = ','.join(colunas)
    query = f"""
    select {colunas_selecionadas}
    from {from_tbl}
    """
    df = spark.sql(query)
    df.createOrReplaceTempView(nome_tempview)
    
    print("✅ O Tratamento de data foi bem-sucedido!!!")
    
    return df 

# COMMAND ----------

# DBTITLE 1,CONTAGEM CATEGORIA
def count_categoria(colunas, from_tbl, nome_tempview, filtro_tbl):
    colunas_selecionadas = ','.join(colunas)
    query = f"""
    select {colunas_selecionadas}
    from {from_tbl}
    {filtro_tbl}
    """
    df = spark.sql(query)
    df.createOrReplaceTempView(nome_tempview)

    print("✅ A contagem de categoria foi bem-sucedido!!!")
    
    return df

# COMMAND ----------

# DBTITLE 1,AGREGANDO CATEGORIA
def categoria_agregada(colunas, from_tbl, nome_tempview, filtro_tbl):
    colunas_selecionadas = ','.join(colunas)
    query = f"""
    select {colunas_selecionadas}
    from {from_tbl}
    {filtro_tbl}
    """
    df = spark.sql(query)
    df.createOrReplaceTempView(nome_tempview)

    print("✅ A contagem de categoria foi bem-sucedido!!!")
    
    return df

# COMMAND ----------

# DBTITLE 1,CONTAGEM PROPOSITO
def count_proposito(colunas, from_tbl, nome_tempview, filtro_tbl):
    colunas_selecionadas = ','.join(colunas)
    query = f"""
    select {colunas_selecionadas}
    from {from_tbl}
    {filtro_tbl}
    """
    df = spark.sql(query)
    df.createOrReplaceTempView(nome_tempview)

    print("✅ A contagem de proposito foi bem-sucedido!!!")

    return df

# COMMAND ----------

# DBTITLE 1,JOIN
def join_tabelas(colunas, from_tbl, tbl_join, on, filtro_tbl, nome_tempview):
    colunas_selecionadas = ','.join(colunas)
    query = f"""
    select {colunas_selecionadas}
    from {from_tbl}
    join {tbl_join}
        on {on}
    {filtro_tbl}
    """
    df = spark.sql(query)
    df.createOrReplaceTempView(nome_tempview)

    print("✅ O join das contagens foi bem-sucedido!!!")

    return df

# COMMAND ----------

# DBTITLE 1,INSERT
def ingestao_dados(tbl_calc, tbl_join):
    query = f"""
    insert into {tbl_calc}
    select * from {tbl_join}
    """
    spark.sql(query)
    
    print("✅ A ingestão foi bem-sucedida!!!")

    return spark.table(tbl_calc)