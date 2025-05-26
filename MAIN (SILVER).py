# Databricks notebook source
# MAGIC %md
# MAGIC ##Criar o processo de tratamento da tabela bronze

# COMMAND ----------

# MAGIC %run
# MAGIC /Users/leonardomares1@gmail.com/CODE_ELEVATE/Diário_de_Bordo/UTILS

# COMMAND ----------

#Variaveis para tratar data
data_filtro = "info_transportes"
view_data = "data"
colunas_data = [
    "*",
    "cast(to_timestamp(DATA_INICIO, 'MM-dd-yyyy HH:mm') as date) AS DT_REFE"
]

#Variaveis para contagem da categoria
view_categoria = "categoria"
filtro_tbl_categ = "group by DT_REFE, DISTANCIA, CATEGORIA"
colunas_categoria = [
    "DT_REFE",
    "DISTANCIA",
    "1 as qtd",
    "case when CATEGORIA = 'Negocio' then 1 else 0 end as qtd_neg",
    "case when CATEGORIA = 'Pessoal' then 1 else 0 end as qtd_pes"
]

#Variaveis par agregar categoria
view_categ_agreg = "categ_agreg"
filtro_tbl= "group by DT_REFE"
colunas_agregadas = [
        "DT_REFE",
        "sum(qtd) as QT_CORR",
        "sum(qtd_neg) as QT_CORR_NEG",
        "sum(qtd_pes) as QT_CORR_PESS",
        "max(DISTANCIA) as VL_MAX_DIST",
        "min(DISTANCIA) as VL_MIN_DIST",
        "avg(DISTANCIA) as VL_AVG_DIST"
    ]

#variaveis para contagem do proposito
view_proposito = "proposito"
filtro_tbl= "group by DT_REFE"
colunas_proposito = [
    "DT_REFE",
    "sum(case when PROPOSITO = 'Reunião' then 1 else 0 end) as QT_CORR_REUNI",
    "sum(case when PROPOSITO IS NOT NULL and PROPOSITO <> 'Reunião' then 1 else 0 end) as QT_CORR_NAO_REUNI"
    ]

#Variaveis para o join
tbl_from = "categ_agreg c"
tbl_join = "proposito p"
view_join ="join_tbls"
on = "c.DT_REFE = p.DT_REFE"
filtro_tbl_join = """
order by c.DT_REFE asc
"""
colunas_join = [
    "c.DT_REFE",
    "c.QT_CORR",
    "c.QT_CORR_NEG",
    "c.QT_CORR_PESS",
    "c.VL_MAX_DIST",
    "c.VL_MIN_DIST",
    "c.VL_AVG_DIST",
    "p.QT_CORR_REUNI",
    "p.QT_CORR_NAO_REUNI"
    ]

#Variaveis para a ingestão
tbl_calc = "info_corridas_do_dia"


# COMMAND ----------

def consolidando_dados():

    # Processo de tratamento dos dados da tabela bronze e ingestando na tabela silver
    tratar_data(colunas_data, data_filtro , view_data)
    count_categoria(colunas_categoria, view_data, view_categoria, filtro_tbl_categ)
    categoria_agregada(colunas_agregadas, view_categoria, view_categ_agreg, filtro_tbl)
    count_proposito(colunas_proposito, view_data, view_proposito, filtro_tbl)
    join_tabelas(colunas_join, tbl_from, tbl_join, on, filtro_tbl_join, view_join)
    ingestao_dados(tbl_calc, view_join)

    print("✅ Processamento bem-sucedido")

consolidando_dados()

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   info_corridas_do_dia