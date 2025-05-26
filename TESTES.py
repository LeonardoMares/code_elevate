# Databricks notebook source
pip install pytest

# COMMAND ----------

# DBTITLE 1,funções
# MAGIC %run
# MAGIC /Users/leonardomares1@gmail.com/CODE_ELEVATE/Diário_de_Bordo/UTILS

# COMMAND ----------

# DBTITLE 1,parametros
import pytest
from unittest.mock import patch

# COMMAND ----------

# DBTITLE 1,Teste download do csv ✅
def test_download_csv_with_curl():
    """Testa se a função chama o subprocess corretamente."""
    url = "https://example.com/test.csv"
    local_path = "/tmp/test.csv"

    #Aqui é feito o mock do subprocess para evitar uma chamada real ao cURL
    with patch("subprocess.run") as mock_run:
        download_csv_with_curl(url, local_path)

        # Verifica se subprocess.run foi chamado corretamente
        mock_run.assert_called_once_with(['curl', '-L', url, '-o', local_path], check=True)

    print("✅ Teste `test_download_csv_with_curl` concluído com sucesso!")

test_download_csv_with_curl()

# COMMAND ----------

# DBTITLE 1,Teste csv to delta ✅
nome_tbl = "teste"

def test_salvar_csv_como_delta():
    schema = StructType([
        StructField("DATA_INICIO", StringType(), True),
        StructField("DATA_FIM", StringType(), True),
        StructField("CATEGORIA", StringType(), True),
        StructField("LOCAL_INICIO", StringType(), True),
        StructField("LOCAL_FIM", StringType(), True),
        StructField("PROPOSITO", StringType(), True),
        StructField("DISTANCIA", DecimalType(10, 2), True)
    ])
    caminho_csv = "/tmp/test.csv"
    delimitador = ","

    #Exclui o csv do teste anterior, para roda esse teste
    dbutils.fs.rm("dbfs:/tmp/test.csv", recurse=True)

    #Criando um CSV ficticio
    df_test = spark.createDataFrame([
    ("05-01-2025 10:00", "05-01-2025 12:00", "negócios", "São Paulo", "Rio de Janeiro", "Reunião", 432.5),
    ("05-01-2025 08:00", "05-01-2025 10:00", "pessoal", "São Paulo", "Campinas", "Visita à família", 98.7),
    ("05-02-2025 15:30", "05-02-2025 17:30", "negócios", "Curitiba", "Florianópolis", "Reunião", 300.2),
    ("05-02-2025 09:00", "05-02-2025 11:00", "pessoal", "Brasília", "Goiânia", "Passeio turístico", 209.4),
    ("05-03-2025 13:00", "05-03-2025 15:00", "negócios", "Porto Alegre", "Blumenau", "Reunião", 150.8),
    ("05-03-2025 07:30", "05-03-2025 09:30", "pessoal", "Recife", "João Pessoa", "Visita a amigos", 120.6),
    ("05-04-2025 11:00", "05-04-2025 13:00", "negócios", "Salvador", "Aracaju", "Reunião", 220.9),
    ("05-04-2025 14:30", "05-04-2025 16:30", "pessoal", "Belo Horizonte", "Ouro Preto", None, 100.3),
    ("05-05-2025 16:00", "05-05-2025 18:00", "negócios", "Fortaleza", "Natal", "Reunião", 410.7),
    ("05-05-2025 10:30", "05-05-2025 12:30", "pessoal", "Manaus", "Belém", "Viagem de lazer", 530.1)],
    ["DATA_INICIO", "DATA_FIM", "CATEGORIA", "LOCAL_INICIO", "LOCAL_FIM", "PROPOSITO", "DISTANCIA"])
    df_test.write.option("header", "true").mode("overwrite").csv(caminho_csv)

    df_resultado = salvar_csv_como_delta(schema, caminho_csv, nome_tbl, delimitador)

    assert df_resultado is not None, "O Dataframe é None"
    
    print("✅ Teste salvar csv como delta foi bem sucedido!")

def test_salvar_csv_como_delta_schema():

    df_resultado = spark.sql(f"SELECT * FROM {nome_tbl}")

    assert df_resultado.count() > 0, "A Tabela esta vazia"

    schema_esperado = ["DATA_INICIO", "DATA_FIM", "CATEGORIA", "LOCAL_INICIO", "LOCAL_FIM", "PROPOSITO", "DISTANCIA"]
    schema_obtido = df_resultado.schema.names

    assert schema_esperado == schema_obtido, (f"Schemas não correspondem. Esperado: {schema_esperado} x Obtido: {schema_obtido}")
    
    print("✅ Teste de schema foi bem sucedido!")

test_salvar_csv_como_delta()
test_salvar_csv_como_delta_schema()

# COMMAND ----------

#Vai apagar tudo dentro do diretório, para não ter o erro 
dbutils.fs.rm("dbfs:/user/hive/warehouse/tabela_teste", recurse=True)

# COMMAND ----------

# DBTITLE 1,Teste criar tablela ✅
colunas = [
    "DT_REFE date",
    "QT_CORR int",
    "QT_CORR_NEG int",
    "QT_CORR_PESS int",
    "VL_MAX_DIST int",
    "VL_MIN_DIST int",
    "VL_AVG_DIST decimal (5,2)",
    "QT_CORR_REUNI int",
    "QT_CORR_NAO_REUNI int"]
nome_tbl = "tabela_teste"
particao = "DT_REFE"


def test_criar_tabela_calculada():
    spark.sql("USE default")
    df_resultado = criar_tabela_calculada(colunas, nome_tbl, particao)
    assert df_resultado is not None, "Erro: O Dataframe retornado é None!"
    
    print("✅ Teste de criação concluido com sucesso!")

def test_criar_tabela_calculada_coluna():

    df_test = spark.sql(f"select * from {nome_tbl}")

    # Extrai apenas os nomes das colunas
    colunas_esperadas = [col.split()[0] for col in colunas]
   
    assert set(df_test.columns) == set(colunas_esperadas), "Colunas não correspondem as esperadas"
    # assert df_test.count() > 0, "O Dataframe esta vasio!"
    print("✅ Teste de colunas concluido com sucesso!")

test_criar_tabela_calculada()
test_criar_tabela_calculada_coluna()


# COMMAND ----------

# DBTITLE 1,test tratar data ✅
def test_tratar_data():
    colunas = [
        "*",
        "CAST(TO_TIMESTAMP(DATA_INICIO, 'MM-dd-yyyy HH:mm') AS DATE) AS DT_REFE"
        ]
    tempview = "teste_data"

    df_resultado = tratar_data(colunas, "teste", tempview)
    assert df_resultado is not None, "Dataframe retornado é None!"

    df_test = spark.sql(f'select * from {tempview}')
    assert df_test.count() > 0, "df_test está vazio"

    colunas_esperadas = ["DATA_INICIO", "DATA_FIM", "CATEGORIA", "LOCAL_INICIO", "LOCAL_FIM", "PROPOSITO", "DISTANCIA", "DT_REFE"]
    assert set(df_test.columns) == set(colunas_esperadas), "As colunas não são correspondentes"
    

    print("✅ O Teste tratar_data foi bem-sucedido!!!")

test_tratar_data()

# COMMAND ----------

# DBTITLE 1,select data tratada
# MAGIC %sql
# MAGIC select * from teste_data

# COMMAND ----------

# DBTITLE 1,test count categoria ✅
def test_count_categoria():
    filtro_tbl = "group by DT_REFE, DISTANCIA, CATEGORIA"
    colunas = [
        "DT_REFE",
        "DISTANCIA",
        "1 as qtd",
        "CASE WHEN CATEGORIA = 'negócios' THEN 1 ELSE 0 END AS qtd_neg",
        "CASE WHEN CATEGORIA = 'pessoal' THEN 1 ELSE 0 END AS qtd_pes"
    ]
    tempview = "teste_categ"

    df_resultado = count_categoria(colunas, "teste_data", tempview, filtro_tbl)
    assert df_resultado is not None, "Dataframe retornado é None!"

    df_test = spark.sql(f'select * from {tempview}')
    assert df_test.count() > 0, "df_test está vazio"

    colunas_esperadas = ["DT_REFE", "DISTANCIA", "qtd", "qtd_neg", "qtd_pes"]
    assert set(df_test.columns) == set(colunas_esperadas), "As colunas não são correspondentes"
    

    print("✅ O Teste count_categoria foi bem-sucedido!!!")

test_count_categoria()
    

# COMMAND ----------

# DBTITLE 1,select categoria
# MAGIC %sql
# MAGIC select * from teste_categ

# COMMAND ----------

filtro_tbl = "group by DT_REFE"
def test_categoria_agregada():
    tempview = "test_categ_agreg"
    colunas = [
        "DT_REFE",
        "sum(qtd) as QT_CORR",
        "sum(qtd_neg) as QT_CORR_NEG",
        "sum(qtd_pes) as QT_CORR_PESS",
        "max(DISTANCIA) as VL_MAX_DIST",
        "min(DISTANCIA) as VL_MIN_DIST",
        "avg(DISTANCIA) as VL_AVG_DIST"
    ]

    df_resultado = count_categoria(colunas, "teste_categ", tempview, filtro_tbl)
    assert df_resultado is not None, "Dataframe retornado é None!"

    df_test = spark.sql(f'select * from {tempview}')
    assert df_test.count() > 0, "df_test está vazio"

    colunas_esperadas = ["DT_REFE", "QT_CORR", "QT_CORR_NEG", "QT_CORR_PESS", "VL_MAX_DIST", "VL_MIN_DIST", "VL_AVG_DIST"]
    assert set(df_test.columns) == set(colunas_esperadas), "As colunas não são correspondentes"
    

    print("✅ O Teste count_categoria foi bem-sucedido!!!")

test_categoria_agregada()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_categ_agreg

# COMMAND ----------

# DBTITLE 1,teste count proposito ✅
def test_count_proposito():
    colunas = [
        "DT_REFE",
        "sum(CASE WHEN PROPOSITO = 'Reunião' THEN 1 ELSE 0 END) AS QT_CORR_REUNI",
        "sum(CASE WHEN PROPOSITO IS NOT NULL AND PROPOSITO <> 'Reunião' THEN 1 ELSE 0 END) AS QT_CORR_NAO_REUNI"
    ]
    tempview = "teste_prop"

    df_resultado = count_proposito(colunas, "teste_data", tempview, filtro_tbl)
    assert df_resultado is not None, "Dataframe retornado é None!"

    df_test = spark.sql(f'select * from {tempview}')
    assert df_test.count() > 0, "df_test está vazio"

    colunas_esperadas = ["DT_REFE", "QT_CORR_REUNI", "QT_CORR_NAO_REUNI"]
    assert set(df_test.columns) == set(colunas_esperadas), "As colunas não são correspondentes"
    

    print("✅ O Teste count_proposito foi bem-sucedido!!!")

test_count_proposito()

# COMMAND ----------

# DBTITLE 1,select proposito
# MAGIC %sql
# MAGIC select * from teste_prop

# COMMAND ----------

# DBTITLE 1,teste tatamento dados ✅
def test_join_tabelas():
    colunas = [
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
    tbl_filtro = "test_categ_agreg c"
    tbl_join = "teste_prop p"
    on = "c.DT_REFE = p.DT_REFE"
    filtro_tbl = """
    order by c.DT_REFE asc
    """
    tempview = "teste_tbl_final"

    df_resultado = join_tabelas(colunas, tbl_filtro, tbl_join, on, filtro_tbl, tempview)
    assert df_resultado is not None, "Dataframe retornado é None!"
    assert "QT_CORR_REUNI" in df_resultado.columns, "A coluna QT_CORR_REUNI, não foi encontrada"

    df_test = spark.sql(f'select * from {tempview}')
    assert df_test.count() > 0, "df_test está vazio"

    assert df_resultado.count() == df_test.count(), "A contangem não bate"

    print("✅ O Teste join_tabelas foi bem-sucedido!!!")

test_join_tabelas()

# COMMAND ----------

# DBTITLE 1,select dados tratados
# MAGIC %sql
# MAGIC select * from teste_tbl_final

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from tabela_teste

# COMMAND ----------

# DBTITLE 1,teste ingestão dados ✅
def test_ingestao_dados():
    tbl_calc = "tabela_teste"
    tbl_join = "teste_tbl_final"

    df_resultado = ingestao_dados(tbl_calc, tbl_join)
    df_test = spark.sql(f"select * from {tbl_join}")

    assert df_resultado.count() == df_test.count(), "A contagem das tabelas não bate"

    print("✅ O Teste ingestão de dados foi bem-sucedido!!!")

test_ingestao_dados()

# COMMAND ----------

# DBTITLE 1,select ingestão
# MAGIC %sql
# MAGIC select * from tabela_teste

# COMMAND ----------

# MAGIC %md
# MAGIC #Teste Técnico

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE teste_sql (
    DATA_INICIO STRING,
    DATA_FIM STRING,
    CATEGORIA STRING,
    LOCAL_INICIO STRING,
    LOCAL_FIM STRING,
    PROPOSITO STRING,
    DISTANCIA DECIMAL(10,2))
    USING DELTA
""")

# COMMAND ----------

# MAGIC  %sql
# MAGIC INSERT INTO teste_sql (DATA_INICIO, DATA_FIM, CATEGORIA, LOCAL_INICIO, LOCAL_FIM, PROPOSITO, DISTANCIA) VALUES
# MAGIC ('05-01-2025 10:00', '05-01-2025 12:00', 'negócios', 'São Paulo', 'Rio de Janeiro', 'Reunião', 432.5),
# MAGIC ('05-01-2025 08:00', '05-01-2025 10:00', 'pessoal', 'São Paulo', 'Campinas', 'Visita à família', 98.7),
# MAGIC ('05-02-2025 15:30', '05-02-2025 17:30', 'negócios', 'Curitiba', 'Florianópolis', 'Reunião', 300.2),
# MAGIC ('05-02-2025 09:00', '05-02-2025 11:00', 'pessoal', 'Brasília', 'Goiânia', 'Passeio turístico', 209.4),
# MAGIC ('05-03-2025 13:00', '05-03-2025 15:00', 'negócios', 'Porto Alegre', 'Blumenau', 'Reunião', 150.8),
# MAGIC ('05-03-2025 07:30', '05-03-2025 09:30', 'pessoal', 'Recife', 'João Pessoa', 'Visita a amigos', 120.6),
# MAGIC ('05-04-2025 11:00', '05-04-2025 13:00', 'negócios', 'Salvador', 'Aracaju', 'Reunião', 220.9),
# MAGIC ('05-04-2025 14:30', '05-04-2025 16:30', 'pessoal', 'Belo Horizonte', 'Ouro Preto', NULL, 100.3),
# MAGIC ('05-05-2025 16:00', '05-05-2025 18:00', 'negócios', 'Fortaleza', 'Natal', 'Reunião', 410.7),
# MAGIC ('05-05-2025 10:30', '05-05-2025 12:30', 'pessoal', 'Manaus', 'Belém', 'Viagem de lazer', 530.1),
# MAGIC ('05-06-2025 08:00', '05-06-2025 10:00', 'negócios', 'São Paulo', 'Brasília', 'Reunião', 850.6),
# MAGIC ('05-06-2025 18:00', '05-06-2025 20:00', 'pessoal', 'Curitiba', 'Joinville', NULL, 130.9),
# MAGIC ('05-07-2025 14:00', '05-07-2025 16:00', 'negócios', 'Vitória', 'Rio de Janeiro', 'Reunião', 400.5),
# MAGIC ('05-07-2025 12:00', '05-07-2025 14:00', 'pessoal', 'São Luís', 'Teresina', 'Evento cultural', 250.8),
# MAGIC ('05-08-2025 17:30', '05-08-2025 19:30', 'negócios', 'Belém', 'Macapá', 'Reunião', 320.2),
# MAGIC ('05-08-2025 09:30', '05-08-2025 11:30', 'pessoal', 'Campo Grande', 'Dourados', 'Encontro com amigos', 150.4),
# MAGIC ('05-09-2025 20:00', '05-09-2025 22:00', 'negócios', 'Florianópolis', 'Porto Alegre', 'Reunião', 470.7),
# MAGIC ('05-09-2025 07:00', '05-09-2025 09:00', 'pessoal', 'Natal', 'Recife', NULL, 300.9),
# MAGIC ('05-10-2025 11:30', '05-10-2025 13:30', 'negócios', 'Aracaju', 'Maceió', 'Reunião', 220.1),
# MAGIC ('05-10-2025 15:00', '05-10-2025 17:00', 'pessoal', 'Rio Branco', 'Porto Velho', 'Viagem de descanso', 350.4),
# MAGIC ('05-11-2025 08:30', '05-11-2025 10:30', 'negócios', 'João Pessoa', 'Natal', 'Reunião', 180.2),
# MAGIC ('05-11-2025 16:30', '05-11-2025 18:30', 'pessoal', 'Maceió', 'Salvador', 'Férias', 700.3),
# MAGIC ('05-12-2025 10:00', '05-12-2025 12:00', 'negócios', 'Brasília', 'São Paulo', 'Reunião', 870.5),
# MAGIC ('05-12-2025 13:30', '05-12-2025 15:30', 'pessoal', 'Porto Alegre', 'Curitiba', NULL, 450.6);

# COMMAND ----------

spark.sql(
    f"""
    create or replace table sql_final(
        DT_REFE date,
        QT_CORR int,
        QT_CORR_NEG int,
        QT_CORR_PESS int,
        VL_MAX_DIST int,
        VL_MIN_DIST int,
        VL_AVG_DIST double,
        QT_CORR_REUNI int,
        QT_CORR_NAO_REUNI int

    )
""")

# COMMAND ----------

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into sql_final 
# MAGIC WITH contagem as
# MAGIC (SELECT 
# MAGIC   CAST(TO_TIMESTAMP(DATA_INICIO, 'MM-dd-yyyy HH:mm') AS DATE) AS DT_REFE,
# MAGIC   count(*) as qtd,
# MAGIC   CASE WHEN CATEGORIA = 'Negocio' THEN COUNT(categoria) else 0 END AS  qt_corrida_negocio,
# MAGIC   CASE WHEN CATEGORIA = 'Pessoal' THEN COUNT(categoria) else 0 END AS  qt_corrida_pessoal,
# MAGIC   DISTANCIA,
# MAGIC   CASE WHEN PROPOSITO = 'Reunião' then count(PROPOSITO) end as reuniao,
# MAGIC   CASE WHEN PROPOSITO IS NOT NULL AND PROPOSITO <> 'Reunião' THEN count(PROPOSITO) else 0 END AS pessoal 
# MAGIC FROM info_transportes
# MAGIC GROUP BY DATA_INICIO, CATEGORIA, DISTANCIA, PROPOSITO
# MAGIC
# MAGIC ORDER BY DATA_INICIO ASC)
# MAGIC
# MAGIC SELECT
# MAGIC   dt_refe,
# MAGIC   sum(qtd) as QT_CORR,
# MAGIC   sum(qt_corrida_negocio) as QT_CORR_NEG,
# MAGIC   sum(qt_corrida_pessoal) as QT_CORR_PESS,
# MAGIC   max(DISTANCIA) as VL_MAX_DIST,
# MAGIC   min(DISTANCIA) as VL_MIN_DIST,
# MAGIC   avg(DISTANCIA) as VL_AVG_DIST,
# MAGIC   sum(reuniao) as QT_CORR_REUNI,
# MAGIC   sum(pessoal) as QT_CORR_NAO_REUNI
# MAGIC   
# MAGIC
# MAGIC
# MAGIC FROM contagem
# MAGIC GROUP BY DT_REFE

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view init as
# MAGIC select
# MAGIC *,
# MAGIC CAST(TO_TIMESTAMP(DATA_INICIO, 'MM-dd-yyyy HH:mm') AS DATE) AS DT_REFE
# MAGIC from info_transportes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM INIT

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view fase_2 as
# MAGIC select
# MAGIC   DT_REFE,
# MAGIC   DISTANCIA,
# MAGIC   1 AS QTD,
# MAGIC   (CASE WHEN CATEGORIA = 'Negocio' THEN 1 else 0 END) AS  qt_corrida_negocio,
# MAGIC   (CASE WHEN CATEGORIA = 'Pessoal' THEN 1 else 0 END) AS  qt_corrida_pessoal
# MAGIC from init

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fase_2

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view fase_3 as
# MAGIC select
# MAGIC   DT_REFE,
# MAGIC   SUM(CASE WHEN PROPOSITO = 'Reunião' then 1 end) as reuniao,
# MAGIC   SUM(CASE WHEN PROPOSITO IS NOT NULL AND PROPOSITO <> 'Reunião' THEN 1 else 0 END) AS pessoal
# MAGIC from init
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fase_3

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC c.DT_REFE,
# MAGIC sum(c.QTD),
# MAGIC sum(c.qt_corrida_negocio),
# MAGIC sum(c.qt_corrida_pessoal),
# MAGIC maX(c.DISTANCIA),
# MAGIC min(c.DISTANCIA),
# MAGIC avg(c.DISTANCIA),
# MAGIC sum(p.reuniao),
# MAGIC sum(p.pessoal)
# MAGIC from fase_2 c
# MAGIC join fase_3 p
# MAGIC on c.DT_REFE = p.DT_REFE
# MAGIC group by c.DT_REFE, p.dt_refe
# MAGIC order by c.DT_REFE desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sql_final

# COMMAND ----------

