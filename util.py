import psycopg2
import os
import json
import time
from faker import Faker
from kafka import KafkaProducer, KafkaConsumer
from random import uniform





#______________________PRODUCER______________________

#Criando o Producer
def criar_producer(servidor):
    producer = KafkaProducer(
        bootstrap_servers=servidor,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=1)
    return producer

#Gerando dados de sensores IoT
def gerar_dados_sensor():
    dados = {
        'sensor_id': fake.uuid4(),
        'temperatura': round(uniform(12.00, 40.00), 2),
        'umidade': round(uniform(20.00, 80.00), 2),
        'data_hora': fake.iso8601(),
        'localizacao': fake.city()}
    return dados

#Enviando dados para o topico
def enviar_dados(producer, topico, dados):
    producer.send(topico, dados).get(timeout=10)
    producer.flush()
    print(f"[INFO] Dados enviados para o '{topico}': {dados}")



#______________________CONSUMER______________________

def criar_consumer(topico, servidor):
    consumer = KafkaConsumer(
        topico,
        bootstrap_servers=servidor,
        group_id='grupo_consumidores_iot',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False, # Desativa o auto commit
        max_poll_interval_ms=10000,  # 10 segundos
        session_timeout_ms=15000,  # 6 segundos
        )
    return consumer

#Consumindo dados do topico
def consumir_dados(consumer):
    for mensagem in consumer:
        dados = mensagem.value
        print(f"[INFO] Dados recebidos : {dados}")

        consumer.commit()  # Confirma o processamento da mensagem

        yield dados

def armazenar_dados(dados, schema, tbl, conexao):
    cursor = conexao.cursor()
    
    try:
        colunas = ', '.join(dados.keys())
        valores = ', '.join(['%s'] * len(dados))
        query = f"INSERT INTO {schema}.{tbl} ({colunas}) VALUES ({valores})"
        
        cursor.execute(query, tuple(dados.values()))
        conexao.commit()
        print(f"[INFO] Dados armazenados na tabela '{tbl}' do schema '{schema}'.")
    except Exception as e:
        conexao.rollback()
        print("[ERROR] Ocorreu um erro ao armazenar os dados:", repr(e))
    finally:
        cursor.close()


#_________________CONEXAO_POSTGRESQL_________________

def criar_conexao_postgresql(database, usuario, senha, host, porta):
    try:
        conexao = psycopg2.connect(
            dbname=database,
            user=usuario,
            password=senha,
            host=host,
            port=porta,
            client_encoding='UTF8'
        )
        print("[INFO] Conexao com o PostgreSQL estabelecida.")
        return conexao
    except Exception as e:
        print("[ERROR]", repr(e))
        return None


#_________________CRIANDO_SCHEMA____________________
def criar_schema(schema, conexao):
    cursor = conexao.cursor()
    
    try:
        cursor.execute(f"""
        create schema if not exists {schema};
        """)
        conexao.commit()
        print(f"[INFO] Criacao do schema '{schema}' foi bem-sucedido.")
    except Exception as e:
        conexao.rollback()
        print("O Schema ja existe ou ocorreu um erro:", repr(e))
    finally:
        cursor.close()


#_________________CRIANDO_TABELA____________________
def criar_tabela(colunas, schema, tbl, conexao):
    colunas_selecionadas = ",\n" .join(colunas)
    cursor = conexao.cursor()
    
    try:
        cursor.execute(f"""
            create table if not exists {schema}.{tbl} (
            {colunas_selecionadas}
            );
            """)
        conexao.commit()
        print(f"[INFO] Criacao de tabela '{tbl}' no schema '{schema}' foi bem-sucedido.")
    except Exception as e:
        conexao.rollback()
        print(f"[ERROR] Ocorreu um erro ao criar a tabela '{tbl}':", repr(e))
    finally:
        cursor.close()