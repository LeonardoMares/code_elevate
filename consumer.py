# -*- coding: utf-8 -*-
import importlib
import util as f
importlib.reload(f)
import os
from dotenv import load_dotenv
load_dotenv()


topico = 'sensores-iot'
servidor= 'localhost:9092'
schema="monitoramento"
tabela="monitoramento_sensores_iot"



database=os.getenv('POSTGRES_DB')
usuario=os.getenv('POSTGRES_USER')
senha=os.getenv('POSTGRES_PASSWORD')
host="localhost"
porta="5432"
conexao = f.criar_conexao_postgresql(database, usuario, senha, host, porta)


def main_consumer():
    consumer = f.criar_consumer(topico, servidor)
    
    try:
        for dados in f.consumir_dados(consumer):
            f.armazenar_dados(dados, schema, tabela, conexao)
    except KeyboardInterrupt:
        print("\n[INFO] Consumer interrompido pelo usuario.")
    except Exception as e:
        print(f"[ERROR] Ocorreu um erro: {repr(e)}")
    finally:
        if consumer:
            consumer.close()
        if conexao:
            conexao.close()
        print("[INFO] Conexões encerradas.")

if __name__ == "__main__":
    main_consumer()