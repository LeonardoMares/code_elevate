import os
import importlib
import util as f
importlib.reload(f)
from dotenv import load_dotenv
load_dotenv()

#____CONEXAO_____
database=os.getenv('POSTGRES_DB')
usuario=os.getenv('POSTGRES_USER')
senha=os.getenv('POSTGRES_PASSWORD')
host="localhost"
porta="5432"

#____CRIANDO_SCHEMA_E_TABELA_____
colunas = [
    "sensor_id UUID PRIMARY KEY",
    "temperatura FLOAT",
    "umidade FLOAT",
    "data_hora TIMESTAMP",
    "localizacao VARCHAR(100)"
]
schema="monitoramento"
tabela="monitoramento_sensores_iot"

def main_postgresql():
    try:
        conexao = f.criar_conexao_postgresql(database, usuario, senha, host, porta)
        
        if conexao is None:
            print("[ERROR] Falha ao conectar ao PostgreSQL.")
            return
        
        f.criar_schema(schema, conexao)
        f.criar_tabela(colunas, schema, tabela, conexao)
        
        print("[INFO] Conexao com PostgreSQL bem-sucedida.")
    
    except Exception as e:
        print(f"[ERROR] Ocorreu um erro: {repr(e)}")
    
    finally:
        if 'conexao' in locals() and conexao:
            conexao.close()
            print("[INFO] Conexao encerrada.")

if __name__ == "__main__":
    main_postgresql()