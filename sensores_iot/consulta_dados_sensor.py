import psycopg2
import importlib
import util as f
importlib.reload(f)
import os
from dotenv import load_dotenv
load_dotenv()

load_dotenv()


database = os.getenv('POSTGRES_DB')
usuario = os.getenv('POSTGRES_USER')
senha = os.getenv('POSTGRES_PASSWORD')
host = "localhost"
porta = "5432"


schema="monitoramento"
tabela="monitoramento_sensores_iot"

def main_consulta_dados_sensor():
    conexao = f.criar_conexao_postgresql(database, usuario, senha, host, porta)
    
    if conexao is None:
        print("[ERROR] Falha ao conectar ao PostgreSQL.")
        return
    
    try:
        cursor = conexao.cursor()
        cursor.execute(f"SELECT * FROM {schema}.{tabela};")
        resultados = cursor.fetchall()
        if resultados:
            for linha in resultados:
                print(linha)
        else:
            print("[INFO] Nenhum dado encontrado na tabela.")
    
    except Exception as e:
        print(f"[ERROR] Ocorreu um erro ao consultar os dados: {repr(e)}")
    
    finally:
        cursor.close()
        conexao.close()
        print("[INFO] Conexão encerrada.")

if __name__ == "__main__":
    main_consulta_dados_sensor()