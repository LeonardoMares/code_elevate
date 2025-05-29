# -*- coding: utf-8 -*-
import importlib
import util as f
importlib.reload(f)
from faker import Faker
import time
from random


fake = Faker('pt_BR')
topico = 'sensores-iot'
servidor= 'localhost:9092'

def main_producer():
    producer = f.criar_producer(servidor)

    try:
        while True:
            try:
                dados = f.gerar_dados_sensor()
                f.enviar_dados(producer, topico, dados)
            except Exception as e:
                print(f"[ERROR] Falha ao enviar dados: {repr(e)}")
            time.sleep(2)  # Espera 2 segundos antes de enviar o pr�ximo
    except KeyboardInterrupt:
        print("\n[INFO] Producer interrompido pelo usuario.")
    finally:
        producer.close()
        print("[INFO] Conexao com o Kafka encerrada.")

if __name__ == "__main__":
    main_producer()