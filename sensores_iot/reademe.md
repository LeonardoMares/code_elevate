 - Projeto: Monitoramento de Sensores IoT<br><br><br>
 
    - Visão Geral<br>
    Este projeto implementa uma pipeline de dados em tempo real para simular, consumir e armazenar informações de sensores IoT utilizando:<br>
    
    Apache Kafka para mensageria<br>
    
    PostgreSQL para persistência dos dados<br>
    
    Docker Compose para orquestração dos serviços<br>
    
    Python com bibliotecas específicas para simulação e integração<br><br><br>
    
    - Objetivo<br>
    Criar um ambiente completo para simulação de sensores IoT<br>
    
    Enviar e processar mensagens em tempo real via Kafka<br>
    
    Armazenar os dados em um banco relacional (PostgreSQL)<br>
    
    Manter o projeto organizado, modular e testável<br>
    
    Demonstrar conhecimento prático em streaming de dados, containerização e persistência de dados<br><br><br>
    
    - Tecnologias Utilizadas<br>
    Apache Kafka: mensageria em tempo real (tópico sensores-iot)<br>
    
    Zookeeper: coordenação dos brokers Kafka<br>
    
    PostgreSQL: persistência dos dados (tabela monitoramento_sensores_iot)<br>
    
    Docker Compose: orquestração dos serviços<br>
    
    Python 3.8+ com as bibliotecas:<br>
    
    kafka-python: conexão com Kafka<br>
    
    psycopg2: integração com PostgreSQL<br>
    
    Faker: geração de dados fictícios<br>
    
    dotenv: leitura de variáveis de ambiente<br><br><br>

    - Pipeline de Execução<br>
    1. Inicie os containers Docker:
       docker-compose up -d<br>
    2. Instale as dependências Python:
       pip install -r requirements.txt<br>
    3. Crie um topico:
       docker exec -it kafka bash -c "kafka-topics.sh --create --topic meu_topico --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1" <br>
    4. Execute o Producer (gera e envia mensagens Kafka):
       python producer.py<br>
    5. Execute o Consumer (lê e armazena no PostgreSQL):
       python consumer.py<br>
    6. (Opcional): Rode consultas nos dados:
        python consulta_dados_sensor.py<br><br><br>

    - Configuração do Ambiente
    Crie um arquivo .env na raiz do projeto com o seguinte conteúdo:<br>

    POSTGRES_USER=sensores_user
    POSTGRES_PASSWORD=docker
    POSTGRES_DB=sensores_db
    (Esse arquivo será lido automaticamente para configurar a conexão com o banco de dados, ele deveser usado dentro do arquivo .gitignore, para que seuas dados fique protegidos)<br><br><br>

    - Testes
    Os testes estão organizados no notebook testes.ipynb, incluindo:<br>
    
    Validação do schema das mensagens Kafka<br>
    
    Verificação de conexão com Kafka e PostgreSQL<br>
    
    Teste de persistência dos dados simulados<br><br><br>

    Para executar:<br>
   jupyter notebook testes.ipynb<br><br><br>

    - Melhorias Futuras<br>
    Funcionalidade    /     Descrição<br>
    Autenticação SASL/SSL    /    Adicionar segurança no broker Kafka<br>
    Particionamento de tópico    /    Melhorar performance e escalabilidade<br>
    Dashboard    /    Visualização via PowerBI<br>
    Alertas    /    Geração de alertas para anomalias dos sensores<br><br><br>

    📌 Considerações Finais<br>
    Este projeto foi desenvolvido com foco em simular um cenário real de IoT + streaming de dados, aplicando conceitos modernos de engenharia de dados em ambientes distribuídos. Ele serve como uma base para evolução futura com ferramentas de monitoramento, analytics e segurança de dados.
    







    

    
