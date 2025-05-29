- Projeto: Diário de Bordo


    - Visão Geral:
    Este projeto implementa uma pipeline de dados baseada em camadas Bronze e Silver, utilizando o Databricks com Delta Lake. O foco foi tratar e consolidar informações de corridas de transporte urbano, segmentando por categoria da corrida e propósito, e agregando as informações por dia de referência.<br><br>



    - Objetivo:
    Realizar a ingestão de dados brutos (CSV) em uma camada Bronze.<br>        
    Tratar, transformar e consolidar esses dados em uma camada Silver, com indicadores por dia.<br>     
    Utilizar boas práticas de SQL + PySpark e manter o projeto modularizado com funções reutilizáveis.<br>      
    Garantir testabilidade do processo com scripts de testes automáticos.<br><br><br>


    
    - Estrutura do Projeto:<br>
    MAIN_BRONZE.ipynb – Ingestão do CSV para a camada Bronze (tabela Delta).<br>
    MAIN_SILVER.ipynb – Tratamento, transformação e consolidação dos dados.<br>      
    UTILS.ipynb – Funções auxiliares reutilizáveis (transformações, joins, insert, etc). <br>       
    CATALOG.ipynb – Criação da tabela final agregada (Silver).  <br>      
    TESTES.ipynb – Execução dos testes unitários com validações de schema, ingestão e join.<br>        
    Dados de origem: CSV disponível via GitHub.<br><br><br>


    
    - Tecnologias Utilizadas<br>
    Databricks        <br>
    Apache Spark com PySpark  <br>      
    Delta Lake  <br>      
    SQL (SparkSQL)  <br>      
    Pytest para testes      <br>  
    DBFS (Databricks File System)  <br>      
    cURL para ingestão externa<br><br><br>


    
    - Pipeline<br>
    Ingestão do CSV (Bronze) <br>
    Download via curl com função download_csv_with_curl <br>       
    Leitura com schema definido + persistência como tabela Delta <br>       
    Transformações (Silver) <br>       
    Conversão de datas com TO_TIMESTAMP e CAST  <br>      
    Contagem por categoria (Negócio, Pessoal)   <br>     
    Contagem por propósito (Reunião, Pessoal)  <br>      
    Agregações por data:<br>        
    Quantidade total de corridas   <br>     
    Distância máxima, mínima e média  <br>      
    Join final entre as contagens  <br>      
    Inserção dos dados finais na tabela Silver (info_corridas_do_dia)<br><br><br>



    - Testes<br>
    Todos os módulos e etapas foram testados com pytest e asserts manuais no notebook TESTES.ipynb.<br>
        - Testes realizados:<br>        
        Teste de download do arquivo  <br>          
        Teste de leitura e persistência como Delta  <br>          
        Verificação de schema  <br>          
        Validação da contagem de categorias e propósitos  <br>          
        Testes do join e da ingestão final<br>



    - Desafios e Soluções Técnicas<br>
    Desafio/Solução Aplicada<br>
    Valores duplicados após JOIN  ---  Identificado que a granularidade da tabela categoria causava um "cartesian join". Foi criada uma view agregada por DT_REFE para corrigir<br>
    Erros ao usar TO_TIMESTAMP no Spark 3.x	  ---  Foi aplicado o parâmetro spark.sql.legacy.timeParserPolicy = LEGACY para manter compatibilidade com o parser antigo<br>
    Divergência entre schema da view e da tabela Delta  ---  Os casts e tipos de dados foram ajustados explicitamente no SELECT final da view_join<br<br>
    Erros com reescrita de arquivos no DBFS  ---  Utilizado dbutils.fs.rm com recurse=True para limpar diretórios antes da escrita<br><br><br>


    
    - Resultado Final<br>
        Tabela consolidada: info_corridas_do_dia<br>
        Com os seguintes campos:<br>
        
        DT_REFE: data de referência<br>
        
        QT_CORR: quantidade total de corridas<br>
        
        QT_CORR_NEG: corridas de negócio<br>
        
        QT_CORR_PESS: corridas pessoais<br>
        
        VL_MAX_DIST: distância máxima no dia<br>
        
        VL_MIN_DIST: distância mínima<br>
        
        VL_AVG_DIST: média de distância<br>
        
        QT_CORR_REUNI: corridas com propósito “Reunião”<br>
        
        QT_CORR_NAO_REUNI: demais propósitos<br>


    
    - Como Executar<br>
    Execute UTIL.ipymb para criar as funções<br>
    Rode MAIN_BRONZE.ipynb para ingestão dos dados      <br>  
    Execute TESTES.ipynb para verificar a integridade da pipeline (execute após a main_bronze par o teste de sql rodar certo)   <br>     
    Execute CATALOG.ipynb para garantir a criação da tabela final <br>       
    Rode MAIN_SILVER.ipynb para processar e consolidar os dados<br>
    
    - Sobre Mim
    Este projeto foi desenvolvido como parte de um desafio técnico para consolidar conhecimentos em engenharia de dados com Databricks.
    Foquei em construir uma pipeline robusta, com testes e modularização de código, simulando um cenário de produção.
