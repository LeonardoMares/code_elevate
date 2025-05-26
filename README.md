  - Projeto: Diário de Bordo

  - Visão Geral

Este projeto implementa uma pipeline de dados baseada em camadas Bronze e Silver, utilizando o Databricks com Delta Lake. O foco foi tratar e consolidar informações de corridas de transporte urbano, segmentando por categoria da corrida e propósito, e agregando as informações por dia de referência.


  - Objetivo:

Garantir testabilidade do processo com scripts de testes automáticos.

Realizar a ingestão de dados brutos (CSV) em uma camada Bronze.

Tratar, transformar e consolidar esses dados em uma camada Silver, com indicadores por dia.

Utilizar boas práticas de SQL + PySpark e manter o projeto modularizado com funções reutilizáveis.


 - Estrutura do Projeto:

UTILS.py – Funções auxiliares reutilizáveis (transformações, joins, insert, etc).

TESTES.py – Execução dos testes unitários com validações de schema, ingestão e join.

MAIN (BRONZE).py – Ingestão do CSV para a camada Bronze (tabela Delta).

CATALOG.py – Criação da tabela final agregada (Silver).

MAIN (SILVER).py – Tratamento, transformação e consolidação dos dados.


  - Tecnologias Utilizadas:

Databricks

Apache Spark com PySpark

Delta Lake

SQL (SparkSQL)

Pytest para testes

DBFS (Databricks File System)

cURL para ingestão externa.


  - Pipeline:

Ingestão do CSV (Bronze)

Download via curl com função download_csv_with_curl

Leitura com schema definido + persistência como tabela Delta

Transformações (Silver)

Conversão de datas com TO_TIMESTAMP e CAST

Contagem por categoria (Negócio, Pessoal)

Contagem por propósito (Reunião, Não Negócio)

Agregações por data

Quantidade total de corridas

Distância máxima, mínima e média

Join final entre as contagens

Inserção dos dados finais na tabela Silver (info_corridas_do_dia).


  - Testes:

Todos os módulos e etapas foram testados com pytest e asserts manuais no notebook TESTES.py.
Testes realizados:

Teste de download do arquivo

Teste de leitura e persistência como Delta

Verificação de schema

Validação da contagem de categorias e propósitos

Testes do join e da ingestão final.


  - Desafios e Soluções Técnicas:
    
Desafio - Solução Aplicada
Valores duplicados após JOIN -- Identificado que a granularidade da tabela categoria causava um "cartesian join". Foi criada uma view agregada por DT_REFE para corrigir
Erros ao usar TO_TIMESTAMP no Spark 3.x --	Foi aplicado o parâmetro spark.sql.legacy.timeParserPolicy = LEGACY para manter compatibilidade com o parser antigo
Divergência entre schema da view e da tabela Delta -- Os casts e tipos de dados foram ajustados explicitamente no SELECT final da view_join
Erros com reescrita de arquivos no DBFS	-- Utilizado dbutils.fs.rm com recurse=True para limpar diretórios antes da escrita.


  - Resultado Final
    
Tabela consolidada: info_corridas_do_dia
Com os seguintes campos:

DT_REFE: Data de referência.

QT_CORR: Quantidade de corridas.

QT_CORR_NEG: Quantidade de corridas com a categoria “Negócio”.

QT_CORR_PESS: Quantidade de corridas com a categoria “Pessoal”.

VL_MAX_DIST: Maior distância percorrida por uma corrida.

VL_MIN_DIST: Menor distância percorrida por uma corrida.

VL_AVG_DIST: Média das distâncias percorridas.

QT_CORR_REUNI: Quantidade de corridas com o propósito de "Reunião".

QT_CORR_NAO_REUNI: Quantidade de corridas com o propósito declarado e diferente de "Reunião".



  - Como Executar:

Rode UTILS.py para configurar todas as funções

Execute TESTES.py para verificar a integridade da pipeline

Rode MAIN (BRONZE).py para ingestão dos dados

Execute CATALOG.py para garantir a criação da tabela final

Rode MAIN (SILVER).py para processar e consolidar os dados


  - Sobre Mim
Este projeto foi desenvolvido como parte de um desafio técnico para consolidar conhecimentos em engenharia de dados .
Foquei em construir uma pipeline robusta, com testes e modularização de código, simulando um cenário de produção
