# Pipeline ETL Turbina Eólica

Este projeto tem como objetivo a coleta, processamento e armazenamento de dados de sensores de uma turbina eólica utilizando um pipeline de dados orquestrado pelo Apache Airflow. 

O fluxo de trabalho envolve a captura periódica de medições como fator de potência, pressão hidráulica e temperatura, garantindo que esses dados sejam armazenados de forma segura em um banco de dados PostgreSQL para futuras análises. 

Além disso, um sistema de alertas baseado na temperatura da turbina foi implementado para enviar notificações por e-mail caso valores críticos sejam detectados.

<img src="https://i.imgur.com/IhRKv1c.png">

Tecnologias Utilizadas

- Docker e Docker Compose: Utilizados para orquestrar os serviços do projeto, garantindo um ambiente controlado e fácil de reproduzir.
- PostgreSQL: Banco de dados relacional onde os dados dos sensores são armazenados.
- Apache Airflow: Responsável por gerenciar e automatizar o pipeline de dados, incluindo a leitura dos arquivos JSON, inserção no banco de dados e envio de alertas.
- Python: Linguagem principal do projeto, utilizada para processar os dados dos sensores, interagir com o banco de dados e definir as DAGs no Airflow.
- MinIO: Serviço de armazenamento de objetos compatível com S3, que pode ser utilizado para armazenar logs e arquivos de backup dos dados coletados.
- FileSensor (Airflow): Monitora continuamente um diretório em busca de novos arquivos JSON contendo dados de sensores.
- EmailOperator (Airflow): Utilizado para enviar notificações de alerta caso a temperatura ultrapasse um limite pré-definido.

<img src="https://i.imgur.com/75mwNfX.png" style="width:100%; height:auto;">

File_sensor_task
- Verifica o arquivo em intervalos regulares.
- Não monitora a pasta indefinidamente
- Não inicializa a DAG quando o arquivo for disponibilizado
- Não tem conhecimento das execuções anteriores da DAG
- filepath: verifica se o arquivo existe antes de prosseguir
- fs_conn_id: conexão com o arquivo através de conexão do Airflow. Conexão padrão fs_default

windturbine
- Gera um aquivo Json
- {"idtemp": "1", "powerfactor": "0.8837929080361997", "hydraulicpressure": "78.86011124702158", "temperature": "25.279809506572597", "timestamp": "2023-03-19 17:26:55.230351"}
- Notebook Python simula a geração do arquivo

PythonOperator
- Deverá ler o json
- Colocar as 5 variáveis no Xcom
- Excluir o arquivo

BranchPythonOperator
- Se a temperatura for >=24 graus manda Email de alerta
- Se não manda um email informativo

PostgresOperator
- Cria a tabela
- Insere os dados

Pré Etapas
- Criar conexão para file_sensor_task
- Criar variável com caminho do arquivo
