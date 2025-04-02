from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os

default_args = {
    'depends_on_past': False,
    'email': ['email@email.com.br'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'windturbine',
    description='Dados da Turbina Eólica',
    schedule_interval=None,
    start_date=datetime(2023, 3, 5),
    catchup=False,
    default_args=default_args,
    default_view='graph',
    doc_md="## DAG para registrar dados de turbina eólica"
)

group_check_temp = TaskGroup("group_check_temp", dag=dag)
group_database = TaskGroup("group_database", dag=dag)

# Sensor de Arquivo
file_sensor_task = FileSensor(
    task_id='file_sensor_task',
    filepath=Variable.get('path_file'),
    fs_conn_id='fs_default',
    poke_interval=10,
    timeout=60 * 10,  # Aguarda até 10 minutos antes de falhar
    dag=dag
)

# Função para processar arquivo JSON
def process_file(**kwargs):
    file_path = Variable.get('path_file')
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo {file_path} não encontrado!")

    with open(file_path) as f:
        data = json.load(f)
    
    ti = kwargs['ti']
    ti.xcom_push('idtemp', data['idtemp'])
    ti.xcom_push('powerfactor', data['powerfactor'])
    ti.xcom_push('hydraulicpressure', data['hydraulicpressure'])
    ti.xcom_push('temperature', data['temperature'])
    ti.xcom_push('timestamp', data['timestamp'])

    os.remove(file_path)  # Remove o arquivo após processamento

# Task de processamento de arquivo
get_data = PythonOperator(
    task_id='get_data',
    python_callable=process_file,
    dag=dag
)

# Criar tabela no PostgreSQL
create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id='postgres',
    sql="""
        CREATE TABLE IF NOT EXISTS sensors (
            idtemp VARCHAR,
            powerfactor VARCHAR,
            hydraulicpressure VARCHAR,
            temperature VARCHAR,
            timestamp VARCHAR
        );
    """,
    task_group=group_database,
    dag=dag
)

# Inserir dados no PostgreSQL
insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres',
    sql="""
        INSERT INTO sensors (idtemp, powerfactor, hydraulicpressure, temperature, timestamp)
        VALUES (
            '{{ ti.xcom_pull(task_ids="get_data", key="idtemp") }}',
            '{{ ti.xcom_pull(task_ids="get_data", key="powerfactor") }}',
            '{{ ti.xcom_pull(task_ids="get_data", key="hydraulicpressure") }}',
            '{{ ti.xcom_pull(task_ids="get_data", key="temperature") }}',
            '{{ ti.xcom_pull(task_ids="get_data", key="timestamp") }}'
        );
    """,
    task_group=group_database,
    dag=dag
)

# Enviar e-mail se temperatura for alta
send_email_alert = EmailOperator(
    task_id='send_email_alert',
    to='email@email.com.br',
    subject='[Alerta] Alta Temperatura Detectada',
    html_content="""<h3>⚠ Alerta: Temperatura Acima do Limite!</h3>
                    <p>DAG: windturbine</p>""",
    task_group=group_check_temp,
    dag=dag
)

# Enviar e-mail se temperatura for normal
send_email_normal = EmailOperator(
    task_id='send_email_normal',
    to='email@email.com.br',
    subject='[Info] Temperatura Normal',
    html_content="""<h3>✅ Temperatura dentro dos parâmetros normais.</h3>
                    <p>DAG: windturbine</p>""",
    task_group=group_check_temp,
    dag=dag
)

# Função para avaliar temperatura e direcionar a branch correta
def avalia_temp(**context):
    temp = float(context['ti'].xcom_pull(task_ids='get_data', key="temperature"))
    return 'send_email_alert' if temp >= 24 else 'send_email_normal'

# Task para avaliar temperatura
check_temp_branc = BranchPythonOperator(
    task_id='check_temp_branc',
    python_callable=avalia_temp,
    dag=dag
)

# Definição do fluxo das tasks
file_sensor_task >> get_data
get_data >> group_database
get_data >> check_temp_branc
check_temp_branc >> send_email_alert
check_temp_branc >> send_email_normal
group_database >> insert_data
