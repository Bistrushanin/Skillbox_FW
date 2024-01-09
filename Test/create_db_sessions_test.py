from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
import logging

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data_to_postgres():
    db_user = 'Aleksey'
    db_password = '0701'
    db_host = 'host.docker.internal'
    db_port = '5432'
    db_name = 'DA_Work'

    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    # Логируем информацию о чтении файла
    logger.info("Reading data from CSV file...")

    data = pd.read_csv('/opt/airflow/dags/sample_ga_sessions.csv', low_memory=False)

    # Логируем информацию о загрузке данных
    logger.info("Loading data to PostgreSQL...")

    data.to_sql('sessions_test', engine, if_exists='replace', index=False)

    # Логируем успешное завершение задачи
    logger.info("Data loading completed successfully.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_data_to_postgres',
    default_args=default_args,
    description='DAG для загрузки данных в PostgreSQL',
    schedule_interval='@once',
)

load_data_task = PythonOperator(
    task_id='load_data_to_postgres_task',
    python_callable=load_data_to_postgres,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
