from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
import logging
import psycopg2
import os
import json

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data_to_postgres():
    # код для загрузки данных из CSV в PostgreSQL
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

def read_json(directory):
    #  код для чтения данных из JSON файлов
    sessions_data = {}
    for file in os.listdir(directory):
        if file.endswith(".json"):
            file_path = os.path.join(directory, file)
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    sessions_data[file] = data
                print(f"Данные из файла {file_path}:")
                print(data)
            except json.JSONDecodeError:
                print(f"Ошибка декодирования JSON в файле {file_path}.")
            except Exception as e:
                print(f"Произошла ошибка при обработке файла {file_path}: {e}")

    return sessions_data

def insert_data_into_table(connection, cursor, table_name, data):
    #  код для вставки данных в таблицу
    for file, json_data in data.items():
        if not json_data:
            continue  # Пропустить JSON-файлы без данных

        # Получение списка всех столбцов в таблице
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
        columns = [column[0] for column in cursor.fetchall()]
        print(f"Столбцы в таблице {table_name}: {columns}")

        for record_list in json_data.values():
            if not record_list:
                continue  # Пропустить записи без данных

            for record in record_list:
                # Определение, какие столбцы присутствуют в JSON-файле
                present_columns = [col for col in columns if record and col in record.keys()]
                print(f"Столбцы в JSON: {present_columns}")

                if not present_columns:
                    continue  # Пропустить записи без совпадающих столбцов

                # Создание динамического SQL-запроса
                columns_str = ', '.join(columns)  # Использание всех столбцов из таблицы
                placeholders = ', '.join(['%s'] * len(columns))
                query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});"

                # Получение всех значений для всех столбцов
                values = [record.get(col) for col in columns]
                print(values)

                try:
                    cursor.execute(query, values)
                except psycopg2.Error as e:
                    print(f"Ошибка при выполнении запроса к файлу {file}: {e}")

    connection.commit()

# Путь к папке с JSON файлами
folder_path = '.'

# Параметры подключения к базе данных PostgreSQL
host = 'host.docker.internal'
port = 5432
database = 'DA_Work'
user = 'Aleksey'
password = '0701'

# Установка соединения с базой данных PostgreSQL
connection = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)

# Создание курсора для выполнения SQL запросов
cursor = connection.cursor()

# Создание DAG
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
    'load_data_to_postgres_and_json',
    default_args=default_args,
    description='DAG для загрузки данных в PostgreSQL из CSV и JSON',
    schedule_interval='@once',
)

# Определение задачи для загрузки данных из CSV
load_data_task = PythonOperator(
    task_id='load_data_to_postgres_task',
    python_callable=load_data_to_postgres,
    dag=dag,
)

# Определение задачи для чтения данных из JSON и вставки в таблицу
read_json_task = PythonOperator(
    task_id='read_json_and_insert_task',
    python_callable=read_json,
    op_args=[folder_path],
    provide_context=True,
    dag=dag,
)

# Установка зависимостей между задачами
load_data_task >> read_json_task

if __name__ == "__main__":
    dag.cli()
