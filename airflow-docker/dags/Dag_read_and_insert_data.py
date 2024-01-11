from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2  # Библиотека для работы с PostgreSQL
import json  # Библиотека для работы с JSON
import os  # Модуль для работы с файловой системой
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_json_files_in_folder(folder_path):
    # Чтение JSON файлов в указанной папке
    json_files = [file for file in os.listdir(folder_path) if file.endswith('.json')]

    # Сортировка файлов, содержащих "hits" и "session" в их именах
    hits_files = [file for file in json_files if "hits" in file]
    session_files = [file for file in json_files if "session" in file]

    # Создание словарей для данных из hits и session файлов
    hits_data = {}
    for file in hits_files:
        with open(os.path.join(folder_path, file), 'r') as f:
            hits_data[file] = json.load(f)

    session_data = {}
    for file in session_files:
        with open(os.path.join(folder_path, file), 'r') as f:
            session_data[file] = json.load(f)

    return hits_data, session_data


# ---------------------------------------------------------------------------

def validate_data_insertion(cursor, table_name):
    # Проверка количества строк в таблице.
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]
    logging.info(f"Текущее количество строк в таблице '{table_name}': {row_count}")


# ---------------------------------------------------------------------------


def insert_data_into_table(table_name, data):
    # Устанавливаем соединение внутри функции
    connection = psycopg2.connect(
        host='host.docker.internal',
        port=5432,
        database='DA_Work',
        user='Aleksey',
        password='0701'
    )
    cursor = connection.cursor()

    for file, json_data in data.items():
        logging.info(f"Обработка файла {file}...")

        if not json_data:
            logging.info(f"Файл {file} не содержит данных. Пропуск.")
            continue  # Пропустить JSON-файлы без данных

        # Получение список всех столбцов в таблице
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
        columns = [column[0] for column in cursor.fetchall()]

        for record_list in json_data.values():
            if not record_list:
                logging.info(f"Нет записей в {file}. Пропуск.")
                continue  # Пропустить записи без данных

            for record in record_list:
                # Определение, какие столбцы присутствуют в JSON-файле

                present_columns = [col for col in columns if col in record]
                if not present_columns:
                    logging.warning(f"В файле {file} отсутствуют соответствующие столбцы. Пропуск записи.")
                    continue  # Пропустить записи без совпадающих столбцов

                # Создание динамического SQL-запроса
                columns_str = ', '.join(columns)  # Использание всех столбцов из таблицы
                placeholders = ', '.join(['%s'] * len(columns))
                query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});"

                # Получение всех значений для всех столбцов
                values = [record.get(col) for col in columns]

                try:
                    cursor.execute(query, values)
                except psycopg2.Error as e:
                    logging.error(f"Ошибка при выполнении запроса к файлу {file}: {e}")

        logging.info(f"Данные из файла {file} загружены в таблицу {table_name}.")
        validate_data_insertion(cursor, table_name)

    connection.commit()
    logging.info(f"Все данные успешно загружены в таблицу {table_name}.")

    cursor.close()
    connection.close()


# ---------------------------------------------------------------------

def read_and_insert_data():
    folder_path = '/opt/airflow/dags/jsons'
    hits_data, session_data = read_json_files_in_folder(folder_path)
    insert_data_into_table('sessions', session_data)
    insert_data_into_table('hits', hits_data)


default_args = {
    'owner': 'Aleksey',
    'start_date': datetime(2024, 1, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'uploading_data_from_json',
    default_args=default_args,
    description='Reading distributing and uploading data from json to Postgresql',
    schedule_interval='@once',
)

# Создаем одну задачу для чтения и вставки данных
read_and_insert_data_task = PythonOperator(
    task_id='read_and_insert_data',
    python_callable=read_and_insert_data,
    dag=dag,
)

