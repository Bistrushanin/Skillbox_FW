import psycopg2  # Библиотека для работы с PostgreSQL
import json  # Библиотека для работы с JSON
import os  # Модуль для работы с файловой системой


def read_json_files_in_folder(folder_path):
    # Получение списка JSON файлов в указанной папке
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



def insert_data_into_table(connection, cursor, table_name, data):
    for file, json_data in data.items():
        if not json_data:
            continue  # Пропустить JSON-файлы без данных

        # Получение список всех столбцов в таблице
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

                # Выполнение SQL-запроса
                cursor.execute(query, values)

    connection.commit()


# ---------------------------------------------------------------------
# ---------------------------------------------------------------------


# Путь к папке с JSON файлами
folder_path = '.'

# Параметры подключения к базе данных PostgreSQL
host = 'host.docker.internal'
port = 5432
database = 'DA_Work'
user = 'Aleksey'
password = '0701'

# Установка соединения с базой данных
connection = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)

# Создание курсора для выполнения SQL запросов
cursor = connection.cursor()

# Чтение данных из JSON файлов
hits_data, session_data = read_json_files_in_folder(folder_path)

insert_data_into_table(connection, cursor, 'sessions', session_data)
insert_data_into_table(connection, cursor, 'hits', hits_data)



from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Aleksey',
    'start_date': datetime(2023, 12, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'uploading_data_from_json',
    default_args=default_args,
    description='Reading_distributing_anduploading_data_from_json_to_Postgresql',
    #schedule_interval=timedelta(minutes=3),
    schedule_interval='@once',
)

# Задача для чтения данных из JSON файлов
read_data_task = PythonOperator(
    task_id='read_data',
    python_callable=read_json_files_in_folder,
    op_args=['.'],
    dag=dag,
)

# Задача для вставки данных в таблицу sessions
insert_sessions_task = PythonOperator(
    task_id='insert_sessions',
    python_callable=insert_data_into_table,
    op_args=[connection, cursor, 'sessions', session_data],
    provide_context=True,
    dag=dag,
)

# Задача для вставки данных в таблицу hits
insert_hits_task = PythonOperator(
    task_id='insert_hits',
    python_callable=insert_data_into_table,
    op_args=[connection, cursor, 'hits', hits_data],
    provide_context=True,
    dag=dag,
)

# Закрытие курсора и соединения после выполнения всех задач
cleanup_task = PythonOperator(
    task_id='cleanup',
    python_callable=lambda: (cursor.close(), connection.close()),
    dag=dag,
)

# Определение порядка выполнения задач
read_data_task >> [insert_sessions_task, insert_hits_task] >> cleanup_task
