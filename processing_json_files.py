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


# ------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------

def insert_data_into_table(connection, cursor, table_name, data):
    for file, json_data in data.items():
        if not json_data:
            continue  # Пропустить JSON-файлы без данных
        # print(json_data)

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
folder_path = 'C:/Users/alvlr/PycharmProjects/pythonProject/Final_Work/data/New_data/'

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

# Закрытие курсора и соединения
cursor.close()
connection.close()
