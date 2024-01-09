import psycopg2
import os
import json


def read_json(directory):
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
sessions_data = read_json(folder_path)

# Вставка данных в таблицу
insert_data_into_table(connection, cursor, 'sessions_test', sessions_data)

# Закрытие курсора и соединения
cursor.close()
connection.close()
