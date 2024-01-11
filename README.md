airflow-docker:
- структура  docker контейнера airflow, с дагом. 
- Json файлы, как и сам пайтон скрипт, располагаются в папке /dags/jsons.
- Dag_read_and_insert_data - скрипт на Python для Apache Airflow, который считывает JSON-файлы из определенной папки и вставляет данные в таблицы PostgreSQL. Скрипт определяет DAG с именем 'uploading_data_from_json' и единственной задачей 'read_and_insert_data'. Эта задача представляет собой PythonOperator, вызывающий функцию read_and_insert_data, которая, в свою очередь, считывает JSON-файлы из указанной папки, обрабатывает данные и вставляет их в две таблицы PostgreSQL с именами 'sessions' и 'hits'.
1. В скрипте используется модуль логирования Python для регистрации информации, предупреждений, ошибок и т.д
2. Скрипт динамически генерирует SQL-запросы на основе столбцов, присутствующих в файлах JSON, и таблиц базы данных.

FinalWork - здесь расположены:
- FinalWork файл первичной обработки DataFrame hits и sessions
- в папке /date_to_DBMS, скрипты создания таблиц в Postgry
- processing_json_files - скрипт подгрузки данных из json файлов через docker (еще не используя airflow)
- в паке /data, расположены csv фалы использованные в создании БД
- в папке /data/New_data - json файлы

Test - папка со скриптами экспериментами
- create_db_sessions_test - создание БД sessions_test
- ADD_DATA_TO_DB_TEST - добавление данных из json (для эксперимента и скорости использовал всего 1 файл, и еще пока без использования Airflow)
- DAG_2_sessions_test - тестовый даг, который объединяет в себе create_db_sessions_test и ADD_DATA_TO_DB_TEST
