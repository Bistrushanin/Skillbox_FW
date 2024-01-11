airflow-docker:
- структура  docker контейнера airflow, с дагом. Даг выполняет считывание json фалов, и записывает считанные данные в уже существующую БД, поднятую через Docker, PostgreSQL.
- Json файлы, как и сам пайтон скрипт, располагаются в папке dags.

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
