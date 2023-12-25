import pandas as pd
from sqlalchemy import create_engine

# Параметры подключения к PostgreSQL
db_user = 'Aleksey'
db_password = '0701'
db_host = 'host.docker.internal'
db_port = '5432'
db_name = 'DA_Work'

# Создание строки подключения к PostgreSQL
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Чтение данных из CSV в DataFrame
data = pd.read_csv('C:/Users/alvlr/PycharmProjects/pythonProject/Final_Work/data/sample_ga_sessions.csv', low_memory=False)

# Загрузка данных в PostgreSQL
data.to_sql('sessions', engine, if_exists='replace', index=False)


