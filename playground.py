import psycopg2
from sqlalchemy import create_engine
import pandas as pd

db_user = 'postgres'
db_password = 'root'
db_host = '127.0.0.1'
db_port = '5432'
db_name = 'propreturns'
table_name = 'record_details_raw'
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

conn = psycopg2.connect(
   database="postgres", user=db_user, password=db_password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cursor = conn.cursor()

createDbQuery = f'CREATE DATABASE {db_name}'

try:
    cursor.execute(createDbQuery)
    conn.close()

except Exception as e:
    if f'database "{db_name}" already exists' in str(e).split('\n'):    
        print(f'{db_name.lower()} was already present, skipping creation')
        pass

data = {
    'Name': ['John', 'Anna', 'Peter', 'Linda'],
    'Age': [28, 35, 40, 25],
    'City': ['New York', 'Paris', 'London', 'Berlin']
}

print('Hello')
# Create a DataFrame from the dictionary
df = pd.DataFrame(data)

df.to_sql(table_name, engine, if_exists='replace', index=True)
