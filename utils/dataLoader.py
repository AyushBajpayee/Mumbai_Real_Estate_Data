import pandas as pd
import os
import psycopg2
import credentials

class dataLoader:
    def __init__(self):
        self.host = credentials.host
        self.port = credentials.port
        self.db_name = credentials.db_name
        self.user= credentials.user
        self.password = credentials.password

    def postgresToPandas(self, table_name) -> pd.DataFrame:
        conn = psycopg2.connect(dbname=self.db_name, user=self.user, password=self.password, host=self.host, port=self.port)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    
    def postgresToSpark(self, spark, table_name):
        jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.db_name}"
        connection_properties = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }
        spark_df = spark \
            .read \
            .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

        return spark_df

    

    print('Data Loader Object Created!')