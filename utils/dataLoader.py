import pandas as pd
import psycopg2
import utils.config as config
import json
from confluent_kafka import Producer

print('Data Loader Object Created!')
class dataLoader:
    def __init__(self):
        self.host = config.host
        self.port = config.port
        self.db_name = config.db_name
        self.user= config.user
        self.password = config.password
        
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
    
    def pandasToKafkaTopic(self, df, topic_name):
        conf = {
            'bootstrap.servers':'localhost:9092'
        }
        producerScrapper = Producer(conf)
        for index, row in df.iterrows():
            temp_dict = {}
            for column in df.columns:
                temp_dict[column] = row[column]    
            producerScrapper.produce(topic=topic_name, key=temp_dict['अनु क्र.'], value=json.dumps(temp_dict).encode(), on_delivery=delivery_report)

        producerScrapper.flush()
        print('Pushed everything to kafka topic')
    