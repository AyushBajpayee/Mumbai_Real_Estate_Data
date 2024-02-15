import warnings
warnings.filterwarnings('ignore')
import pandas as pd
from deep_translator import GoogleTranslator
from googletrans import Translator
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

#Functions
def translator(s,source):
    return GoogleTranslator(source=source, target='en').translate(s)


def translator_v2(s,source='auto'):
        translator = Translator()   
        return translator.translate(s, src='hi',dest='en').text if s != '' else 'null' 

translate_udf = f.udf(translator_v2,StringType())

spark = SparkSession.builder.getOrCreate()
print('Spark Session Started')

host = '127.0.0.1'
port = '5432'
database = 'propreturns'
username = 'postgres'
password = 'root'
url = f"jdbc:postgresql://{host}:{port}/{database}"
table_name = 'record_details_raw'

spark_df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password).load()

print(f'Table {table_name} Loaded. Rows: {spark_df.count()}')

new_names = []
for col in spark_df.columns:
    new_names.append(translator(col,source='auto'))

new_names = [x.replace('.','') for x in new_names]
spark_df = spark_df.toDF(*new_names)
spark_df = spark_df.withColumnRenamed(existing='Will write',new='buyer_name')
spark_df = spark_df.withColumnRenamed(existing='Will write down',new='seller_name')
print('Headers Translated')
spark_df = spark_df.withColumn("to_translate",f.concat(f.col("diarrhea type"),f.lit('$'),
                                          f.col('Du Prohibit Office'),f.lit('$'),
                                          f.col('buyer_name'),f.lit('$'),
                                          f.col('seller_name'),f.lit('$'),
                                          f.col('Other information')))

spark_df = spark_df.withColumn('to_translate',translate_udf(spark_df['to_translate']))
spark_df = spark_df.withColumn('diarrhea type',f.split(spark_df['to_translate'],'\\$')[0])\
        .withColumn('Du Prohibit Office',f.split(spark_df['to_translate'],'\\$')[1])\
        .withColumn('buyer_name',f.split(spark_df['to_translate'],'\\$')[2])\
        .withColumn('seller_name',f.split(spark_df['to_translate'],'\\$')[3])\
        .withColumn('Other information',f.split(spark_df['to_translate'],'\\$')[4])
print('Translate Spark Job defined')

table_name_sink = 'record_details_translated'

print('Executing Spark Jobs.....')
spark_df.select("*").write.format("jdbc")\
    .option("url", url) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", table_name_sink) \
    .option("user", username) \
    .option("password", password).save()

print('Finished..... Table Sinked in PostgresDB!')