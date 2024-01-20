from selenium import webdriver 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from googletrans import Translator
import os
from sqlalchemy import create_engine
import warnings
import sys
from deep_translator import GoogleTranslator
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from time import sleep
import pandas as pd
import numpy as np
warnings.filterwarnings('ignore')

def getRecordsViaWebDriver():
    input_params_dropdown_by_id = {}
    input_params_dropdown_by_id['dbselect'] = '2023' #Select Year
    input_params_dropdown_by_id['district_id'] = 'मुंबई उपनगर' #District
    input_params_dropdown_by_id['taluka_id'] = 'अंधेरी' #Taluka
    input_params_dropdown_by_id['village_id'] = 'बांद्रा' #Village

    input_params_text_by_id = {}
    input_params_text_by_id['free_text'] = '2023' 

    url = 'https://pay2igr.igrmaharashtra.gov.in/eDisplay/Propertydetails/index' 
    driver = webdriver.Chrome() 
    driver.implicitly_wait(10)
    driver.get(url)

    for key, value in input_params_dropdown_by_id.items():
        print(key,value)
        dropdown = driver.find_element(by=By.ID,value=key)
        dropdown_select = Select(dropdown)
        for option in dropdown_select.options:
            if option.text == value:
                option.click()
                print(f'Clicked {option.text} for {key}')
                break
        sleep(5)
        driver.implicitly_wait(10)

    #Input reg year
    driver.find_element(by=By.ID, value='free_text').send_keys(input_params_text_by_id['free_text'])
    # driver.implicitly_wait(10)
    print('Enter Captcha now on the screen')
    sleep(20)
    
    #Input Captcha
    # captcha = input()
    # driver.find_element(by=By.ID, value='cpatchaTextBox').send_keys(captcha)
    # print(f'Entered Captcha {captcha}')
    # driver.implicitly_wait(2)

    #Submit to get result set
    driver.find_element(by=By.ID,value='submit').click()
    sleep(10)

    #Click on 50 Pages
    dropdown = driver.find_element(by=By.NAME, value='tableparty_length')
    dropdown_select = Select(dropdown)
    for option in dropdown_select.options:
        if option.text == 'All':
            option.click()
            break

    records_raw = pd.DataFrame(columns=['अनु क्र.','दस्त क्र.','दस्त प्रकार','दू. नि. कार्यालय','वर्ष','लिहून देणार','लिहून घेणार','इतर माहीती','सूची क्र. २'])

    for index, table in enumerate(driver.find_elements(by=By.ID, value='tbdata')):
        data = [item.text if item.text != 'सूची क्र. २' else item.find_element(by=By.TAG_NAME,value='a').get_attribute('href') for item in table.find_elements(by=By.XPATH, value=".//*[self::td or self::th]")]
        records_raw.loc[len(records_raw)] = data
        # print(data)
        print(f'{index} Row Scraped')

    return records_raw

def dfToSql(df,table_name):
# PostgreSQL database connection parameters
    db_user = 'postgres'
    db_password = 'Sunrise12345'
    db_host = '127.0.0.1'
    db_port = '5432'
    db_name = 'propReturns'

    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    # table_name = 'record_details_raw'

    df.to_sql(table_name, engine, if_exists='replace', index=True)

    print(f"{len(df)} DataFrame has been inserted into the '{table_name}' table.")

def translator_v2(s,source='auto'):
        translator = Translator()
        return translator.translate(s, src='hi',dest='en').text if s != '' else 'null'

def translator(s,source):
    return GoogleTranslator(source=source, target='en').translate(s)    

def translateDfViaSpark(spark,df):
    spark_df = spark.createDataFrame(df.copy())
    new_names = []
    for col in df.columns:
        new_names.append(translator(col,source='auto'))

    new_names = [x.replace('.','') for x in new_names]
    spark_df = spark_df.toDF(*new_names)

    translate_udf = udf(translator_v2,StringType())

    cols_to_translate = [
        'diarrhea type',
        'Du Prohibit Office',
        'Will write', 
        'Will write down', 
        'Other information'
        ]

    for column in cols_to_translate:
        spark_df = spark_df.withColumn(column, translate_udf(spark_df[column]))

    spark_df = spark_df.withColumnRenamed(existing='Will write',new='buyer_name')
    spark_df = spark_df.withColumnRenamed(existing='Will write down',new='seller_name')
    records_translated = spark_df.toPandas()
    return records_translated

# Start Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
print('Spark Initialised and Running')

#Main
print('Starting WebDriver')
records_raw = getRecordsViaWebDriver()
print(f'Finished Scrapping! Total records scrapped: {len(records_raw)}')
dfToSql(df=records_raw,table_name='record_details_raw')
print('Translating records via Spark')
records_translated = translateDfViaSpark(spark=spark,df=records_raw)
print('Records Translated')
dfToSql(df=records_translated,table_name='record_details_raw')
