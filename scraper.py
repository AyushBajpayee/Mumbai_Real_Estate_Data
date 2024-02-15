import warnings
warnings.filterwarnings('ignore')
from selenium import webdriver 
from selenium.webdriver.support.ui import Select 
from selenium.webdriver.common.by import By
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
from time import sleep
import json
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

def delivery_report(errmsg, msg):
	if errmsg is not None:
		print(f"Delivery failed for Message: {msg.key()} : {errmsg}")
		return
	print(f'Message: {msg.key()} successfully produced to Topic: {msg.topic()} Partition: [{msg.partition()}] at offset {msg.offset()}')

def ScrapeSinkPostgresDB(driver):
    print('Sink Selected: postgresDB')
    records_raw = pd.DataFrame(columns=['अनु क्र.','दस्त क्र.','दस्त प्रकार','दू. नि. कार्यालय','वर्ष','लिहून देणार','लिहून घेणार','इतर माहीती','सूची क्र. २'])
    for table in tqdm(driver.find_elements(by=By.ID, value='tbdata')):
        data = [item.text if item.text != 'सूची क्र. २' else item.find_element(by=By.TAG_NAME,value='a').get_attribute('href') for item in table.find_elements(by=By.XPATH, value=".//*[self::td or self::th]")]
        records_raw.loc[len(records_raw)] = data

    print('Finished Scraping!')
    db_user = 'postgres'
    db_password = 'root'
    db_host = '127.0.0.1'
    db_port = '5432'
    db_name = 'propreturns'

    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    table_name = 'record_details_raw'

    records_raw.to_sql(table_name, engine, if_exists='replace', index=True)
    print(f"{len(records_raw)} DataFrame has been inserted into the '{table_name}' table.")
    
def ScrapSinkKafkaTopic(driver):
    print('Sink Selected: kafka Topic')
    conf = {
        "bootstrap.servers": "localhost:9092"
    }
    admin_client = AdminClient(conf=conf)

    topic_name = 'records_raw_topic'
    records_raw_topic = NewTopic(topic=topic_name,num_partitions=1,replication_factor=1)
    
    if topic_name not in admin_client.list_topics().topics.keys():
        admin_client.create_topics([records_raw_topic])
        print('Topic Created')

    else:
        print('Topic already present, skipping creation')

    sample_dict = {}
    producerScrapper = Producer(conf)

    for table in tqdm(driver.find_elements(by=By.ID, value='tbdata')):
        data = [item.text if item.text != 'सूची क्र. २' else item.find_element(by=By.TAG_NAME,value='a').get_attribute('href') for item in table.find_elements(by=By.XPATH, value=".//*[self::td or self::th]")]
        sample_dict['अनु क्र.'] = data[0]
        sample_dict['दस्त क्र.'] = data[1]
        sample_dict['दस्त प्रकार'] = data[2]
        sample_dict['दू. नि. कार्यालय'] = data[3]
        sample_dict['वर्ष'] = data[4]
        sample_dict['लिहून देणार'] = data[5]
        sample_dict['लिहून घेणार'] = data[6]
        sample_dict['इतर माहीती'] = data[7]
        sample_dict['सूची क्र. २'] = data[8]
        producerScrapper.produce(topic=topic_name, key=sample_dict['अनु क्र.'], value=json.dumps(sample_dict).encode(), on_delivery=delivery_report)

    producerScrapper.flush()
    print('Finished Producing!')

def ScraperWebDriver(choice):
    input_params_dropdown_by_id = {}
    input_params_dropdown_by_id['dbselect'] = '2023' #Select Year
    input_params_dropdown_by_id['district_id'] = 'मुंबई उपनगर' #District
    input_params_dropdown_by_id['taluka_id'] = 'अंधेरी' #Taluka
    input_params_dropdown_by_id['village_id'] = 'बांद्रा' #Village
    input_params_text_by_id = {}
    input_params_text_by_id['free_text'] = '2023' 

    print('Starting WebDriver...........')
    url = 'https://pay2igr.igrmaharashtra.gov.in/eDisplay/Propertydetails/index' 
    driver = webdriver.Chrome() 
    driver.implicitly_wait(10)
    driver.get(url)
    print('WebDriver Active!')

    for key, value in input_params_dropdown_by_id.items():
        print(key,value)
        dropdown = driver.find_element(by=By.ID,value=key)
        dropdown_select = Select(dropdown)
        for option in dropdown_select.options:
            if option.text == value:
                option.click()
                print(f'Clicked {option.text} for {key}')
                break
        driver.implicitly_wait(2)

    #Input reg year
    driver.find_element(by=By.ID, value='free_text').send_keys(input_params_text_by_id['free_text'])
    driver.implicitly_wait(2)

    #Input Captcha
    print('Enter Captcha in the box [DO NOT PRESS ENTER]')
    sleep(10)

    #Submit to get result set
    driver.find_element(by=By.ID,value='submit').click()
    print('Captcha Detected')
    print('Table Loading')
    sleep(10)


    #Click on all pages
    dropdown = driver.find_element(by=By.NAME, value='tableparty_length')
    dropdown_select = Select(dropdown)
    for option in dropdown_select.options:
        if option.text == 'All':
            option.click()
            print('Set to All Records')
            break

    print('Table Loaded')
    print('Data Loading...')
    sleep(10)

    if choice == 0:
        ScrapeSinkPostgresDB(driver=driver)
    
    if choice == 1:
        ScrapSinkKafkaTopic(driver=driver)


#MAIN
# Insert Scraped Raw Data in postgres
choice = int(input('Sink Scrapped Data to -> \n postgresDB : 0\n Kafka Topic: 1\n[0/1]:'))
ScraperWebDriver(choice=choice)
print('Scraping Job Finished Successfully!')