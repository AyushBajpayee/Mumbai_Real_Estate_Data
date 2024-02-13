#imports
from selenium import webdriver 
from selenium.webdriver.support.ui import Select 
from selenium.webdriver.common.by import By
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
from time import sleep
import warnings
import psycopg2
warnings.filterwarnings('ignore')

#selenium webdriver
def customScraper():
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
        driver.implicitly_wait(2)

    #Input reg year
    driver.find_element(by=By.ID, value='free_text').send_keys(input_params_text_by_id['free_text'])
    driver.implicitly_wait(2)

    #Input Captcha
    print('Enter Captcha in the box [DO NOT PRESS ENTER]')
    sleep(10)

    #Submit to get result set
    driver.find_element(by=By.ID,value='submit').click()
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
    #scrap records
    records_raw = pd.DataFrame(columns=['अनु क्र.','दस्त क्र.','दस्त प्रकार','दू. नि. कार्यालय','वर्ष','लिहून देणार','लिहून घेणार','इतर माहीती','सूची क्र. २'])

    print('Data Loading...')
    sleep(10)
    for index, table in enumerate(tqdm(driver.find_elements(by=By.ID, value='tbdata'))):
        data = [item.text if item.text != 'सूची क्र. २' else item.find_element(by=By.TAG_NAME,value='a').get_attribute('href') for item in table.find_elements(by=By.XPATH, value=".//*[self::td or self::th]")]
        records_raw.loc[len(records_raw)] = data

    print('Finished Scraping!')
    return records_raw

#MAIN
# Insert Scraped Raw Data in postgres
db_user = 'postgres'
db_password = 'root'
db_host = '127.0.0.1'
db_port = '5432'
db_name = 'propreturns'
table_name = 'record_details_raw'

conn = psycopg2.connect(
   database="postgres", user=db_user, password=db_password, host=db_host, port= db_port
)
conn.autocommit = True
cursor = conn.cursor()

records_raw = customScraper()
createDbQuery = f'CREATE DATABASE {db_name}'

try:
    cursor.execute(createDbQuery)
    print(f'DATABASE {db_name} created!')
    conn.close()

except Exception as e:
    if f'database "{db_name}" already exists' in str(e).split('\n'):    
        print(f'{db_name.lower()} was already present, skipping creation')
        pass

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

records_raw.to_sql(table_name, engine, if_exists='replace', index=False)

print(f"{len(records_raw)} DataFrame has been inserted into the '{table_name}' table.")
print('FINISHED!')