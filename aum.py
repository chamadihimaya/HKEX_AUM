#!/usr/bin/env python
# coding: utf-8

# In[1]:


import snowflake.connector
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime
import time


# In[2]:


# Snowflake connection
"""conn = snowflake.connector.connect(
    user='cham00',
    password='Meowmeow1',
    account='mkjgyfb-br89169'
)"""
conn = snowflake.connector.connect(
    user='SQRR',
    password='Panama9-Latticed8-Chamomile5-Scary3-Krypton9',
    account='tlnygtx-hc90982'
)


# In[3]:


# Create a cursor object
cursor = conn.cursor()


# In[4]:


# Specify the database and schema to use
cursor.execute("USE DATABASE HKEX")
cursor.execute("USE SCHEMA HKEX")


# In[5]:


# SQL command to create the table if it doesn't exist, with date as DATE data type
create_table_sql = """
CREATE TABLE IF NOT EXISTS etf_aum_data (
    date DATE,
    aum_9008 FLOAT,
    aum_9042 FLOAT,
    aum_9439 FLOAT
);
"""


# In[6]:


# Execute the SQL command to create the table
cursor.execute(create_table_sql)


# In[7]:


# Selenium setup
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=chrome_options)


# In[8]:


# Function to get AUM and update time for a given ETF
def get_aum_and_time(sym):
    url = f"https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym={sym}&sc_lang=en"
    driver.get(url)
    time.sleep(5)
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    
    aum_element = soup.find('dt', {'class': 'ico_data col_aum'})
    aum_value = aum_element.text.strip() if aum_element else "N/A"
    if aum_value.startswith("US$"):
        aum_value = aum_value[3:]  # Remove "US$"
    if aum_value.endswith("M"):
        aum_value = float(aum_value[:-1])*1000000  # Remove "M" and convert to float
    
    time_element = soup.find('dt', {'class': 'ico_data col_aum_date'})
    update_time = time_element.text.strip() if time_element else "N/A"
    
    return aum_value, update_time


# In[9]:


# Get AUMs and update times
aum_9008, time_9008 = get_aum_and_time("9008")
aum_9042, time_9042 = get_aum_and_time("9042")
aum_9439, time_9439 = get_aum_and_time("9439")
driver.quit()


# In[10]:


# Function to convert scraped date format to YYYY-MM-DD
def convert_scraped_date(date_str):
    # Remove brackets if present
    date_str = date_str.replace('(', '').replace(')', '').strip()
    return datetime.strptime(date_str, '%d %b %Y').strftime('%Y-%m-%d')


# In[11]:


# Use the update time from the first ETF as the current date
time_9008_date = time_9008.replace('as at ', '').strip()
current_date = convert_scraped_date(time_9008_date)


# In[12]:


# Check if a record for the current date already exists
cursor.execute(f"SELECT COUNT(*) FROM etf_aum_data WHERE date = '{current_date}'")
record_exists = cursor.fetchone()[0] > 0

if record_exists:
    # Update the existing record
    cursor.execute(f"""
        UPDATE etf_aum_data
        SET aum_9008 = {aum_9008}, aum_9042 = {aum_9042}, aum_9439 = {aum_9439}
        WHERE date = '{current_date}'
    """)
else:
    # Insert new data into Snowflake
    cursor.execute(f"""
        INSERT INTO etf_aum_data (date, aum_9008, aum_9042, aum_9439) 
        VALUES ('{current_date}', {aum_9008}, {aum_9042}, {aum_9439})
    """)

conn.commit()


# In[13]:


cursor.close()
conn.close()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




