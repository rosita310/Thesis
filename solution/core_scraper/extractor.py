import requests
import logging
import codecs
from bs4 import BeautifulSoup
import time
import datetime
from database import Postgress, Saver
import re
import configparser

# Schema where the resulting tables should be placed
CORE_DATABASE_SCHEMA = "core"

logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

def read_config(path) -> configparser.SectionProxy:
    logging.info('Reading configuration')
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']


config = read_config('../.env')
db = Postgress(
    server=config['POSTGRES_SERVER'], 
    database=config['POSTGRES_DB'],
    user=config['POSTGRES_USER'],
    password=config['POSTGRES_PASSWORD']
    )
saver = Saver(db)

def parse(content, dts, url) -> list:
    
    soup = BeautifulSoup(content, 'html.parser')
    titles = [t.find('b').text for t in soup.find_all('th')]
    evenrows = soup.find_all('tr', class_='evenrow')
    oddrows = soup.find_all('tr', class_='oddrow')
    data = []
    for row in evenrows + oddrows:
        v = {t: None for t in titles}
        # print(row)
        attributes = row.find_all('td')
        i = 0
        for a in attributes:
            value = a.text.strip()
            if a.text.strip() == 'view':
                value = a.find('a')['href']
            v[titles[i]] = value
            i = i + 1
        v["$_extract_dts"] = timestamp
        v["$_rec_src"] = url
        data.append(v)
    return data
    

if __name__ == '__main__':
    types = ['jnl-ranks', 'conf-ranks']
    for t in types:
        logging.info(f"Working on {t}")
        for i in range(1, 20):
            logging.info(f"site: {i}")
            url = f"http://portal.core.edu.au/{t}/?search=&by=all&source=CORE2020&sort=atitle&page={i}"
            timestamp = str(datetime.datetime.now())
            response = requests.get(url)
            if response.status_code != 200:
                logging.info(f"Got {response.status_code}: stopping script")
                break
            data = parse(response.content, timestamp, url)
            saver.save(CORE_DATABASE_SCHEMA, t.replace('-', '_'), data)
            time.sleep(1)
    logging.info(f"done")

