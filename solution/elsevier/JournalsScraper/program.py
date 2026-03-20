import configparser
import logging
from typing import Callable, Dict, List
import requests
from bs4 import BeautifulSoup
import datetime
from ...database import Saver, Postgress
import requests
import time
from stem import Signal
from stem.control import Controller

ELSEVIER_DATABASE_SCHEMA = 'elsevier'

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


def main(
    get_page, 
    get_journals,
    save
    ):
    print("Starting main")
    logging.info('Starting program')
    page_number = 1
    while True:
        ip = get_current_ip()
        logging.info(f"Executing from ip: {ip}")
        extract_dts_utc = str(datetime.datetime.utcnow())
        content = get_page(page_number)
        journals = get_journals(content)
        if len(journals) == 0:
            logging.info("No more journals found")
            break
        for j in journals:
            j["extract_dts_utc"] = extract_dts_utc
        save(ELSEVIER_DATABASE_SCHEMA, 'journals', journals)
        page_number += 1
        # renew tor
        renew_tor_ip()
        time.sleep(5)
    logging.info('Finished')


def get_current_ip():
    session = requests.session()

    # TO Request URL with SOCKS over TOR
    session.proxies = {}
    session.proxies['http']='socks5h://localhost:9050'
    session.proxies['https']='socks5h://localhost:9050'

    try:
        r = session.get('http://httpbin.org/ip')
    except Exception as e:
        print(str(e))
    else:
        return r.text


def renew_tor_ip():
    with Controller.from_port(port = 9051) as controller:
        controller.authenticate(password=config['TOR_PASSWORD'])
        controller.signal(Signal.NEWNYM)


def get_page(page_number: int):
    logging.info(f'Getting page {page_number}')
    url = f"https://www.elsevier.com/search-results?labels=journals&page={page_number}"
    session = requests.session()

    # TO Request URL with SOCKS over TOR
    session.proxies = {}
    session.proxies['http']='socks5h://localhost:9050'
    session.proxies['https']='socks5h://localhost:9050'
    response = session.get(url)
    return response.content


def get_journals(content) -> List[Dict]:
    logging.info(f'Getting journals from content')
    soup = BeautifulSoup(content, 'html.parser')
    journal_entries = soup.find_all('article', class_='search-result')
    result = []
    for journal in journal_entries:
        anchor = journal \
            .find(class_='search-result-journal-title') \
            .find('a')
        href = anchor.attrs["href"]
        title = anchor.text
        issn = journal \
            .find(class_='journal-result-issn') \
            .text
        result.append(
            {
                'href': href,
                'title': title,
                'issn': issn
            }
        )
    logging.info(f"Number of journals found: {len(result)}")
    return result


if __name__ == '__main__':
    if db.table_exists('elsevier', 'journals'):
        db.execute_query('TRUNCATE TABLE elsevier.journals')
    main(get_page, get_journals, saver.save)