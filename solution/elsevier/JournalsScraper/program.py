import sys
import os
import configparser
import logging

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, root_path)

import configparser
import logging
from typing import Callable, Dict, List
import requests
from bs4 import BeautifulSoup
import datetime
from python_packages.database.database import Saver, Postgress
import requests
import time
from stem import Signal
from stem.control import Controller

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

import re

ELSEVIER_DATABASE_SCHEMA = 'elsevier'

logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

def read_config(path) -> configparser.SectionProxy:
    logging.info('Reading configuration')
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']

# --- PATH ADJUSTMENT LOGIC ---
# Get the directory of program.py (...\solution\elsevier\JournalsScraper)
current_dir = os.path.dirname(os.path.abspath(__file__))

# Go up two levels to reach the 'solution' folder
solution_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))

# Create the full path to config.env
config_path = os.path.join(solution_dir, 'config.env')

# Now call the function with the absolute path
config = read_config(config_path)

db = Postgress(
    server=config['POSTGRES_SERVER'], 
    database=config['POSTGRES_DB'],
    user=config['POSTGRES_USER'],
    password=config['POSTGRES_PASSWORD']
    )
saver = Saver(db)

# Adjusted to only get 1 page of journals for now (10 journals)
def main(
    get_page, 
    get_journals,
    save
    ):
    print("Starting main")
    logging.info('Starting program')
    page_number = 1
    #while True:
        #ip = get_current_ip()
        #logging.info(f"Executing from ip: {ip}")
    extract_dts_utc = str(datetime.datetime.utcnow())
    content = get_page(page_number)
    journals = get_journals(content)
    if len(journals) == 0:
        logging.info("No more journals found")
    else:    
        for j in journals:
            j["extract_dts_utc"] = extract_dts_utc
        save(ELSEVIER_DATABASE_SCHEMA, 'journals', journals)
        #page_number += 1
        # renew tor
        #renew_tor_ip()
        #time.sleep(5)
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
    logging.info(f'Getting page {page_number} via Selenium Headless')
    url = f"https://www.elsevier.com/products/journals?query=&page={page_number}&subjectArea=physical-sciences-and-engineering%2Fcomputer-science&sortBy=relevance"
    session = requests.session()

    # Setup Chrome Options for Headless Mode
    chrome_options = Options()
    chrome_options.add_argument("--headless") # Runs without a window
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Appear less like a bot
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.get(url)

        # Wait for the first h2 to appear
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "h2")))

        # Scroll down to trigger lazy-loading and give some time to load
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2) 

        # Return the fully rendered HTML
        return driver.page_source
    
    finally:
        driver.quit() # Close the browser

    # Now that we use Selenium we have to look into how TOR will work with it, it can not be done exactly the same.
    # The following lines (commented out) are thus useless but I'm leaving them until we properly replace the functionality

    # TO Request URL with SOCKS over TOR
    #session.proxies = {}
    #session.proxies['http']='socks5h://localhost:9050'
    #session.proxies['https']='socks5h://localhost:9050'


def format_issn(raw_val: str) -> str:
    # Adds a dash to an 8-digit ISSN if it's missing.
    clean = re.sub(r'\D', '', raw_val) # Remove anything not a digit
    if len(clean) == 8:
        return f"{clean[:4]}-{clean[4:]}"
    return raw_val

def get_journals(content) -> List[Dict]:
    logging.info(f'Extracting journals from rendered HTML')
    soup = BeautifulSoup(content, 'html.parser')
    
    # Finds all <h2> tags where the id attribute starts with 'title-'
    # Example: <h2 id="title-aacn-advanced-critical-care">
    journal_headings = soup.find_all('h2', id=lambda x: x and x.startswith('title-'))
    
    result = []
    for h2 in journal_headings:
        try:
            anchor = h2.find('a')
            if not anchor: continue
            
            href = anchor.get('href', '')
            display_name = anchor.get_text(strip=True)

            # --- ISSN EXTRACTION LOGIC ---
            issn = "Needs Manual Visit" 
            
            # Pattern for ScienceDirect ISSNs (8 digits at the end)
            # Example: .../journal/23760605
            sd_match = re.search(r'journal/(\d{8})', href)
            
            if sd_match:
                issn = format_issn(sd_match.group(1))
            else:
                # We have to visit the site at a later point to find the ISSN
                logging.info(f"External site detected for {display_name}: {href}")

            result.append({
                'href': href,
                'title': display_name,
                'issn': issn
            })
        except Exception as e:
            logging.error(f"Error parsing journal entry: {e}")

    logging.info(f"Number of journals found: {len(result)}")
    return result

if __name__ == '__main__':
    if db.table_exists('elsevier', 'journals'):
        db.execute_query('TRUNCATE TABLE elsevier.journals')
    main(get_page, get_journals, saver.save)