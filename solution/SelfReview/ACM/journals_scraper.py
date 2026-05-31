import configparser
import logging
import time
from pathlib import Path

from bs4 import BeautifulSoup
from database import Postgress, Saver

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://dl.acm.org/journals"
DB_SCHEMA = "acm"
DB_TABLE = "journals"

def read_config(path) -> dict:
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']

def get_journals() -> str:
    url = BASE_URL
    logging.info("Connecting to the manually opened Chrome instance...")

    chrome_options = Options()
    # Tell Selenium to hijack the existing browser session on port 9222
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

    # Connect using your existing driver setup
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        logging.info(f"Current page title on manual browser: {driver.title}")
        logging.info("Executing scroll routines on the manual browser...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        return driver.page_source
    
    finally:
        driver.close()

def parse_journals(html: str) -> list[dict]:
    logging.info("Parsing journals from HTML content")
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find all anchor tags that act as the journal covers
    journal_links = soup.find_all('a', class_='browse-item-cover')
    
    result = []
    for anchor in journal_links:
        try:
            # Extract the raw attribute values
            href_suffix = anchor.get('href', '')
            title = anchor.get('title', '').strip()
            
            # Skip if vital information is missing
            if not href_suffix or not title:
                continue
                
            # Construct the absolute URL
            # Ensuring it handles potential leading slashes gracefully
            if not href_suffix.startswith('/'):
                href_suffix = '/' + href_suffix
            full_link = f"dl.acm.org{href_suffix}"
            
            result.append({
                'title': title,
                'link': full_link
            })
            
        except Exception as e:
            logging.error(f"Error parsing individual journal anchor: {e}")
            
    logging.info(f"Successfully parsed {len(result)} journals.")
    return result


def main():
    
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / '..' / '..' / 'config.env'
    absolute_config_path = config_path.resolve()
    config = read_config(absolute_config_path)

    db = Postgress(
        server=config['POSTGRES_SERVER'],
        database=config['POSTGRES_DB'],
        user=config['POSTGRES_USER'],
        password=config['POSTGRES_PASSWORD']
    )
    saver = Saver(db)

    total = 0

    content = get_journals()
    journals = parse_journals(content)

    if not journals:
        logging.info("No journals found.")
    
    saver.save(DB_SCHEMA, DB_TABLE, journals)
    total += len(journals)
    logging.info(f"Done. {total} journals saved to {DB_SCHEMA}.{DB_TABLE}.")


if __name__ == '__main__':
    main()