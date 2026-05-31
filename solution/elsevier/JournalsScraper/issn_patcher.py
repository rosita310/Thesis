import sys
import os
import time

# --- Path Logic ---
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

import re
import requests
import logging
from program import Postgress, Saver
from program import config 

# --- Selenium Imports ---
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

# Regex to find XXXX-XXXX or XXXX XXXX (where X can be a digit or 'X')
ISSN_PATTERN = r'(\d{4})[-\s](\d{3}[\dX])'

def find_issn_in_text(text):
    anchor_pattern = r'ISSN(?:=|\s|:\s*)(\d{4}[-\s]\d{3}[\dX])'
    
    match = re.search(anchor_pattern, text, re.IGNORECASE)
    
    if match:
        clean_issn = match.group(1).replace(' ', '-')
        return clean_issn.upper()
    
    return None

def update_missing_issns():
    # Database Setup
    db = Postgress(
        server=config['POSTGRES_SERVER'], 
        database=config['POSTGRES_DB'],
        user=config['POSTGRES_USER'],
        password=config['POSTGRES_PASSWORD']
    )
    
    query = "SELECT href, title FROM elsevier.journals WHERE issn = 'Needs Manual Visit'"
    journals_to_fix = db.execute_query_result(query)
    
    if not journals_to_fix:
        logging.info("No journals found that need an ISSN update.")
        return

    logging.info(f"Found {len(journals_to_fix)} journals to update via Selenium.")

    # Selenium Setup 
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        for journal in journals_to_fix:
            url = journal['href']
            title = journal['title']
            
            logging.info(f"Visiting {title}...")
            
            try:
                driver.get(url)
                # Give the page 5 seconds to load dynamic content
                time.sleep(5) 
                
                page_source = driver.page_source
                found_issn = find_issn_in_text(page_source)
                
                if found_issn:
                    # Escape single quotes in titles if necessary, but href is safer for WHERE
                    update_sql = f"""
                        UPDATE elsevier.journals 
                        SET issn = '{found_issn}' 
                        WHERE href = '{url}'
                    """
                    db.execute_query(update_sql)
                    logging.info(f"Successfully updated {title}: {found_issn}")
                else:
                    logging.warning(f"ISSN pattern not found on page for {title}")
                    
            except Exception as e:
                logging.error(f"Failed to process {title}: {e}")
                
    finally:
        driver.quit() 
        logging.info("Selenium patcher finished.")

if __name__ == "__main__":
    update_missing_issns()