import configparser
import logging
import time
from pathlib import Path
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from database import Postgress

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_SCHEMA = "ieee"
DB_TABLE = "journals"
BASE_DOMAIN = "ieeexplore.ieee.org"

# Base URL framework - notice {page_num} string interpolation replacement field
URL_TEMPLATE = "https://ieeexplore.ieee.org/browse/periodicals/title?contentType=periodicals&rowsPerPage=100&pageNumber={page_num}&refinements=ContentType:Journals&refinements=Publisher:IEEE"

ANGULAR_WAIT_TIME_SECONDS = 5  # Time to wait for Angular to render the page content
CAPTCHA_WAIT_TIME_SECONDS = 120  # Time to wait for human to solve CAPTCHA if it appears

def read_config(path) -> dict:
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']

def get_debugging_driver():
    """Connects to running manual Chrome instance on debugging port 9222.""" 
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def parse_journals_page(html: str) -> tuple[list[dict], bool]:
    """
    Parses a single page source. 
    Returns a list of extracted journals and a boolean indicating if 'No Results Found' was hit.
    """
    soup = BeautifulSoup(html, 'html.parser')
    
    # Check for the terminal condition flag
    no_results_tag = soup.find('li', class_='article-list-item no-results')
    if no_results_tag and "no results found" in no_results_tag.get_text(strip=True).lower():
        logging.info("Terminal condition hit: 'No Results Found' element detected.")
        return [], True

    # Extract journal anchors
    # The links match /xpl/RecentIssue.jsp
    # This ignores virtual journals, whose links look different (there are currently 3 virutal journals within our search parameters (see URL_TEMPLATE))
    journal_links = soup.find_all('a', class_='text-md-md-lh')
    
    page_journals = []
    for anchor in journal_links:
        href = anchor.get('href', '')
        title = anchor.get_text(strip=True)
        
        if '/xpl/RecentIssue.jsp' in href and title:
            full_link = f"{BASE_DOMAIN}{href}" if not href.startswith('http') else href
            
            page_journals.append({
                'title': title,
                'link': full_link
            })
            
    return page_journals, False

def main():
    script_dir = Path(__file__).resolve().parent
    # Navigate up to hit the root where .env lives
    config_path = script_dir / '..' / '..' / '.env'
    config = read_config(config_path.resolve())

    db = Postgress(
        server=config['POSTGRES_SERVER'],
        database=config['POSTGRES_DB'],
        user=config['POSTGRES_USER'],
        password=config['POSTGRES_PASSWORD']
    )
    
    # Ensure the target schema exists
    if not db.schema_exists(DB_SCHEMA):
        db.create_schema(DB_SCHEMA)

    driver = get_debugging_driver()
    logging.info("Connected to manual Chrome session.")

    all_journals = []
    page = 1
    
    try:
        while True:
            target_url = URL_TEMPLATE.format(page_num=page)
            logging.info(f"Navigating to page {page}: {target_url}")
            driver.get(target_url)
            
            # IEEE's Angular rendering wrapper takes a moment to paint the results layout
            logging.info(f"Waiting {ANGULAR_WAIT_TIME_SECONDS} seconds for Angular elements to paint...")
            time.sleep(ANGULAR_WAIT_TIME_SECONDS) 
            
            # Parse the fully evaluated page DOM
            html_source = driver.page_source
            page_journals, no_results_found = parse_journals_page(html_source)
            
            if no_results_found:
                logging.info("Pagination sequence completed successfully.")
                break
                
            if not page_journals:
                # Fallback safeguard check in case a CAPTCHA locked the page load entirely
                logging.warning(f"No items found on page {page}. If a CAPTCHA is present, solve it now.")
                time.sleep(CAPTCHA_WAIT_TIME_SECONDS)
                continue
                
            logging.info(f"Extracted {len(page_journals)} journals from page {page}.")
            all_journals.extend(page_journals)
            page += 1
            
    finally:
        logging.info("Detaching driver from session.")
        driver.close()

    if not all_journals:
        logging.info("No records gathered. Database write aborted.")
        return

    # Use the native insert_into method built into your shared database.py
    logging.info(f"Saving {len(all_journals)} journals into {DB_SCHEMA}.{DB_TABLE}...")
    
    # Safe check: if table doesn't exist, create it with expected structural headings
    if not db.table_exists(DB_SCHEMA, DB_TABLE):
        db.create_table(DB_SCHEMA, DB_TABLE, {'title': 'TEXT', 'link': 'TEXT'})
        
    db.insert_into(DB_SCHEMA, DB_TABLE, all_journals)
    logging.info("Database commit completed.")

if __name__ == '__main__':
    main()