import configparser
import logging
import time
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from database import Postgress, Saver

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DOMAIN = "https://dl.acm.org"

# --- CONFIGURATION ---
MAX_JOURNALS = 5             # Limit journals per session
MAX_ISSUES_PER_JOURNAL = 3   # Limit issues per journal
MAX_WAIT_TIME_SECONDS = 126  # Max time to wait for manual captcha solve (2+4+8+16+32+64 = 126s)

def read_config(path) -> dict:
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']

def get_debugging_driver():
    """Connects to the manually opened Chrome instance on port 9222."""
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def wait_for_human_and_page(driver, expected_selector, description):
    """
    Checks if the expected element is present. If not, assumes a block/captcha
    and waits exponentially (2s, 4s, 8s...) for human intervention.
    """
    wait_time = 2
    total_waited = 0
    
    # Give the page a baseline 2 seconds to load
    time.sleep(2) 

    while total_waited <= MAX_WAIT_TIME_SECONDS:
        elements = driver.find_elements(*expected_selector)
        if elements:
            logging.info(f"Page validated: {description} found.")
            return True
            
        logging.warning(f"Blocked or loading! Waiting {wait_time}s for human to solve captcha/verify page...")
        time.sleep(wait_time)
        total_waited += wait_time
        wait_time *= 2  # Exponential backoff

    logging.error(f"Max wait time reached ({MAX_WAIT_TIME_SECONDS}s). Terminating script.")
    return False

def extract_issue_data(html_source):
    """Parses the issue page to find Research Article links and the Previous Issue link."""
    soup = BeautifulSoup(html_source, 'html.parser')
    
    # Find abstract links for Research Articles and Surveys
    abstract_links = []
    containers = soup.find_all('div', class_='issue-item-container')
    
    for container in containers:
        heading = container.find('div', class_='issue-heading')
        if heading and (heading.get_text(strip=True).lower() == 'research-article' or heading.get_text(strip=True).lower() == 'survey'):
            # Find the abstract link inside this specific container
            # Using data-title or aria-label for resilience
            abs_btn = container.find('a', attrs={'data-title': 'Abstract'}) or \
                      container.find('a', attrs={'aria-label': 'Abstract'})
            
            if abs_btn and abs_btn.get('href'):
                full_link = urljoin(BASE_DOMAIN, abs_btn.get('href'))
                abstract_links.append(full_link)

    # Find Previous Issue link
    previous_issue_url = None
    prev_btn = soup.find('a', class_='content-navigation__btn--pre')
    
    if prev_btn:
        classes = prev_btn.get('class', [])
        # Check if we hit the disabled button (earliest issue)
        if 'content-navigation__btn--disabled' not in classes and prev_btn.get('href'):
            # Some hrefs might be javascript:void(0) even without disabled class, so we check
            if 'javascript' not in prev_btn.get('href'):
                previous_issue_url = urljoin(BASE_DOMAIN, prev_btn.get('href'))
            
    return abstract_links, previous_issue_url

def main():
    # Load config using pathlib for safe relative pathing
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / '../../.env'
    config = read_config(config_path.resolve())

    # Connect to Database and fetch journals
    db = Postgress(
        server=config['POSTGRES_SERVER'],
        database=config['POSTGRES_DB'],
        user=config['POSTGRES_USER'],
        password=config['POSTGRES_PASSWORD']
    )
    
    logging.info("Fetching journal links from the database...")
    db_results = db.execute_query_result('SELECT "link" FROM "acm"."journals"')
    
    journals_to_process = []
    for row in db_results:
        link = row['link']
        # Ensure the link is a full URL for Selenium
        if not link.startswith('http'):
            link = f"https://{link}"
        journals_to_process.append(link)
        
    logging.info(f"Successfully loaded {len(journals_to_process)} journals from database.")

    # 3. Initialize Selenium Debugging Session
    driver = get_debugging_driver()
    logging.info("Connected to manual Chrome session.")
    
    try:
        journals_processed = 0
        
        for journal_url in journals_to_process:
            if journals_processed >= MAX_JOURNALS:
                logging.info("Reached MAX_JOURNALS limit. Stopping.")
                break
                
            logging.info(f"\n--- Processing Journal: {journal_url} ---")
            driver.get(journal_url)
            
            if not wait_for_human_and_page(driver, (By.CSS_SELECTOR, "a[title='Latest Issue']"), "Latest Issue Button"):
                break 
                
            latest_issue_elem = driver.find_element(By.CSS_SELECTOR, "a[title='Latest Issue']")
            current_issue_url = latest_issue_elem.get_attribute('href')
            
            issues_processed = 0
            
            while current_issue_url and issues_processed < MAX_ISSUES_PER_JOURNAL:
                logging.info(f"Navigating to Issue: {current_issue_url}")
                driver.get(current_issue_url)
                
                if not wait_for_human_and_page(driver, (By.CLASS_NAME, "issue-item-container"), "Issue Container"):
                    return 
                
                html_source = driver.page_source
                
                if "Issue-in-Progress" in html_source:
                    logging.info("Detected 'Issue-in-Progress'. Skipping extraction and moving to previous issue.")
                    _, current_issue_url = extract_issue_data(html_source)
                    continue 
                
                abstract_links, current_issue_url = extract_issue_data(html_source)
                logging.info(f"Found {len(abstract_links)} Research Article abstracts.")
                
                for abstract_link in abstract_links:
                    logging.info(f"  -> Browsing Abstract: {abstract_link}")
                    driver.get(abstract_link)
                    
                    # UPDATED: Waiting for 'core-published' instead of 'abstractSection'
                    if not wait_for_human_and_page(driver, (By.CLASS_NAME, "core-published"), "Abstract Content (core-published)"):
                        return
                    
                    time.sleep(2)
                
                issues_processed += 1
                
                if issues_processed >= MAX_ISSUES_PER_JOURNAL:
                    logging.info("Reached MAX_ISSUES_PER_JOURNAL limit for this journal.")
                elif current_issue_url is None:
                    logging.info("Reached the earliest available issue. No previous issue found.")

            journals_processed += 1
    finally:
        logging.info("Script finished.")
        driver.close()

if __name__ == '__main__':
    main()