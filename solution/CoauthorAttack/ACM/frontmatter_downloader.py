import configparser
import logging
import time
import json
import re
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from database import Postgress

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DOMAIN = "https://dl.acm.org"

# --- CONFIGURATION ---
MAX_JOURNALS = 1000          # Set high to process the whole DB
MAX_WAIT_TIME_SECONDS = 126  # Max time to wait for manual captcha solve
MAX_MISSING_PDFS = 5         # Consecutive missing PDFs before giving up
MIN_SCRAPE_YEAR = 2000       # Earliest year to scrape

def read_config(path) -> dict:
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']

def get_debugging_driver():
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def wait_for_human_and_page(driver, expected_selector, description):
    """Checks if the page loaded. Exponential wait for CAPTCHA."""
    wait_time = 2
    total_waited = 0
    time.sleep(2) 

    while total_waited <= MAX_WAIT_TIME_SECONDS:
        elements = driver.find_elements(*expected_selector)
        if elements:
            logging.info(f"Page validated: {description} found.")
            return True
            
        logging.warning(f"Blocked or loading! Waiting {wait_time}s for human to verify page...")
        time.sleep(wait_time)
        total_waited += wait_time
        wait_time *= 2

    logging.error(f"Max wait time reached ({MAX_WAIT_TIME_SECONDS}s). Terminating script.")
    return False

# --- STATE MANAGEMENT ---

def load_progress(progress_file):
    if progress_file.exists():
        with open(progress_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"completed_journals": [], "in_progress": {}}

def save_progress(progress_file, state):
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)

# --- EXTRACTION & DOWNLOADING ---

def download_pdf(driver, pdf_url, filepath):
    """Uses the Selenium session cookies to cleanly download the PDF via requests."""
    # Extract cookies and user-agent to bypass basic firewall checks
    cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
    user_agent = driver.execute_script("return navigator.userAgent;")
    headers = {'User-Agent': user_agent}
    
    try:
        response = requests.get(pdf_url, cookies=cookies, headers=headers, timeout=30)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                f.write(response.content)
            return True
        else:
            logging.error(f"Failed to download PDF. HTTP Status: {response.status_code}")
            return False
    except Exception as e:
        logging.error(f"Network error during PDF download: {e}")
        return False

def extract_issue_metadata(html_source):
    soup = BeautifulSoup(html_source, 'html.parser')
    
    # Find Front Matter PDF Link
    pdf_url = None
    pdf_tag = soup.find('a', href=re.compile(r'/action/showFmPdf'))
    if pdf_tag:
        pdf_url = urljoin(BASE_DOMAIN, pdf_tag.get('href'))

    # Find Volume/Issue Name
    vol_issue_text = "Unknown_Issue"
    vol_issue_elem = soup.find('h2', class_='left-bordered-title')
    if vol_issue_elem:
        spans = vol_issue_elem.find_all('span')
        # Grab the first two spans (e.g., 'Volume 1' and ', Issue 2')
        parts = [s.get_text(strip=True).replace(',', '') for s in spans[:2]]
        vol_issue_text = "_".join(parts).replace(" ", "_")

    # Find Previous Issue Link
    previous_issue_url = None
    prev_btn = soup.find('a', class_='content-navigation__btn--pre')
    if prev_btn:
        classes = prev_btn.get('class', [])
        if 'content-navigation__btn--disabled' not in classes and prev_btn.get('href'):
            if 'javascript' not in prev_btn.get('href'):
                previous_issue_url = urljoin(BASE_DOMAIN, prev_btn.get('href'))
                
    return pdf_url, vol_issue_text, previous_issue_url


def main():
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / '../../.env'
    config = read_config(config_path.resolve())

    data_dir = script_dir / 'data'
    data_dir.mkdir(exist_ok=True)
    
    progress_file = data_dir / 'progress.json'
    state = load_progress(progress_file)

    db = Postgress(
        server=config['POSTGRES_SERVER'], database=config['POSTGRES_DB'],
        user=config['POSTGRES_USER'], password=config['POSTGRES_PASSWORD']
    )
    
    logging.info("Fetching journal titles and links from the database...")
    db_results = db.execute_query_result('SELECT "title", "link" FROM "acm"."journals"')
    
    journals_to_process = [{'title': row['title'], 'link': f"https://{row['link']}" if not row['link'].startswith('http') else row['link']} for row in db_results]

    driver = get_debugging_driver()
    logging.info("Connected to manual Chrome session. Press Ctrl+C to pause safely.")
    
    try:
        journals_processed = 0
        
        for journal in journals_to_process:
            if journals_processed >= MAX_JOURNALS:
                break
                
            journal_title = journal['title']
            journal_url = journal['link']
            
            # Check if journal is fully completed
            if journal_url in state['completed_journals']:
                continue
            
            logging.info(f"\n=====================================")
            logging.info(f"Processing Journal: {journal_title}")
            logging.info(f"=====================================")
            
            # Resume from an 'in_progress' state if available
            current_issue_url = state['in_progress'].get(journal_url)
            
            if not current_issue_url:
                driver.get(journal_url)
                if not wait_for_human_and_page(driver, (By.CSS_SELECTOR, "a[title='Latest Issue']"), "Latest Issue Button"):
                    break 
                    
                latest_issue_elem = driver.find_element(By.CSS_SELECTOR, "a[title='Latest Issue']")
                current_issue_url = latest_issue_elem.get_attribute('href')
            
            # Initialize our missing PDF counter for this journal
            consecutive_missing_pdfs = 0

            # Navigate backwards through the issues
            while current_issue_url:
                logging.info(f"Navigating to Issue: {current_issue_url}")

                # Attempt to extract a 4-digit year from the URL
                year_match = re.search(r'/toc/[^/]+/(\d{4})/', current_issue_url)
                
                if year_match:
                    issue_year = int(year_match.group(1))
                    if issue_year < MIN_SCRAPE_YEAR:
                        logging.info(f"Issue year {issue_year} is below the {MIN_SCRAPE_YEAR} threshold. Moving to next Journal.")
                        break # Exit the while loop to move to the next journal
                elif 'current' in current_issue_url:
                    logging.info("Current issue detected. Assuming year is within bounds.")
                else:
                    logging.warning(f"Could not determine year from URL ({current_issue_url}). Continuing scraping the journal.")

                driver.get(current_issue_url)
                
                # Wait for the Volume/Issue Header to confirm the page loaded
                if not wait_for_human_and_page(driver, (By.CLASS_NAME, "colored-block__title"), "Issue Header"):
                    return 
                
                pdf_url, vol_issue_text, next_issue_url = extract_issue_metadata(driver.page_source)
                
                if not pdf_url:
                    consecutive_missing_pdfs += 1
                    logging.info(f"No Front Matter PDF found. (Missing {consecutive_missing_pdfs}/{MAX_MISSING_PDFS} in a row)")
                    
                    if consecutive_missing_pdfs >= MAX_MISSING_PDFS:
                        logging.info("Missing PDF limit reached. Assuming no more exist. Moving to next Journal.")
                        break # Exit the while loop to move to the next journal
                else:
                    consecutive_missing_pdfs = 0 # Reset counter when a PDF is found
                    
                    # Generate safe filename and check if we already downloaded it
                    safe_jtitle = re.sub(r'[\\/*?:"<>|]', "", journal_title).strip()
                    safe_vol = re.sub(r'[\\/*?:"<>|]', "", vol_issue_text).strip()
                    pdf_filename = f"{safe_jtitle}_{safe_vol}.pdf"
                    pdf_filepath = data_dir / pdf_filename
                    
                    if not pdf_filepath.exists():
                        logging.info(f"  -> Downloading Front Matter: {pdf_filename}")
                        success = download_pdf(driver, pdf_url, pdf_filepath)
                        if success:
                            time.sleep(2) # Friendly delay after download
                    else:
                        logging.info(f"  -> File already exists: {pdf_filename}. Skipping download.")
                
                # Update loop and save incremental progress
                current_issue_url = next_issue_url
                
                if current_issue_url:
                    state['in_progress'][journal_url] = current_issue_url
                else:
                    logging.info("Reached the earliest available issue.")
                    if journal_url in state['in_progress']:
                        del state['in_progress'][journal_url]
                        
                save_progress(progress_file, state)

            # Mark journal as fully complete
            state['completed_journals'].append(journal_url)
            if journal_url in state['in_progress']:
                del state['in_progress'][journal_url]
            save_progress(progress_file, state)
            journals_processed += 1
            
    except KeyboardInterrupt:
        logging.warning("\n[!] Script manually interrupted (Ctrl+C). Exiting cleanly...")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        logging.info("Detaching from browser.")
        try:
            driver.close()
        except:
            pass

if __name__ == '__main__':
    main()