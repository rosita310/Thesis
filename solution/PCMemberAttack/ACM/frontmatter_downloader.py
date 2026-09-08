import logging
import time
import json
import re
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DOMAIN = "https://dl.acm.org"

# --- CONFIGURATION ---
MAX_WAIT_TIME_SECONDS = 126  # Max time to wait for manual captcha solve
MIN_SCRAPE_YEAR = 2000       # Earliest year to scrape

def get_debugging_driver():
    """Connects to an existing Chrome instance launched with --remote-debugging-port=9222"""
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
    return {"gathered_links": [], "completed_proceedings": []}

def save_progress(progress_file, state):
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)

# --- EXTRACTION & DOWNLOADING ---

def download_pdf(driver, pdf_url, filepath):
    """Uses the Selenium session cookies to cleanly download the PDF via requests."""
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

def extract_proceeding_metadata(html_source):
    """Finds the front matter link, proceeding title, ISBN, and published year."""
    soup = BeautifulSoup(html_source, 'html.parser')
    
    # 1. Find Front Matter PDF Link
    pdf_url = None
    pdf_tag = soup.find('a', href=re.compile(r'/action/showFmPdf'))
    if pdf_tag:
        pdf_url = urljoin(BASE_DOMAIN, pdf_tag.get('href'))

    # 2. Find Proceeding Name from left-bordered-title
    proceeding_title = "Unknown_Proceeding"
    title_elem = soup.find('h2', class_='left-bordered-title')
    if title_elem and title_elem.find('span'):
        proceeding_title = title_elem.find('span').get_text(strip=True)

    # 3. Extract ISBN and Published Year from metadata rows
    isbn = "Unknown_ISBN"
    published_year = None
    
    meta_rows = soup.find_all('div', class_='item-meta-row')
    for row in meta_rows:
        label_elem = row.find('div', class_='item-meta-row__label')
        if not label_elem:
            continue
            
        label_text = label_elem.get_text(strip=True).lower()
        
        # Look for ISBN
        if 'isbn:' in label_text:
            value_elem = row.find('div', class_='item-meta-row__value')
            if value_elem:
                isbn = value_elem.get_text(strip=True)
                
        # Look for Published Date
        elif 'published:' in label_text:
            value_elem = row.find('div', class_='item-meta-row__value')
            if value_elem:
                date_str = value_elem.get_text(strip=True)
                # Regex to find a 4-digit year in the date string (e.g., "12 October 2025")
                year_match = re.search(r'\b(\d{4})\b', date_str)
                if year_match:
                    published_year = int(year_match.group(1))

    return pdf_url, proceeding_title, isbn, published_year

def gather_proceeding_links(driver):
    """Navigates the proceedings page, opens all tabs, and extracts all links."""
    driver.get(f"{BASE_DOMAIN}/proceedings")
    
    if not wait_for_human_and_page(driver, (By.CSS_SELECTOR, "a.proceedings-browse__control"), "Alphanumeric Tabs"):
        return []

    main_tabs = driver.find_elements(By.CSS_SELECTOR, "a.proceedings-browse__control")
    logging.info(f"Opening {len(main_tabs)} main alphanumeric tabs...")
    for tab in main_tabs:
        if tab.get_attribute("aria-expanded") == "false":
            driver.execute_script("arguments[0].click();", tab)
            time.sleep(0.5)

    conference_tabs = driver.find_elements(By.CSS_SELECTOR, "a.accordion-tabbed__control.loadAjax")
    logging.info(f"Opening {len(conference_tabs)} conference tabs (this will take a moment)...")
    
    for idx, conf_tab in enumerate(conference_tabs):
        if conf_tab.get_attribute("aria-expanded") == "false":
            driver.execute_script("arguments[0].click();", conf_tab)
            time.sleep(0.25)
            
        if (idx + 1) % 50 == 0:
            logging.info(f"  ...Opened {idx + 1}/{len(conference_tabs)} tabs...")

    logging.info("Waiting for final AJAX elements to render...")
    time.sleep(10)

    proceeding_elements = driver.find_elements(By.CSS_SELECTOR, "ul.dotted-list a")
    links = [elem.get_attribute("href") for elem in proceeding_elements if elem.get_attribute("href")]
    
    links = [link for link in links if "/doi/proceedings/" in link]
    links = list(dict.fromkeys(links))
    
    logging.info(f"Successfully gathered {len(links)} proceeding URLs.")
    return links

# --- MAIN EXECUTION ---

def main():
    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir / 'data'
    data_dir.mkdir(exist_ok=True)
    
    progress_file = data_dir / 'proceedings_progress.json'
    state = load_progress(progress_file)

    driver = get_debugging_driver()
    logging.info("Connected to manual Chrome session. Press Ctrl+C to pause safely.")
    
    try:
        # Phase 1: Gather links
        if not state.get("gathered_links"):
            logging.info("No gathered links found in state. Starting Phase 1: Gathering links...")
            gathered_links = gather_proceeding_links(driver)
            if not gathered_links:
                logging.error("Failed to gather links. Exiting.")
                return
            state["gathered_links"] = gathered_links
            save_progress(progress_file, state)
        else:
            logging.info(f"Loaded {len(state['gathered_links'])} gathered links from progress file.")

        # Phase 2: Iterate through links
        links_to_process = [link for link in state["gathered_links"] if link not in state["completed_proceedings"]]
        logging.info(f"{len(links_to_process)} proceedings left to process.")

        for url in links_to_process:
            logging.info(f"\nNavigating to Proceeding: {url}")
            driver.get(url)
            
            if not wait_for_human_and_page(driver, (By.CSS_SELECTOR, "h2.left-bordered-title"), "Proceeding Title Header"):
                break 
            
            pdf_url, proceeding_title, isbn, published_year = extract_proceeding_metadata(driver.page_source)
            
            # Check the year threshold
            if published_year and published_year < MIN_SCRAPE_YEAR:
                logging.info(f"Proceeding published in {published_year} is below the {MIN_SCRAPE_YEAR} threshold. Skipping download.")
                
                # Mark as complete so we don't re-check it on next run
                state["completed_proceedings"].append(url)
                save_progress(progress_file, state)
                continue
            elif not published_year:
                logging.warning("Could not determine published year. Attempting download anyway.")
            
            if not pdf_url:
                logging.info("No Front Matter PDF found for this proceeding.")
            else:
                # Generate safe filename with Title and ISBN
                safe_title = re.sub(r'[\\/*?:"<>|]', "", proceeding_title).strip()
                safe_title = (safe_title[:100] + '..') if len(safe_title) > 100 else safe_title # Increased safety margin for OS filename limits
                safe_isbn = re.sub(r'[\\/*?:"<>|]', "", isbn).strip()
                
                pdf_filename = f"{safe_title}_{safe_isbn}.pdf"
                pdf_filepath = data_dir / pdf_filename
                
                if not pdf_filepath.exists():
                    logging.info(f"  -> Downloading Front Matter: {pdf_filename}")
                    success = download_pdf(driver, pdf_url, pdf_filepath)
                    if success:
                        time.sleep(2)
                else:
                    logging.info(f"  -> File already exists: {pdf_filename}. Skipping download.")
            
            state["completed_proceedings"].append(url)
            save_progress(progress_file, state)

    except KeyboardInterrupt:
        logging.warning("\n[!] Script manually interrupted (Ctrl+C). Exiting cleanly...")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        logging.info("Detaching from browser.")

if __name__ == '__main__':
    main()