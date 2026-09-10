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
MAX_WAIT_TIME_SECONDS = 126  
MIN_SCRAPE_YEAR = 2000       

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
            state = json.load(f)
            # Ensure all keys exist
            if "gathered_links" not in state: state["gathered_links"] = []
            if "completed_proceedings" not in state: state["completed_proceedings"] = []
            if "processed_endpoints" not in state: state["processed_endpoints"] = []
            if "extracted_endpoints" not in state: state["extracted_endpoints"] = []
            if "phase1_complete" not in state: state["phase1_complete"] = False 
            return state
            
    return {
        "gathered_links": [], 
        "completed_proceedings": [],
        "processed_endpoints": [],
        "extracted_endpoints": [],
        "phase1_complete": False
    }

def save_progress(progress_file, state):
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)

# EXTRACTION & DOWNLOADING 

def download_pdf(driver, pdf_url, filepath):
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
    soup = BeautifulSoup(html_source, 'html.parser')
    
    pdf_url = None
    pdf_tag = soup.find('a', href=re.compile(r'/action/showFmPdf'))
    if pdf_tag:
        pdf_url = urljoin(BASE_DOMAIN, pdf_tag.get('href'))

    proceeding_title = "Unknown_Proceeding"
    title_elem = soup.find('h2', class_='left-bordered-title')
    if title_elem and title_elem.find('span'):
        proceeding_title = title_elem.find('span').get_text(strip=True)

    isbn = "Unknown_ISBN"
    published_year = None
    
    meta_rows = soup.find_all('div', class_='item-meta-row')
    for row in meta_rows:
        label_elem = row.find('div', class_='item-meta-row__label')
        if not label_elem:
            continue
            
        label_text = label_elem.get_text(strip=True).lower()
        
        if 'isbn:' in label_text:
            value_elem = row.find('div', class_='item-meta-row__value')
            if value_elem:
                isbn = value_elem.get_text(strip=True)
                
        elif 'published:' in label_text:
            value_elem = row.find('div', class_='item-meta-row__value')
            if value_elem:
                date_str = value_elem.get_text(strip=True)
                year_match = re.search(r'\b(\d{4})\b', date_str)
                if year_match:
                    published_year = int(year_match.group(1))

    return pdf_url, proceeding_title, isbn, published_year

# --- PHASE 1: ENDPOINT DISCOVERY & FAST FETCHING ---

def gather_proceeding_links(driver, state, progress_file):
    """Finds all endpoints from the main page and attempts to fetch them via requests."""
    driver.get(f"{BASE_DOMAIN}/proceedings")
    
    if not wait_for_human_and_page(driver, (By.CSS_SELECTOR, "a.proceedings-browse__control"), "Proceedings Page"):
        return False

    logging.info("Scrolling down the page to load all hidden tabs...")
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    logging.info("Scrolling complete.")

    cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
    user_agent = driver.execute_script("return navigator.userAgent;")
    headers = {'User-Agent': user_agent}

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    conf_tabs = soup.find_all('a', attrs={'data-ajaxurl': True})
    logging.info(f"Found {len(conf_tabs)} total conference endpoints.")

    if len(conf_tabs) < len(state["processed_endpoints"]):
        logging.error(f"Found fewer endpoints ({len(conf_tabs)}) than already processed. Page likely didn't load fully. Aborting.")
        return False

    interrupted = False

    for idx, tab in enumerate(conf_tabs):
        ajax_url = tab.get('data-ajaxurl')
        
        if not ajax_url or ajax_url in state["processed_endpoints"]:
            continue
            
        full_url = urljoin(BASE_DOMAIN, ajax_url)
        
        try:
            res = requests.get(full_url, cookies=cookies, headers=headers, timeout=15)
            
            if res.status_code == 200:
                # Attempt to parse as JSON first
                try:
                    json_data = res.json()
                    proceedings = json_data.get("data", {}).get("proceedings", [])
                    for proc in proceedings:
                        link = proc.get("link")
                        if link:
                            full_link = urljoin(BASE_DOMAIN, link)
                            if full_link not in state["gathered_links"]:
                                state["gathered_links"].append(full_link)
                except json.JSONDecodeError:
                    # Fallback if it actually returned HTML
                    snippet_soup = BeautifulSoup(res.text, 'html.parser')
                    for a_tag in snippet_soup.find_all('a', href=re.compile(r'/doi/proceedings/')):
                        link = urljoin(BASE_DOMAIN, a_tag['href'])
                        if link not in state["gathered_links"]:
                            state["gathered_links"].append(link)
                
                state["processed_endpoints"].append(ajax_url)
                if ajax_url not in state["extracted_endpoints"]:
                    state["extracted_endpoints"].append(ajax_url)
                save_progress(progress_file, state)
                
            elif res.status_code in [403, 429]:
                logging.warning(f"HTTP {res.status_code} blocked! Stopping Phase 1 requests. Passing the baton to Phase 1.5...")
                interrupted = True
                break
            else:
                logging.warning(f"HTTP {res.status_code} for {tab.get('title', 'Unknown')}")
                
        except Exception as e:
            logging.error(f"Error fetching endpoint: {e}")
            interrupted = True
            break

        time.sleep(1)

        if (idx + 1) % 25 == 0:
            logging.info(f"  ...Checked {len(state['processed_endpoints'])}/{len(conf_tabs)} endpoints...")

    for tab in conf_tabs:
        url = tab.get('data-ajaxurl')
        if url and url not in state["processed_endpoints"]:
            state["processed_endpoints"].append(url)

    state["phase1_complete"] = True
    save_progress(progress_file, state)
    
    if interrupted:
        logging.info("Phase 1 finished early due to block. Unextracted endpoints queued for Phase 1.5.")
    else:
        logging.info("Phase 1 complete. All endpoints discovered and fast-fetched.")
        
    return True

# --- PHASE 1.5: ENDPOINT TO LINK CONVERTER VIA SELENIUM ---

def convert_endpoints_to_links(driver, state, progress_file):
    """Uses Selenium to safely visit any endpoints that Phase 1 couldn't successfully extract."""
    endpoints_to_process = [ep for ep in state["processed_endpoints"] if ep not in state["extracted_endpoints"]]
    
    if not endpoints_to_process:
        return True
        
    logging.info(f"Starting Phase 1.5: Safely converting {len(endpoints_to_process)} remaining endpoints into usable links.")

    for idx, endpoint in enumerate(endpoints_to_process):
        full_url = urljoin(BASE_DOMAIN, endpoint)
        driver.get(full_url)
        
        # Anti-bot safety loop
        while "Just a moment" in driver.title or "captcha" in driver.page_source.lower() or "challenge" in driver.title.lower():
            logging.warning("Captcha or Cloudflare block detected! Please solve it in the browser.")
            time.sleep(10)
            
        links_found = 0
        
        # Extract raw text from the browser body (bypasses Chrome's HTML wrappers)
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text
            json_data = json.loads(body_text)
            proceedings = json_data.get("data", {}).get("proceedings", [])
            
            for proc in proceedings:
                link = proc.get("link")
                if link:
                    full_link = urljoin(BASE_DOMAIN, link)
                    if full_link not in state["gathered_links"]:
                        state["gathered_links"].append(full_link)
                        links_found += 1
                        
        except (json.JSONDecodeError, Exception) as e:
            # Fallback if it's somehow not JSON
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            for a_tag in soup.find_all('a', href=re.compile(r'/doi/proceedings/')):
                link = urljoin(BASE_DOMAIN, a_tag['href'])
                if link not in state["gathered_links"]:
                    state["gathered_links"].append(link)
                    links_found += 1
                
        if links_found > 0:
            logging.info(f"  -> Found {links_found} proceedings links.")
            
        state["extracted_endpoints"].append(endpoint)
        save_progress(progress_file, state)
        
        time.sleep(1.5) 
        
        if (idx + 1) % 50 == 0:
            logging.info(f"  ...Converted {idx + 1}/{len(endpoints_to_process)} endpoints...")

    return True

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
        # Phase 1: Discover all endpoints and fast-fetch if possible
        if not state.get("phase1_complete"):
            logging.info(f"Starting/Resuming Phase 1: Gathering endpoints...")
            success = gather_proceeding_links(driver, state, progress_file)
            if not success:
                logging.info("Exiting early so you can bypass bot protection or resume later.")
                return
        else:
            logging.info("Phase 1 complete. Endpoints gathered.")

        # Phase 1.5: Convert collected endpoints into browseable links safely
        if len(state["extracted_endpoints"]) < len(state["processed_endpoints"]):
            success = convert_endpoints_to_links(driver, state, progress_file)
            if not success:
                return
        else:
            logging.info("Phase 1.5 complete. All endpoints converted to links.")
        
        # Phase 2: Iterate through links and download PDFs
        links_to_process = [link for link in state["gathered_links"] if link not in state["completed_proceedings"]]
        logging.info(f"{len(links_to_process)} total proceedings left to process.")

        for url in links_to_process:
            logging.info(f"\nNavigating to Proceeding: {url}")
            driver.get(url)
            
            if not wait_for_human_and_page(driver, (By.CSS_SELECTOR, "h2.left-bordered-title"), "Proceeding Title Header"):
                break 
            
            pdf_url, proceeding_title, isbn, published_year = extract_proceeding_metadata(driver.page_source)
            
            if published_year and published_year < MIN_SCRAPE_YEAR:
                logging.info(f"Proceeding published in {published_year} is below the {MIN_SCRAPE_YEAR} threshold. Skipping.")
                state["completed_proceedings"].append(url)
                save_progress(progress_file, state)
                continue
            elif not published_year:
                logging.warning("Could not determine published year. Attempting download anyway.")
            
            if not pdf_url:
                logging.info("No Front Matter PDF found for this proceeding.")
            else:
                safe_title = re.sub(r'[\\/*?:"<>|]', "", proceeding_title).strip()
                safe_title = (safe_title[:100] + '..') if len(safe_title) > 100 else safe_title
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