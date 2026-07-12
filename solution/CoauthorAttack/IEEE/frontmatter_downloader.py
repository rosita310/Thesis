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

BASE_DOMAIN = "https://ieeexplore.ieee.org"

# --- CONFIGURATION ---
MIN_YEAR = 2000              # Stop scraping issues published before this year
MAX_WAIT_TIME_SECONDS = 126  # Max time to wait for manual captcha solve

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
    return {"completed_journals": [], "completed_issues": []}

def save_progress(progress_file, state):
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)

# --- EXTRACTION & DOWNLOADING ---

def download_pdf(driver, stamp_url, filepath):
    """
    Bypasses the /?denied= error by fetching the stamp wrapper, 
    extracting the raw iframe URL, and downloading the PDF directly.
    """
    for attempt in range(3):
        # Always grab fresh cookies in case the session token rotated
        cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
        user_agent = driver.execute_script("return navigator.userAgent;")
        headers = {'User-Agent': user_agent}
        
        try:
            # Fetch the wrapper page
            response = requests.get(stamp_url, cookies=cookies, headers=headers, timeout=30)
            
            if 'denied' in response.url or response.status_code != 200:
                logging.warning(f"Access denied on attempt {attempt + 1}. Retrying in 5s...")
                time.sleep(5)
                continue
                
            # Extract the actual PDF iframe URL
            soup = BeautifulSoup(response.text, 'html.parser')
            iframe = soup.find('iframe')
            if not iframe or not iframe.get('src'):
                logging.warning("Could not locate PDF iframe on stamp page. Retrying...")
                time.sleep(5)
                continue
                
            actual_pdf_url = iframe.get('src')
            if not actual_pdf_url.startswith('http'):
                actual_pdf_url = urljoin(BASE_DOMAIN, actual_pdf_url)
                
            # Download the raw PDF
            pdf_response = requests.get(actual_pdf_url, cookies=cookies, headers=headers, timeout=30)
            
            # Ensure we actually downloaded a PDF (usually > 5KB), not a hidden error page
            if pdf_response.status_code == 200 and len(pdf_response.content) > 5000:
                with open(filepath, 'wb') as f:
                    f.write(pdf_response.content)
                return True
            else:
                logging.warning(f"Downloaded file seems corrupt or empty. Retrying...")
                time.sleep(5)
                
        except Exception as e:
            logging.error(f"Network error during PDF download: {e}")
            time.sleep(5)
            
    logging.error("Failed to download PDF after 3 attempts.")
    return False

def extract_frontmatter_link(html_source, journal_title):
    """Scans the issue page for Masthead or Information PDFs."""
    soup = BeautifulSoup(html_source, 'html.parser')
    
    valid_titles = [
        journal_title.lower(),
        f"{journal_title.lower()} information",
        "masthead"
    ]
    
    # Iterate through all results on the page
    results = soup.find_all('div', class_='result-item')
    for result in results:
        title_tag = result.find('h2', class_='text-md-md-lh')
        if not title_tag:
            continue
            
        title_text = title_tag.get_text(strip=True).lower()
        
        # Check if the title matches our expected frontmatter names
        if title_text in valid_titles:
            # Find the PDF link inside this specific result block
            pdf_btn = result.find('a', class_=re.compile(r'stats_PDF'))
            if pdf_btn and pdf_btn.get('href'):
                return urljoin(BASE_DOMAIN, pdf_btn.get('href'))
                
    return None

def main():
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / '..' / '..' / '.env'
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
    db_results = db.execute_query_result('SELECT "title", "link" FROM "ieee"."journals"')
    journals_to_process = [{'title': row['title'], 'link': row['link']} for row in db_results]

    driver = get_debugging_driver()
    logging.info("Connected to manual Chrome session. Press Ctrl+C to pause safely.")
    
    try:
        for journal in journals_to_process:
            journal_title = journal['title']
            journal_url = journal['link']
            
            if journal_url in state['completed_journals']:
                continue
            
            logging.info(f"\n=====================================")
            logging.info(f"Processing Journal: {journal_title}")
            logging.info(f"=====================================")
            
            driver.get(journal_url)
            
            # Click "All Issues" tab
            if not wait_for_human_and_page(driver, (By.CSS_SELECTOR, "a.stats-jhp-AllIssues"), "All Issues Tab"):
                return
                
            all_issues_tab = driver.find_element(By.CSS_SELECTOR, "a.stats-jhp-AllIssues")
            driver.execute_script("arguments[0].click();", all_issues_tab)
            time.sleep(3) # Wait for Angular to swap the view
            
            reached_target_year = False
            
            # Iterate through Year tabs
            while not reached_target_year:
                # Find all available year links in the sidebar
                year_elements = driver.find_elements(By.CSS_SELECTOR, "a[data-analytics_identifier='past_issue_selected_year']")
                
                if not year_elements:
                    logging.warning("No year navigation found. Moving to next journal.")
                    break
                
                # Extract text and sort years descending so we always process newest first
                years_available = []
                for elem in year_elements:
                    try:
                        years_available.append(int(elem.text.strip()))
                    except:
                        pass
                
                years_available.sort(reverse=True)
                
                for year in years_available:
                    if year < MIN_YEAR:
                        logging.info(f"Reached year {year}, which is below cutoff {MIN_YEAR}. Moving to next journal.")
                        reached_target_year = True
                        break
                        
                    logging.info(f"--- Loading Issues for Year: {year} ---")
                    
                    # Locate the specific year link and click it via JS
                    year_link = driver.find_element(By.XPATH, f"//a[@data-analytics_identifier='past_issue_selected_year' and text()='{year}']")
                    driver.execute_script("arguments[0].click();", year_link)
                    time.sleep(4) # Wait for Angular to fetch and render the issues for this year
                    
                    # Extract all issues for this year
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    issue_links = []
                    
                    # IEEE has two layouts (that we know of): .issue-details and .issue-list-item
                    issue_anchors = soup.find_all('a', href=re.compile(r'/xpl/tocresult\.jsp'))
                    for anchor in issue_anchors:
                        href = anchor.get('href')
                        issue_text = anchor.get_text(strip=True)
                        if href and "Issue" in issue_text:
                            full_url = urljoin(BASE_DOMAIN, href)
                            if full_url not in issue_links:
                                issue_links.append((issue_text, full_url))
                                
                    # Process each issue
                    for issue_name, issue_url in issue_links:
                        if issue_url in state['completed_issues']:
                            continue
                            
                        logging.info(f"Navigating to {issue_name} ({year})")
                        driver.get(issue_url)
                        
                        if not wait_for_human_and_page(driver, (By.CLASS_NAME, "result-item"), "Issue Contents"):
                            return
                            
                        stamp_url = extract_frontmatter_link(driver.page_source, journal_title)
                        
                        if not stamp_url:
                            logging.info(f"No Masthead/Information PDF found for {issue_name}.")
                        else:
                            safe_jtitle = re.sub(r'[\\/*?:"<>|]', "", journal_title).strip()
                            safe_issue = re.sub(r'[\\/*?:"<>|]', "", issue_name).replace(" ", "_")
                            pdf_filename = f"{safe_jtitle}_{year}_{safe_issue}.pdf"
                            pdf_filepath = data_dir / pdf_filename
                            
                            if not pdf_filepath.exists():
                                logging.info(f"  -> Downloading Front Matter: {pdf_filename}")
                                success = download_pdf(driver, stamp_url, pdf_filepath)
                                if success:
                                    time.sleep(2)
                            else:
                                logging.info(f"  -> File already exists: {pdf_filename}. Skipping.")
                                
                        # Save progress after every issue
                        state['completed_issues'].append(issue_url)
                        save_progress(progress_file, state)
                        
                # If we processed all available year tabs and none were below MIN_YEAR, 
                # we need to check if there is a previous decade tab to click.
                if not reached_target_year:
                    # Look for decade tabs (e.g., "2010s")
                    decade_elements = driver.find_elements(By.XPATH, "//li/a[contains(text(), 's') and string-length(text()) = 5]")
                    clicked_decade = False
                    
                    for decade_elem in decade_elements:
                        decade_text = decade_elem.text.strip()
                        try:
                            decade_year = int(decade_text[:4])
                            # Only click a decade if it contains years >= MIN_YEAR
                            if decade_year + 9 >= MIN_YEAR:
                                driver.execute_script("arguments[0].click();", decade_elem)
                                time.sleep(3)
                                clicked_decade = True
                                break
                        except:
                            pass
                            
                    if not clicked_decade:
                        reached_target_year = True # No more valid decades to explore

            # Mark journal as fully complete
            state['completed_journals'].append(journal_url)
            save_progress(progress_file, state)
            
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