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
RETRY_ATTEMPTS = 10           # Number of attempts to download a PDF before giving up
RETRY_WAIT_TIME = 5          # Wait time between retry attempts for PDF download
MAX_CONSECUTIVE_MISSING = 5  # Skip journal if this many consecutive issues lack a frontmatter PDF

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
    for attempt in range(RETRY_ATTEMPTS):
        cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
        user_agent = driver.execute_script("return navigator.userAgent;")
        headers = {'User-Agent': user_agent}
        
        try:
            response = requests.get(stamp_url, cookies=cookies, headers=headers, timeout=30)
            
            if 'denied' in response.url or response.status_code != 200:
                logging.warning(f"Access denied on attempt {attempt + 1}. Retrying in {RETRY_WAIT_TIME}s...")
                time.sleep(RETRY_WAIT_TIME)
                continue
                
            soup = BeautifulSoup(response.text, 'html.parser')
            iframe = soup.find('iframe')
            if not iframe or not iframe.get('src'):
                logging.warning("Could not locate PDF iframe on stamp page. Retrying...")
                time.sleep(RETRY_WAIT_TIME)
                continue
                
            actual_pdf_url = iframe.get('src')
            if not actual_pdf_url.startswith('http'):
                actual_pdf_url = urljoin(BASE_DOMAIN, actual_pdf_url)
                
            pdf_response = requests.get(actual_pdf_url, cookies=cookies, headers=headers, timeout=30)
            
            if pdf_response.status_code == 200 and len(pdf_response.content) > 5000:
                with open(filepath, 'wb') as f:
                    f.write(pdf_response.content)
                return True
            else:
                logging.warning(f"Downloaded file seems corrupt or empty. Retrying...")
                time.sleep(RETRY_WAIT_TIME)
                
        except Exception as e:
            logging.error(f"Network error during PDF download: {e}")
            time.sleep(RETRY_WAIT_TIME)
            
    logging.error(f"Failed to download PDF after {RETRY_ATTEMPTS} attempts.")
    return False

def extract_frontmatter_link(html_source, journal_title):
    """
    Scans the IEEE issue page DOM for Masthead or Information PDFs.
    Uses expanded keywords and targets the stamp URL structure rather than CSS classes.
    """
    soup = BeautifulSoup(html_source, 'html.parser')
    
    escaped_journal_title = re.escape(journal_title.strip())
    
    # Expanded keyword search to catch common IEEE front matter naming conventions
    keywords = rf"({escaped_journal_title}|masthead|publication information|frontmatter)"
    target_pattern = re.compile(keywords, re.IGNORECASE)
    
    # IEEE sometimes uses 'result-item' classes or custom 'xpl' tags
    results = soup.find_all(lambda tag: tag.name == 'xpl-issue-results-items' or 
                                        (tag.has_attr('class') and 'result-item' in tag.get('class')))
    
    for result in results:
        # Search all h2 (and h3 just in case) tags inside this item
        header_tags = result.find_all(['h2', 'h3'])
        matched = False
        
        for header in header_tags:
            title_text = header.get_text(separator=" ", strip=True)
            
            if target_pattern.search(title_text):
                matched = True
                break
                
        if matched:
            # Find the link by checking the href for the stamp URL
            pdf_btn = result.find('a', href=re.compile(r'/stamp/stamp\.jsp', re.IGNORECASE))
            if pdf_btn and pdf_btn.get('href'):
                return urljoin("https://ieeexplore.ieee.org", pdf_btn.get('href'))
                
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

            
            journal_url = journal_url.strip() if journal_url else ""
            if journal_url and not journal_url.startswith(("http://", "https://")):
                journal_url = f"https://{journal_url}"

            
            if journal_url in state['completed_journals']:
                continue
            
            logging.info(f"\n=====================================")
            logging.info(f"Processing Journal: {journal_title}")
            logging.info(f"=====================================")

            driver.get(journal_url)
            
            tab_selector = (By.CSS_SELECTOR, "a.stats-jhp-AllIssues, a.stats-jhp-AllVolumes")
            
            if not wait_for_human_and_page(driver, tab_selector, "All Issues / Volumes Tab"):
                return
                
            all_issues_tab = driver.find_element(By.CSS_SELECTOR, "a.stats-jhp-AllIssues, a.stats-jhp-AllVolumes")
            
            if "stats-jhp-AllVolumes" in all_issues_tab.get_attribute("class"):
                logging.info(f"Skipping {journal_title}: Detected 'All Volumes' (Continuous publication).")
                state['completed_journals'].append(journal_url)
                save_progress(progress_file, state)
                continue

            driver.execute_script("arguments[0].click();", all_issues_tab)
            time.sleep(3)
            
            reached_target_year = False
            consecutive_missing_pdfs = 0
            
            # Store the main window handle to return to after processing each issue in a new tab
            main_window = driver.current_window_handle
            
            while not reached_target_year:
                year_elements = driver.find_elements(By.CSS_SELECTOR, "a[data-analytics_identifier='past_issue_selected_year']")
                
                if not year_elements:
                    logging.warning("No year navigation found. Moving to next journal.")
                    break
                
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
                    
                    year_link = driver.find_element(By.XPATH, f"//a[@data-analytics_identifier='past_issue_selected_year' and text()='{year}']")
                    driver.execute_script("arguments[0].click();", year_link)
                    time.sleep(4)
                    
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    issue_links = []
                    
                    issue_anchors = soup.find_all('a', href=re.compile(r'/xpl/tocresult\.jsp'))
                    for anchor in issue_anchors:
                        href = anchor.get('href')
                        issue_text = anchor.get_text(strip=True)
                        if href and "Issue" in issue_text:
                            full_url = urljoin(BASE_DOMAIN, href)
                            if full_url not in [url for _, url in issue_links]:
                                issue_links.append((issue_text, full_url))
                                
                    for issue_name, issue_url in issue_links:
                        if issue_url in state['completed_issues']:
                            continue
                            
                        logging.info(f"Navigating to {issue_name} ({year})")
                        
                        # Open the issue in a NEW TAB to preserve the "All Issues" Angular state
                        driver.execute_script("window.open(arguments[0], '_blank');", issue_url)
                        driver.switch_to.window(driver.window_handles[-1])
                        
                        if not wait_for_human_and_page(driver, (By.CLASS_NAME, "result-item"), "Issue Contents"):
                            driver.close()
                            driver.switch_to.window(main_window)
                            return

                        # Give angular time to inject the href variables into the <a> tags
                        time.sleep(1)
                        
                        stamp_url = extract_frontmatter_link(driver.page_source, journal_title)
                        
                        if not stamp_url:
                            logging.info(f"No Masthead/Information PDF found for {issue_name}.")
                            consecutive_missing_pdfs += 1
                        else:
                            consecutive_missing_pdfs = 0
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
                                
                        state['completed_issues'].append(issue_url)
                        save_progress(progress_file, state)
                        
                        # Close the issue tab and return to the main tab
                        driver.close()
                        driver.switch_to.window(main_window)

                        # Check consecutive missing limit
                        if consecutive_missing_pdfs >= MAX_CONSECUTIVE_MISSING:
                            logging.warning(f"Skipping rest of journal: {MAX_CONSECUTIVE_MISSING} consecutive issues missing PDFs.")
                            reached_target_year = True
                            break
                    
                    if reached_target_year:
                        break # Break the year loop if cutoff or missing limit is hit
                        
                if not reached_target_year:
                    decade_elements = driver.find_elements(By.XPATH, "//li/a[contains(text(), 's') and string-length(text()) = 5]")
                    clicked_decade = False
                    
                    for decade_elem in decade_elements:
                        decade_text = decade_elem.text.strip()
                        try:
                            decade_year = int(decade_text[:4])
                            if decade_year + 9 >= MIN_YEAR:
                                driver.execute_script("arguments[0].click();", decade_elem)
                                time.sleep(3)
                                clicked_decade = True
                                break
                        except:
                            pass
                            
                    if not clicked_decade:
                        reached_target_year = True

            state['completed_journals'].append(journal_url)
            save_progress(progress_file, state)
            
    except KeyboardInterrupt:
        logging.warning("\n[!] Script manually interrupted (Ctrl+C). Exiting cleanly...")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        logging.info("Detaching from browser.")
        try:
            # Safely close any remaining tabs
            while len(driver.window_handles) > 1:
                driver.switch_to.window(driver.window_handles[-1])
                driver.close()
            driver.switch_to.window(driver.window_handles[0])
            driver.close()
        except:
            pass

if __name__ == '__main__':
    main()