import configparser
import logging
import time
import json
import re
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
MAX_JOURNALS = 5             # Limit journals per session
MIN_PUBLICATION_YEAR = 2018  # The scraper will stop when it hits an article published before this year
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
            
        logging.warning(f"Blocked or loading! Waiting {wait_time}s for human to solve captcha/verify page...")
        time.sleep(wait_time)
        total_waited += wait_time
        wait_time *= 2

    logging.error(f"Max wait time reached ({MAX_WAIT_TIME_SECONDS}s). Terminating script.")
    return False

# --- STATE MANAGEMENT FUNCTIONS ---

def load_progress(progress_file):
    """Loads the progress tracking file."""
    if progress_file.exists():
        with open(progress_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"completed_journals": [], "completed_issues": []}

def save_progress(progress_file, state):
    """Saves the current progress state."""
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)

def append_articles_to_json(filepath, new_articles):
    """Appends new articles to the journal's JSON file safely."""
    if not new_articles:
        return
        
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = []
        
    data.extend(new_articles)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def extract_year(date_string):
    """Extracts a 4-digit year from a date string using regex."""
    if not date_string:
        return None
    match = re.search(r'\d{4}', date_string)
    return int(match.group()) if match else None

# --- EXTRACTION FUNCTIONS (Unchanged from previous iteration) ---
def extract_issue_data(html_source):
    soup = BeautifulSoup(html_source, 'html.parser')
    abstract_links = []
    containers = soup.find_all('div', class_='issue-item-container')
    for container in containers:
        heading = container.find('div', class_='issue-heading')
        if heading and (not heading.get_text(strip=True).lower() == 'editorial'):
            abs_btn = container.find('a', attrs={'data-title': 'Abstract'}) or container.find('a', attrs={'aria-label': 'Abstract'})
            if abs_btn and abs_btn.get('href'):
                abstract_links.append(urljoin(BASE_DOMAIN, abs_btn.get('href')))
    previous_issue_url = None
    prev_btn = soup.find('a', class_='content-navigation__btn--pre')
    if prev_btn:
        classes = prev_btn.get('class', [])
        if 'content-navigation__btn--disabled' not in classes and prev_btn.get('href'):
            if 'javascript' not in prev_btn.get('href'):
                previous_issue_url = urljoin(BASE_DOMAIN, prev_btn.get('href'))
    return abstract_links, previous_issue_url

def extract_article_data(html_source, article_url, journal_title):
    soup = BeautifulSoup(html_source, 'html.parser')
    doi = article_url.split('/doi/abs/')[-1] if '/doi/abs/' in article_url else article_url.split('/doi/')[-1] if '/doi/' in article_url else ""
    title_tag = soup.find('h1', property='name')
    title = title_tag.get_text(strip=True) if title_tag else ""
    type_tag = soup.find('div', class_='meta-panel__type')
    article_type = type_tag.get_text(strip=True) if type_tag else ""
    page_start, page_end = soup.find('span', property='pageStart'), soup.find('span', property='pageEnd')
    first_page, last_page = page_start.get_text(strip=True) if page_start else "", page_end.get_text(strip=True) if page_end else ""
    
    dates = {'received': None, 'accepted': None, 'published': None}
    for label in soup.find_all('b', class_='core-label'):
        label_text = label.get_text(strip=True).replace(':', '').lower()
        if label_text in dates:
            dates[label_text] = label.next_sibling.strip() if label.next_sibling else ""
            
    authors = []
    for adiv in soup.find_all('div', property='author', typeof='Person'):
        gn, fn = adiv.find('span', property='givenName'), adiv.find('span', property='familyName')
        affil_tag = adiv.find('div', property='affiliation')
        orcid_tag = adiv.find('div', class_='core-orcid-link')
        authors.append({
            'name': f"{gn.get_text(strip=True) if gn else ''} {fn.get_text(strip=True) if fn else ''}".strip(),
            'affiliation': affil_tag.get_text(strip=True) if affil_tag else "",
            'orcid': orcid_tag.find('a').get('href', '').strip() if orcid_tag and orcid_tag.find('a') else ""
        })
        
    return {
        'doi': doi, 'title': title, 'journal_title': journal_title, 'article_type': article_type,
        'first_page': first_page, 'last_page': last_page,
        'received_date': dates['received'], 'accepted_date': dates['accepted'], 'published_date': dates['published'],
        'authors': authors
    }

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
    logging.info("Connected to manual Chrome session. You can press Ctrl+C at any time to pause safely.")
    
    try:
        journals_processed = 0
        
        for journal in journals_to_process:
            if journals_processed >= MAX_JOURNALS:
                logging.info("Reached MAX_JOURNALS limit. Stopping.")
                break
                
            journal_title = journal['title']
            journal_url = journal['link']
            
            # Check if journal is fully completed
            if journal_url in state['completed_journals']:
                logging.info(f"Skipping {journal_title} - Already completed in previous run.")
                continue
            
            logging.info(f"\n=====================================")
            logging.info(f"Processing Journal: {journal_title}")
            logging.info(f"=====================================")
            
            safe_filename = re.sub(r'[\\/*?:"<>|]', "", journal_title)
            json_filepath = data_dir / f"{safe_filename}.json"
            
            driver.get(journal_url)
            
            if not wait_for_human_and_page(driver, (By.CSS_SELECTOR, "a[title='Latest Issue']"), "Latest Issue Button"):
                break 
                
            latest_issue_elem = driver.find_element(By.CSS_SELECTOR, "a[title='Latest Issue']")
            current_issue_url = latest_issue_elem.get_attribute('href')
            
            reached_target_year = False
            
            while current_issue_url and not reached_target_year:
                # Check if this specific issue is already completed
                if current_issue_url in state['completed_issues']:
                    logging.info(f"Skipping Issue: {current_issue_url} - Already processed.")
                    # We still need to fetch the page to find the 'Previous Issue' link
                    driver.get(current_issue_url)
                    wait_for_human_and_page(driver, (By.CLASS_NAME, "issue-item-container"), "Issue Container")
                    _, current_issue_url = extract_issue_data(driver.page_source)
                    continue

                logging.info(f"Navigating to Issue: {current_issue_url}")
                driver.get(current_issue_url)
                
                if not wait_for_human_and_page(driver, (By.CLASS_NAME, "issue-item-container"), "Issue Container"):
                    return 
                
                html_source = driver.page_source
                
                if "Issue-in-Progress" in html_source:
                    logging.info("Detected 'Issue-in-Progress'. Skipping extraction and moving to previous issue.")
                    _, current_issue_url = extract_issue_data(html_source)
                    continue 
                
                try:
                    show_all_buttons = driver.find_elements(By.CLASS_NAME, "showAllProceedings")
                    if show_all_buttons and show_all_buttons.is_displayed():
                        driver.execute_script("arguments.click();", show_all_buttons)
                        time.sleep(5) 
                except Exception as pagination_ex:
                    logging.warning(f"Error handling pagination expansion: {pagination_ex}")

                last_height = driver.execute_script("return document.body.scrollHeight")
                while True:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(3) 
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height

                html_source = driver.page_source
                abstract_links, next_issue_url = extract_issue_data(html_source)
                logging.info(f"Found {len(abstract_links)} Research Article abstracts in this issue.")
                
                issue_articles_data = []
                
                for abstract_link in abstract_links:
                    logging.info(f"  -> Browsing Abstract: {abstract_link}")
                    driver.get(abstract_link)
                                        
                    if not wait_for_human_and_page(driver, (By.CLASS_NAME, "core-published"), "Abstract Content (core-published)"):
                        return
                    
                    article_data = extract_article_data(driver.page_source, abstract_link, journal_title)
                    issue_articles_data.append(article_data)
                    
                    # Year Check Logic
                    pub_year = extract_year(article_data['published_date'])
                    if pub_year and pub_year < MIN_PUBLICATION_YEAR:
                        logging.warning(f"Hit article from {pub_year}. Reached our cutoff limit of {MIN_PUBLICATION_YEAR}.")
                        reached_target_year = True
                        break # Stop processing articles in this issue
                    
                    time.sleep(2)
                
                # ISSUE COMPLETE: SAVE INCREMENTAL PROGRESS 
                append_articles_to_json(json_filepath, issue_articles_data)
                
                state['completed_issues'].append(current_issue_url)
                save_progress(progress_file, state)
                logging.info(f"Progress saved: {len(issue_articles_data)} articles appended.")
                
                if not reached_target_year:
                    current_issue_url = next_issue_url
                
                if current_issue_url is None:
                    logging.info("Reached the earliest available issue.")

            # Mark journal as fully complete if we navigated back as far as needed
            state['completed_journals'].append(journal_url)
            save_progress(progress_file, state)
            journals_processed += 1
            
    except KeyboardInterrupt:
        logging.warning("\n[!] Script manually interrupted (Ctrl+C).")
        logging.info("Progress has already been saved up to the last completed issue. Exiting cleanly...")
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