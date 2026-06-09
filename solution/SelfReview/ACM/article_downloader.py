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
MAX_ISSUES_PER_JOURNAL = 3   # Limit issues per journal
MAX_WAIT_TIME_SECONDS = 126  # Max time to wait for manual captcha solve

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

def extract_issue_data(html_source):
    """Parses the issue page to find Research Article links and the Previous Issue link."""
    soup = BeautifulSoup(html_source, 'html.parser')
    
    abstract_links = []
    containers = soup.find_all('div', class_='issue-item-container')
    
    for container in containers:
        heading = container.find('div', class_='issue-heading')
        if heading and (not heading.get_text(strip=True).lower() == 'editorial'):
            abs_btn = container.find('a', attrs={'data-title': 'Abstract'}) or \
                      container.find('a', attrs={'aria-label': 'Abstract'})
            
            if abs_btn and abs_btn.get('href'):
                full_link = urljoin(BASE_DOMAIN, abs_btn.get('href'))
                abstract_links.append(full_link)

    previous_issue_url = None
    prev_btn = soup.find('a', class_='content-navigation__btn--pre')
    
    if prev_btn:
        classes = prev_btn.get('class', [])
        if 'content-navigation__btn--disabled' not in classes and prev_btn.get('href'):
            if 'javascript' not in prev_btn.get('href'):
                previous_issue_url = urljoin(BASE_DOMAIN, prev_btn.get('href'))
            
    return abstract_links, previous_issue_url

def extract_article_data(html_source, article_url, journal_title):
    """Parses the abstract page to extract detailed metadata."""
    soup = BeautifulSoup(html_source, 'html.parser')
    
    # DOI (extracted directly from the URL)
    doi = ""
    if '/doi/abs/' in article_url:
        doi = article_url.split('/doi/abs/')[-1]
    elif '/doi/' in article_url:
        doi = article_url.split('/doi/')[-1]
        
    # Title
    title_tag = soup.find('h1', property='name')
    title = title_tag.get_text(strip=True) if title_tag else ""
    
    # Type
    type_tag = soup.find('div', class_='meta-panel__type')
    article_type = type_tag.get_text(strip=True) if type_tag else ""
    
    # Pages
    page_start = soup.find('span', property='pageStart')
    page_end = soup.find('span', property='pageEnd')
    first_page = page_start.get_text(strip=True) if page_start else ""
    last_page = page_end.get_text(strip=True) if page_end else ""
    
    # Dates
    dates = {'received': None, 'accepted': None, 'published': None}
    core_labels = soup.find_all('b', class_='core-label')
    for label in core_labels:
        label_text = label.get_text(strip=True).replace(':', '').lower()
        if label_text in dates:
            # The actual date string is usually the text node immediately following the <b> tag
            dates[label_text] = label.next_sibling.strip() if label.next_sibling else ""
            
    # Authors
    authors = []
    author_divs = soup.find_all('div', property='author', typeof='Person')
    for adiv in author_divs:
        gn = adiv.find('span', property='givenName')
        fn = adiv.find('span', property='familyName')
        given = gn.get_text(strip=True) if gn else ""
        family = fn.get_text(strip=True) if fn else ""
        
        affil_tag = adiv.find('div', property='affiliation')
        affil = affil_tag.get_text(strip=True) if affil_tag else ""
        
        orcid_tag = adiv.find('div', class_='core-orcid-link')
        orcid = ""
        if orcid_tag:
            a_tag = orcid_tag.find('a')
            if a_tag: 
                orcid = a_tag.get('href', '').strip()
            
        authors.append({
            'name': f"{given} {family}".strip(),
            'affiliation': affil,
            'orcid': orcid
        })
        
    return {
        'doi': doi,
        'title': title,
        'journal_title': journal_title,
        'article_type': article_type,
        'first_page': first_page,
        'last_page': last_page,
        'received_date': dates['received'],
        'accepted_date': dates['accepted'],
        'published_date': dates['published'],
        'authors': authors
    }

def main():
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / '../../.env'
    config = read_config(config_path.resolve())

    # Create the 'data' directory if it doesn't exist
    data_dir = script_dir / 'data'
    data_dir.mkdir(exist_ok=True)

    db = Postgress(
        server=config['POSTGRES_SERVER'],
        database=config['POSTGRES_DB'],
        user=config['POSTGRES_USER'],
        password=config['POSTGRES_PASSWORD']
    )
    
    logging.info("Fetching journal titles and links from the database...")
    # Updated query to pull both title and link
    db_results = db.execute_query_result('SELECT "title", "link" FROM "acm"."journals"')
    
    journals_to_process = []
    for row in db_results:
        link = row['link']
        if not link.startswith('http'):
            link = f"https://{link}"
        journals_to_process.append({'title': row['title'], 'link': link})
        
    logging.info(f"Successfully loaded {len(journals_to_process)} journals from database.")

    driver = get_debugging_driver()
    logging.info("Connected to manual Chrome session.")
    
    try:
        journals_processed = 0
        
        for journal in journals_to_process:
            if journals_processed >= MAX_JOURNALS:
                logging.info("Reached MAX_JOURNALS limit. Stopping.")
                break
                
            journal_title = journal['title']
            journal_url = journal['link']
            
            logging.info(f"\n=====================================")
            logging.info(f"Processing Journal: {journal_title}")
            logging.info(f"=====================================")
            
            # List to hold all scraped articles for this specific journal
            journal_articles_data = []
            
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
                
                # FIXED: BUG in showAllProceedings handling
                logging.info("Checking if issue requires expanding hidden articles...")
                try:
                    show_all_buttons = driver.find_elements(By.CLASS_NAME, "showAllProceedings")
                    if show_all_buttons and show_all_buttons.is_displayed():
                        logging.info(f"Found 'Show All' button. Expanding hidden articles...")
                        driver.execute_script("arguments.click();", show_all_buttons)
                        time.sleep(5) 
                    else:
                        logging.info("No 'Show All' button detected. All articles already visible.")
                except Exception as pagination_ex:
                    logging.warning(f"Error handling pagination expansion: {pagination_ex}. Proceeding with current view.")

                logging.info("Scrolling down the page to load all entries...")
                last_height = driver.execute_script("return document.body.scrollHeight")
                while True:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(3) 
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height

                html_source = driver.page_source

                abstract_links, current_issue_url = extract_issue_data(html_source)
                logging.info(f"Found {len(abstract_links)} Research Article abstracts.")
                
                for abstract_link in abstract_links:
                    logging.info(f"  -> Browsing Abstract: {abstract_link}")
                    driver.get(abstract_link)
                                        
                    if not wait_for_human_and_page(driver, (By.CLASS_NAME, "core-published"), "Abstract Content (core-published)"):
                        return
                    
                    # EXTRACT AND STORE DATA
                    article_html = driver.page_source
                    article_data = extract_article_data(article_html, abstract_link, journal_title)
                    journal_articles_data.append(article_data)
                    
                    time.sleep(2)
                
                issues_processed += 1
                
                if issues_processed >= MAX_ISSUES_PER_JOURNAL:
                    logging.info("Reached MAX_ISSUES_PER_JOURNAL limit for this journal.")
                elif current_issue_url is None:
                    logging.info("Reached the earliest available issue. No previous issue found.")

            # SAVE JOURNAL DATA TO JSON
            if journal_articles_data:
                # Create a safe filename (remove invalid Windows path characters)
                safe_filename = re.sub(r'[\\/*?:"<>|]', "", journal_title)
                json_filepath = data_dir / f"{safe_filename}.json"
                
                logging.info(f"Saving {len(journal_articles_data)} articles to {json_filepath}")
                with open(json_filepath, 'w', encoding='utf-8') as f:
                    json.dump(journal_articles_data, f, indent=4, ensure_ascii=False)

            journals_processed += 1
            
    finally:
        logging.info("Script finished.")
        driver.close()

if __name__ == '__main__':
    main()