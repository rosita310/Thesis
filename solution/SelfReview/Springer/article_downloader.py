from __future__ import annotations

import configparser
import csv
import json
import logging
import os
import random
import time

import requests
from bs4 import BeautifulSoup
from database import Postgress
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://link.springer.com"
OUTPUT_DIR = "data"
MIN_YEAR = 2000

# CSV file listing journals to skip (journal_id, name).
# Add a journal here if you know it never publishes received dates.
# The file is created automatically with a header if it does not exist.
SKIP_JOURNALS_FILE = "skip_journals.csv"

# Maximum number of articles to download per journal.
# Set to a low number (e.g. 5) for testing, None for a full production run.
MAX_ARTICLES_PER_JOURNAL = 5

# Random delay range between requests to appear more human-like (seconds)
REQUEST_DELAY_MIN = 1.5
REQUEST_DELAY_MAX = 3.5
# Extra wait time when a 429 Too Many Requests is received (seconds)
RATE_LIMIT_WAIT = 60
# Maximum retries per request before giving up
MAX_RETRIES = 3


def read_config(path) -> dict:
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/124.0.0.0 Safari/537.36'
        ),
        'Accept-Language': 'en-US,en;q=0.9',
    })
    return session


def fetch_with_retry(session: requests.Session, url: str) -> bytes | None:
    """
    Fetch a URL with retries and exponential backoff on failure.

    Handles 429 (rate limited) with a long wait before retrying.
    Raises BlockedException if Springer appears to have blocked us,
    so the caller can stop the run rather than silently saving bad data.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=30)

            if response.status_code == 429:
                logging.warning(f"Rate limited (429). Waiting {RATE_LIMIT_WAIT}s before retry.")
                time.sleep(RATE_LIMIT_WAIT)
                continue

            response.raise_for_status()

            # Sanity check: if the response looks like a CAPTCHA or block page,
            # Springer still returns HTTP 200 but the expected content is missing.
            if is_blocked(response.content):
                raise BlockedException(f"Springer returned a block/CAPTCHA page for {url}")

            return response.content

        except BlockedException:
            raise
        except requests.RequestException as e:
            wait = 2 ** attempt
            logging.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}. Retrying in {wait}s.")
            time.sleep(wait)

    logging.error(f"Giving up on {url} after {MAX_RETRIES} attempts.")
    return None


def is_blocked(content: bytes) -> bool:
    """
    Return True if the response looks like a CAPTCHA or block page.

    Springer returns HTTP 200 for these pages, so we check for the absence
    of expected content rather than relying on the status code.

    Note: avoid overly broad terms like "robot" (appears in <meta name="robots">
    on every page) or "automated" (appears in legitimate content).
    """
    text = content.decode('utf-8', errors='ignore').lower()

    # Specific phrases that only appear on block/CAPTCHA pages
    block_signals = [
        'captcha',
        'access denied',
        'you have been blocked',
        'automated access to this service',
        'detected unusual traffic',
    ]
    for signal in block_signals:
        if signal in text:
            logging.warning(f"Block signal detected: '{signal}'")
            return True

    # A valid Springer page always contains its site header
    if 'springer' not in text:
        logging.warning("Block detected: 'springer' not found in response body.")
        return True

    return False


class BlockedException(Exception):
    """Raised when Springer detects automated access."""
    pass


def get_article_links(session: requests.Session, journal_id: str, page: int) -> tuple[list[dict], bool]:
    """
    Fetch one page of a journal's article list and return article stubs.

    Each stub contains the DOI and title from the listing card.
    Also returns a boolean indicating whether to stop paginating (reached MIN_YEAR).

    Card structure:
      <article class="app-card-open">
        <h2 class="app-card-open__heading">
          <a data-track-label="{doi}" ...>{title}</a>
        </h2>
        ...
        <span class="c-meta__item">{date}</span>   ← last span, e.g. "19 March 2026"
      </article>
    """
    url = f"{BASE_URL}/journal/{journal_id}/articles?page={page}"
    logging.info(f"  Fetching article list page {page}: {url}")
    content = fetch_with_retry(session, url)
    if not content:
        return [], True

    soup = BeautifulSoup(content, 'html.parser')
    cards = soup.select('article.app-card-open')
    if not cards:
        return [], True

    articles = []
    stop = False
    for card in cards:
        anchor = card.select_one('h2.app-card-open__heading a')
        if not anchor:
            continue

        doi = anchor.get('data-track-label', '').strip()
        title = anchor.get_text(strip=True)

        # Publication date is in the last span.c-meta__item, e.g. "19 March 2026"
        date_spans = card.select('span.c-meta__item')
        pub_date_str = date_spans[-1].get_text(strip=True) if date_spans else ''
        pub_year = parse_year(pub_date_str)

        if pub_year is not None and pub_year < MIN_YEAR:
            logging.info(f"  Reached article from {pub_year} — stopping pagination for this journal.")
            stop = True
            break

        if doi:
            articles.append({'doi': doi, 'title': title})

    return articles, stop


def parse_year(date_str: str) -> int | None:
    """Extract the four-digit year from a date string like '19 March 2026'."""
    parts = date_str.strip().split()
    for part in reversed(parts):
        if part.isdigit() and len(part) == 4:
            return int(part)
    return None


def extract_article_data(html: bytes) -> dict:
    """
    Extract all metadata from an article page.

    Dates come from <li class="c-bibliographic-information__list-item"> elements.
    All other fields come from <meta> tags or specific HTML elements.

    Date labels: Received, Accepted, Published, Version of record, Issue date, DOI.
    If Received/Accepted/Published are missing, the first available date is stored
    as fallback so missing data is easily identified in the database.
    """
    soup = BeautifulSoup(html, 'html.parser')

    # --- Dates ---
    data = {
        'received': None,
        'accepted': None,
        'published': None,
        'fallback_date_label': None,
        'fallback_date_value': None,
    }

    known_labels = {'received', 'accepted', 'published', 'version of record', 'doi'}

    for item in soup.select('li.c-bibliographic-information__list-item'):
        p = item.find('p')
        if not p:
            continue

        # Extract label from the direct text node inside <p>, ignoring child elements.
        # Using get_text() would also include the hidden <span class="u-hide">: </span>,
        # which would add a stray colon to the value.
        label_raw = p.find(string=True, recursive=False)
        if not label_raw:
            continue
        label = label_raw.strip().lower()
        if not label:
            continue

        # Use the machine-readable datetime attribute (e.g. "2026-03-24") from the
        # <time> element rather than the human-readable text ("24 March 2026").
        time_el = p.find('time')
        if not time_el:
            continue
        value = time_el.get('datetime', '').strip()
        if not value:
            continue

        if label == 'received':
            data['received'] = value
        elif label == 'accepted':
            data['accepted'] = value
        elif label == 'published':
            data['published'] = value
        elif label not in known_labels and data['fallback_date_label'] is None:
            data['fallback_date_label'] = label_raw.strip()
            data['fallback_date_value'] = value

    # --- Authors ---
    # <a data-test="author-name">Name</a>
    # dict.fromkeys() removes duplicates while preserving order
    data['authors'] = list(dict.fromkeys(
        el.get_text(strip=True)
        for el in soup.select('a[data-test="author-name"]')
    ))

    # --- Affiliations with linked authors ---
    # Structure in HTML:
    #   <ol class="c-article-author-affiliation__list">
    #     <li>
    #       <p class="c-article-author-affiliation__address">Institution, Country</p>
    #       <p class="c-article-author-affiliation__authors-list">Name1, Name2 & Name3</p>
    #     </li>
    #   </ol>
    affiliations = []
    for li in soup.select('ol.c-article-author-affiliation__list > li'):
        institution_el = li.find('p', class_='c-article-author-affiliation__address')
        authors_el = li.find('p', class_='c-article-author-affiliation__authors-list')
        if not institution_el:
            continue
        institution = institution_el.get_text(strip=True)
        # Parse "Name1, Name2 & Name3" → ["Name1", "Name2", "Name3"]
        if authors_el:
            raw = authors_el.get_text(strip=True).replace(' & ', ', ')
            authors_at_aff = [a.strip() for a in raw.split(',') if a.strip()]
        else:
            authors_at_aff = []
        affiliations.append({'institution': institution, 'authors': authors_at_aff})
    data['affiliations'] = affiliations

    # --- Open access ---
    # Present as <span class="u-color-open-access"> on open access articles
    data['open_access'] = bool(soup.select_one('span.u-color-open-access'))

    # --- Article type, volume, pages (from <meta> tags in <head>) ---
    def meta(name: str) -> str | None:
        el = soup.find('meta', attrs={'name': name})
        return el['content'].strip() if el and el.get('content') else None

    data['article_type'] = meta('citation_article_type')
    data['volume'] = meta('citation_volume')
    data['first_page'] = meta('citation_firstpage')
    data['last_page'] = meta('citation_lastpage')
    data['issn'] = meta('citation_issn')

    # --- Date of retrieval ---
    data['retrieved_at'] = datetime.now().isoformat()

    return data


def save_json(data: dict, journal_id: str, doi: str) -> None:
    """Save article metadata as a JSON file in data/{journal_id}/{doi}.json."""
    # Replace slashes in DOI with underscores to make a valid filename
    filename = doi.replace('/', '_') + '.json'
    directory = os.path.join(OUTPUT_DIR, journal_id)
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def already_downloaded(journal_id: str, doi: str) -> bool:
    """Return True if the JSON file for this article already exists."""
    filename = doi.replace('/', '_') + '.json'
    return os.path.exists(os.path.join(OUTPUT_DIR, journal_id, filename))


def load_skip_journals() -> set[str]:
    """
    Read journal IDs to skip from SKIP_JOURNALS_FILE.
    Creates the file with a header if it does not exist yet.
    Returns a set of journal_id strings.
    """
    if not os.path.exists(SKIP_JOURNALS_FILE):
        with open(SKIP_JOURNALS_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=['journal_id', 'name']).writeheader()
        logging.info(f"Created empty skip list: {SKIP_JOURNALS_FILE}")
        return set()

    with open(SKIP_JOURNALS_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        ids = {row['journal_id'].strip() for row in reader if row['journal_id'].strip()}

    logging.info(f"Loaded {len(ids)} journal(s) to skip from {SKIP_JOURNALS_FILE}.")
    return ids


def suggest_skip(journal_id: str, name: str) -> None:
    """
    Log a suggestion to add this journal to the skip list,
    and append it to SKIP_JOURNALS_FILE automatically.
    """
    logging.warning(
        f"Journal [{journal_id}] '{name}' had no articles with a 'received' date. "
        f"Consider skipping it in future runs. Adding to {SKIP_JOURNALS_FILE}."
    )
    with open(SKIP_JOURNALS_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['journal_id', 'name'])
        writer.writerow({'journal_id': journal_id, 'name': name})


def process_journal(session: requests.Session, journal_id: str, name: str) -> None:
    logging.info(f"Processing journal [{journal_id}] {name}")
    page = 1
    total = 0
    received_count = 0

    while True:
        article_stubs, stop = get_article_links(session, journal_id, page)

        for stub in article_stubs:
            if MAX_ARTICLES_PER_JOURNAL is not None and total >= MAX_ARTICLES_PER_JOURNAL:
                logging.info(f"  Reached MAX_ARTICLES_PER_JOURNAL ({MAX_ARTICLES_PER_JOURNAL}) — stopping.")
                return

            doi = stub['doi']

            if already_downloaded(journal_id, doi):
                logging.info(f"    Skipping already downloaded: {doi}")
                continue

            article_url = f"{BASE_URL}/article/{doi}"
            logging.info(f"    Fetching article: {article_url}")
            content = fetch_with_retry(session, article_url)
            if not content:
                continue

            article_data = extract_article_data(content)
            if article_data['received']:
                received_count += 1

            data = {
                'doi': doi,
                'title': stub['title'],
                'journal_id': journal_id,
                **article_data
            }
            save_json(data, journal_id, doi)
            total += 1
            time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        if stop or not article_stubs:
            break

        page += 1
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    logging.info(f"  Done. {total} articles saved for journal [{journal_id}] ({received_count} with received date).")

    # If no article had a received date, the journal likely never publishes it
    if total > 0 and received_count == 0:
        suggest_skip(journal_id, name)


def main():
    config = read_config('../../.env')
    db = Postgress(
        server=config['POSTGRES_SERVER'],
        database=config['POSTGRES_DB'],
        user=config['POSTGRES_USER'],
        password=config['POSTGRES_PASSWORD']
    )

    journals = db.execute_query_result("SELECT journal_id, name FROM springer.journals ORDER BY journal_id")
    logging.info(f"Found {len(journals)} journals to process.")

    skip_ids = load_skip_journals()
    session = make_session()

    try:
        for journal in journals:
            if journal['journal_id'] in skip_ids:
                logging.info(f"Skipping journal [{journal['journal_id']}] {journal['name']} (in skip list).")
                continue
            try:
                process_journal(session, journal['journal_id'], journal['name'])
            except BlockedException as e:
                logging.error(f"BLOCKED by Springer: {e}")
                logging.error("Stopping the run to avoid saving corrupt data. Resume later.")
                break
            break # Remove this break to process all journals in a real run. It's here to limit scope during testing.
    except KeyboardInterrupt:
        logging.info("Interrupted by user (Ctrl+C). Progress is saved — safe to resume.")

    logging.info("All journals processed.")


if __name__ == '__main__':
    main()
