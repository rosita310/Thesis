import configparser
import logging
import sys
import time

import requests
from bs4 import BeautifulSoup
from database import Postgress, Saver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://link.springer.com/journals/browse-subject"
SUBJECT = "COMPUTER_SCIENCE"
DB_SCHEMA = "springer"
DB_TABLE = "journals"

REQUEST_DELAY_SECONDS = 2


def read_config(path) -> dict:
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']


def get_page(session: requests.Session, page_number: int) -> bytes:
    """Fetch one page from the Springer journal browse page."""
    url = f"{BASE_URL}?subject={SUBJECT}&page={page_number}"
    logging.info(f"Fetching page {page_number}: {url}")
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.content


def parse_journals(html: bytes) -> list[dict]:
    """
    Extract journal name and ID from the HTML.

    Page structure:
      <h2 class="app-card-open__heading">
        <a data-track-label="146" ...>AI & SOCIETY</a>
      </h2>
    """
    soup = BeautifulSoup(html, 'html.parser')
    headings = soup.select('h2.app-card-open__heading')

    journals = []
    for heading in headings:
        anchor = heading.find('a')
        if not anchor:
            continue
        name = heading.get_text(strip=True)
        journal_id = anchor.get('data-track-label', '').strip()
        if name and journal_id:
            journals.append({'journal_id': journal_id, 'name': name})

    logging.info(f"  Found {len(journals)} journals on this page")
    return journals


def main():
    config = read_config('../../.env')

    db = Postgress(
        server=config['POSTGRES_SERVER'],
        database=config['POSTGRES_DB'],
        user=config['POSTGRES_USER'],
        password=config['POSTGRES_PASSWORD']
    )
    saver = Saver(db)

    session = requests.Session()
    session.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/124.0.0.0 Safari/537.36'
        ),
        'Accept-Language': 'en-US,en;q=0.9',
    })

    page = 1
    total = 0

    while True:
        content = get_page(session, page)
        journals = parse_journals(content)

        if not journals:
            logging.info("No more journals found — done.")
            break

        saver.save(DB_SCHEMA, DB_TABLE, journals)

        total += len(journals)
        logging.info(f"Page {page} saved. Total so far: {total}")

        page += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    logging.info(f"Done. {total} journals saved to {DB_SCHEMA}.{DB_TABLE}.")


if __name__ == '__main__':
    main()
