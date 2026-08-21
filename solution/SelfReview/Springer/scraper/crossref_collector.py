"""
Collect Springer article metadata from the Crossref REST API.

Replaces article_downloader.py, which can no longer pass link.springer.com's
Fastly client challenge. Writes the same JSON shape to the same
data/{journal_id}/{doi}.json layout, so parse_output.py ingests both unchanged.

Received/Accepted come from Crossref's `assertion` array. These are voluntary
publisher deposits — Springer makes them, Elsevier and IEEE do not — so this
collector is Springer-only. See README.md for the validation figures.
"""
from __future__ import annotations

import configparser
import csv
import json
import logging
import os
import sys
import time
from datetime import date, datetime

import requests
from database import Postgress

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CROSSREF_API = "https://api.crossref.org"
OUTPUT_DIR = "data"

# Applied as Crossref's from-pub-date filter, which anchors on the earliest of
# published-print/published-online — a slightly different anchor than the
# listing-card date article_downloader.py used, so boundary articles may differ.
MIN_YEAR = 2016

# Crossref's "polite pool": without a contact address you land in the shared
# pool and hit 429s much sooner.
CONTACT_EMAIL = "r.sijm@salco.nl"
USER_AGENT = f"VAFAF-thesis-collector/1.0 (mailto:{CONTACT_EMAIL})"

ROWS_PER_PAGE = 500
MAX_RETRIES = 5
RATE_LIMIT_WAIT = 30  # fallback when a 429 arrives without Retry-After

# journal_id -> ISSN, built from the ISSNs in already-scraped files. Hand-edited
# values win. Unresolved journals are skipped rather than looked up by title,
# because a fuzzy match would silently collect a different journal.
ISSN_MAP_FILE = "crossref_issn_map.csv"

# Separate from the scraper's progress.json so neither clobbers the other.
PROGRESS_FILE = "crossref_progress.json"

MONTHS = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
    'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11,
    'december': 12,
}


def read_config(path) -> configparser.SectionProxy:
    if not os.path.exists(path):
        sys.exit(
            f"\nERROR: no .env found at {path}\n"
            f"Copy code/env-example to code/.env and fill in the POSTGRES_* values.\n"
        )
    with open(path, "r") as f:
        config_string = "[SECTION]\n" + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config["SECTION"]


# ---------------------------------------------------------------------------
# Crossref HTTP
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT})
    return session


def crossref_get(session: requests.Session, url: str, params: dict) -> dict | None:
    """GET a Crossref URL with retries. Returns the "message" object, or None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=60)
        except requests.RequestException as e:
            wait = 2 ** attempt
            logging.warning(f"  Request failed ({e}). Retry {attempt}/{MAX_RETRIES} in {wait}s.")
            time.sleep(wait)
            continue

        if response.status_code == 404:
            logging.warning(f"  Crossref has no record for {url}")
            return None

        if response.status_code == 429:
            retry_after = response.headers.get('Retry-After')
            wait = int(retry_after) if (retry_after or '').isdigit() else RATE_LIMIT_WAIT
            logging.warning(f"  Rate limited (429). Waiting {wait}s before retry "
                            f"{attempt}/{MAX_RETRIES}.")
            time.sleep(wait)
            continue

        if response.status_code >= 500:
            wait = 2 ** attempt
            logging.warning(f"  Crossref {response.status_code}. Retry {attempt}/{MAX_RETRIES} "
                            f"in {wait}s.")
            time.sleep(wait)
            continue

        if not response.ok:
            logging.error(f"  Crossref returned {response.status_code} for {url}: "
                          f"{response.text[:200]}")
            return None

        try:
            return response.json()['message']
        except (ValueError, KeyError) as e:
            logging.error(f"  Unparseable Crossref response for {url}: {e}")
            return None

    logging.error(f"  Giving up on {url} after {MAX_RETRIES} attempts.")
    return None


def iterate_journal_works(session: requests.Session, issn: str):
    """
    Yield every journal-article for one ISSN from MIN_YEAR onward.

    Uses cursor paging (required past 10 000 results). The bulk listing carries
    the assertions, so no per-DOI follow-up request is needed.
    """
    url = f"{CROSSREF_API}/journals/{issn}/works"
    params = {
        'filter': f'type:journal-article,from-pub-date:{MIN_YEAR}-01-01',
        'rows': ROWS_PER_PAGE,
        'cursor': '*',
    }

    total = None
    seen = 0
    while True:
        message = crossref_get(session, url, params)
        if message is None:
            return

        if total is None:
            total = message.get('total-results', 0)
            logging.info(f"  Crossref reports {total} articles from {MIN_YEAR} onward.")

        items = message.get('items') or []
        if not items:
            return

        for item in items:
            yield item
        seen += len(items)

        cursor = message.get('next-cursor')
        if not cursor or seen >= (total or 0):
            return
        params['cursor'] = cursor


# ---------------------------------------------------------------------------
# Mapping a Crossref record onto the scraper's JSON shape
# ---------------------------------------------------------------------------

def parse_assertion_date(value: str | None) -> str | None:
    """Convert an assertion date ("3 April 2018") to ISO, or None if it won't parse."""
    if not value:
        return None
    parts = value.strip().split()
    if len(parts) == 3:
        try:
            return date(int(parts[2]), MONTHS[parts[1].lower()], int(parts[0])).isoformat()
        except (ValueError, KeyError):
            pass
    try:
        return date.fromisoformat(value.strip()).isoformat()
    except ValueError:
        return None


def date_from_parts(date_parts_container: dict | None) -> str | None:
    """
    Convert a Crossref date-parts container to an ISO date.

    Returns None for partial dates (published-print is often [[2020, 2]]) rather
    than inventing a day.
    """
    if not date_parts_container:
        return None
    parts = (date_parts_container.get('date-parts') or [[]])[0]
    if len(parts) != 3:
        return None
    try:
        return date(int(parts[0]), int(parts[1]), int(parts[2])).isoformat()
    except (ValueError, TypeError):
        return None


def assertions(item: dict) -> dict[str, str]:
    """Return the item's assertions as a lowercased label -> value mapping."""
    out = {}
    for a in item.get('assertion') or []:
        label = (a.get('label') or a.get('name') or '').strip().lower()
        if label:
            out[label] = a.get('value')
    return out


def split_pages(page: str | None) -> tuple[str | None, str | None]:
    """Split a Crossref page range ("17-23") into first and last page."""
    if not page:
        return None, None
    if '-' in page:
        first, _, last = page.partition('-')
        return first.strip() or None, last.strip() or None
    return page.strip() or None, None


def build_article_json(item: dict, journal_id: str) -> dict:
    """Map one Crossref work onto the JSON shape article_downloader.py produces."""
    a = assertions(item)

    received = parse_assertion_date(a.get('received'))
    accepted = parse_assertion_date(a.get('accepted'))
    # The scraper's "published" corresponds to published-online (359/359 match).
    published = (date_from_parts(item.get('published-online'))
                 or date_from_parts(item.get('published')))

    # Mirror the scraper: only record a fallback when none of the three primary
    # dates was found, so it flags missing data instead of duplicating a date.
    fallback_label = None
    fallback_value = None
    if not (received or accepted or published):
        for label, container in (('Issue Date', item.get('published-print')),
                                 ('Issued', item.get('issued'))):
            value = date_from_parts(container)
            if value:
                fallback_label, fallback_value = label, value
                break

    authors = []
    for author in item.get('author') or []:
        name = ' '.join(p for p in (author.get('given'), author.get('family')) if p).strip()
        if not name:
            name = (author.get('name') or '').strip()
        if name:
            authors.append(name)
    authors = list(dict.fromkeys(authors))

    first_page, last_page = split_pages(item.get('page'))

    # Prefer the electronic ISSN — the variant the scraper captured from
    # citation_issn, so both collectors write the same value per journal.
    issn = None
    for entry in item.get('issn-type') or []:
        if entry.get('type') == 'electronic' and entry.get('value'):
            issn = entry['value']
            break
    if not issn:
        issn = (item.get('ISSN') or [None])[0]

    return {
        'doi': item.get('DOI'),
        'title': (item.get('title') or [None])[0],
        'journal_id': journal_id,
        'received': received,
        'accepted': accepted,
        'published': published,
        'fallback_date_label': fallback_label,
        'fallback_date_value': fallback_value,
        'authors': authors,
        'affiliations': [],     # Crossref has none for Springer
        'open_access': None,    # the licence array is not an OA signal
        'article_type': None,   # Crossref only reports "journal-article"
        'volume': item.get('volume'),
        'first_page': first_page,
        'last_page': last_page,
        'issn': issn,
        'retrieved_at': datetime.now().isoformat(),
        'source': 'crossref',   # ignored at ingest; marks API rows in the corpus
    }


# ---------------------------------------------------------------------------
# Output files
# ---------------------------------------------------------------------------

def save_json(data: dict, journal_id: str, doi: str) -> None:
    """Save to data/{journal_id}/{doi}.json, atomically."""
    filename = doi.replace('/', '_') + '.json'
    directory = os.path.join(OUTPUT_DIR, journal_id)
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)
    tmp = filepath + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, filepath)


def existing_dois(journal_id: str) -> set[str]:
    """
    Return the DOIs already saved for a journal, lowercased.

    Lowercased because Crossref echoes DOIs as deposited while the scraper took
    them from the listing markup; comparing case-insensitively avoids writing a
    duplicate file for the same article.
    """
    directory = os.path.join(OUTPUT_DIR, journal_id)
    if not os.path.isdir(directory):
        return set()
    return {
        os.path.splitext(fn)[0].replace('_', '/', 1).lower()
        for fn in os.listdir(directory)
        if fn.endswith('.json')
    }


def load_progress() -> dict:
    if not os.path.exists(PROGRESS_FILE):
        return {}
    with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_progress(progress: dict) -> None:
    tmp = PROGRESS_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)
    os.replace(tmp, PROGRESS_FILE)


# ---------------------------------------------------------------------------
# journal_id -> ISSN mapping
# ---------------------------------------------------------------------------

def issn_from_scraped_data(journal_id: str) -> str | None:
    """
    Recover a journal's ISSN from its already-scraped article JSON files.

    Reads a handful of files because older articles occasionally lack the field.
    Crossref returns identical results for either ISSN variant.
    """
    directory = os.path.join(OUTPUT_DIR, journal_id)
    if not os.path.isdir(directory):
        return None
    files = [fn for fn in os.listdir(directory) if fn.endswith('.json')]
    for fn in files[:25]:
        try:
            with open(os.path.join(directory, fn), encoding='utf-8') as f:
                issn = json.load(f).get('issn')
        except (OSError, ValueError):
            continue
        if issn:
            return issn.strip()
    return None


def load_issn_map(journal_ids: list[str]) -> dict[str, str]:
    """Build and cache the journal_id -> ISSN mapping (see ISSN_MAP_FILE)."""
    mapping: dict[str, str] = {}
    if os.path.exists(ISSN_MAP_FILE):
        with open(ISSN_MAP_FILE, 'r', newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                jid = (row.get('journal_id') or '').strip()
                issn = (row.get('issn') or '').strip()
                if jid and issn:
                    mapping[jid] = issn
        logging.info(f"Loaded {len(mapping)} journal->ISSN entries from {ISSN_MAP_FILE}.")

    recovered = 0
    unresolved = []
    for jid in journal_ids:
        if jid in mapping:
            continue
        issn = issn_from_scraped_data(jid)
        if issn:
            mapping[jid] = issn
            recovered += 1
        else:
            unresolved.append(jid)

    if recovered:
        logging.info(f"Recovered {recovered} ISSN(s) from already-scraped data.")

    with open(ISSN_MAP_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['journal_id', 'issn'])
        writer.writeheader()
        for jid in journal_ids:
            writer.writerow({'journal_id': jid, 'issn': mapping.get(jid, '')})

    if unresolved:
        logging.warning(
            f"{len(unresolved)} journal(s) have no ISSN and no scraped data to recover one "
            f"from: {', '.join(unresolved)}. They are skipped. To include them, add their "
            f"ISSN to {ISSN_MAP_FILE} by hand."
        )

    return mapping


# ---------------------------------------------------------------------------
# Per-journal collection
# ---------------------------------------------------------------------------

def process_journal(session: requests.Session, journal_id: str, name: str,
                    issn: str, progress: dict) -> None:
    logging.info(f"Processing journal [{journal_id}] {name} (ISSN {issn})")

    # Articles already on disk are never overwritten, so the affiliation data in
    # previously scraped files survives and this run only fills gaps.
    already = existing_dois(journal_id)
    if already:
        logging.info(f"  {len(already)} article(s) already on disk — those are left untouched.")

    written = 0
    skipped = 0
    received_count = 0
    no_doi = 0

    for item in iterate_journal_works(session, issn):
        doi = item.get('DOI')
        if not doi:
            no_doi += 1
            continue

        if doi.lower() in already:
            skipped += 1
            continue

        article = build_article_json(item, journal_id)
        save_json(article, journal_id, doi)
        already.add(doi.lower())
        written += 1
        if article['received']:
            received_count += 1

        if written % 250 == 0:
            logging.info(f"  {written} written so far ({skipped} already present).")

    progress[journal_id] = {
        'status': 'done',
        'articles_written': written,
        'already_present': skipped,
        'received_count': received_count,
        'min_year': MIN_YEAR,
        'issn': issn,
        'source': 'crossref',
        'updated_at': datetime.now().isoformat(),
    }
    save_progress(progress)

    if no_doi:
        logging.warning(f"  {no_doi} Crossref record(s) had no DOI and were skipped.")
    logging.info(
        f"  Done. {written} new article(s) written, {skipped} already present, "
        f"{received_count} of the new ones have a received date."
    )


def main():
    config = read_config('../../../.env')
    db = Postgress(
        server=config['POSTGRES_SERVER'],
        database=config['POSTGRES_DB'],
        user=config['POSTGRES_USER'],
        password=config['POSTGRES_PASSWORD'],
    )

    journals = db.execute_query_result(
        "SELECT journal_id, name FROM springer.journals ORDER BY journal_id"
    )
    logging.info(f"Found {len(journals)} journals in springer.journals.")

    journal_ids = [j['journal_id'] for j in journals]
    issn_map = load_issn_map(journal_ids)

    progress = load_progress()
    session = make_session()

    total_written = 0
    try:
        for journal in journals:
            journal_id = journal['journal_id']
            name = journal['name']

            # A journal recorded at a higher MIN_YEAR still has older articles
            # to fetch, so it is re-run automatically.
            record = progress.get(journal_id, {})
            if record.get('status') == 'done' and record.get('min_year', MIN_YEAR) <= MIN_YEAR:
                logging.info(
                    f"Skipping completed journal [{journal_id}] {name} "
                    f"(done at min_year={record.get('min_year')})."
                )
                continue

            issn = issn_map.get(journal_id)
            if not issn:
                continue  # already reported by load_issn_map

            process_journal(session, journal_id, name, issn, progress)
            total_written += progress[journal_id]['articles_written']
    except KeyboardInterrupt:
        logging.info("Interrupted by user (Ctrl+C). Progress is saved — safe to resume.")

    logging.info(f"All journals processed. {total_written} new article file(s) written.")


if __name__ == '__main__':
    main()
