from __future__ import annotations

import configparser
import csv
import json
import logging
import os
import random
import sys
import time

import requests
from bs4 import BeautifulSoup
from database import Postgress
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://link.springer.com"
OUTPUT_DIR = "data"
MIN_YEAR = 2020

# Browser identity shared by the requests fast path and the Selenium fallback,
# so both look like the same client to Springer.
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/124.0.0.0 Safari/537.36'
)

# CSV file listing journals to skip (journal_id, name).
# Add a journal here if you know it never publishes received dates.
# The file is created automatically with a header if it does not exist.
SKIP_JOURNALS_FILE = "skip_journals.csv"

# Per-journal progress file (JSON). Records, per journal_id, the last fully
# completed article-list page, cumulative counts, the status (in_progress/done),
# and the MIN_YEAR that a "done" status reflects. This lets a resumed run skip
# completed journals entirely and continue an interrupted journal from the next
# page instead of re-fetching every list page from page 1.
#
# Iterative-scrape workflow: when you lower MIN_YEAR (e.g. 2020 -> 2010) to grab
# older articles, flip the relevant journals' "status" from "done" back to
# "in_progress" by hand but KEEP their "last_page". Because Springer lists newest
# first, the older articles you now want live on the later pages, so the run will
# continue at last_page + 1 straight into the new data without re-fetching what
# you already have. The "min_year" field tells you which threshold each "done"
# reflects.
PROGRESS_FILE = "progress.json"

# Persistent Chrome profile for the Selenium fallback. Reusing a profile lets a
# session that has cleared a CAPTCHA stay trusted across runs. Safe to delete.
CHROME_PROFILE_DIR = ".chrome_profile"

# Maximum number of articles to download per journal.
# Set to a low number (e.g. 5) for testing, None for a full production run.
MAX_ARTICLES_PER_JOURNAL = None

# Random delay range between requests to appear more human-like (seconds).
# Kept low for throughput; the adaptive backoff below widens it automatically
# whenever Springer starts blocking, then decays it back once requests succeed.
REQUEST_DELAY_MIN = 0.1
REQUEST_DELAY_MAX = 0.5
# Adaptive backoff: the delay is multiplied by this factor each time we are
# blocked, capped at MAX_DELAY_MULTIPLIER, and decayed back toward 1.0 on success.
DELAY_BACKOFF_FACTOR = 2.0
DELAY_DECAY_FACTOR = 0.9
MAX_DELAY_MULTIPLIER = 8.0
# Extra wait time when a 429 Too Many Requests is received (seconds)
RATE_LIMIT_WAIT = 60
# Maximum retries per request before giving up
MAX_RETRIES = 3


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


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        'User-Agent': USER_AGENT,
        'Accept-Language': 'en-US,en;q=0.9',
    })
    return session


def fetch_with_retry(session: requests.Session, url: str) -> bytes | None:
    """
    Fetch a URL with retries and exponential backoff on failure.

    Handles 429 (rate limited) with a long wait before retrying.
    Handles 403 and block/CAPTCHA pages by raising BlockedException immediately.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=30)

            if response.status_code == 429:
                logging.warning(f"Rate limited (429). Waiting {RATE_LIMIT_WAIT}s before retry.")
                time.sleep(RATE_LIMIT_WAIT)
                continue

            if response.status_code == 403:
                raise BlockedException(f"Springer returned 403 Forbidden for {url}")

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

    Springer returns HTTP 200 for soft-block/challenge pages, so we cannot rely
    on the status code alone.

    IMPORTANT — content false positives: do NOT match bare words like "captcha",
    "robot" or "blocked" anywhere in the page. Article *titles* legitimately
    contain such words (e.g. a real paper titled "Proposal and Evaluation for
    Color Constancy CAPTCHA"), which would otherwise be misread as a block.
    Instead we first confirm the page carries genuine content, and only then look
    for full block-page UI strings.
    """
    text = content.decode('utf-8', errors='ignore').lower()

    # Positive content signals. A real listing page carries article cards; a real
    # article page carries citation metadata. If either is present, the page is
    # genuine content and never a block — no matter what words appear in a title.
    if 'app-card-open' in text or 'citation_title' in text:
        return False

    # No expected content. A real Springer page always carries its site header;
    # bot-challenge / WAF pages typically do not.
    if 'springer' not in text:
        logging.warning("Block detected: 'springer' not found in response body.")
        return True

    # Full UI strings that appear on block/challenge pages (never in a title).
    block_signals = [
        'you have been blocked',
        'automated access to this service',
        'detected unusual traffic',
        'verify you are a human',
        'verifying you are human',
        'why have i been blocked',
    ]
    for signal in block_signals:
        if signal in text:
            logging.warning(f"Block signal detected: '{signal}'")
            return True

    return False


class BlockedException(Exception):
    """
    Raised when Springer serves a block or CAPTCHA page instead of the
    requested article. This typically happens when our scraping traffic
    has been flagged, so the response is HTTP 200 but the body does not
    contain the expected article content (see is_blocked).
    """
    pass


class Fetcher:
    """
    Hybrid fetcher: fast `requests` path with a Selenium browser fallback.

    Normal fetches go through a `requests.Session` (fast). When Springer serves
    a block/CAPTCHA page (BlockedException), the fetcher escalates to a *visible*
    Chrome window via Selenium, navigates to the URL, and — if a CAPTCHA is shown
    — pauses for the operator to solve it by hand before continuing. After the
    block clears, the browser's cookies are copied back into the requests session
    so subsequent fetches return to the fast path.

    Also owns the inter-request delay, applying adaptive backoff: the delay widens
    on every block and decays back toward the baseline as requests succeed.
    """

    def __init__(self):
        self.session = make_session()
        self.driver = None  # lazily created on first block
        self.delay_multiplier = 1.0

    def fetch(self, url: str) -> bytes | None:
        """Fetch a URL, escalating to the browser fallback if blocked."""
        try:
            content = fetch_with_retry(self.session, url)
            self._on_success()
            return content
        except BlockedException as e:
            logging.warning(f"Blocked on fast path: {e}. Escalating to browser.")
            self._on_block()
            return self._fetch_with_browser(url)

    def sleep(self) -> None:
        """Sleep a randomised, adaptively-scaled delay between requests."""
        base = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        time.sleep(base * self.delay_multiplier)

    def close(self) -> None:
        """
        Close the browser if one was opened.

        If the operator already closed the Chrome window by hand, chromedriver is
        gone and the graceful quit() retries against a dead local port, logging
        noisy urllib3 warnings. We suppress that chatter and, as a fallback, kill
        the chromedriver subprocess directly (no network needed).
        """
        if self.driver is None:
            return

        urllib3_logger = logging.getLogger('urllib3')
        prev_level = urllib3_logger.level
        urllib3_logger.setLevel(logging.ERROR)  # hide retry warnings during teardown
        try:
            self.driver.quit()
        except Exception:
            pass
        finally:
            urllib3_logger.setLevel(prev_level)
            # Belt and suspenders: ensure the chromedriver process is terminated.
            try:
                proc = getattr(getattr(self.driver, 'service', None), 'process', None)
                if proc is not None:
                    proc.kill()
            except Exception:
                pass
            self.driver = None

    def _on_success(self) -> None:
        # Decay the delay back toward the baseline after a clean fetch.
        if self.delay_multiplier > 1.0:
            self.delay_multiplier = max(1.0, self.delay_multiplier * DELAY_DECAY_FACTOR)

    def _on_block(self) -> None:
        self.delay_multiplier = min(MAX_DELAY_MULTIPLIER, self.delay_multiplier * DELAY_BACKOFF_FACTOR)

    def _ensure_driver(self) -> None:
        """Lazily start a visible Chrome. Selenium is imported only when needed."""
        if self.driver is not None:
            return
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
        except ImportError as e:
            raise BlockedException(
                "Blocked by Springer and the Selenium fallback is unavailable. "
                "Install it with: pip install selenium"
            ) from e

        options = Options()
        options.add_argument(f"--user-data-dir={os.path.abspath(CHROME_PROFILE_DIR)}")
        options.add_argument(f"--user-agent={USER_AGENT}")
        # Light de-automation tweaks; the operator solves the CAPTCHA regardless.
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        # Strategy 1: let Selenium find the driver itself. Works when Selenium
        # Manager is present (Selenium >= 4.6) or chromedriver is already on PATH.
        try:
            self.driver = webdriver.Chrome(options=options)
            return
        except Exception as e1:
            logging.warning(f"Chrome startup via Selenium's own driver lookup failed ({e1}).")

        # Strategy 2: let webdriver-manager download a matching chromedriver.
        try:
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            logging.info("Started Chrome using webdriver-manager.")
            return
        except Exception as e2:
            raise BlockedException(
                "Could not start Chrome for the Selenium fallback. Easiest fix is to "
                "upgrade Selenium so its built-in driver manager handles this:\n"
                "    pip install -U selenium\n"
                "Alternatively install webdriver-manager:\n"
                "    pip install webdriver-manager\n"
                f"(underlying error: {e2})"
            ) from e2

    def _fetch_with_browser(self, url: str) -> bytes | None:
        """
        Open the URL in Chrome, pausing for a manual CAPTCHA solve if needed.

        If the block cannot be solved (e.g. a hard IP ban), the operator can quit
        the whole run cleanly by typing 'q' at the prompt, pressing Ctrl+C, or
        simply closing the Chrome window — all of these raise KeyboardInterrupt,
        which main() handles gracefully. Progress is saved per page, so a later
        resume picks up safely.
        """
        self._ensure_driver()
        try:
            self.driver.get(url)
            while is_blocked(self.driver.page_source.encode('utf-8', 'ignore')):
                logging.warning("CAPTCHA / block page detected in the Chrome window.")
                answer = input(
                    f"\n>>> Solve the CAPTCHA in the Chrome window for:\n    {url}\n"
                    f"    Press ENTER to continue, or type 'q' + ENTER to quit "
                    f"(e.g. on an IP ban): "
                )
                if answer.strip().lower() in ('q', 'quit'):
                    raise KeyboardInterrupt("Aborted by operator at CAPTCHA prompt.")
                # The page may have navigated after solving; reload to be sure.
                self.driver.get(url)
        except Exception as e:
            # KeyboardInterrupt (Ctrl+C / 'q') is a BaseException and passes through.
            # Anything else here means the browser/session is gone (window closed) —
            # treat that as a clean abort rather than crashing the run.
            logging.warning(f"Browser unavailable during manual solve ({e}). Aborting run.")
            raise KeyboardInterrupt("Chrome window closed or session lost.") from e

        self._sync_cookies()
        logging.info("Block cleared — resuming via the fast requests path.")
        return self.driver.page_source.encode('utf-8', 'ignore')

    def _sync_cookies(self) -> None:
        """Copy the browser's cookies into the requests session."""
        for c in self.driver.get_cookies():
            self.session.cookies.set(
                c['name'], c['value'],
                domain=c.get('domain'), path=c.get('path', '/'),
            )


def get_article_links(fetcher: 'Fetcher', journal_id: str, page: int) -> tuple[list[dict], bool]:
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
    content = fetcher.fetch(url)
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

        # The date span is identified by its month name (its position varies).
        pub_year = extract_card_year(card)

        if pub_year is not None and pub_year < MIN_YEAR:
            logging.info(f"  Reached article from {pub_year} — stopping pagination for this journal.")
            stop = True
            break

        if doi:
            articles.append({'doi': doi, 'title': title})

    return articles, stop


MONTH_NAMES = {
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december',
}


def parse_year(date_str: str) -> int | None:
    """Extract a plausible four-digit year (1900–2100) from a date string."""
    parts = date_str.strip().split()
    for part in reversed(parts):
        if part.isdigit() and len(part) == 4 and 1900 <= int(part) <= 2100:
            return int(part)
    return None


def extract_card_year(card) -> int | None:
    """
    Return the publication year shown on a listing card.

    The card has several <span class="c-meta__item"> elements in an order that is
    NOT fixed, e.g. ['Original Article', '29 November 2019', 'Pages: 64 - 72'].
    The date is identified by its month name, so we never mistake a page range
    like 'Pages: 2018 - 2025' for a year. Falls back to any plausible 4-digit year.
    """
    spans = [s.get_text(strip=True) for s in card.select('span.c-meta__item')]
    for txt in spans:
        low = txt.lower()
        if any(month in low for month in MONTH_NAMES):
            year = parse_year(txt)
            if year is not None:
                return year
    for txt in spans:
        year = parse_year(txt)
        if year is not None:
            return year
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

    # Remember the first non-primary date (e.g. "Issue Date") but only promote it
    # to the fallback if none of received/accepted/published turn up (see below).
    first_other_label = None
    first_other_value = None

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
        elif label not in known_labels and first_other_label is None:
            first_other_label = label_raw.strip()
            first_other_value = value

    # Only record a fallback date when NONE of the three primary dates were found,
    # so it flags genuinely missing data instead of duplicating e.g. an issue date.
    if not (data['received'] or data['accepted'] or data['published']):
        data['fallback_date_label'] = first_other_label
        data['fallback_date_value'] = first_other_value

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
    # Present as <a data-test="open-access"> on open access articles.
    data['open_access'] = bool(soup.select_one('[data-test="open-access"]'))

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
    """Save article metadata as a JSON file in data/{journal_id}/{doi}.json.

    Written atomically (temp file + os.replace) so an interruption mid-write can
    never leave a truncated .json that already_downloaded() would mistake for a
    completed article and never re-fetch.
    """
    # Replace slashes in DOI with underscores to make a valid filename
    filename = doi.replace('/', '_') + '.json'
    directory = os.path.join(OUTPUT_DIR, journal_id)
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)
    tmp = filepath + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, filepath)


def already_downloaded(journal_id: str, doi: str) -> bool:
    """Return True if the JSON file for this article already exists."""
    filename = doi.replace('/', '_') + '.json'
    return os.path.exists(os.path.join(OUTPUT_DIR, journal_id, filename))


def load_progress() -> dict:
    """Load the per-journal progress map from PROGRESS_FILE (empty if absent)."""
    if not os.path.exists(PROGRESS_FILE):
        return {}
    with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_progress(progress: dict) -> None:
    """
    Persist the progress map atomically (write to a temp file, then replace),
    so an interruption mid-write can never corrupt the existing progress file.
    """
    tmp = PROGRESS_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)
    os.replace(tmp, PROGRESS_FILE)


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


def record_progress(progress: dict, journal_id: str, page: int, total: int,
                    received_count: int, status: str) -> None:
    """Update and persist the progress entry for one journal after a page."""
    progress[journal_id] = {
        'status': status,                      # 'in_progress' or 'done'
        'last_page': page,                     # last fully completed list page
        'articles_saved': total,               # cumulative articles saved
        'received_count': received_count,      # cumulative articles with a received date
        'min_year': MIN_YEAR,                  # threshold this progress reflects
        'updated_at': datetime.now().isoformat(),
    }
    save_progress(progress)


def process_journal(fetcher: Fetcher, journal_id: str, name: str, progress: dict) -> None:
    # Resume from where a previous run left off. last_page is the last *fully
    # completed* page, so we continue at the next one; cumulative counts carry over.
    record = progress.get(journal_id, {})
    page = record.get('last_page', 0) + 1
    total = record.get('articles_saved', 0)
    received_count = record.get('received_count', 0)

    if page > 1:
        logging.info(f"Resuming journal [{journal_id}] {name} from page {page} ({total} already saved).")
    else:
        logging.info(f"Processing journal [{journal_id}] {name}")

    while True:
        article_stubs, stop = get_article_links(fetcher, journal_id, page)

        for stub in article_stubs:
            if MAX_ARTICLES_PER_JOURNAL is not None and total >= MAX_ARTICLES_PER_JOURNAL:
                logging.info(f"  Reached MAX_ARTICLES_PER_JOURNAL ({MAX_ARTICLES_PER_JOURNAL}) — stopping.")
                # Mark the previous page as the last completed one (this page is partial).
                record_progress(progress, journal_id, page - 1, total, received_count, 'in_progress')
                return

            doi = stub['doi']

            if already_downloaded(journal_id, doi):
                logging.info(f"    Skipping already downloaded: {doi}")
                continue

            article_url = f"{BASE_URL}/article/{doi}"
            logging.info(f"    Fetching article: {article_url}")
            content = fetcher.fetch(article_url)
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
            fetcher.sleep()

        done = stop or not article_stubs
        # When we stop (boundary reached or empty page), the current page was only
        # PARTIALLY processed: articles older than MIN_YEAR on it were intentionally
        # skipped. So record the *previous* page as the last fully completed one.
        # That way a later iteration that lowers MIN_YEAR (and flips this journal
        # back to 'in_progress') resumes by re-fetching this boundary page and picks
        # up the now-in-range older articles — already_downloaded() skips the half
        # already saved — instead of jumping past them. A fully in-range page (not
        # done) is complete, so we record it as-is.
        completed_page = page - 1 if done else page
        record_progress(progress, journal_id, completed_page, total, received_count,
                         'done' if done else 'in_progress')

        if done:
            break

        page += 1
        fetcher.sleep()

    logging.info(f"  Done. {total} articles saved for journal [{journal_id}] ({received_count} with received date).")

    # If no article had a received date, the journal likely never publishes it
    if total > 0 and received_count == 0:
        suggest_skip(journal_id, name)


def main():
    config = read_config('../../../.env')
    db = Postgress(
        server=config['POSTGRES_SERVER'],
        database=config['POSTGRES_DB'],
        user=config['POSTGRES_USER'],
        password=config['POSTGRES_PASSWORD']
    )

    journals = db.execute_query_result("SELECT journal_id, name FROM springer.journals ORDER BY journal_id")
    logging.info(f"Found {len(journals)} journals to process.")

    skip_ids = load_skip_journals()
    progress = load_progress()
    fetcher = Fetcher()

    try:
        for journal in journals:
            journal_id = journal['journal_id']
            if journal_id in skip_ids:
                logging.info(f"Skipping journal [{journal_id}] {journal['name']} (in skip list).")
                continue
            if progress.get(journal_id, {}).get('status') == 'done':
                logging.info(
                    f"Skipping completed journal [{journal_id}] {journal['name']} "
                    f"(done at min_year={progress[journal_id].get('min_year')})."
                )
                continue
            try:
                process_journal(fetcher, journal_id, journal['name'], progress)
            except BlockedException as e:
                logging.error(f"BLOCKED by Springer: {e}")
                logging.error("Stopping the run to avoid saving corrupt data. Progress is saved — resume later.")
                break
            #break # Remove this break to process all journals in a real run. It's here to limit scope during testing.
    except KeyboardInterrupt:
        logging.info("Interrupted by user (Ctrl+C). Progress is saved — safe to resume.")
    finally:
        fetcher.close()

    logging.info("All journals processed.")


if __name__ == '__main__':
    main()
