from __future__ import annotations

import configparser
import json
import logging
import os
from datetime import date, datetime
from pathlib import Path

from database import Postgress

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCHEMA = "acm"
DATA_DIR = "data"
LOGS_DIR = "logs"

# Batch size controls how many articles we hold in memory before sending to PostgreSQL
BATCH_SIZE = 500

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging() -> None:
    os.makedirs(LOGS_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(LOGS_DIR, f'parse_output_{timestamp}.log')

    fmt = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(),
        ],
    )
    logging.info(f"Logging to: {log_file}")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def read_config(path) -> configparser.SectionProxy:
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']

# ---------------------------------------------------------------------------
# Table setup (Aligned with ACM Data points)
# ---------------------------------------------------------------------------

def ensure_schema_and_tables(db: Postgress) -> None:
    """Create schema and all four tables with correct column types if they don't exist."""
    if not db.schema_exists(SCHEMA):
        db.create_schema(SCHEMA)

    db.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."articles" (
            doi                 TEXT,
            journal_title       TEXT,
            title               TEXT,
            received            DATE,
            accepted            DATE,
            published           DATE,
            article_type        TEXT,
            review_days         INTEGER,
            first_page          INTEGER,
            last_page           INTEGER
        )
    """)
    db.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."authors" (
            doi           TEXT,
            journal_title TEXT,
            position      TEXT,
            name          TEXT,
            orcid         TEXT
        )
    """)
    db.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."affiliations" (
            doi               TEXT,
            journal_title     TEXT,
            affiliation_index TEXT,
            institution       TEXT
        )
    """)
    db.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."affiliation_authors" (
            doi               TEXT,
            journal_title     TEXT,
            affiliation_index TEXT,
            author            TEXT
        )
    """)

# ---------------------------------------------------------------------------
# Parsing Helpers
# ---------------------------------------------------------------------------

def str_or_none(v) -> str | None:
    if v is None or str(v).strip() == "":
        return None
    return str(v).strip()

def parse_int(v) -> int | None:
    if v is None or str(v).strip() == "":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None

def parse_acm_date(v) -> date | None:
    """Converts natural language dates like ': 18 April 2024' into a standard Date object."""
    if not v:
        return None
    
    clean_str = str(v).replace(":", "").strip()
    if not clean_str:
        return None
        
    try:
        # Converts "18 April 2024" to a native python datetime object
        return datetime.strptime(clean_str, "%d %B %Y").date()
    except ValueError:
        return None

def parse_article(data: dict) -> tuple[dict, list[dict], list[dict], list[dict]]:
    """Parse one article dictionary and return rows for all four target tables."""
    
    doi = data.get('doi')
    journal_title = data.get('journal_title')
    
    received = parse_acm_date(data.get('received_date'))
    accepted = parse_acm_date(data.get('accepted_date'))
    published = parse_acm_date(data.get('published_date'))

    article = {
        'doi':             str_or_none(doi),
        'journal_title':   str_or_none(journal_title),
        'title':           str_or_none(data.get('title')),
        'received':        received,
        'accepted':        accepted,
        'published':       published,
        'review_days':     (accepted - received).days if received and accepted else None,
        'article_type':    str_or_none(data.get('article_type')),
        'first_page':      parse_int(data.get('first_page')),
        'last_page':       parse_int(data.get('last_page'))
    }

    authors = []
    affiliations = []
    affiliation_authors = []
    
    # We use a mapping dict to prevent duplicating the same institution multiple times
    # for a single article if multiple authors share the same affiliation.
    aff_map = {} 
    
    for i, auth_data in enumerate(data.get('authors') or []):
        name = str_or_none(auth_data.get('name'))
        aff_str = str_or_none(auth_data.get('affiliation'))
        orcid = str_or_none(auth_data.get('orcid'))
        
        # 1. Author record
        authors.append({
            'doi':           str_or_none(doi),
            'journal_title': str_or_none(journal_title),
            'position':      str(i + 1), # 1-based indexing for position
            'name':          name,
            'orcid':         orcid
        })
        
        # 2. Affiliation and Linking records
        if aff_str:
            if aff_str not in aff_map:
                aff_index = str(len(aff_map) + 1)
                aff_map[aff_str] = aff_index
                affiliations.append({
                    'doi':               str_or_none(doi),
                    'journal_title':     str_or_none(journal_title),
                    'affiliation_index': aff_index,
                    'institution':       aff_str
                })
            else:
                aff_index = aff_map[aff_str]
                
            affiliation_authors.append({
                'doi':               str_or_none(doi),
                'journal_title':     str_or_none(journal_title),
                'affiliation_index': aff_index,
                'author':            name
            })

    return article, authors, affiliations, affiliation_authors

# ---------------------------------------------------------------------------
# IO & DB Operations
# ---------------------------------------------------------------------------

def collect_all_files(data_dir: str) -> list[str]:
    """Recursively collect all .json files under data_dir, skipping progress.json."""
    files = []
    for root, _, filenames in os.walk(data_dir):
        for fn in filenames:
            if fn.endswith('.json') and fn != 'progress.json':
                files.append(os.path.join(root, fn))
    files.sort()
    return files

def load_processed_dois(db: Postgress) -> set[str]:
    """Return the set of DOIs already present in acm.articles to prevent duplicates."""
    if not db.table_exists(SCHEMA, 'articles'):
        return set()
    rows = db.execute_query_result(f'SELECT doi FROM "{SCHEMA}"."articles" WHERE doi IS NOT NULL')
    dois = {row['doi'] for row in rows}
    logging.info(f"Loaded {len(dois)} already-processed DOIs from the database.")
    return dois

def flush(
    db: Postgress,
    articles: list[dict],
    authors: list[dict],
    affiliations: list[dict],
    affiliation_authors: list[dict],
    total_flushed: int,
) -> int:
    """Write all four tables sequentially and return updated total count."""
    if not articles:
        return total_flushed

    # Utilizing the insert_into method from your database.py file
    db.insert_into(SCHEMA, 'articles', articles)
    if authors:
        db.insert_into(SCHEMA, 'authors', authors)
    if affiliations:
        db.insert_into(SCHEMA, 'affiliations', affiliations)
    if affiliation_authors:
        db.insert_into(SCHEMA, 'affiliation_authors', affiliation_authors)

    total_flushed += len(articles)
    logging.info(
        f"  Flushed batch: {len(articles)} articles "
        f"| {len(authors)} authors "
        f"| {len(affiliations)} affiliations "
        f"| {len(affiliation_authors)} affiliation_authors "
        f"(total articles written so far: {total_flushed})"
    )
    return total_flushed

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    setup_logging()

    # Safely locate .env using pathlib relative to this script
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / '../../.env'
    config = read_config(config_path.resolve())
    
    db = Postgress(
        server=config['POSTGRES_SERVER'],
        database=config['POSTGRES_DB'],
        user=config['POSTGRES_USER'],
        password=config['POSTGRES_PASSWORD'],
    )

    ensure_schema_and_tables(db)

    files = collect_all_files(DATA_DIR)
    processed_dois = load_processed_dois(db)
    
    logging.info(f"Found {len(files)} JSON journal files. Starting parsing sequence...")

    batch_articles: list[dict] = []
    batch_authors: list[dict] = []
    batch_affiliations: list[dict] = []
    batch_affiliation_authors: list[dict] = []

    total_flushed = 0
    skipped_articles = 0

    for i, path in enumerate(files, 1):
        logging.info(f"[{i}/{len(files)}] Reading journal file: {path}")
        
        try:
            with open(path, encoding='utf-8') as f:
                articles_data = json.load(f)
        except Exception as e:
            logging.warning(f"  Skipping {path} — File parse error: {e}")
            continue

        for article_data in articles_data:
            doi = article_data.get('doi')
            
            # Skip if DOI is completely empty or already mapped in the database
            if not doi or doi in processed_dois:
                continue

            try:
                article, authors, affiliations, affiliation_authors = parse_article(article_data)
            except Exception as e:
                logging.warning(f"  Skipping article {doi} — data extraction error: {e}")
                skipped_articles += 1
                continue

            batch_articles.append(article)
            batch_authors.extend(authors)
            batch_affiliations.extend(affiliations)
            batch_affiliation_authors.extend(affiliation_authors)

            # Flush to database when batch size is met
            if len(batch_articles) >= BATCH_SIZE:
                total_flushed = flush(
                    db,
                    batch_articles, batch_authors, batch_affiliations, batch_affiliation_authors,
                    total_flushed,
                )
                batch_articles.clear()
                batch_authors.clear()
                batch_affiliations.clear()
                batch_affiliation_authors.clear()

    # Final flush for any remaining rows
    total_flushed = flush(
        db,
        batch_articles, batch_authors, batch_affiliations, batch_affiliation_authors,
        total_flushed,
    )

    logging.info(
        f"Done. {total_flushed} distinct articles written to the database. "
        f"{skipped_articles} articles skipped due to parse errors."
    )

if __name__ == '__main__':
    main()