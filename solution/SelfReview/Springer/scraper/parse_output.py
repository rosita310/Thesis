from __future__ import annotations

import configparser
import json
import logging
import os
from datetime import date, datetime
import sys

from database import Postgress

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCHEMA = "springer"
DATA_DIR = "data"
LOGS_DIR = "logs"

# With ~400k articles expected, BATCH_SIZE=500 means ~800 DB flush cycles —
# a good balance between memory use and number of round-trips.
BATCH_SIZE = 500


# ---------------------------------------------------------------------------
# Logging setup — both terminal and a timestamped file in springer/logs/
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
# Table setup
# ---------------------------------------------------------------------------

def ensure_schema_and_tables(db: Postgress) -> None:
    """Create schema and all four tables with correct column types if they don't exist."""
    if not db.schema_exists(SCHEMA):
        db.create_schema(SCHEMA)

    db.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."articles" (
            doi                 TEXT,
            journal_id          TEXT,
            title               TEXT,
            received            DATE,
            accepted            DATE,
            published           DATE,
            fallback_date_label TEXT,
            fallback_date_value DATE,
            open_access         BOOLEAN,
            article_type        TEXT,
            review_days         INTEGER,
            volume              INTEGER,
            first_page          INTEGER,
            last_page           INTEGER,
            issn                TEXT,
            retrieved_at        TEXT
        )
    """)
    db.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."authors" (
            doi        TEXT,
            journal_id TEXT,
            position   TEXT,
            name       TEXT
        )
    """)
    db.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."affiliations" (
            doi               TEXT,
            journal_id        TEXT,
            affiliation_index TEXT,
            institution       TEXT
        )
    """)
    db.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."affiliation_authors" (
            doi               TEXT,
            journal_id        TEXT,
            affiliation_index TEXT,
            author            TEXT
        )
    """)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def str_or_none(v) -> str | None:
    if v is None:
        return None
    return str(v)


def parse_date(v) -> date | None:
    if not v:
        return None
    try:
        return date.fromisoformat(str(v))
    except ValueError:
        return None


def parse_int(v) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def parse_bool(v) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    return str(v).lower() in ('true', '1', 'yes')


def parse_file(path: str) -> tuple[dict, list[dict], list[dict], list[dict]]:
    """Parse one JSON file and return rows for all four target tables."""
    with open(path, encoding='utf-8') as f:
        data = json.load(f)

    doi        = data.get('doi')
    journal_id = data.get('journal_id')
    received   = parse_date(data.get('received'))
    accepted   = parse_date(data.get('accepted'))

    article = {
        'doi':                  str_or_none(doi),
        'journal_id':           str_or_none(journal_id),
        'title':                str_or_none(data.get('title')),
        'received':             received,
        'accepted':             accepted,
        'published':            parse_date(data.get('published')),
        'fallback_date_label':  str_or_none(data.get('fallback_date_label')),
        'fallback_date_value':  parse_date(data.get('fallback_date_value')),
        'open_access':          parse_bool(data.get('open_access')),
        'review_days':          (accepted - received).days if received and accepted else None,
        'article_type':         str_or_none(data.get('article_type')),
        'volume':               parse_int(data.get('volume')),
        'first_page':           parse_int(data.get('first_page')),
        'last_page':            parse_int(data.get('last_page')),
        'issn':                 str_or_none(data.get('issn')),
        'retrieved_at':         str_or_none(data.get('retrieved_at')),
    }

    authors = [
        {
            'doi':        str_or_none(doi),
            'journal_id': str_or_none(journal_id),
            'position':   str(i),
            'name':       str_or_none(name),
        }
        for i, name in enumerate(data.get('authors') or [])
    ]

    affiliations = []
    affiliation_authors = []
    for i, aff in enumerate(data.get('affiliations') or []):
        affiliations.append({
            'doi':               str_or_none(doi),
            'journal_id':        str_or_none(journal_id),
            'affiliation_index': str(i),
            'institution':       str_or_none(aff.get('institution')),
        })
        for author in aff.get('authors') or []:
            affiliation_authors.append({
                'doi':               str_or_none(doi),
                'journal_id':        str_or_none(journal_id),
                'affiliation_index': str(i),
                'author':            str_or_none(author),
            })

    return article, authors, affiliations, affiliation_authors


def collect_all_files(data_dir: str) -> list[str]:
    """Recursively collect all .json files under data_dir, sorted by path."""
    files = []
    for root, _, filenames in os.walk(data_dir):
        for fn in filenames:
            if fn.endswith('.json'):
                files.append(os.path.join(root, fn))
    files.sort()
    return files


def doi_from_filename(path: str) -> str:
    """Derive the original DOI from a JSON filename.

    The downloader saves files as doi.replace('/', '_') + '.json'.
    A DOI contains exactly one slash (e.g. 10.1007/s10015-026-01123-8),
    so reversing is safe: replace the first underscore with a slash.

    Example:
        data/10015/10.1007_s10015-026-01123-8.json  ->  10.1007/s10015-026-01123-8
    """
    stem = os.path.splitext(os.path.basename(path))[0]  # e.g. 10.1007_s10015-026-01123-8
    return stem.replace('_', '/', 1)


def load_processed_dois(db: Postgress) -> set[str]:
    """Return the set of DOIs already present in springer.articles.

    Fetched once at startup so each file can be checked in O(1)
    without any extra database round-trips.
    Returns an empty set if the table does not exist yet.
    """
    if not db.table_exists(SCHEMA, 'articles'):
        return set()
    rows = db.execute_query_result(f'SELECT doi FROM "{SCHEMA}"."articles" WHERE doi IS NOT NULL')
    dois = {row['doi'] for row in rows}
    logging.info(f"Loaded {len(dois)} already-processed DOIs from the database.")
    return dois


# ---------------------------------------------------------------------------
# DB flush — all four tables written in one atomic transaction
# ---------------------------------------------------------------------------

def flush(
    db: Postgress,
    articles: list[dict],
    authors: list[dict],
    affiliations: list[dict],
    affiliation_authors: list[dict],
    total_flushed: int,
) -> int:
    """Write all four tables atomically and return updated total count."""
    if not articles:
        return total_flushed

    db.insert_atomic([
        (SCHEMA, 'articles',            articles),
        (SCHEMA, 'authors',             authors),
        (SCHEMA, 'affiliations',        affiliations),
        (SCHEMA, 'affiliation_authors', affiliation_authors),
    ])

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

    config = read_config('../../../.env')
    db = Postgress(
        server=config['POSTGRES_SERVER'],
        database=config['POSTGRES_DB'],
        user=config['POSTGRES_USER'],
        password=config['POSTGRES_PASSWORD'],
    )

    ensure_schema_and_tables(db)

    files = collect_all_files(DATA_DIR)
    processed_dois = load_processed_dois(db)
    already_done = sum(1 for f in files if doi_from_filename(f) in processed_dois)
    logging.info(
        f"Found {len(files)} JSON files. "
        f"{already_done} already in database, {len(files) - already_done} to process "
        f"(batch size: {BATCH_SIZE})."
    )

    batch_articles: list[dict] = []
    batch_authors: list[dict] = []
    batch_affiliations: list[dict] = []
    batch_affiliation_authors: list[dict] = []

    total_flushed = 0
    skipped = 0

    for i, path in enumerate(files, 1):
        doi = doi_from_filename(path)
        if doi in processed_dois:
            continue

        logging.info(f"[{i}/{len(files)}] Parsing: {path}")
        try:
            article, authors, affiliations, affiliation_authors = parse_file(path)
        except Exception as e:
            logging.warning(f"  Skipping {path} — parse error: {e}")
            skipped += 1
            continue

        batch_articles.append(article)
        batch_authors.extend(authors)
        batch_affiliations.extend(affiliations)
        batch_affiliation_authors.extend(affiliation_authors)

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
        f"Done. {total_flushed} articles written to database. "
        f"{skipped} files skipped due to parse errors."
    )


if __name__ == '__main__':
    main()
