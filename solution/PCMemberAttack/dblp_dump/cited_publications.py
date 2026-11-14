import argparse
import datetime
import logging
from pathlib import Path

from articles import author_rows, read_records
from volumes import DEFAULT_DUMP, SCHEMA, create_indexes, db, saver

PUBLICATION_TABLE = 'cited_publication'
AUTHOR_TABLE = 'cited_publication_author'
ALL_RECORDS = b'<https://dblp.org/rec/'
BATCH = 100000


def get_cited_dois() -> set:
    """The DOIs cited in every reference table of opencitations_dump (LNCS and ACM)."""
    dois = set()
    for table in ('reference', 'acm_reference'):
        if not db.table_exists('opencitations_dump', table):
            logging.warning(f"opencitations_dump.{table} does not exist: its cited works are not loaded")
            continue
        query = f'SELECT DISTINCT cited_dois FROM opencitations_dump.{table} WHERE cited_dois IS NOT NULL'
        dois.update(doi.lower() for r in db.execute_query_result(query) for doi in r['cited_dois'].split())
    return dois


def doi_of(record: dict) -> str:
    return record.get('doi', '').replace('https://doi.org/', '').lower()


def save(table: str, rows: list):
    db.execute_query(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')
    for start in range(0, len(rows), BATCH):
        saver.save(SCHEMA, table, rows[start:start + BATCH])


def main():
    parser = argparse.ArgumentParser(description="Load the dblp records (with authors) of the DOIs cited in opencitations_dump.reference and acm_reference")
    parser.add_argument('--dump', type=Path, default=DEFAULT_DUMP, help="dblp.nt or dblp.nt.gz")
    args = parser.parse_args()

    cited_dois = get_cited_dois()
    records = read_records(args.dump, lambda record: doi_of(record) in cited_dois, ALL_RECORDS)
    logging.info(f"{len(records)} dblp records for {len(cited_dois)} cited DOIs")

    timestamp = str(datetime.datetime.now())
    publications, authors = [], []
    for dblp_key, record, signatures in records:
        publications.append({
            'dblp_key': dblp_key,
            'doi': doi_of(record),
            'title': record.get('title'),
            'year': record.get('yearOfPublication'),
            'type': record.get('bibtexType', '').rpartition('#')[2] or None,
            'venue': record.get('publishedIn'),
            'cnt_authors': str(len(signatures)),
            '$_extract_dts': timestamp,
            '$_rec_src': args.dump.name,
        })
        authors.extend(author_rows(dblp_key, signatures, 'publication_dblp_key', timestamp, args.dump.name))

    save(PUBLICATION_TABLE, publications)
    save(AUTHOR_TABLE, authors)
    create_indexes(PUBLICATION_TABLE, ('dblp_key', 'doi'))
    create_indexes(AUTHOR_TABLE, ('publication_dblp_key', 'dblp_pid'))
    logging.info(f"Saved {len(publications)} publications and {len(authors)} authorships")


if __name__ == '__main__':
    main()
