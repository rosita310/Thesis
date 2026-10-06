import argparse
import datetime
import logging
from pathlib import Path

from lncs_articles import author_rows, read_records
from lncs_volumes import DEFAULT_DUMP, SCHEMA, db, saver

PUBLICATION_TABLE = 'cited_publication'
AUTHOR_TABLE = 'cited_publication_author'
ALL_RECORDS = b'<https://dblp.org/rec/'
BATCH = 100000


def get_cited_dois() -> set:
    query = 'SELECT DISTINCT cited_dois FROM opencitations_dump.reference WHERE cited_dois IS NOT NULL'
    return {doi.lower() for r in db.execute_query_result(query) for doi in r['cited_dois'].split()}


def doi_of(record: dict) -> str:
    return record.get('doi', '').replace('https://doi.org/', '').lower()


def save(table: str, rows: list):
    db.execute_query(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')
    for start in range(0, len(rows), BATCH):
        saver.save(SCHEMA, table, rows[start:start + BATCH])


def main():
    parser = argparse.ArgumentParser(description="Load the dblp records (with authors) of the DOIs cited in opencitations_dump.reference")
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
    logging.info(f"Saved {len(publications)} publications and {len(authors)} authorships")


if __name__ == '__main__':
    main()
