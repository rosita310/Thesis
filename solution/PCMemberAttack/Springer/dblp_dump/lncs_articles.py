import argparse
import datetime
import gzip
import logging
import re
from pathlib import Path

from lncs_volumes import DEFAULT_DUMP, SCHEMA, db, saver

ARTICLE_TABLE = 'lncs_article'
AUTHOR_TABLE = 'lncs_article_author'

RECORD_PREFIX = b'<https://dblp.org/rec/conf/'
SIGNATURE_PREFIX = b'_:Sig_'
RECORD = re.compile(r'^<https://dblp\.org/rec/(\S+)> <https://dblp\.org/rdf/schema#(\w+)> (.+) \.$')
SIGNATURE = re.compile(r'^_:(Sig_\S+) <https://dblp\.org/rdf/schema#(\w+)> (.+) \.$')
RECORD_PREDICATES = {'publishedAsPartOf', 'doi', 'title', 'yearOfPublication', 'pagination', 'hasSignature'}
SIGNATURE_PREDICATES = {'signatureDblpName', 'signatureCreator', 'signatureOrdinal', 'signatureOrcid'}

LITERAL = re.compile(r'^"((?:[^"\\]|\\.)*)"')
ESCAPE = re.compile(r'\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|.)')
ESCAPES = {'t': '\t', 'b': '\b', 'n': '\n', 'r': '\r', 'f': '\f', '"': '"', "'": "'", '\\': '\\'}
HOMONYM_NUMBER = re.compile(r' \d{4}$')


def unescape(match) -> str:
    code = match.group(1)
    if code[0] in 'uU':
        return chr(int(code[1:], 16))
    return ESCAPES.get(code, code)


def value_of(obj: str) -> str:
    literal = LITERAL.match(obj)
    if literal:
        return ESCAPE.sub(unescape, literal.group(1))
    return obj.strip('<>')


def get_volume_keys() -> set:
    query = f'SELECT DISTINCT dblp_key FROM "{SCHEMA}"."lncs_volume"'
    return {r['dblp_key'] for r in db.execute_query_result(query)}


def read_dump(path: Path, volume_keys: set) -> list:
    """
    Returns the papers that are part of one of the given volumes, with their signatures (authors).
    Assumes the triples of a record, including its signatures, are contiguous.
    """
    logging.info(f"Reading {path}")
    opener = gzip.open if path.suffix == '.gz' else open
    articles = []
    subject, record, signatures = None, {}, {}

    def flush():
        volume = record.get('publishedAsPartOf', '').replace('https://dblp.org/rec/', '')
        if volume in volume_keys:
            articles.append((subject, volume, record, signatures))

    with opener(path, 'rb') as f:
        for line in f:
            if line.startswith(RECORD_PREFIX):
                m = RECORD.match(line.decode('utf-8').rstrip())
                if m is None or m.group(2) not in RECORD_PREDICATES:
                    continue
                s, predicate, obj = m.groups()
                if s != subject:
                    flush()
                    subject, record, signatures = s, {}, {}
                if predicate != 'hasSignature':
                    record[predicate] = value_of(obj)
            elif line.startswith(SIGNATURE_PREFIX):
                m = SIGNATURE.match(line.decode('utf-8').rstrip())
                if m is None or m.group(2) not in SIGNATURE_PREDICATES:
                    continue
                signature, predicate, obj = m.groups()
                signatures.setdefault(signature, {})[predicate] = value_of(obj)
    flush()
    logging.info(f"Found {len(articles)} papers in {len(volume_keys)} volumes")
    return articles


def to_rows(articles: list, source: str) -> tuple:
    timestamp = str(datetime.datetime.now())
    article_rows, author_rows = [], []
    for dblp_key, volume, record, signatures in articles:
        doi = record.get('doi')
        article_rows.append({
            'dblp_key': dblp_key,
            'volume_dblp_key': volume,
            'doi': doi.replace('https://doi.org/', '') if doi else None,
            'title': record.get('title'),
            'year': record.get('yearOfPublication'),
            'pagination': record.get('pagination'),
            'cnt_authors': str(len(signatures)),
            '$_extract_dts': timestamp,
            '$_rec_src': source,
        })
        for signature in signatures.values():
            dblp_name = signature.get('signatureDblpName')
            author_rows.append({
                'article_dblp_key': dblp_key,
                'ordinal': signature.get('signatureOrdinal'),
                'dblp_pid': signature.get('signatureCreator', '').replace('https://dblp.org/pid/', '') or None,
                'dblp_name': dblp_name,
                'name': HOMONYM_NUMBER.sub('', dblp_name) if dblp_name else None,
                'orcid': signature.get('signatureOrcid', '').replace('https://orcid.org/', '') or None,
                '$_extract_dts': timestamp,
                '$_rec_src': source,
            })
    return article_rows, author_rows


def main():
    parser = argparse.ArgumentParser(description="Load the papers of the LNCS volumes in dblp_dump.lncs_volume from the dblp RDF dump")
    parser.add_argument('--dump', type=Path, default=DEFAULT_DUMP, help="dblp.nt or dblp.nt.gz")
    args = parser.parse_args()

    articles = read_dump(args.dump, get_volume_keys())
    article_rows, author_rows = to_rows(articles, args.dump.name)
    logging.info(f"{len(article_rows)} papers, {len(author_rows)} authorships")

    for table, rows in ((ARTICLE_TABLE, article_rows), (AUTHOR_TABLE, author_rows)):
        db.execute_query(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')
        saver.save(SCHEMA, table, rows)
    logging.info("Done")


if __name__ == '__main__':
    main()
