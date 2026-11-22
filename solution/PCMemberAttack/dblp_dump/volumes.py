import argparse
import configparser
import datetime
import gzip
import logging
import re
from collections import Counter, defaultdict
from pathlib import Path

from database import Postgress, Saver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCHEMA = 'dblp_dump'
CORE_RANKS = ('A*', 'A', 'B')
LNCS = 'Lecture Notes in Computer Science'
SOURCES = ('lncs', 'acm')

SOLUTION_DIR = Path(__file__).resolve().parents[2]
DEFAULT_DUMP = SOLUTION_DIR / 'dblp.nt' / 'dblp.nt'
ACM_DATA_DIR = SOLUTION_DIR / 'PCMemberAttack' / 'ACM' / 'data'
ACM_ISBN = re.compile(r'_([0-9X]+(?:-[0-9X]+)+)\.json$')
FILE_YEAR = re.compile(r"'(\d{2})(?!\d)|\b((?:19|20)\d{2})\b")
VOLUME_NUMBER = re.compile(r'-\d+$')

RECORD_PREFIX = b'<https://dblp.org/rec/conf/'
TRIPLE = re.compile(r'^<https://dblp\.org/rec/(\S+)> <https://dblp\.org/rdf/schema#(\w+)> (.+) \.$')
PREDICATES = {
    'publishedInSeries', 'publishedInSeriesVolume', 'publishedInStream',
    'yearOfPublication', 'doi', 'publishedAsPartOf', 'isbn',
}


def read_config(path) -> configparser.SectionProxy:
    logging.info('Reading configuration')
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']


config = read_config(SOLUTION_DIR / '.env')
db = Postgress(
    server=config['POSTGRES_SERVER'],
    database=config['POSTGRES_DB'],
    user=config['POSTGRES_USER'],
    password=config['POSTGRES_PASSWORD']
    )
saver = Saver(db)


def create_indexes(table: str, columns: tuple):
    for column in columns:
        db.execute_query(f'CREATE INDEX ON "{SCHEMA}"."{table}" ("{column}")')


def volume_table(source: str) -> str:
    return f'{source}_volume'


def isbn13(isbn: str) -> str:
    """'urn:isbn:1-59593-016-8' or '1-59593-016-8' -> '9781595930163'."""
    digits = re.sub(r'[^0-9X]', '', isbn.upper().replace('URN:ISBN:', ''))
    if len(digits) != 10:
        return digits
    body = '978' + digits[:9]
    check = (10 - sum(int(d) * (3 if i % 2 else 1) for i, d in enumerate(body)) % 10) % 10
    return body + str(check)


def acm_isbns(data_dir: Path) -> dict:
    """ISBN-13 -> the front matter JSON files of that ISBN (the ACM DL reuses some ISBNs)."""
    isbns, without = defaultdict(list), 0
    for path in sorted(data_dir.glob('*.json')):
        m = ACM_ISBN.search(path.name)
        if m:
            isbns[isbn13(m.group(1))].append(path.name)
        else:
            without += 1
    logging.info(f"{sum(map(len, isbns.values()))} ACM front matters with an ISBN, {without} without")
    return isbns


def file_year(filename: str):
    """"AAMAS '08 Proceedings ..." -> 2008, "CATS 2011 Proceedings ..." -> 2011, else None."""
    m = FILE_YEAR.search(filename.rsplit('_', 1)[0])
    if m is None:
        return None
    if m.group(1):
        year = int(m.group(1))
        return year + (1900 if year > 50 else 2000)
    return int(m.group(2))


def file_acronym(filename: str) -> str:
    """"SAGT'10 Proceedings ..." -> 'sagt'."""
    return re.sub(r'[^a-z]', '', re.split(r"[\s']", filename, 1)[0].lower())


def record_years(dblp_key: str, record: dict) -> set:
    years = {int(y) for y in record.get('yearOfPublication', [])}
    m = re.match(r'(\d{4})', dblp_key.rsplit('/', 1)[-1])
    return years | ({int(m.group(1))} if m else set())


def acronyms(record: dict, conferences: dict) -> set:
    streams = [v.replace('https://dblp.org/streams/', '') for v in record.get('publishedInStream', [])]
    names = {s.rsplit('/', 1)[-1] for s in streams}
    names |= {conferences[s]['core_acronym'] for s in streams if s in conferences}
    return {re.sub(r'[^a-z]', '', n.lower()) for n in names}


def in_core(record: dict, conferences: dict) -> bool:
    return any(v.replace('https://dblp.org/streams/', '') in conferences
               for v in record.get('publishedInStream', []))


def assign_front_matter(volumes: dict, papers: Counter, isbns: dict, conferences: dict) -> dict:
    """
    dblp key -> its front matter. A file is a candidate for a volume with its ISBN and, when the
    file name has a year, of that year. Several candidates are narrowed to those whose acronym is
    the conference's; a volume still without exactly one gets none. A file that still has volumes
    in several editions keeps the edition whose acronym is the conference's, else the one with the
    most papers.
    """
    front_matter, dropped = {}, Counter()
    for dblp_key, record in volumes.items():
        files = {f for i in record.get('isbn', []) for f in isbns.get(isbn13(i), [])}
        files = {f for f in files if file_year(f) in record_years(dblp_key, record) | {None}}
        if len(files) > 1:
            files = {f for f in files if file_acronym(f) in acronyms(record, conferences)}
        if len(files) == 1:
            front_matter[dblp_key] = files.pop()
        else:
            dropped['no file' if not files else 'several files'] += 1

    editions_of = defaultdict(lambda: defaultdict(list))
    for dblp_key, filename in front_matter.items():
        editions_of[filename][VOLUME_NUMBER.sub('', dblp_key)].append(dblp_key)
    for filename, editions in editions_of.items():
        if len(editions) == 1:
            continue
        def rank(edition):
            keys = editions[edition]
            matched = any(file_acronym(filename) in acronyms(volumes[k], conferences) for k in keys)
            return matched, sum(papers.get(k, 0) for k in keys)
        keep = max(editions, key=rank)
        for edition, keys in editions.items():
            if edition != keep:
                for k in keys:
                    del front_matter[k]
                dropped['other edition of its file'] += len(keys)
    logging.info(f"{len(front_matter)} volumes with a front matter; left out: {dict(dropped)}")
    return front_matter


def get_core_conferences() -> dict:
    logging.info("Fetch CORE conferences from database")
    ranks = ", ".join(f"'{r}'" for r in CORE_RANKS)
    # 'https://dblp.uni-trier.de/db/conf/esorics' -> 'conf/esorics'
    query = f"""
    SELECT "Rank" AS core_rank,
           "Acronym" AS core_acronym,
           "Title" AS core_title,
           regexp_replace(
               regexp_replace("DBLP", '^https?://dblp\\.(uni-trier\\.de|org)/db/', ''),
               '(/index\\.html|/)$', '') AS stream
    FROM core.conf_ranks
    WHERE "Rank" IN ({ranks})
      AND "DBLP" IS NOT NULL
      AND "DBLP" <> 'none'
    """
    return {r['stream']: r for r in db.execute_query_result(query)}


def read_dump(path: Path, keep) -> tuple:
    """
    Returns the conference volumes in the dump for which keep(record) is true, plus the number
    of papers per volume. Assumes the triples of a record are contiguous.
    """
    logging.info(f"Reading {path}")
    opener = gzip.open if path.suffix == '.gz' else open
    volumes = {}
    papers = Counter()
    subject, record = None, {}

    def flush():
        if subject is not None and keep(record):
            volumes[subject] = record

    with opener(path, 'rb') as f:
        for line in f:
            if not line.startswith(RECORD_PREFIX):
                continue
            m = TRIPLE.match(line.decode('utf-8').rstrip())
            if m is None or m.group(2) not in PREDICATES:
                continue
            s, predicate, obj = m.groups()
            value = obj.split('"')[1] if obj.startswith('"') else obj.strip('<>')
            if predicate == 'publishedAsPartOf':
                papers[value.replace('https://dblp.org/rec/', '')] += 1
                continue
            if s != subject:
                flush()
                subject, record = s, {}
            record.setdefault(predicate, []).append(value)
    flush()
    logging.info(f"Found {len(volumes)} conference volumes")
    return volumes, papers


def to_rows(volumes: dict, papers: Counter, conferences: dict, source: str, front_matter: dict = None) -> list:
    """front_matter: dblp key -> front matter file, for ACM volumes; LNCS volumes get their volume number instead."""
    timestamp = str(datetime.datetime.now())
    rows = []
    for dblp_key, record in volumes.items():
        streams = [v.replace('https://dblp.org/streams/', '') for v in record.get('publishedInStream', [])]
        doi = record.get('doi', [None])[0]
        for stream in streams:
            if stream not in conferences:
                continue
            conference = conferences[stream]
            row = {
                'dblp_key': dblp_key,
                'stream': stream,
                'core_rank': conference['core_rank'],
                'core_acronym': conference['core_acronym'],
                'core_title': conference['core_title'],
                'year': record.get('yearOfPublication', [None])[0],
                'doi': doi.replace('https://doi.org/', '') if doi else None,
                'cnt_papers': str(papers.get(dblp_key, 0)),
                '$_extract_dts': timestamp,
                '$_rec_src': source,
            }
            if front_matter is None:
                row['lncs_volume'] = record.get('publishedInSeriesVolume', [None])[0]
            else:
                row['front_matter'] = front_matter[dblp_key]
            rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser(description="Load the LNCS or ACM volumes of CORE conferences from the dblp RDF dump")
    parser.add_argument('--source', choices=SOURCES, default='lncs')
    parser.add_argument('--dump', type=Path, default=DEFAULT_DUMP, help="dblp.nt or dblp.nt.gz")
    parser.add_argument('--acm-data', type=Path, default=ACM_DATA_DIR, help="the ACM front matter JSON files")
    args = parser.parse_args()

    conferences = get_core_conferences()
    if args.source == 'lncs':
        front_matter = None
        volumes, papers = read_dump(args.dump, lambda record: LNCS in record.get('publishedInSeries', []))
    else:
        isbns = acm_isbns(args.acm_data)
        volumes, papers = read_dump(args.dump, lambda record: (
            LNCS not in record.get('publishedInSeries', [])
            and any(isbn13(i) in isbns for i in record.get('isbn', []))))
        volumes = {k: r for k, r in volumes.items() if in_core(r, conferences)}
        front_matter = assign_front_matter(volumes, papers, isbns, conferences)
        volumes = {k: r for k, r in volumes.items() if k in front_matter}
    rows = to_rows(volumes, papers, conferences, args.dump.name, front_matter)
    logging.info(f"{len(rows)} volumes belong to CORE {', '.join(CORE_RANKS)} conferences")

    table = volume_table(args.source)
    db.execute_query(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')
    saver.save(SCHEMA, table, rows)
    create_indexes(table, ('dblp_key', 'stream'))
    logging.info("Done")


if __name__ == '__main__':
    main()
