import argparse
import configparser
import datetime
import gzip
import logging
import re
from collections import Counter
from pathlib import Path

from database import Postgress, Saver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCHEMA = 'dblp_dump'
TABLE = 'lncs_volume'
CORE_RANKS = ('A*', 'A', 'B')
LNCS = 'Lecture Notes in Computer Science'

SOLUTION_DIR = Path(__file__).resolve().parents[3]
DEFAULT_DUMP = SOLUTION_DIR / 'dblp.nt' / 'dblp.nt'

RECORD_PREFIX = b'<https://dblp.org/rec/conf/'
TRIPLE = re.compile(r'^<https://dblp\.org/rec/(\S+)> <https://dblp\.org/rdf/schema#(\w+)> (.+) \.$')
PREDICATES = {
    'publishedInSeries', 'publishedInSeriesVolume', 'publishedInStream',
    'yearOfPublication', 'doi', 'publishedAsPartOf',
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


def read_dump(path: Path) -> tuple:
    """
    Returns the LNCS volumes of conferences in the dump, plus the number of
    papers per volume. Assumes the triples of a record are contiguous.
    """
    logging.info(f"Reading {path}")
    opener = gzip.open if path.suffix == '.gz' else open
    volumes = {}
    papers = Counter()
    subject, record = None, {}

    def flush():
        if LNCS in record.get('publishedInSeries', []):
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
    logging.info(f"Found {len(volumes)} LNCS conference volumes")
    return volumes, papers


def to_rows(volumes: dict, papers: Counter, conferences: dict, source: str) -> list:
    timestamp = str(datetime.datetime.now())
    rows = []
    for dblp_key, record in volumes.items():
        streams = [v.replace('https://dblp.org/streams/', '') for v in record.get('publishedInStream', [])]
        doi = record.get('doi', [None])[0]
        for stream in streams:
            if stream not in conferences:
                continue
            conference = conferences[stream]
            rows.append({
                'dblp_key': dblp_key,
                'stream': stream,
                'core_rank': conference['core_rank'],
                'core_acronym': conference['core_acronym'],
                'core_title': conference['core_title'],
                'year': record.get('yearOfPublication', [None])[0],
                'doi': doi.replace('https://doi.org/', '') if doi else None,
                'lncs_volume': record.get('publishedInSeriesVolume', [None])[0],
                'cnt_papers': str(papers.get(dblp_key, 0)),
                '$_extract_dts': timestamp,
                '$_rec_src': source,
            })
    return rows


def main():
    parser = argparse.ArgumentParser(description="Load LNCS volumes of CORE conferences from the dblp RDF dump")
    parser.add_argument('--dump', type=Path, default=DEFAULT_DUMP, help="dblp.nt or dblp.nt.gz")
    args = parser.parse_args()

    conferences = get_core_conferences()
    volumes, papers = read_dump(args.dump)
    rows = to_rows(volumes, papers, conferences, args.dump.name)
    logging.info(f"{len(rows)} volumes belong to CORE {', '.join(CORE_RANKS)} conferences")

    db.execute_query(f'DROP TABLE IF EXISTS "{SCHEMA}"."{TABLE}"')
    saver.save(SCHEMA, TABLE, rows)
    create_indexes(TABLE, ('dblp_key', 'stream'))
    logging.info("Done")


if __name__ == '__main__':
    main()
