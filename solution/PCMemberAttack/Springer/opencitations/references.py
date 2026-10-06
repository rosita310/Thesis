import argparse
import configparser
import datetime
import io
import logging
import zipfile
from multiprocessing import Pool
from pathlib import Path

from database import Postgress, Saver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCHEMA = 'opencitations_dump'
TABLE = 'reference'
BATCH = 100000

SOLUTION_DIR = Path(__file__).resolve().parents[3]


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


def get_citing_dois() -> set:
    query = "SELECT DISTINCT LOWER(doi) AS doi FROM dblp_dump.lncs_article WHERE doi IS NOT NULL"
    return {r['doi'] for r in db.execute_query_result(query)}


def csv_lines(zip_path: Path):
    with zipfile.ZipFile(zip_path) as archive:
        for name in archive.namelist():
            if name.endswith('.csv'):
                with archive.open(name) as f:
                    yield from io.TextIOWrapper(f, encoding='utf-8')


def read_omid_index(omid_dir: Path, dois: set = None, omids: set = None) -> dict:
    """
    Reads the OMID index ("doi:10.1007/...,omid:br/06..." per line). Returns doi -> omids for the
    given DOIs, or omid -> dois for the given OMIDs (e.g. a publisher and a repository DOI).
    """
    result = {}
    for zip_path in sorted(omid_dir.glob('meta_br*.zip')):
        logging.info(f"Reading {zip_path.name}")
        for line in csv_lines(zip_path):
            if not line.startswith('doi:'):
                continue
            pid, _, omid_part = line.rstrip('\n').partition(',')
            doi = pid[4:].lower()
            line_omids = [o.replace('omid:', '') for o in omid_part.strip('"').split()]
            if dois is not None and doi in dois:
                result.setdefault(doi, set()).update(line_omids)
            if omids is not None:
                for omid in line_omids:
                    if omid in omids and doi not in result.setdefault(omid, []):
                        result[omid].append(doi)
    return result


_citing = None


def _init_worker(citing_omids):
    global _citing
    _citing = citing_omids


def scan_index_file(zip_path: Path) -> list:
    rows = []
    for line in csv_lines(zip_path):
        parts = line.rstrip('\n').split(',')
        if len(parts) == 7 and parts[1][5:] in _citing:
            rows.append(parts)
    return rows


def scan_index(index_dir: Path, citing_omids: set, processes: int) -> list:
    files = sorted(index_dir.glob('*.zip'))
    logging.info(f"Scanning {len(files)} index files with {processes} processes")
    rows = []
    with Pool(processes, initializer=_init_worker, initargs=(citing_omids,)) as pool:
        for i, file_rows in enumerate(pool.imap_unordered(scan_index_file, files), start=1):
            rows.extend(file_rows)
            logging.info(f"{i}/{len(files)} index files scanned, {len(rows)} citations found")
    return rows


def main():
    parser = argparse.ArgumentParser(description="Load the OpenCitations citations made by the LNCS papers in dblp_dump.lncs_article")
    parser.add_argument('--data-dir', type=Path, default=Path(config['RAW_DATA']) / 'opencitations')
    parser.add_argument('--processes', type=int, default=8)
    args = parser.parse_args()
    source = f"OpenCitations Index CSV ({args.data_dir / 'index'}), OMID index ({args.data_dir / 'omid'})"

    citing_dois = get_citing_dois()
    doi_omids = read_omid_index(args.data_dir / 'omid', dois=citing_dois)
    logging.info(f"{len(doi_omids)} of {len(citing_dois)} LNCS DOIs have an OMID")
    omid_doi = {omid: doi for doi, omids in doi_omids.items() for omid in omids}

    citations = scan_index(args.data_dir / 'index', set(omid_doi), args.processes)
    cited_omids = {parts[2][5:] for parts in citations}
    cited_dois = read_omid_index(args.data_dir / 'omid', omids=cited_omids)
    logging.info(f"{len(cited_dois)} of {len(cited_omids)} cited OMIDs have a DOI")

    timestamp = str(datetime.datetime.now())
    rows = [{
        'oci': oci.replace('oci:', ''),
        'citing': omid_doi[citing[5:]],
        'cited': cited_dois.get(cited[5:], [None])[0],
        'cited_dois': ' '.join(cited_dois.get(cited[5:], [])) or None,
        'citing_omid': citing[5:],
        'cited_omid': cited[5:],
        'creation': creation,
        'timespan': timespan,
        'journal_sc': journal_sc,
        'author_sc': author_sc,
        '$_extract_dts': timestamp,
        '$_rec_src': source,
    } for oci, citing, cited, creation, timespan, journal_sc, author_sc in citations]

    db.execute_query(f'DROP TABLE IF EXISTS "{SCHEMA}"."{TABLE}"')
    for start in range(0, len(rows), BATCH):
        saver.save(SCHEMA, TABLE, rows[start:start + BATCH])
        logging.info(f"Saved {min(start + BATCH, len(rows))} of {len(rows)} citations")
    logging.info("Done")


if __name__ == '__main__':
    main()
