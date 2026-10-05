import argparse
import configparser
import datetime
import logging
import re
import time
from pathlib import Path

import requests

from database import Postgress, Saver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCHEMA = 'springer_lncs'
TABLE = 'download_process_info'
CORE_RANKS = ('A*', 'A', 'B')
MIN_YEAR = 2000
SLEEP_SECONDS = 1
MAX_RETRIES = 5

BASE_URL = 'https://link.springer.com'
SOLUTION_DIR = Path(__file__).resolve().parents[3]
FRONT_MATTER_LINK = re.compile(r'/content/pdf/bfm:[^"/]+/1(?=")')

SUCCEEDED = 'SUCCEEDED'
NO_FRONT_MATTER = 'NO_FRONT_MATTER'
FAILED = 'FAILED'


class BlockedError(Exception):
    pass


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


def get_workload() -> list:
    """
    Returns the volumes in scope that have not been processed successfully.
    Volumes without front matter are not retried.
    """
    ranks = ", ".join(f"'{r}'" for r in CORE_RANKS)
    query = f"""
    SELECT DISTINCT dblp_key, doi, year
    FROM dblp_dump.lncs_volume
    WHERE core_rank IN ({ranks})
      AND CAST(year AS INTEGER) >= {MIN_YEAR}
      AND doi IS NOT NULL
    """
    if db.table_exists(SCHEMA, TABLE):
        query += f"""
      AND dblp_key NOT IN (
          SELECT "$_dblp_key"
          FROM "{SCHEMA}"."{TABLE}"
          WHERE status IN ('{SUCCEEDED}', '{NO_FRONT_MATTER}')
      )
        """
    query += " ORDER BY year, dblp_key"
    return db.execute_query_result(query)


def get(session, url) -> requests.Response:
    wait = 30
    for _ in range(MAX_RETRIES):
        response = session.get(url, timeout=60)
        if response.status_code != 429:
            if '<title>Client Challenge</title>' in response.text[:5000]:
                raise BlockedError(f"Springer served a bot challenge for {url}")
            return response
        logging.warning(f"Got 429, waiting {wait}s")
        time.sleep(wait)
        wait *= 2
    raise BlockedError(f"Still rate limited after {MAX_RETRIES} attempts: {url}")


def output_filename(dblp_key) -> str:
    # The pdf parser and analysis queries map the filename back to the dblp key
    return dblp_key.replace('/', '_').strip() + '.pdf'


def download_front_matter(session, workitem, output_dir) -> dict:
    result = {
        '$_dblp_key': workitem['dblp_key'],
        'doi': workitem['doi'],
        'book_url': f"{BASE_URL}/book/{workitem['doi'].lower()}",
        'download_dts': str(datetime.datetime.now()),
    }
    try:
        book = get(session, result['book_url'])
        if book.status_code == 404:
            book = get(session, f"https://doi.org/{workitem['doi']}")
            result['book_url'] = book.url
        if book.status_code == 404:
            result['status'] = NO_FRONT_MATTER
            result['error_message'] = 'Book page not found'
            return result
        book.raise_for_status()

        links = FRONT_MATTER_LINK.findall(book.text)
        if not links:
            result['status'] = NO_FRONT_MATTER
            result['error_message'] = 'No front matter link on book page'
            return result

        result['front_matter_url'] = BASE_URL + links[0]
        time.sleep(SLEEP_SECONDS)
        pdf = get(session, result['front_matter_url'])
        pdf.raise_for_status()
        if not pdf.content.startswith(b'%PDF'):
            raise ValueError(f"Response is not a PDF ({pdf.headers.get('content-type')})")

        location = output_dir / output_filename(workitem['dblp_key'])
        location.write_bytes(pdf.content)
        result['location'] = str(location)
        result['status'] = SUCCEEDED
    except BlockedError:
        raise
    except Exception as e:
        result['status'] = FAILED
        result['error_message'] = f"{type(e).__name__}: {e}"
    return result


def main():
    parser = argparse.ArgumentParser(description="Download LNCS front matter PDFs from Springer")
    parser.add_argument('--output-dir', type=Path,
                        default=Path(config['RAW_DATA']) / config['LNCS_FRONT_MATTER_SUBDIR'].strip('/'))
    parser.add_argument('--limit', type=int, help="Process at most this many volumes")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    workload = get_workload()
    if args.limit:
        workload = workload[:args.limit]
    logging.info(f"{len(workload)} volumes to process, writing to {args.output_dir}")

    session = requests.Session()
    counts = {}
    for i, workitem in enumerate(workload, start=1):
        try:
            result = download_front_matter(session, workitem, args.output_dir)
        except BlockedError as e:
            logging.error(f"{e}. Stopping; rerun later to continue.")
            break
        saver.save(SCHEMA, TABLE, [result])
        counts[result['status']] = counts.get(result['status'], 0) + 1
        if result['status'] != SUCCEEDED:
            logging.warning(f"{workitem['dblp_key']}: {result['status']} - {result.get('error_message')}")
        logging.info(f"{i}/{len(workload)} {workitem['dblp_key']}: {result['status']}")
        time.sleep(SLEEP_SECONDS)
    logging.info(f"Done: {counts}")


if __name__ == '__main__':
    main()
