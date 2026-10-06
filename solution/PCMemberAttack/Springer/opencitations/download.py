import argparse
import configparser
import logging
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SOLUTION_DIR = Path(__file__).resolve().parents[3]

# figshare article id -> version of the OpenCitations dumps used
DATASETS = {
    'index': (24356626, 8),       # OpenCitations Index, citation data (CSV)
    'omid': (24427156, 4),        # OMID index: identifiers (DOI, ...) per OMID
}
CHUNK = 8 * 1024 * 1024


def read_config(path) -> configparser.SectionProxy:
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']


def list_files(article_id: int, version: int) -> list:
    url = f"https://api.figshare.com/v2/articles/{article_id}/versions/{version}"
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.json()['files']


def download(file: dict, target: Path):
    size = target.stat().st_size if target.exists() else 0
    if size == file['size']:
        logging.info(f"{target.name} already complete")
        return
    headers = {'Range': f'bytes={size}-'} if size else {}
    with requests.get(file['download_url'], headers=headers, stream=True, timeout=120) as response:
        response.raise_for_status()
        if size and response.status_code != 206:
            size = 0
        with open(target, 'ab' if size else 'wb') as f:
            for chunk in response.iter_content(CHUNK):
                f.write(chunk)
    if target.stat().st_size != file['size']:
        raise IOError(f"{target.name}: expected {file['size']} bytes, got {target.stat().st_size}")
    logging.info(f"{target.name} downloaded")


def main():
    config = read_config(SOLUTION_DIR / '.env')
    parser = argparse.ArgumentParser(description="Download the OpenCitations dumps from figshare")
    parser.add_argument('datasets', nargs='*', default=list(DATASETS), choices=list(DATASETS))
    parser.add_argument('--output-dir', type=Path, default=Path(config['RAW_DATA']) / 'opencitations')
    args = parser.parse_args()

    for name in args.datasets:
        article_id, version = DATASETS[name]
        target_dir = args.output_dir / name
        target_dir.mkdir(parents=True, exist_ok=True)
        files = list_files(article_id, version)
        logging.info(f"{name}: {len(files)} files, {sum(f['size'] for f in files) / 1e9:.1f} GB")
        for i, file in enumerate(files, start=1):
            logging.info(f"{name} {i}/{len(files)}: {file['name']}")
            download(file, target_dir / file['name'])


if __name__ == '__main__':
    main()
