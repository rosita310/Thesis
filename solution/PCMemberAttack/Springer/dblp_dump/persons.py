import argparse
import datetime
import gzip
import logging
import re
from pathlib import Path

from lncs_articles import value_of
from lncs_volumes import DEFAULT_DUMP, SCHEMA, create_indexes, db, saver

TABLE = 'person_name'
BATCH = 100000

PERSON_PREFIX = b'<https://dblp.org/pid/'
PERSON = re.compile(r'^<https://dblp\.org/pid/(\S+)> <https://dblp\.org/rdf/schema#(\w+)> (.+) \.$')
PREDICATES = {'primaryCreatorName', 'creatorName', 'primaryAffiliation', 'orcid'}


def read_persons(path: Path) -> list:
    """
    One row per (person, name): the primary name and every alias dblp lists for the person.
    Assumes the triples of a person are contiguous.
    """
    logging.info(f"Reading {path}")
    opener = gzip.open if path.suffix == '.gz' else open
    rows = []
    pid, person = None, {}

    def flush():
        if pid is None:
            return
        primary = person.get('primaryCreatorName')
        for name in dict.fromkeys([primary] + person.get('creatorName', [])):
            if name:
                rows.append({
                    'dblp_pid': pid,
                    'name': name,
                    'is_primary': str(int(name == primary)),
                    'orcid': person.get('orcid', '').replace('https://orcid.org/', '') or None,
                    'primary_affiliation': person.get('primaryAffiliation'),
                })

    with opener(path, 'rb') as f:
        for line in f:
            if not line.startswith(PERSON_PREFIX):
                continue
            m = PERSON.match(line.decode('utf-8').rstrip())
            if m is None or m.group(2) not in PREDICATES:
                continue
            p, predicate, obj = m.groups()
            if p != pid:
                flush()
                pid, person = p, {}
            if predicate == 'creatorName':
                person.setdefault(predicate, []).append(value_of(obj))
            else:
                person[predicate] = value_of(obj)
    flush()
    return rows


def main():
    parser = argparse.ArgumentParser(description="Load the names (primary and aliases) of all dblp persons from the dblp RDF dump")
    parser.add_argument('--dump', type=Path, default=DEFAULT_DUMP, help="dblp.nt or dblp.nt.gz")
    args = parser.parse_args()

    rows = read_persons(args.dump)
    timestamp = str(datetime.datetime.now())
    for row in rows:
        row['$_extract_dts'] = timestamp
        row['$_rec_src'] = args.dump.name
    logging.info(f"{len(rows)} names of {len({r['dblp_pid'] for r in rows})} persons")

    db.execute_query(f'DROP TABLE IF EXISTS "{SCHEMA}"."{TABLE}"')
    for start in range(0, len(rows), BATCH):
        saver.save(SCHEMA, TABLE, rows[start:start + BATCH])
        logging.info(f"Saved {min(start + BATCH, len(rows))} of {len(rows)} names")
    create_indexes(TABLE, ('dblp_pid', 'name'))
    logging.info("Done")


if __name__ == '__main__':
    main()
