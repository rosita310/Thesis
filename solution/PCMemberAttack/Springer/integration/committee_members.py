import configparser
import datetime
import logging
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

from database import Postgress, Saver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCHEMA = 'lncs_front_matter'
TABLE = 'committee_member'
BATCH = 100000

SOLUTION_DIR = Path(__file__).resolve().parents[3]

PROGRAM = r'(program+e?|technical\s+program+e?|scientific|pc)'
COMMITTEE = r'(commit+e+s?|board|members?)'
# First match wins, so the order matters: a "Program Committee Chair" is a chair.
ROLE_CATEGORIES = [
    ('other', re.compile(r'preface|welcome|message|foreword', re.I)),
    ('pc_chair', re.compile(rf'\b{PROGRAM}\b.*\bchair|\bpc\s*(co-?)?chair|\bprogram+e?\s+(co-?)?chairs?\b', re.I)),
    ('senior_pc', re.compile(r'\barea\s+chair|\btrack\s+chair|\bsenior\s+(program+e?\s+committee|pc)\b|\bmeta-?reviewer', re.I)),
    ('pc', re.compile(rf'\b{PROGRAM}\s*{COMMITTEE}|\bpc\b|^track\b|^programme?\s*$', re.I)),
    ('reviewer', re.compile(r'review(er|ing)|referee|sub-?reviewer', re.I)),
]
COUNTRIES = sorted([
    'usa', 'us', 'united states', 'uk', 'united kingdom', 'germany', 'france', 'italy', 'spain',
    'portugal', 'the netherlands', 'netherlands', 'belgium', 'luxembourg', 'switzerland', 'austria',
    'denmark', 'sweden', 'norway', 'finland', 'iceland', 'ireland', 'poland', 'czech republic',
    'czech', 'czechia', 'slovakia', 'hungary', 'romania', 'bulgaria', 'greece', 'cyprus', 'malta',
    'slovenia', 'croatia', 'serbia', 'estonia', 'latvia', 'lithuania', 'ukraine', 'russia', 'turkey',
    'israel', 'iran', 'india', 'pakistan', 'bangladesh', 'china', 'pr china', 'hong kong', 'macau',
    'taiwan', 'japan', 'korea', 'south korea', 'singapore', 'malaysia', 'thailand', 'vietnam',
    'indonesia', 'philippines', 'australia', 'new zealand', 'canada', 'mexico', 'brazil',
    'argentina', 'chile', 'colombia', 'uruguay', 'peru', 'south africa', 'egypt', 'tunisia',
    'morocco', 'algeria', 'nigeria', 'saudi arabia', 'qatar', 'uae', 'united arab emirates',
], key=len, reverse=True)
HOMONYM_NUMBER = re.compile(r' \d{4}$')
# Letters without a Unicode decomposition; the dotless ı also appears in PDF text for í
UNDECOMPOSABLE = str.maketrans({'ı': 'i', 'ł': 'l', 'ø': 'o', 'đ': 'd', 'ð': 'd', 'þ': 'th', 'ß': 'ss', 'æ': 'ae', 'œ': 'oe'})
VOLUME_NUMBER = re.compile(r'-\d+$')


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


def role_category(role: str) -> str:
    role = re.sub(r'\s+', ' ', role or '').strip()
    for category, pattern in ROLE_CATEGORIES:
        if pattern.search(role):
            return category
    return 'other'


def name_key(name: str) -> str:
    """'José-Luis  Pérez, Jr.' -> 'jose luis perez jr'"""
    name = unicodedata.normalize('NFKD', HOMONYM_NUMBER.sub('', name or ''))
    name = ''.join(c for c in name if not unicodedata.combining(c)).lower().translate(UNDECOMPOSABLE)
    return ' '.join(re.sub(r'[^\w\s]|_', ' ', name).split())


def edition_of(dblp_key: str) -> str:
    """'conf/eccv/2020-12' -> 'conf/eccv/2020'; workshop volumes keep their own key."""
    return VOLUME_NUMBER.sub('', dblp_key)


def most_common(counter: Counter):
    values = [(n, v) for v, n in counter.items() if v]
    return max(values)[1] if values else None


def read_volumes() -> dict:
    """filename -> volumes (a volume shared by two conferences has a row for each)."""
    volumes = defaultdict(list)
    for r in db.execute_query_result("""
        SELECT DISTINCT dblp_key, stream, core_rank, core_acronym, year
        FROM dblp_dump.lncs_volume
    """):
        volumes[r['dblp_key'].replace('/', '_') + '.pdf'].append(r)
    return volumes


def read_members() -> list:
    return db.execute_query_result("""
        SELECT f.filename, m.role, m.name, m.affiliation
        FROM lncs_front_matter.member m
        JOIN lncs_front_matter.section s ON s.id = m.section_id
        JOIN lncs_front_matter.file f ON f.id = s.file_id
        WHERE f.run_id = (SELECT MAX(run_id) FROM lncs_front_matter.file)
    """)


def without_country(key: str):
    """'india naveen prakash' -> 'naveen prakash' (the parser sometimes prefixes the country of the line above)."""
    for country in COUNTRIES:
        if key.startswith(country + ' ') and len(key.split()) - len(country.split()) >= 2:
            return key[len(country) + 1:]
    return None


def without_initials(key: str):
    """'michael j wooldridge' -> 'michael wooldridge'; None when fewer than two full name parts remain."""
    parts = [p for p in key.split() if len(p) > 1]
    return ' '.join(parts) if len(parts) >= 2 else None


class NamePool:
    """dblp persons by normalised name, within a scope (a conference stream, or None for all of dblp)."""

    def __init__(self):
        self.names = {}

    def _add(self, key, pid):
        current = self.names.get(key)
        if current is None:
            self.names[key] = pid
        elif isinstance(current, str):
            if current != pid:
                self.names[key] = {current, pid}
        else:
            current.add(pid)

    def add(self, scope, name: str, pid: str):
        key = name_key(name)
        self._add((scope, False, key), pid)
        reduced = without_initials(key)
        if reduced:
            self._add((scope, True, reduced), pid)

    def get(self, scope, key: str, reduced: bool) -> set:
        found = self.names.get((scope, reduced, key))
        if found is None:
            return set()
        return {found} if isinstance(found, str) else found


def read_dblp_names() -> tuple:
    """
    Persons by name per conference stream (the authors of its LNCS papers, as printed on the
    paper) and over all of dblp (every primary and alias name). Also the primary name per person.
    """
    stream_pool, dblp_pool, primary_name = NamePool(), NamePool(), {}
    for r in db.execute_query_result("""
        SELECT DISTINCT v.stream, a.dblp_pid, a.dblp_name
        FROM dblp_dump.lncs_article_author a
        JOIN dblp_dump.lncs_article p ON p.dblp_key = a.article_dblp_key
        JOIN dblp_dump.lncs_volume v ON v.dblp_key = p.volume_dblp_key
    """):
        stream_pool.add(r['stream'], r['dblp_name'], r['dblp_pid'])
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT dblp_pid, name, is_primary FROM dblp_dump.person_name")
        for pid, name, is_primary in cursor:
            dblp_pool.add(None, name, pid)
            if is_primary == '1':
                primary_name[pid] = name
    return stream_pool, dblp_pool, primary_name


def match(stream: str, key: str, stream_pool: NamePool, dblp_pool: NamePool) -> tuple:
    """
    Tries the name as printed, without a leading country, and without initials; each first among
    the authors of the same conference, then in all of dblp. The first variant that finds anyone
    decides: one person is a match, several persons with that name is ambiguous (no pid chosen).
    Returns (pid, status, rule).
    """
    country_free = without_country(key)
    variants = (('exact', key, False), ('without_country', country_free, False),
                ('without_initials', key, True), ('without_country_initials', country_free, True))
    for rule, variant, reduced in variants:
        if variant and reduced:
            variant = without_initials(variant)
        if not variant:
            continue
        for pool_name, pool, scope in (('stream', stream_pool, stream), ('dblp', dblp_pool, None)):
            candidates = pool.get(scope, variant, reduced)
            if len(candidates) == 1:
                return next(iter(candidates)), f'matched_{pool_name}', rule
            if len(candidates) > 1:
                return None, f'ambiguous_{pool_name}', rule
    return None, 'not_found', None


def main():
    volumes = read_volumes()
    members = read_members()
    logging.info(f"{len(members)} parsed members")

    editions = {}
    for m in members:
        key = name_key(m['name'])
        if not key:
            continue
        category = role_category(m['role'])
        for volume in volumes.get(m['filename'], []):
            edition_key = edition_of(volume['dblp_key'])
            entry = editions.setdefault((volume['stream'], edition_key, category, key), {
                'volume': volume, 'roles': Counter(), 'names': Counter(),
                'affiliations': Counter(), 'files': set(),
            })
            entry['roles'][m['role']] += 1
            entry['names'][m['name']] += 1
            entry['affiliations'][m['affiliation']] += 1
            entry['files'].add(m['filename'])
    logging.info(f"{len(editions)} members after deduplication per edition")
    del members

    stream_pool, dblp_pool, primary_name = read_dblp_names()
    timestamp = str(datetime.datetime.now())
    rows = []
    for (stream, edition_key, category, key), entry in editions.items():
        pid, status, rule = match(stream, key, stream_pool, dblp_pool)
        volume = entry['volume']
        rows.append({
            'edition_key': edition_key,
            'stream': stream,
            'core_rank': volume['core_rank'],
            'core_acronym': volume['core_acronym'],
            'year': volume['year'],
            'role_category': category,
            'role': most_common(entry['roles']),
            'name': most_common(entry['names']),
            'name_key': key,
            'affiliation': most_common(entry['affiliations']),
            'dblp_pid': pid,
            'dblp_name': primary_name.get(pid),
            'match_status': status,
            'match_rule': rule,
            'cnt_volumes': str(len(entry['files'])),
            '$_extract_dts': timestamp,
            '$_rec_src': 'lncs_front_matter.member, dblp_dump',
        })
    logging.info(f"Match status: {Counter((r['match_status'], r['match_rule']) for r in rows)}")

    db.execute_query(f'DROP TABLE IF EXISTS "{SCHEMA}"."{TABLE}"')
    for start in range(0, len(rows), BATCH):
        saver.save(SCHEMA, TABLE, rows[start:start + BATCH])
    for column in ('edition_key', 'stream', 'dblp_pid', 'name_key'):
        db.execute_query(f'CREATE INDEX ON "{SCHEMA}"."{TABLE}" ("{column}")')
    logging.info("Done")


if __name__ == '__main__':
    main()
