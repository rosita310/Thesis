"""
Builds the BBN corpus for the PC member citation case study from the front matter,
dblp and OpenCitations tables in the database, of every source in SOURCES (LNCS, ACM)
together.

    python bbn_extract.py --stage citations
        Counts the citations to every cited work in the OpenCitations Index, or reads
        them from bbn/index_counts.pkl when saved for the same works (--rescan counts
        anew), and materialises SCHEMA.edition_map (volumes of one conference and year
        that list the same committee are one edition), SCHEMA.well_known (the works
        well known at an edition),
        SCHEMA.citation (one row per citing paper, cited reference and author
        of the cited work) and SCHEMA.paper (the papers with their reference
        counts).
    python bbn_extract.py --stage seats
        Writes bbn/seats.csv: one row per pc or senior_pc seat of a matched
        member, with its citations, the member's expected citations, the surplus and
        the depth (citations per citing paper over those expected).
    python bbn_extract.py --stage publications
        Writes bbn/publications.csv from the dblp dump: the publications of every
        member in bbn/seats.csv per year.
    python bbn_extract.py --stage corpus
        Writes bbn/bbn_cs3_corpus.json from bbn/seats.csv and bbn/publications.csv:
        the seats per member, each with the member's publications before its year.

The last two need no database.
"""

from __future__ import annotations

import argparse
import configparser
import csv
import io
import json
import logging
import pickle
import re
import statistics
import sys
import time
import zipfile
from array import array
from collections import Counter, defaultdict
from multiprocessing import Pool
from pathlib import Path

from cs3_config import (BBN_DIR, DUMP_PATH, ENV_PATH, GATE_REFERENCES, MAX_SPAN,
                        MIN_OFF_PAPERS, MIN_PC_SHARE, MIN_PC_SIZE, PC_SIZE_WINDOW, PUB_TYPES,
                        RATE_PSEUDO_COUNT, RATE_WINDOW, ROLES, SAME_COMMITTEE, SCAN_PROCESSES,
                        SCHEMA, SOURCES, WELL_KNOWN_MIN_WORKS, WELL_KNOWN_TOP)


def edition(column: str) -> str:
    """'conf/eccv/2020-12' -> 'conf/eccv/2020', as edition_of in committee_members.py."""
    return rf"regexp_replace({column}, '-\d+$', '')"


SOURCE_TABLES = {
    "lncs": {"volume": "dblp_dump.lncs_volume", "article": "dblp_dump.lncs_article",
             "author": "dblp_dump.lncs_article_author",
             "reference": "opencitations_dump.reference",
             "committee": "lncs_front_matter.committee_member"},
    "acm": {"volume": "dblp_dump.acm_volume", "article": "dblp_dump.acm_article",
            "author": "dblp_dump.acm_article_author",
            "reference": "opencitations_dump.acm_reference",
            "committee": "acm_front_matter.committee_member"},
}


def union(table: str, columns: str, sources=SOURCES) -> str:
    """`columns` of `table` over all sources, with the source in a column of its own."""
    return "(" + "\n    UNION ALL ".join(
        f"SELECT {columns}, '{source}' AS source FROM {SOURCE_TABLES[source][table]}"
        for source in sources) + ")"


VOLUME = union("volume", "dblp_key, stream, year, core_rank")
AUTHOR = union("author", "article_dblp_key, dblp_pid")
REFERENCE = union("reference", "citing, cited, cited_omid, cited_dois")
VOLUME_COMMITTEE = union("committee", "edition_key, role_category, dblp_pid, dblp_name, name_key")

# volume_edition (the key of edition()) -> edition_key, see same_committee_editions.
EDITION_MAP = f'"{SCHEMA}".edition_map'

ARTICLE = f"""(
    SELECT a.dblp_key, a.doi, a.source, m.edition_key
    FROM {union("article", "dblp_key, volume_dblp_key, doi")} a
    JOIN {EDITION_MAP} m ON m.volume_edition = {edition('a.volume_dblp_key')})"""

COMMITTEE = f"""(
    SELECT coalesce(m.edition_key, c.edition_key) AS edition_key, c.role_category,
           c.dblp_pid, c.dblp_name, c.name_key, c.source
    FROM {VOLUME_COMMITTEE} c
    LEFT JOIN {EDITION_MAP} m ON m.volume_edition = c.edition_key)"""

VOLUME_EDITIONS_SQL = f"SELECT DISTINCT {edition('dblp_key')} AS volume_edition FROM {VOLUME} v"

SAME_COMMITTEE_SQL = f"""
SELECT DISTINCT edition_key, dblp_pid FROM {VOLUME_COMMITTEE} c
WHERE role_category IN ({", ".join(f"'{r}'" for r in ROLES)}) AND dblp_pid IS NOT NULL
"""

# The works every edition cites.
CITED_WORKS_SQL = rf"""
SELECT DISTINCT a.edition_key, r.cited_omid
FROM {REFERENCE} r
JOIN {ARTICLE} a ON a.doi = r.citing
"""

# A cited work can have several DOIs; cited_dois holds all of them, cited only one.
CITATION_SQL = rf"""
CREATE TABLE "{SCHEMA}".citation AS
WITH ref AS (
    SELECT citing, cited, cited_omid, unnest(string_to_array(cited_dois, ' ')) AS doi
    FROM {REFERENCE} r
    WHERE cited_dois IS NOT NULL
)
SELECT
    a.dblp_key AS article_dblp_key,
    a.edition_key,
    ref.cited,
    ca.dblp_pid AS cited_pid,
    bool_or(aa.dblp_pid IS NOT NULL) AS is_self,
    bool_or(w.cited_omid IS NOT NULL) AS is_well_known
FROM ref
JOIN {ARTICLE} a ON a.doi = ref.citing
JOIN dblp_dump.cited_publication p ON p.doi = ref.doi
JOIN dblp_dump.cited_publication_author ca ON ca.publication_dblp_key = p.dblp_key
LEFT JOIN {AUTHOR} aa
    ON aa.article_dblp_key = a.dblp_key AND aa.dblp_pid = ca.dblp_pid
LEFT JOIN "{SCHEMA}".well_known w
    ON w.edition_key = a.edition_key AND w.cited_omid = ref.cited_omid
GROUP BY 1, 2, 3, 4
"""

PAPER_SQL = rf"""
CREATE TABLE "{SCHEMA}".paper AS
SELECT
    a.dblp_key AS article_dblp_key,
    a.edition_key,
    (SELECT count(*) FROM {REFERENCE} r
     WHERE r.citing = a.doi) AS n_references,
    (SELECT count(DISTINCT c.cited) FROM "{SCHEMA}".citation c
     WHERE c.article_dblp_key = a.dblp_key) AS n_resolved
FROM {ARTICLE} a
"""

INDEXES = {
    "well_known": [("edition_key", "cited_omid")],
    "citation": [("edition_key", "cited_pid"), ("cited_pid",), ("article_dblp_key",)],
    "paper": [("edition_key",), ("article_dblp_key",)],
}

VOLUMES_SQL = rf"""
SELECT m.edition_key, min(stream) AS stream,
       min(year)::int AS year, min(core_rank) AS core_rank, min(source) AS source
FROM {VOLUME} v
JOIN {EDITION_MAP} m ON m.volume_edition = {edition('v.dblp_key')}
GROUP BY 1
"""

# papers: the papers of an edition with at least one reference resolved to dblp.
EDITIONS_SQL = rf"""
SELECT v.edition_key, v.stream, v.year, v.core_rank, v.source, coalesce(p.papers, 0) AS papers
FROM ({VOLUMES_SQL}) v
LEFT JOIN (
    SELECT edition_key, count(*) AS papers FROM "{SCHEMA}".paper
    WHERE n_resolved > 0 GROUP BY 1
) p ON p.edition_key = v.edition_key
"""

COMMITTEE_SQL = f"""
SELECT DISTINCT edition_key, role_category, dblp_pid, dblp_name, name_key
FROM {COMMITTEE} c
"""

MEMBERS = f"""
member AS (
    SELECT DISTINCT dblp_pid FROM {COMMITTEE} c
    WHERE role_category IN ({", ".join(f"'{r}'" for r in ROLES)}) AND dblp_pid IS NOT NULL
)"""

# cites: references, self-citations and well-known works left out; citing: the papers they
# come from; per_paper: their cites, most first; cites_gate: all references.
CITES_SQL = f"""
WITH {MEMBERS},
per_paper AS (
    SELECT c.edition_key, c.cited_pid, c.article_dblp_key, count(*) AS refs,
           count(*) FILTER (WHERE NOT c.is_self AND NOT c.is_well_known) AS cites
    FROM "{SCHEMA}".citation c
    JOIN member m ON m.dblp_pid = c.cited_pid
    GROUP BY 1, 2, 3
)
SELECT edition_key, cited_pid AS dblp_pid, sum(cites)::int AS cites,
       count(*) FILTER (WHERE cites > 0) AS citing,
       string_agg(cites::text, ' ' ORDER BY cites DESC) FILTER (WHERE cites > 0) AS per_paper,
       sum(refs)::int AS cites_gate
FROM per_paper
GROUP BY 1, 2
"""

# The member's own papers among the edition's papers.
OWN_SQL = f"""
WITH {MEMBERS}
SELECT p.edition_key, aa.dblp_pid, count(DISTINCT p.article_dblp_key) AS own_papers
FROM "{SCHEMA}".paper p
JOIN {AUTHOR} aa ON aa.article_dblp_key = p.article_dblp_key
JOIN member m ON m.dblp_pid = aa.dblp_pid
WHERE p.n_resolved > 0
GROUP BY 1, 2
"""


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def read_config(path) -> configparser.SectionProxy:
    path = Path(path)
    if not path.exists():
        sys.exit(f"\nERROR: no .env found at {path.resolve()}\n"
                 f"Copy env-example to .env and fill in the POSTGRES_* values.\n")
    with open(path, "r") as f:
        config_string = "[SECTION]\n" + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config["SECTION"]


def connect(env_path):
    """A Postgres handle built from the .env."""
    from database import Postgress      # here, so the other scripts run without pyodbc
    config = read_config(env_path)
    return Postgress(server=config["POSTGRES_SERVER"],
                     database=config["POSTGRES_DB"],
                     user=config["POSTGRES_USER"],
                     password=config["POSTGRES_PASSWORD"])


def build_table(db, table: str, sql: str) -> None:
    start = time.time()
    db.execute_query(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')
    db.execute_query(sql)
    for columns in INDEXES[table]:
        db.execute_query(f'CREATE INDEX ON "{SCHEMA}"."{table}" '
                         f'({", ".join(columns)})')
    logging.info(f"{SCHEMA}.{table} built in {time.time() - start:.0f}s")


def read_editions(db, sql: str) -> dict[str, dict]:
    """edition -> its row, with the conference's year from the key where it has one."""
    return {r["edition_key"]: {**r, "year": conference_year(r["edition_key"]) or r["year"]}
            for r in db.execute_query_result(sql)}


def pct(part, whole) -> str:
    return f"{part / whole:6.1%}" if whole else "     -"


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        logging.warning(f"No rows for {path.name}; writing header only")
    columns = list(rows[0].keys()) if rows else []
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  wrote {path}  ({len(rows)} rows)")


# ---------------------------------------------------------------------------
# Well-known works (OpenCitations Index)
# ---------------------------------------------------------------------------

def index_citations(lines, works: dict[str, int]) -> Counter:
    """(work, year) -> citations to the works in `works` (omid -> work), from lines of the
    Index CSV: oci,citing,cited,creation,timespan,journal_sc,author_sc. Citations of an
    author of the cited work and citations without a year are left out."""
    counts = Counter()
    for line in lines:
        parts = line.rstrip("\n").split(",")
        if len(parts) != 7 or parts[6] == "yes":
            continue
        work = works.get(parts[2][len("omid:"):])
        if work is not None and parts[3][:4].isdigit():
            counts[(work, int(parts[3][:4]))] += 1
    return counts


def zip_lines(zip_path: Path):
    with zipfile.ZipFile(zip_path) as archive:
        for name in archive.namelist():
            if name.endswith(".csv"):
                with archive.open(name) as f:
                    yield from io.TextIOWrapper(f, encoding="utf-8")


_works = None


def _init_scan(works):
    global _works
    _works = works


def _scan_file(zip_path: Path) -> Counter:
    return index_citations(zip_lines(zip_path), _works)


class CitationCounts:
    """The citations of every work up to and including a year in [first_year, last_year].
    Citations before first_year count in first_year, those after last_year not at all."""

    def __init__(self, n_works: int, first_year: int, last_year: int, totals: bytes = None):
        self.first_year, self.span = first_year, last_year - first_year + 1
        self.n_works = n_works
        self.totals = (array("I", totals) if totals is not None
                       else array("I", [0]) * (n_works * self.span))

    def add(self, counts: Counter) -> None:
        """Adds (work, year) -> citations, as from index_citations."""
        for (work, year), citations in counts.items():
            offset = max(year - self.first_year, 0)
            if offset < self.span:
                self.totals[work * self.span + offset] += citations

    def accumulate(self) -> None:
        """Turns the counts per year into counts up to and including the year."""
        for work in range(self.n_works):
            start = work * self.span
            for i in range(start + 1, start + self.span):
                self.totals[i] += self.totals[i - 1]

    def up_to(self, work: int, year: int) -> int:
        return self.totals[work * self.span + year - self.first_year]


def top_threshold(values: list[int], top: float) -> int:
    """The fewest citations of a value in the top `top` share of `values`."""
    values = sorted(values)
    return values[min(int(len(values) * (1 - top)), len(values) - 1)]


def well_known_works(cited, editions, counts: CitationCounts, top=WELL_KNOWN_TOP,
                     min_works=WELL_KNOWN_MIN_WORKS) -> set[tuple[str, int]]:
    """The (edition, work) pairs of `cited` whose work is well known at the edition: its
    citations up to the edition's year are in the top `top` of the works the stream cites
    that have a citation by then, or of the works of all streams when the stream has fewer
    than `min_works` of those."""
    works_of, years_of = defaultdict(set), defaultdict(set)
    for edition_key, work in cited:
        stream = editions[edition_key]["stream"]
        works_of[stream].add(work)
        years_of[stream].add(editions[edition_key]["year"])
    all_works = set().union(*works_of.values())
    pooled = {}

    def threshold(works, year):
        values = [citations for citations in (counts.up_to(work, year) for work in works)
                  if citations]
        return top_threshold(values, top) if values else None, len(values)

    thresholds = {}
    for stream, works in works_of.items():
        for year in years_of[stream]:
            value, n_cited = threshold(works, year)
            if n_cited < min_works:
                if year not in pooled:
                    pooled[year] = threshold(all_works, year)[0]
                value = pooled[year]
            thresholds[(stream, year)] = value
    known = set()
    for edition_key, work in cited:
        stream, year = editions[edition_key]["stream"], editions[edition_key]["year"]
        citations = counts.up_to(work, year)
        if citations and citations >= thresholds[(stream, year)]:
            known.add((edition_key, work))
    return known


def scan_index(index_dir: Path, works: dict[str, int], counts: CitationCounts,
               processes=SCAN_PROCESSES) -> None:
    files = sorted(index_dir.glob("*.zip"))
    if not files:
        sys.exit(f"No OpenCitations Index zips in {index_dir.resolve()}; pass --index.")
    start = time.time()
    with Pool(processes, initializer=_init_scan, initargs=(works,)) as pool:
        for i, file_counts in enumerate(pool.imap_unordered(_scan_file, files), 1):
            counts.add(file_counts)
            logging.info(f"{i}/{len(files)} index files, {time.time() - start:.0f}s")


def save_counts(path: Path, omids: list[str], counts: CitationCounts) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump({"omids": omids, "first_year": counts.first_year,
                     "last_year": counts.first_year + counts.span - 1,
                     "totals": counts.totals.tobytes()}, f)


def load_counts(path: Path, omids: list[str], first_year: int,
                last_year: int) -> CitationCounts | None:
    """The counts saved by save_counts, or None when saved for other works or years."""
    if not path.exists():
        return None
    with open(path, "rb") as f:
        saved = pickle.load(f)
    if (saved["omids"], saved["first_year"], saved["last_year"]) != (omids, first_year, last_year):
        return None
    return CitationCounts(len(omids), first_year, last_year, saved["totals"])


def same_committee_editions(volume_editions, committee,
                            min_overlap=SAME_COMMITTEE) -> dict[str, str]:
    """volume edition -> edition. Editions of one conference and year whose matched
    committees overlap by at least `min_overlap` (Jaccard) are one edition, under the
    shortest of their keys: 'conf/gecco/2010c' -> 'conf/gecco/2010'."""
    pids = defaultdict(set)
    for r in committee:
        pids[r["edition_key"]].add(r["dblp_pid"])
    parent = {key: key for key in volume_editions}

    def root(key):
        while parent[key] != key:
            key = parent[key]
        return key

    conference = defaultdict(list)
    for key in volume_editions:
        if pids[key] and conference_year(key):
            conference[(key.rsplit("/", 1)[0], conference_year(key))].append(key)
    for keys in conference.values():
        keys.sort()
        for i, a in enumerate(keys):
            for b in keys[i + 1:]:
                if len(pids[a] & pids[b]) >= min_overlap * len(pids[a] | pids[b]):
                    ra, rb = root(a), root(b)
                    if ra != rb:
                        parent[max(ra, rb, key=lambda k: (len(k), k))] = \
                            min(ra, rb, key=lambda k: (len(k), k))
    return {key: root(key) for key in volume_editions}


def build_edition_map(db) -> None:
    volume_editions = [r["volume_edition"] for r in db.execute_query_result(VOLUME_EDITIONS_SQL)]
    mapping = same_committee_editions(volume_editions, db.execute_query_result(SAME_COMMITTEE_SQL))
    db.execute_query(f"DROP TABLE IF EXISTS {EDITION_MAP}")
    db.execute_query(f"CREATE TABLE {EDITION_MAP} (volume_edition text, edition_key text)")
    db.execute_many(f"INSERT INTO {EDITION_MAP} VALUES (?, ?)", sorted(mapping.items()))
    db.execute_query(f"CREATE INDEX ON {EDITION_MAP} (volume_edition)")
    merged = sum(1 for key, to in mapping.items() if key != to)
    logging.info(f"{EDITION_MAP} built: {len(mapping)} volume editions, {merged} taken "
                 f"into another edition with the same committee")


def build_well_known(db, known: set[tuple[str, str]]) -> None:
    db.execute_query(f'DROP TABLE IF EXISTS "{SCHEMA}".well_known')
    db.execute_query(f'CREATE TABLE "{SCHEMA}".well_known (edition_key text, cited_omid text)')
    db.execute_many(f'INSERT INTO "{SCHEMA}".well_known VALUES (?, ?)', sorted(known))
    for columns in INDEXES["well_known"]:
        db.execute_query(f'CREATE INDEX ON "{SCHEMA}".well_known ({", ".join(columns)})')
    logging.info(f"{SCHEMA}.well_known built: {len(known)} rows")


# ---------------------------------------------------------------------------
# Seats and the off-power rate
# ---------------------------------------------------------------------------

def pc_sizes(committee) -> dict[str, int]:
    """edition -> distinct pc + senior_pc rows, matched or not."""
    members = defaultdict(set)
    for r in committee:
        if r["role_category"] in ROLES:
            members[r["edition_key"]].add(r["dblp_pid"] or r["name_key"])
    return {edition_key: len(people) for edition_key, people in members.items()}


def complete_editions(editions, pc_size, min_size=MIN_PC_SIZE, min_share=MIN_PC_SHARE,
                      window=PC_SIZE_WINDOW) -> set[str]:
    """The editions whose parsed PC is plausibly complete: at least `min_size`, and at
    least `min_share` of the median PC of the same stream within `window` years."""
    sizes_of = defaultdict(list)
    for edition_key, size in pc_size.items():
        if edition_key in editions and size:
            sizes_of[editions[edition_key]["stream"]].append(
                (editions[edition_key]["year"], size))
    complete = set()
    for edition_key, size in pc_size.items():
        if edition_key not in editions or size < min_size:
            continue
        year = editions[edition_key]["year"]
        nearby = [other for other_year, other in sizes_of[editions[edition_key]["stream"]]
                  if abs(other_year - year) <= window]
        if size >= min_share * statistics.median(nearby):
            complete.add(edition_key)
    return complete


def is_main(edition_key: str) -> bool:
    """'conf/cpm/2007' is a main edition; 'conf/eccv/2020w' and 'conf/eccv/2024-w8' are
    satellites."""
    return bool(re.fullmatch(r"\d{2}|\d{4}", edition_key.rsplit("/", 1)[-1]))


def conference_year(edition_key: str) -> int | None:
    """The year in the key, not of publication: 'conf/caise/2006' (published 2007) -> 2006,
    'conf/sigir/82' -> 1982, 'conf/eccv/2024-w8' -> 2024."""
    match = re.match(r"(\d{4}|\d{2})(?!\d)", edition_key.rsplit("/", 1)[-1])
    if not match:
        return None
    year = int(match.group(1))
    return year + 1900 if year < 100 else year


def on_power_editions(committee) -> dict[str, set[str]]:
    """pid -> the editions where they hold a role of any kind. An unmatched row with
    one of the names the member is matched under elsewhere counts too."""
    editions_of, names_of, unmatched = defaultdict(set), defaultdict(set), defaultdict(set)
    for r in committee:
        if r["dblp_pid"]:
            editions_of[r["dblp_pid"]].add(r["edition_key"])
            names_of[r["dblp_pid"]].add(r["name_key"])
        else:
            unmatched[r["name_key"]].add(r["edition_key"])
    for pid, names in names_of.items():
        for name in names:
            editions_of[pid] |= unmatched.get(name, set())
    return dict(editions_of)


def scored_seats(committee) -> dict[tuple[str, str], dict]:
    """(edition, pid) -> role and name, for every matched seat in ROLES. A member listed
    as both pc and senior_pc holds the senior_pc seat."""
    seats = {}
    for r in committee:
        if r["role_category"] not in ROLES or not r["dblp_pid"]:
            continue
        seat = seats.setdefault((r["edition_key"], r["dblp_pid"]),
                                {"role": r["role_category"], "name": r["dblp_name"] or ""})
        if r["role_category"] == "senior_pc":
            seat["role"] = "senior_pc"
    return seats


class OffPower:
    """Citations and papers over the editions of a stream in which a member has no role.

    `cites` maps pid -> edition -> citations (self and well-known works left out), `own`
    pid -> edition -> the member's own papers among the edition's papers. Only complete
    main editions count.
    """

    def __init__(self, editions, complete, on_power, cites, own, window=RATE_WINDOW,
                 max_span=MAX_SPAN):
        self.editions, self.window, self.max_span = editions, window, max_span
        self.on_power, self.cites, self.own = on_power, cites, own
        self.by_stream = defaultdict(list)
        for edition_key in complete:
            if is_main(edition_key):
                self.by_stream[editions[edition_key]["stream"]].append(edition_key)

    def conference(self, pid, stream, year, min_papers=MIN_OFF_PAPERS,
                   counts=None) -> tuple[int, int, int]:
        """(citations, papers, span) over the stream's off-power editions within `window`
        years, and beyond that nearest first, up to `max_span` years, while they hold fewer
        than `min_papers` papers. Editions equally far are taken together; span is the
        distance in years of the furthest one taken. `counts`, in the form of `cites`, sums
        another count over the same editions."""
        cites, own = (counts or self.cites).get(pid, {}), self.own.get(pid, {})
        by_distance = defaultdict(list)
        for edition_key in self.by_stream.get(stream, ()):
            if edition_key not in self.on_power.get(pid, ()):
                by_distance[abs(self.editions[edition_key]["year"] - year)].append(edition_key)
        off_cites = off_papers = span = 0
        for distance in sorted(by_distance):
            if distance > self.max_span or (distance > self.window and off_papers >= min_papers):
                break
            for edition_key in by_distance[distance]:
                off_cites += cites.get(edition_key, 0)
                off_papers += self.editions[edition_key]["papers"] - own.get(edition_key, 0)
            span = distance
        return off_cites, off_papers, span


def off_power_rate(levels, min_papers=MIN_OFF_PAPERS, pseudo=RATE_PSEUDO_COUNT):
    """(src, cites, papers, rate) of the first (src, (cites, papers)) level with at least
    `min_papers` papers; ("", 0, 0, None) when none has."""
    for src, (cites, papers) in levels:
        if papers >= min_papers:
            return src, cites, papers, (cites + pseudo) / papers
    return "", 0, 0, None


def expected_count(conference, peers, papers):
    """(off-power level, peer level, expected) of one count of a seat, from (count, papers)
    of its off-power editions and of its peers; expected is papers times the larger rate."""
    off = off_power_rate([("conference", conference), ("peers", peers)])
    peer = off_power_rate([("peers", peers)])
    expected = None if off[3] is None else max(off[3], peer[3] or 0) * papers
    return off, peer, expected


def fit_papers(per_paper, surplus) -> int:
    """The fewest citing papers that hold the surplus; `per_paper` most first."""
    total = 0
    for n, cites in enumerate(per_paper):
        if total >= surplus:
            return n
        total += cites
    return len(per_paper)


def seat_rows(seats, editions, cites, citing, gate_cites, own, pc_size, off: OffPower,
              min_peers=MIN_PC_SIZE - 1, per_paper=None) -> list[dict]:
    """One row per seat. `citing` maps pid -> edition -> the papers citing the member,
    `gate_cites` (edition, pid) -> the references to the member, `pc_size` edition -> its
    pc + senior_pc rows, `per_paper` (edition, pid) -> the cites per citing paper, most
    first. Without enough off-power
    papers the rate is that of the other seats of the edition, when there are at least
    `min_peers`. The expected citations take the larger of the rate and that of the other
    seats; the expected citing papers likewise."""
    rows = []
    for (edition_key, pid), seat in sorted(seats.items()):
        ed = editions[edition_key]
        rows.append({
            "edition_key": edition_key, "stream": ed["stream"], "year": ed["year"],
            "core_rank": ed["core_rank"] or "", "source": ed["source"], "dblp_pid": pid,
            "name": seat["name"],
            "role": seat["role"], "cites": cites.get(pid, {}).get(edition_key, 0),
            "citing": citing.get(pid, {}).get(edition_key, 0),
            "cites_gate": gate_cites.get((edition_key, pid), 0),
            "papers": ed["papers"] - own.get(pid, {}).get(edition_key, 0),
            "pc_size": pc_size.get(edition_key, 0),
        })
    totals = defaultdict(lambda: [0, 0, 0, 0])
    for r in rows:
        total = totals[r["edition_key"]]
        total[0] += r["cites"]
        total[1] += r["citing"]
        total[2] += r["papers"]
        total[3] += 1
    for r in rows:
        conf_cites, conf_papers, span = off.conference(r["dblp_pid"], r["stream"], r["year"])
        conf_citing = off.conference(r["dblp_pid"], r["stream"], r["year"], counts=citing)[0]
        edition_cites, edition_citing, edition_papers, n_seats = totals[r["edition_key"]]
        with_peers = n_seats - 1 >= min_peers
        peers = (edition_cites - r["cites"], edition_papers - r["papers"]) if with_peers else (0, 0)
        peers_citing = ((edition_citing - r["citing"], edition_papers - r["papers"])
                        if with_peers else (0, 0))
        off_level, peer_level, expected = expected_count((conf_cites, conf_papers), peers,
                                                         r["papers"])
        src, off_cites, off_papers, rate = off_level
        _, peer_cites, peer_papers, peer_rate = peer_level
        *_, expected_citing = expected_count((conf_citing, conf_papers), peers_citing, r["papers"])
        depth = ((r["cites"] / r["citing"]) / (expected / expected_citing)
                 if r["citing"] and expected else None)
        r.update({
            "src": src, "span": span if src == "conference" else "",
            "off_cites": off_cites, "off_papers": off_papers,
            "rate": "" if rate is None else round(rate, 6),
            "peer_cites": peer_cites, "peer_papers": peer_papers,
            "peer_rate": "" if peer_rate is None else round(peer_rate, 6),
            "expected": "" if expected is None else round(expected, 4),
            "surplus": "" if expected is None else round(r["cites"] - expected, 4),
            "fit_papers": "" if expected is None else fit_papers(
                (per_paper or {}).get((r["edition_key"], r["dblp_pid"]), []),
                r["cites"] - expected),
            "expected_citing": "" if expected_citing is None else round(expected_citing, 4),
            "depth": "" if depth is None else round(depth, 4),
        })
    return rows


def gated_members(rows, gate=GATE_REFERENCES) -> dict[str, list[dict]]:
    """pid -> their seats with more than `gate` references (Westerbaan's count)."""
    gated = defaultdict(list)
    for r in rows:
        if int(r["cites_gate"]) > gate:
            gated[r["dblp_pid"]].append(r)
    return dict(gated)


# ---------------------------------------------------------------------------
# Publications (dblp dump)
# ---------------------------------------------------------------------------

REC_PREFIX = "<https://dblp.org/rec/"
PID_PREFIX = "<https://dblp.org/pid/"
P_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
P_AUTHORED_BY = "<https://dblp.org/rdf/schema#authoredBy>"
P_YEAR = "<https://dblp.org/rdf/schema#yearOfPublication>"
PROGRESS_EVERY = 100_000_000   # lines


def credit(counts, is_pub, year, authors) -> None:
    if is_pub and year:
        for pid in authors:
            counts[(pid, year)] += 1


def publication_counts(lines, wanted, types=PUB_TYPES) -> Counter:
    """(pid, year) -> the records of one of `types` that pid authored, for the pids in
    `wanted`. The triples of a record are contiguous in the dump."""
    types = {f"<https://dblp.org/rdf/schema#{rdf_type}>" for rdf_type in types}
    counts = Counter()
    subject, is_pub, year, authors = None, False, None, set()
    for line in lines:
        if not line.startswith(REC_PREFIX):
            continue
        record, predicate, obj = line.split(" ", 2)
        if record != subject:
            credit(counts, is_pub, year, authors)
            subject, is_pub, year, authors = record, False, None, set()
        if predicate == P_TYPE:
            is_pub = is_pub or obj[:obj.find(">") + 1] in types
        elif predicate == P_AUTHORED_BY and obj.startswith(PID_PREFIX):
            pid = obj[len(PID_PREFIX):obj.find(">")]
            if pid in wanted:
                authors.add(pid)
        elif predicate == P_YEAR:
            year = int(obj[1:5])
    credit(counts, is_pub, year, authors)
    return counts


def with_progress(lines, every=PROGRESS_EVERY):
    start = time.time()
    for i, line in enumerate(lines, 1):
        if i % every == 0:
            logging.info(f"{i / 1e6:.0f}M lines, {time.time() - start:.0f}s")
        yield line


def pubs_before(pubs_by_year: dict, year: int) -> int:
    return sum(pubs for pub_year, pubs in pubs_by_year.items() if pub_year < year)


# ---------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------

SEAT_INTS = ["year", "cites", "citing", "cites_gate", "papers", "pc_size", "off_cites",
             "off_papers", "fit_papers"]
SEAT_FLOATS = ["rate", "expected", "expected_citing"]


def build_corpus(rows, pubs=None) -> dict:
    """The seats of seats.csv per member. Seats without a rate or without papers are left
    out of the evidence; the member's largest cites_gate is over all their seats.
    `pubs` maps pid -> year -> dblp publications; a seat gets those before its year."""
    pubs = pubs or {}
    members, dropped = {}, Counter()
    n_seats = 0
    for r in rows:
        n_seats += 1
        member = members.setdefault(r["dblp_pid"],
                                    {"name": "", "cites_gate_max": 0, "seats": []})
        member["name"] = member["name"] or r["name"]
        member["cites_gate_max"] = max(member["cites_gate_max"],
                                             int(r["cites_gate"]))
        if r["rate"] == "":
            dropped["rate"] += 1
            continue
        if int(r["papers"]) == 0:
            dropped["papers"] += 1
            continue
        seat = {key: r[key] for key in ("edition_key", "stream", "core_rank", "source", "role",
                                        "src")}
        seat.update({key: int(r[key]) for key in SEAT_INTS})
        seat.update({key: float(r[key]) for key in SEAT_FLOATS})
        seat["pubs"] = pubs_before(pubs.get(r["dblp_pid"], {}), seat["year"])
        member["seats"].append(seat)
    kept = {pid: member for pid, member in members.items() if member["seats"]}
    funnel = {"seats": n_seats, "seats without a rate": dropped["rate"],
              "seats without papers": dropped["papers"],
              "seats kept": sum(len(member["seats"]) for member in kept.values()),
              "members": len(members), "members with a seat kept": len(kept)}
    return {"model": "cs3-seat-surplus", "funnel": funnel, "members": kept}


# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------

def stage_citations(db, index_dir: Path, counts_path: Path, rescan: bool) -> None:
    db.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
    build_edition_map(db)
    editions = read_editions(db, VOLUMES_SQL)
    pairs = [(r["edition_key"], r["cited_omid"])
             for r in db.execute_query_result(CITED_WORKS_SQL) if r["edition_key"] in editions]
    omids = sorted({omid for _, omid in pairs})
    works = {omid: work for work, omid in enumerate(omids)}
    years = [ed["year"] for ed in editions.values()]
    logging.info(f"{len(omids)} cited works, {len(pairs)} edition-work pairs")
    counts = None if rescan else load_counts(counts_path, omids, min(years), max(years))
    if counts:
        logging.info(f"Index counts read from {counts_path}")
    else:
        counts = CitationCounts(len(omids), min(years), max(years))
        scan_index(index_dir, works, counts)
        counts.accumulate()
        save_counts(counts_path, omids, counts)
    known = well_known_works([(edition_key, works[omid]) for edition_key, omid in pairs],
                             editions, counts)
    build_well_known(db, {(edition_key, omids[work]) for edition_key, work in known})
    build_table(db, "citation", CITATION_SQL)
    build_table(db, "paper", PAPER_SQL)

    citations = db.execute_query_result(f"""
        SELECT count(*) AS rows, count(DISTINCT (article_dblp_key, cited)) AS refs,
               count(DISTINCT article_dblp_key) AS citing, count(DISTINCT cited_pid) AS pids,
               avg(is_self::int) AS self_share, avg(is_well_known::int) AS well_known_share
        FROM "{SCHEMA}".citation""")[0]
    papers = db.execute_query_result(f"""
        SELECT count(*) AS papers, count(*) FILTER (WHERE n_references > 0) AS in_oc,
               count(*) FILTER (WHERE n_resolved > 0) AS resolved
        FROM "{SCHEMA}".paper""")[0]
    logging.info(f"well_known: {len({work for _, work in known})} of {len(omids)} works "
                 f"well known at an edition, {len(known)} of {len(pairs)} pairs")
    logging.info(f"citation: {citations['rows']} rows, {citations['refs']} references from "
                 f"{citations['citing']} papers to {citations['pids']} authors, self share "
                 f"{float(citations['self_share']):.3f}, well-known share "
                 f"{float(citations['well_known_share']):.3f}")
    logging.info(f"paper: {papers['papers']} papers, {papers['in_oc']} with references in "
                 f"OpenCitations, {papers['resolved']} with a reference resolved to dblp")


def print_seats_summary(rows, editions, pc_size, complete, n_unknown) -> None:
    with_committee = [edition_key for edition_key in pc_size if edition_key in editions]
    members = {r["dblp_pid"] for r in rows}
    by_src = Counter(r["src"] for r in rows)
    gated = gated_members(rows)
    years = Counter(min(len({r["year"] for r in seats}), 3) for seats in gated.values())
    n_gated_seats = sum(len(seats) for seats in gated.values())

    print(f"\n=== SEATS: {len(rows)} seats of {len(members)} members ===")
    print(f"  editions with a PC                   {len(with_committee):>7}")
    print(f"  ...plausibly complete                {len(complete):>7}  "
          f"({pct(len(complete), len(with_committee))})")
    n_main = sum(1 for edition_key in complete if is_main(edition_key))
    print(f"  ...of which main editions (off-power) {n_main:>6}  "
          f"({pct(n_main, len(with_committee))})")
    if n_unknown:
        print(f"  seats of an edition without volumes  {n_unknown:>7}  (left out)")
    for role, count in sorted(Counter(r["role"] for r in rows).items()):
        print(f"  role {role:<32}{count:>7}")
    n_zero = sum(1 for r in rows if r["papers"] == 0)
    print(f"  seats without papers                 {n_zero:>7}  ({pct(n_zero, len(rows))})")
    for src, label in (("conference", "same stream"), ("peers", "PC peers"), ("", "none")):
        print(f"  rate from {label:<27}{by_src[src]:>7}  ({pct(by_src[src], len(rows))})")
    n_floor = sum(1 for r in rows if r["src"] == "conference" and r["peer_rate"] != ""
                  and r["peer_rate"] > r["rate"])
    print(f"  ...same stream, expected from peers  {n_floor:>7}  "
          f"({pct(n_floor, by_src['conference'])})")
    spans = Counter(min(r["span"], 10) for r in rows if r["src"] == "conference")
    print("  same stream, furthest edition taken:  "
          + "  ".join(f"{span if span < 10 else '10+'}y "
                      f"{pct(count, by_src['conference']).strip()}"
                      for span, count in sorted(spans.items())))
    print(f"\n  gate: seats with more than {GATE_REFERENCES} references  {n_gated_seats:>7}")
    print(f"  ...members                           {len(gated):>7}  "
          f"({pct(len(gated), len(members))})")
    for n_years, label in ((1, "1 year"), (2, "2 years"), (3, "3 or more years")):
        print(f"  ...gated in {label:<25}{years[n_years]:>7}")


def stage_seats(db, out_dir: Path) -> None:
    start = time.time()
    editions = read_editions(db, EDITIONS_SQL)
    committee = db.execute_query_result(COMMITTEE_SQL)
    cites, citing, gate_cites, per_paper = defaultdict(dict), defaultdict(dict), {}, {}
    for r in db.execute_query_result(CITES_SQL):
        cites[r["dblp_pid"]][r["edition_key"]] = r["cites"]
        citing[r["dblp_pid"]][r["edition_key"]] = r["citing"]
        gate_cites[(r["edition_key"], r["dblp_pid"])] = r["cites_gate"]
        if r["per_paper"]:
            per_paper[(r["edition_key"], r["dblp_pid"])] = [int(n) for n in r["per_paper"].split()]
    own = defaultdict(dict)
    for r in db.execute_query_result(OWN_SQL):
        own[r["dblp_pid"]][r["edition_key"]] = r["own_papers"]
    logging.info(f"read in {time.time() - start:.0f}s")

    pc_size = pc_sizes(committee)
    complete = complete_editions(editions, pc_size)
    seats = scored_seats(committee)
    unknown = {key for key in seats if key[0] not in editions}
    seats = {key: seat for key, seat in seats.items() if key not in unknown}
    off = OffPower(editions, complete, on_power_editions(committee), cites, own)
    rows = seat_rows(seats, editions, cites, citing, gate_cites, own, pc_size, off,
                     per_paper=per_paper)

    write_csv(out_dir / "seats.csv", rows)
    print_seats_summary(rows, editions, pc_size, complete, len(unknown))


def read_rows(path: Path, stage: str) -> list[dict]:
    if not path.exists():
        sys.exit(f"Run bbn_extract.py --stage {stage} first; {path} not found.")
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def stage_publications(out_dir: Path, dump: Path) -> None:
    wanted = {r["dblp_pid"] for r in read_rows(out_dir / "seats.csv", "seats")}
    if not dump.exists():
        sys.exit(f"No dblp dump at {dump.resolve()}; pass --dump.")
    start = time.time()
    with open(dump, encoding="utf-8", errors="replace", buffering=1 << 22) as f:
        counts = publication_counts(with_progress(f), wanted)
    logging.info(f"dump read in {time.time() - start:.0f}s")
    write_csv(out_dir / "publications.csv",
              [{"dblp_pid": pid, "year": year, "pubs": pubs}
               for (pid, year), pubs in sorted(counts.items())])
    print(f"  {len({pid for pid, _ in counts})} of {len(wanted)} members have a publication "
          f"of type {', '.join(PUB_TYPES)}")


def stage_corpus(out_dir: Path) -> None:
    pubs = defaultdict(dict)
    for r in read_rows(out_dir / "publications.csv", "publications"):
        pubs[r["dblp_pid"]][int(r["year"])] = int(r["pubs"])
    corpus = build_corpus(read_rows(out_dir / "seats.csv", "seats"), pubs)
    path = out_dir / "bbn_cs3_corpus.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(corpus, f, ensure_ascii=False, separators=(",", ":"))
    print(f"  wrote {path}")
    print("\n=== CORPUS ===")
    for label, count in corpus["funnel"].items():
        print(f"  {label:<36}{count:>7}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the CS3 PC member citation BBN corpus.")
    parser.add_argument("--stage", required=True,
                        choices=["citations", "seats", "publications", "corpus"])
    parser.add_argument("--out-dir", default=str(BBN_DIR), help="where the BBN files go")
    parser.add_argument("--env", default=None,
                        help=f"path to the .env with the POSTGRES_* values (default: {ENV_PATH})")
    parser.add_argument("--dump", default=None,
                        help=f"path to the unzipped dblp .nt (default: {DUMP_PATH})")
    parser.add_argument("--index", default=None,
                        help="folder with the OpenCitations Index zips "
                             "(default: RAW_DATA/opencitations/index from the .env)")
    parser.add_argument("--rescan", action="store_true",
                        help="count the Index anew instead of reading bbn/index_counts.pkl")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    if args.stage == "publications":
        stage_publications(Path(args.out_dir), Path(args.dump) if args.dump else DUMP_PATH)
        return
    if args.stage == "corpus":
        stage_corpus(Path(args.out_dir))
        return
    env_path = Path(args.env) if args.env else ENV_PATH
    db = connect(env_path)
    if args.stage == "citations":
        index_dir = (Path(args.index) if args.index
                     else Path(read_config(env_path)["RAW_DATA"]) / "opencitations" / "index")
        stage_citations(db, index_dir, Path(args.out_dir) / "index_counts.pkl", args.rescan)
    if args.stage == "seats":
        stage_seats(db, Path(args.out_dir))


if __name__ == "__main__":
    main()
