"""
Builds the BBN corpus for the PC member citation case study from the LNCS
front matter, dblp and OpenCitations tables in the database.

    python bbn_extract.py --stage citations
        Materialises SCHEMA.citation (one row per citing LNCS paper, cited
        reference and author of the cited work) and SCHEMA.paper (the LNCS
        papers with their reference counts).
    python bbn_extract.py --stage seats
        Writes bbn/seats.csv: one row per pc or senior_pc seat of a matched
        member, with its citations, the member's expected citations and the citation ratio.
"""

from __future__ import annotations

import argparse
import configparser
import csv
import logging
import re
import statistics
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

from cs3_config import (BBN_DIR, ENV_PATH, GATE_REFERENCES, MIN_OFF_PAPERS, MIN_PC_SHARE,
                        MIN_PC_SIZE, PC_SIZE_WINDOW, RATE_PSEUDO_COUNT, RATE_WINDOW,
                        RATIO_PSEUDO_COUNT, ROLES, SCHEMA)


def edition(column: str) -> str:
    """'conf/eccv/2020-12' -> 'conf/eccv/2020', as edition_of in committee_members.py."""
    return rf"regexp_replace({column}, '-\d+$', '')"


# A cited work can have several DOIs; cited_dois holds all of them, cited only one.
CITATION_SQL = rf"""
CREATE TABLE "{SCHEMA}".citation AS
WITH ref AS (
    SELECT citing, cited, unnest(string_to_array(cited_dois, ' ')) AS doi
    FROM opencitations_dump.reference
    WHERE cited_dois IS NOT NULL
)
SELECT DISTINCT
    a.dblp_key AS article_dblp_key,
    {edition('a.volume_dblp_key')} AS edition_key,
    ref.cited,
    ca.dblp_pid AS cited_pid,
    aa.dblp_pid IS NOT NULL AS is_self
FROM ref
JOIN dblp_dump.lncs_article a ON a.doi = ref.citing
JOIN dblp_dump.cited_publication p ON p.doi = ref.doi
JOIN dblp_dump.cited_publication_author ca ON ca.publication_dblp_key = p.dblp_key
LEFT JOIN dblp_dump.lncs_article_author aa
    ON aa.article_dblp_key = a.dblp_key AND aa.dblp_pid = ca.dblp_pid
"""

PAPER_SQL = rf"""
CREATE TABLE "{SCHEMA}".paper AS
SELECT
    a.dblp_key AS article_dblp_key,
    {edition('a.volume_dblp_key')} AS edition_key,
    (SELECT count(*) FROM opencitations_dump.reference r
     WHERE r.citing = a.doi) AS n_references,
    (SELECT count(DISTINCT c.cited) FROM "{SCHEMA}".citation c
     WHERE c.article_dblp_key = a.dblp_key) AS n_resolved
FROM dblp_dump.lncs_article a
"""

INDEXES = {
    "citation": [("edition_key", "cited_pid"), ("cited_pid",), ("article_dblp_key",)],
    "paper": [("edition_key",), ("article_dblp_key",)],
}

# n: the papers of an edition with at least one reference resolved to dblp.
EDITIONS_SQL = rf"""
SELECT v.edition_key, v.stream, v.year, v.core_rank, coalesce(p.n, 0) AS n
FROM (
    SELECT {edition('dblp_key')} AS edition_key, min(stream) AS stream,
           min(year)::int AS year, min(core_rank) AS core_rank
    FROM dblp_dump.lncs_volume
    GROUP BY 1
) v
LEFT JOIN (
    SELECT edition_key, count(*) AS n FROM "{SCHEMA}".paper
    WHERE n_resolved > 0 GROUP BY 1
) p ON p.edition_key = v.edition_key
"""

COMMITTEE_SQL = """
SELECT DISTINCT edition_key, role_category, dblp_pid, dblp_name, name_key
FROM lncs_front_matter.committee_member
"""

MEMBERS = f"""
member AS (
    SELECT DISTINCT dblp_pid FROM lncs_front_matter.committee_member
    WHERE role_category IN ({", ".join(f"'{r}'" for r in ROLES)}) AND dblp_pid IS NOT NULL
)"""

# x: references, self-citations left out; x_westerbaan: references, self included.
CITES_SQL = f"""
WITH {MEMBERS}
SELECT c.edition_key, c.cited_pid AS dblp_pid,
       count(*) FILTER (WHERE NOT c.is_self) AS x,
       count(*) AS x_westerbaan
FROM "{SCHEMA}".citation c
JOIN member m ON m.dblp_pid = c.cited_pid
GROUP BY 1, 2
"""

# The member's own papers among those counted in n.
OWN_SQL = f"""
WITH {MEMBERS}
SELECT p.edition_key, aa.dblp_pid, count(DISTINCT p.article_dblp_key) AS n_own
FROM "{SCHEMA}".paper p
JOIN dblp_dump.lncs_article_author aa ON aa.article_dblp_key = p.article_dblp_key
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
# Seats and the off-power rate
# ---------------------------------------------------------------------------

def pc_sizes(committee) -> dict[str, int]:
    """edition -> distinct pc + senior_pc rows, matched or not."""
    members = defaultdict(set)
    for r in committee:
        if r["role_category"] in ROLES:
            members[r["edition_key"]].add(r["dblp_pid"] or r["name_key"])
    return {e: len(m) for e, m in members.items()}


def complete_editions(editions, pc_size, min_size=MIN_PC_SIZE, min_share=MIN_PC_SHARE,
                      window=PC_SIZE_WINDOW) -> set[str]:
    """The editions whose parsed PC is plausibly complete: at least `min_size`, and at
    least `min_share` of the median PC of the same stream within `window` years."""
    sizes_of = defaultdict(list)
    for e, size in pc_size.items():
        if e in editions and size:
            sizes_of[editions[e]["stream"]].append((editions[e]["year"], size))
    complete = set()
    for e, size in pc_size.items():
        if e not in editions or size < min_size:
            continue
        year = editions[e]["year"]
        nearby = [s for y, s in sizes_of[editions[e]["stream"]] if abs(y - year) <= window]
        if size >= min_share * statistics.median(nearby):
            complete.add(e)
    return complete


def is_main(edition_key: str) -> bool:
    """'conf/cpm/2007' is a main edition; 'conf/eccv/2020w' and 'conf/eccv/2024-w8' are
    satellites."""
    return bool(re.fullmatch(r"\d{2}|\d{4}", edition_key.rsplit("/", 1)[-1]))


def conference_year(edition_key: str) -> int | None:
    """The year in the key, not of publication: 'conf/caise/2006' (published 2007) -> 2006,
    'conf/sigir/82' -> 1982, 'conf/eccv/2024-w8' -> 2024."""
    m = re.match(r"(\d{4}|\d{2})(?!\d)", edition_key.rsplit("/", 1)[-1])
    if not m:
        return None
    year = int(m.group(1))
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

    `cites` maps pid -> edition -> citations (self left out), `own` pid -> edition -> the
    member's own papers among the edition's n. Only complete main editions count.
    """

    def __init__(self, editions, complete, on_power, cites, own, window=RATE_WINDOW):
        self.editions, self.window = editions, window
        self.on_power, self.cites, self.own = on_power, cites, own
        self.by_stream = defaultdict(list)
        for e in complete:
            if is_main(e):
                self.by_stream[editions[e]["stream"]].append(e)

    def conference(self, pid, stream, year, min_papers=MIN_OFF_PAPERS) -> tuple[int, int, int]:
        """(citations, papers, span) over the stream's off-power editions within `window`
        years, and beyond that nearest first while they hold fewer than `min_papers`
        papers. Editions equally far are taken together; span is the distance in years of
        the furthest one taken."""
        cites, own = self.cites.get(pid, {}), self.own.get(pid, {})
        by_distance = defaultdict(list)
        for e in self.by_stream.get(stream, ()):
            if e not in self.on_power.get(pid, ()):
                by_distance[abs(self.editions[e]["year"] - year)].append(e)
        k = n = span = 0
        for distance in sorted(by_distance):
            if distance > self.window and n >= min_papers:
                break
            for e in by_distance[distance]:
                k += cites.get(e, 0)
                n += self.editions[e]["n"] - own.get(e, 0)
            span = distance
        return k, n, span


def off_power_rate(levels, min_papers=MIN_OFF_PAPERS, pseudo=RATE_PSEUDO_COUNT):
    """(src, k, n, rate) of the first (src, (k, n)) level with at least `min_papers`
    papers; ("", 0, 0, None) when none has."""
    for src, (k, n) in levels:
        if n >= min_papers:
            return src, k, n, (k + pseudo) / n
    return "", 0, 0, None


def citation_ratio(x, expected, pseudo=RATIO_PSEUDO_COUNT) -> float:
    """Observed over expected citations, with `pseudo` of slack."""
    return (x + pseudo) / (expected + pseudo)


def seat_rows(seats, editions, cites, westerbaan, own, off: OffPower,
              min_peers=MIN_PC_SIZE - 1) -> list[dict]:
    """One row per seat. `westerbaan` maps (edition, pid) to the references to the member.
    Without enough off-power papers the rate is that of the other seats of the edition,
    when there are at least `min_peers`."""
    rows = []
    for (e, pid), seat in sorted(seats.items()):
        ed = editions[e]
        rows.append({
            "edition_key": e, "stream": ed["stream"], "year": ed["year"],
            "core_rank": ed["core_rank"] or "", "dblp_pid": pid, "name": seat["name"],
            "role": seat["role"], "x": cites.get(pid, {}).get(e, 0),
            "x_westerbaan": westerbaan.get((e, pid), 0),
            "n": ed["n"] - own.get(pid, {}).get(e, 0),
        })
    totals = defaultdict(lambda: [0, 0, 0])
    for r in rows:
        t = totals[r["edition_key"]]
        t[0], t[1], t[2] = t[0] + r["x"], t[1] + r["n"], t[2] + 1
    for r in rows:
        k_conf, n_conf, span = off.conference(r["dblp_pid"], r["stream"], r["year"])
        k_all, n_all, n_seats = totals[r["edition_key"]]
        peers = (k_all - r["x"], n_all - r["n"]) if n_seats - 1 >= min_peers else (0, 0)
        src, k_off, n_off, rate = off_power_rate(
            [("conference", (k_conf, n_conf)), ("peers", peers)])
        r.update({
            "src": src, "span": span if src == "conference" else "",
            "k_off": k_off, "n_off": n_off,
            "rate": "" if rate is None else round(rate, 6),
            "E": "" if rate is None else round(rate * r["n"], 4),
            "ratio": "" if rate is None else round(citation_ratio(r["x"], rate * r["n"]), 4),
        })
    return rows


def gated_members(rows, gate=GATE_REFERENCES) -> dict[str, list[dict]]:
    """pid -> their seats with more than `gate` references (Westerbaan's count)."""
    gated = defaultdict(list)
    for r in rows:
        if int(r["x_westerbaan"]) > gate:
            gated[r["dblp_pid"]].append(r)
    return dict(gated)


# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------

def stage_citations(db) -> None:
    db.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
    build_table(db, "citation", CITATION_SQL)
    build_table(db, "paper", PAPER_SQL)

    c = db.execute_query_result(f"""
        SELECT count(*) AS rows, count(DISTINCT (article_dblp_key, cited)) AS refs,
               count(DISTINCT article_dblp_key) AS citing, count(DISTINCT cited_pid) AS pids,
               avg(is_self::int) AS self_share
        FROM "{SCHEMA}".citation""")[0]
    p = db.execute_query_result(f"""
        SELECT count(*) AS papers, count(*) FILTER (WHERE n_references > 0) AS in_oc,
               count(*) FILTER (WHERE n_resolved > 0) AS resolved
        FROM "{SCHEMA}".paper""")[0]
    logging.info(f"citation: {c['rows']} rows, {c['refs']} references from {c['citing']} "
                 f"papers to {c['pids']} authors, self share {float(c['self_share']):.3f}")
    logging.info(f"paper: {p['papers']} papers, {p['in_oc']} with references in "
                 f"OpenCitations, {p['resolved']} with a reference resolved to dblp")


def print_seats_summary(rows, editions, pc_size, complete, n_unknown) -> None:
    with_committee = [e for e in pc_size if e in editions]
    members = {r["dblp_pid"] for r in rows}
    by_src = Counter(r["src"] for r in rows)
    gated = gated_members(rows)
    years = Counter(min(len({r["year"] for r in seats}), 3) for seats in gated.values())
    n_gated_seats = sum(len(seats) for seats in gated.values())

    print(f"\n=== SEATS: {len(rows)} seats of {len(members)} members ===")
    print(f"  editions with a PC                   {len(with_committee):>7}")
    print(f"  ...plausibly complete                {len(complete):>7}  "
          f"({pct(len(complete), len(with_committee))})")
    n_main = sum(1 for e in complete if is_main(e))
    print(f"  ...of which main editions (off-power) {n_main:>6}  "
          f"({pct(n_main, len(with_committee))})")
    if n_unknown:
        print(f"  seats of an edition without volumes  {n_unknown:>7}  (left out)")
    for role, count in sorted(Counter(r["role"] for r in rows).items()):
        print(f"  role {role:<32}{count:>7}")
    n_zero = sum(1 for r in rows if r["n"] == 0)
    print(f"  seats with n = 0                     {n_zero:>7}  ({pct(n_zero, len(rows))})")
    for src, label in (("conference", "same stream"), ("peers", "PC peers"), ("", "none")):
        print(f"  rate from {label:<27}{by_src[src]:>7}  ({pct(by_src[src], len(rows))})")
    spans = Counter(min(r["span"], 10) for r in rows if r["src"] == "conference")
    print("  same stream, furthest edition taken:  "
          + "  ".join(f"{s if s < 10 else '10+'}y {pct(c, by_src['conference']).strip()}"
                      for s, c in sorted(spans.items())))
    print(f"\n  gate: seats with more than {GATE_REFERENCES} references  {n_gated_seats:>7}")
    print(f"  ...members                           {len(gated):>7}  "
          f"({pct(len(gated), len(members))})")
    for n_years, label in ((1, "1 year"), (2, "2 years"), (3, "3 or more years")):
        print(f"  ...gated in {label:<25}{years[n_years]:>7}")


def stage_seats(db, out_dir: Path) -> None:
    start = time.time()
    editions = {r["edition_key"]: {**r, "year": conference_year(r["edition_key"]) or r["year"]}
                for r in db.execute_query_result(EDITIONS_SQL)}
    committee = db.execute_query_result(COMMITTEE_SQL)
    cites, westerbaan = defaultdict(dict), {}
    for r in db.execute_query_result(CITES_SQL):
        cites[r["dblp_pid"]][r["edition_key"]] = r["x"]
        westerbaan[(r["edition_key"], r["dblp_pid"])] = r["x_westerbaan"]
    own = defaultdict(dict)
    for r in db.execute_query_result(OWN_SQL):
        own[r["dblp_pid"]][r["edition_key"]] = r["n_own"]
    logging.info(f"read in {time.time() - start:.0f}s")

    pc_size = pc_sizes(committee)
    complete = complete_editions(editions, pc_size)
    seats = scored_seats(committee)
    unknown = {k for k in seats if k[0] not in editions}
    seats = {k: v for k, v in seats.items() if k not in unknown}
    off = OffPower(editions, complete, on_power_editions(committee), cites, own)
    rows = seat_rows(seats, editions, cites, westerbaan, own, off)

    write_csv(out_dir / "seats.csv", rows)
    print_seats_summary(rows, editions, pc_size, complete, len(unknown))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the CS3 PC member citation BBN corpus.")
    parser.add_argument("--stage", required=True, choices=["citations", "seats"])
    parser.add_argument("--out-dir", default=str(BBN_DIR), help="where the BBN files go")
    parser.add_argument("--env", default=None,
                        help=f"path to the .env with the POSTGRES_* values (default: {ENV_PATH})")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    db = connect(Path(args.env) if args.env else ENV_PATH)
    if args.stage == "citations":
        stage_citations(db)
    if args.stage == "seats":
        stage_seats(db, Path(args.out_dir))


if __name__ == "__main__":
    main()
