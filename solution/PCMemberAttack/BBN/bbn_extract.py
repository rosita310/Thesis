"""
Builds the BBN corpus for the PC member citation case study from the LNCS
front matter, dblp and OpenCitations tables in the database.

    python bbn_extract.py --stage citations
        Materialises SCHEMA.citation (one row per citing LNCS paper, cited
        reference and author of the cited work) and SCHEMA.paper (the LNCS
        papers with their reference counts).
"""

from __future__ import annotations

import argparse
import configparser
import logging
import sys
import time
from pathlib import Path

from cs3_config import ENV_PATH, SCHEMA

# 'conf/eccv/2020-12' -> 'conf/eccv/2020', as edition_of in committee_members.py.
EDITION = r"regexp_replace(a.volume_dblp_key, '-\d+$', '')"

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
    {EDITION} AS edition_key,
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
    {EDITION} AS edition_key,
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the CS3 PC member citation BBN corpus.")
    parser.add_argument("--stage", required=True, choices=["citations"])
    parser.add_argument("--env", default=None,
                        help=f"path to the .env with the POSTGRES_* values (default: {ENV_PATH})")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    if args.stage == "citations":
        stage_citations(connect(Path(args.env) if args.env else ENV_PATH))


if __name__ == "__main__":
    main()
