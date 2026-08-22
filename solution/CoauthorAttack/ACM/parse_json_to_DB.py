"""
Loads the parsed ACM front matter into the database for the co-author editor case
study (SQ1.2).

parse_md_to_json.py writes one JSON per issue (the editorial board).
This script loads that corpus into two tables, adding the issue key taken from the
file name.

Outputs (schema `acm`):
    front_matter_issues   one row per issue: journal, volume, issue
    front_matter_board    one row per (issue, editor, role)

Issues already in the database are skipped, so a re-run only adds what is new;
to reload from scratch, empty the two tables by hand first.

Run from this directory (with the scraper venv):
    python parse_json_to_DB.py --dry-run           # load nothing, report quality
    python parse_json_to_DB.py                     # load issues not yet in the database
    python parse_json_to_DB.py --selftest          # pure-function tests, no data/DB
"""

from __future__ import annotations

import argparse
import configparser
import json
import logging
import re
import sys
from collections import Counter
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
ENV_PATH = BASE_DIR / "../../.env"

SCHEMA = "acm"
TABLE_ISSUES = "front_matter_issues"
TABLE_BOARD = "front_matter_board"

# ~3.4k issues and ~100k board rows, so one batch of 500 issues carries a few
# thousand board rows.
BATCH_SIZE = 500

FILENAME_RE = re.compile(r"^(?P<journal>.+?)_Volume_(?P<volume>[^_]+)__Issue_(?P<issue>.+)$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_config(path) -> configparser.SectionProxy:
    path = Path(path)
    if not path.exists():
        sys.exit(f"\nERROR: no .env found at {path.resolve()}\n"
                 f"Copy code/env-example to code/.env and fill in the POSTGRES_* values.\n")
    with open(path, "r") as f:
        config_string = "[SECTION]\n" + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config["SECTION"]


def parse_filename(stem: str) -> dict | None:
    """Split '<journal>_Volume_<vol>__Issue_<issue>' into its parts.
    """
    m = FILENAME_RE.match(stem)
    if not m:
        return None
    volume_label = m["volume"].strip()
    issue_label = m["issue"].strip()

    volume_digits = re.match(r"^(\d+)", volume_label)
    issue_digits = [int(n) for n in re.findall(r"\d+", issue_label)]
    return {
        "journal_name": m["journal"].strip(),
        "volume_label": volume_label,
        "volume_num": int(volume_digits.group(1)) if volume_digits else None,
        "issue_label": issue_label,
        "issue_first_num": issue_digits[0] if issue_digits else None,
        "issue_last_num": issue_digits[-1] if issue_digits else None,
        # an 's'/'S' suffix marks a supplement; a hyphen marks issues bound together
        "is_supplement": bool(re.search(r"s$", issue_label, re.IGNORECASE))
                         or bool(re.search(r"s$", volume_label, re.IGNORECASE)),
        "is_combined_issue": len(issue_digits) > 1,
    }


# ---------------------------------------------------------------------------
# Corpus assembly
# ---------------------------------------------------------------------------

def build_issue_board(key: dict, editors: list, stats: Counter) -> list[dict]:
    """Board rows for one issue, de-duplicated on (name, role, association).

    The association is part of the key, because a board can carry two *different* people
    with the same name. Keying on (name, role) alone deletes one of them.
    """
    seen: set[tuple[str, str, str]] = set()
    seen_name_role: set[tuple[str, str]] = set()
    rows = []
    for editor in editors:
        name = (editor.get("name") or "").strip()
        role = (editor.get("role") or "").strip()
        association = (editor.get("association") or "").strip()
        if not name:
            continue
        if (name, role, association) in seen:
            stats["duplicate_editor_rows"] += 1
            continue
        if (name, role) in seen_name_role:
            # same name and role, different affiliation: two people, both kept
            stats["shared_name_rows_kept"] += 1
        seen.add((name, role, association))
        seen_name_role.add((name, role))
        rows.append({
            **key,
            "name": name,
            "role": role,
            "association": association or None,
        })

    # One person holding two roles in the same issue keeps both rows, but they are 
    # one board member, which is what n_board counts.
    per_person = Counter((r["name"], r["association"]) for r in rows)
    stats["multi_role_people"] += sum(1 for n in per_person.values() if n > 1)
    return rows

def load_corpus(data_dir: Path, limit: int | None = None) -> dict:
    """Read every issue JSON into issues/board rows plus quality counters.
    """
    stats = Counter()
    raw_issues = []

    json_paths = sorted(data_dir.glob("*.json"))
    if limit:
        json_paths = json_paths[:limit]

    for path in json_paths:
        parts = parse_filename(path.stem)
        if parts is None:
            stats["bad_filename"] += 1
            logging.warning(f"Unparseable filename: {path.name}")
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            stats["unreadable_json"] += 1
            logging.warning(f"Cannot read {path.name}: {exc}")
            continue

        raw_issues.append({
            **parts,
            "editors": payload.get("editors", []),
            "source_file": path.name,
        })

    # --- flatten into table rows -------------------------------------------
    issues, board, records = [], [], []
    for issue in raw_issues:
        key = {
            "journal_name": issue["journal_name"],
            "volume_label": issue["volume_label"],
            "issue_label": issue["issue_label"],
        }
        issue_board = build_issue_board(key, issue["editors"], stats)

        # A board member is a person, not a row. The same person can be listed under two
        # roles in one issue, and two different people can share a name, so neither the
        # row count nor the distinct-name count is right on its own.
        n_board = len({(r["name"], r["association"]) for r in issue_board})
        issue_row = {
            "journal_name": issue["journal_name"],
            "volume_label": issue["volume_label"], "volume_num": issue["volume_num"],
            "issue_label": issue["issue_label"],
            "issue_first_num": issue["issue_first_num"],
            "issue_last_num": issue["issue_last_num"],
            "is_supplement": issue["is_supplement"],
            "is_combined_issue": issue["is_combined_issue"],
            "n_board": n_board,
            "source_file": issue["source_file"],
        }
        issues.append(issue_row)
        board.extend(issue_board)
        records.append({"issue": issue_row, "board": issue_board})
        if not n_board:
            stats["issues_without_board"] += 1

    stats["issues"] = len(issues)
    stats["board_rows"] = len(board)
    return {"issues": issues, "board": board, "records": records, "stats": stats}


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_report(result: dict) -> None:
    stats, issues, board = result["stats"], result["issues"], result["board"]

    print("\n=== CORPUS ===")
    print(f"  issues loaded          {stats['issues']:>7}")
    print(f"  journals               {len({i['journal_name'] for i in issues}):>7}")
    print(f"  board rows             {stats['board_rows']:>7}")
    print(f"  distinct editors       {len({r['name'] for r in board}):>7}")

    print("\n=== ANOMALIES ===")
    for key in ("bad_filename", "unreadable_json", "duplicate_editor_rows",
                "issues_without_board"):
        print(f"  {key:<30} {stats[key]:>7}")

    # Two ways one person can occupy more than one row of an issue's board. Both are
    # kept on purpose; both matter to the DBLP person match, so both are
    # reported rather than folded into `duplicate_editor_rows`.
    print(f"  {'shared_name_rows_kept':<30} {stats['shared_name_rows_kept']:>7}"
          f"   (same name+role, different affiliation -> distinct people)")
    print(f"  {'multi_role_people':<30} {stats['multi_role_people']:>7}"
          f"   (one person, >1 role in the same issue)")

    print("\n=== ROLES (all roles count as board membership) ===")
    for role, count in Counter(r["role"] for r in board).most_common():
        print(f"  {count:>7}  {role}")

    # The most-repeated names, as a check that the parser is not inventing editors:
    # boilerplate mistaken for a name recurs across every issue of a journal.
    print("\n=== MOST FREQUENT EDITOR NAMES (eyeball for boilerplate) ===")
    for name, count in Counter(r["name"] for r in board).most_common(20):
        print(f"  {count:>7}  {name}")


# ---------------------------------------------------------------------------
# Database load
# ---------------------------------------------------------------------------

def ensure_schema_and_tables(db) -> None:
    """Create the schema and the two front-matter tables if they do not exist."""
    if not db.schema_exists(SCHEMA):
        db.create_schema(SCHEMA)

    db.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{TABLE_ISSUES}" (
            journal_name      TEXT,
            volume_label      TEXT,
            volume_num        INTEGER,
            issue_label       TEXT,
            issue_first_num   INTEGER,
            issue_last_num    INTEGER,
            is_supplement     BOOLEAN,
            is_combined_issue BOOLEAN,
            n_board           INTEGER,
            source_file       TEXT
        )
    """)
    db.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{TABLE_BOARD}" (
            journal_name TEXT,
            volume_label TEXT,
            issue_label  TEXT,
            name         TEXT,
            role         TEXT,
            association  TEXT
        )
    """)
    db.execute_query(f'CREATE INDEX IF NOT EXISTS ix_{TABLE_BOARD}_name '
                     f'ON "{SCHEMA}"."{TABLE_BOARD}" USING hash (name)')
    db.execute_query(f'CREATE INDEX IF NOT EXISTS ix_{TABLE_BOARD}_issue '
                     f'ON "{SCHEMA}"."{TABLE_BOARD}" (journal_name, volume_label, issue_label)')


def load_processed_source_files(db) -> set[str]:
    """Return the source files already present in the issues table.

    The Springer loader keys its resume on the DOI; here the natural key is the JSON
    file name, since parse_md_to_json.py writes exactly one per issue. Fetched once at
    startup so each record is an O(1) check with no extra round-trips.
    """
    if not db.table_exists(SCHEMA, TABLE_ISSUES):
        return set()
    rows = db.execute_query_result(
        f'SELECT source_file FROM "{SCHEMA}"."{TABLE_ISSUES}" WHERE source_file IS NOT NULL')
    files = {row["source_file"] for row in rows}
    logging.info(f"Loaded {len(files)} already-loaded source files from the database.")
    return files


def iter_batches(records: list[dict], loaded: set[str], batch_size: int):
    """Yield (issue_rows, board_rows) batches for the records not yet in the database.

    Each batch carries whole issues, so no flush can split an issue from its board.
    """
    batch_issues: list[dict] = []
    batch_board: list[dict] = []
    for record in records:
        if record["issue"]["source_file"] in loaded:
            continue
        batch_issues.append(record["issue"])
        batch_board.extend(record["board"])
        if len(batch_issues) >= batch_size:
            yield batch_issues, batch_board
            batch_issues, batch_board = [], []
    if batch_issues:
        yield batch_issues, batch_board


def flush(db, issues: list[dict], board: list[dict], total_flushed: int) -> int:
    """Write both tables in one transaction and return the updated total."""
    if not issues:
        return total_flushed

    db.insert_atomic([
        (SCHEMA, TABLE_ISSUES, issues),
        (SCHEMA, TABLE_BOARD, board),
    ])

    total_flushed += len(issues)
    logging.info(f"  Flushed batch: {len(issues)} issues | {len(board)} board rows "
                 f"(total issues written so far: {total_flushed})")
    return total_flushed


def load(db, result: dict) -> None:
    """Insert the issues not already in the database, in batches.
    """
    ensure_schema_and_tables(db)
    loaded = load_processed_source_files(db)

    records = result["records"]
    already = sum(1 for r in records if r["issue"]["source_file"] in loaded)
    logging.info(f"Parsed {len(records)} issues. {already} already in the database, "
                 f"{len(records) - already} to load (batch size: {BATCH_SIZE}).")

    total_flushed = 0
    for batch_issues, batch_board in iter_batches(records, loaded, BATCH_SIZE):
        total_flushed = flush(db, batch_issues, batch_board, total_flushed)

    logging.info(f"Wrote {total_flushed} issues to {SCHEMA}.{TABLE_ISSUES} "
                 f"and their board rows to {SCHEMA}.{TABLE_BOARD}.")


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

def selftest() -> None:
    """Covers this loader only; the parser has test_parse_md_to_json.py."""
    ok = True

    def check(label, condition):
        nonlocal ok
        print(f"  [{'PASS' if condition else 'FAIL'}] {label}")
        ok = ok and condition

    # --- issue key from the file name (this module) -------------------------
    def name_parts(stem):
        parts = parse_filename(stem)
        assert parts is not None, stem
        return parts

    parts = name_parts("ACM Transactions on Computation Theory_Volume_10__Issue_3")
    check("filename: journal/volume/issue",
          parts["journal_name"] == "ACM Transactions on Computation Theory"
          and parts["volume_num"] == 10 and parts["issue_label"] == "3"
          and not parts["is_combined_issue"])
    parts = name_parts("ACM Transactions on Computer Systems_Volume_37__Issue_1-4")
    check("filename: combined issue '1-4' keeps its label and exposes 1..4",
          parts["issue_label"] == "1-4" and parts["issue_first_num"] == 1
          and parts["issue_last_num"] == 4 and parts["is_combined_issue"])
    parts = name_parts("ACM Transactions on Autonomous and Adaptive Systems_Volume_7S__Issue_1s")
    check("filename: supplement '7S' -> volume_num 7, flagged",
          parts["volume_num"] == 7 and parts["is_supplement"])
    check("filename: a journal name containing underscores is not split early",
          name_parts("A_B Journal_Volume_3__Issue_2")["journal_name"] == "A_B Journal")
    check("filename: unparseable name returns None", parse_filename("random.md") is None)

    # --- board de-duplication --------------------------------
    def board(editors):
        stats = Counter()
        return build_issue_board({"issue_label": "1"}, editors, stats), stats

    rows, stats = board([
        {"name": "Ada Lovelace", "role": "Associate Editor", "association": "Tsinghua"},
        {"name": "Ada Lovelace", "role": "Associate Editor", "association": "UT Dallas"},
    ])
    check("dedup: same name+role at different institutions are two people",
          len(rows) == 2 and stats["duplicate_editor_rows"] == 0
          and stats["shared_name_rows_kept"] == 1)

    rows, stats = board([
        {"name": "Ada Lovelace", "role": "Editor-in-Chief", "association": "X"},
        {"name": "Ada Lovelace", "role": "Editor-in-Chief", "association": "X"},
    ])
    check("dedup: a repeated identical entry still collapses",
          len(rows) == 1 and stats["duplicate_editor_rows"] == 1)

    rows, stats = board([
        {"name": "Ada Lovelace", "role": "Associate Editor", "association": "X"},
        {"name": "Ada Lovelace", "role": "Special Issue Editor", "association": "X"},
    ])
    check("dedup: one person under two roles keeps both rows, counted once",
          len(rows) == 2 and stats["multi_role_people"] == 1
          and len({(r["name"], r["association"]) for r in rows}) == 1)

    rows, stats = board([{"name": "  ", "role": "Editor", "association": "X"},
                         {"name": "Grace Hopper", "role": "Editor", "association": None}])
    check("dedup: a blank name is dropped and a missing association becomes NULL",
          len(rows) == 1 and rows[0]["name"] == "Grace Hopper"
          and rows[0]["association"] is None)

    # --- batching for the database load (this module) ----------------------
    records = [{"issue": {"source_file": f"{i}.json"}, "board": [{"n": i}] * (i + 1)}
               for i in range(5)]
    batches = list(iter_batches(records, {"1.json"}, 2))
    check("batching: issues already in the database are skipped",
          [len(b[0]) for b in batches] == [2, 2])
    check("batching: board rows stay in the same batch as their issue",
          [len(b[1]) for b in batches] == [1 + 3, 4 + 5])
    check("batching: an empty corpus yields no batches",
          list(iter_batches([], set(), 2)) == [])

    print("\nSELFTEST:", "ALL PASS" if ok else "FAILURES PRESENT")
    if not ok:
        raise SystemExit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load the parsed ACM front matter into the database.")
    parser.add_argument("--dry-run", action="store_true",
                        help="report only; do not touch the database")
    parser.add_argument("--limit", type=int, default=None,
                        help="load only the first N issues (smoke test)")
    parser.add_argument("--selftest", action="store_true",
                        help="run the pure-function tests and exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    if args.selftest:
        return selftest()

    if not DATA_DIR.exists():
        sys.exit(f"Data directory not found: {DATA_DIR}")

    logging.info(f"Reading parsed front matter from {DATA_DIR}")
    result = load_corpus(DATA_DIR, args.limit)
    print_report(result)

    if args.dry_run:
        print("\nDry run: nothing written to the database.")
        return

    # DB import stays inside main() so the helpers import without pyodbc.
    from database import Postgress

    config = read_config(ENV_PATH)
    db = Postgress(
        server=config["POSTGRES_SERVER"],
        database=config["POSTGRES_DB"],
        user=config["POSTGRES_USER"],
        password=config["POSTGRES_PASSWORD"],
    )
    load(db, result)
    logging.info("Done.")


if __name__ == "__main__":
    main()
