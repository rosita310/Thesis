"""
Loads the parsed IEEE front matter into the database.

parse_md_to_json.py writes one JSON per issue (the editorial board), named
'<journal>_<year>_Issue_<issue>'. This script loads them into schema `ieee`:

    front_matter_issues   one row per issue: journal, year, issue
    front_matter_board    one row per (issue, editor, role)

Issues already in the database are skipped; to reload, empty the two tables first.

    python parse_json_to_DB.py --dry-run           # load nothing, report quality
    python parse_json_to_DB.py                     # load issues not yet in the database
    python parse_json_to_DB.py --selftest
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

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
ENV_PATH = BASE_DIR / "../../.env"

SCHEMA = "ieee"
TABLE_ISSUES = "front_matter_issues"
TABLE_BOARD = "front_matter_board"

BATCH_SIZE = 500

FILENAME_RE = re.compile(r"^(?P<journal>.+?)_(?P<year>\d{4})_Issue_(?P<issue>.+)$")


def read_config(path) -> configparser.SectionProxy:
    path = Path(path)
    if not path.exists():
        sys.exit(f"\nERROR: no .env found at {path.resolve()}\n")
    with open(path, "r") as f:
        config_string = "[SECTION]\n" + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config["SECTION"]


def parse_filename(stem: str) -> dict | None:
    """'<journal>_<year>_Issue_<issue>' -> its parts. The issue number is the
    first number of the label ('11_Part_1' -> 11), None when it has none."""
    m = FILENAME_RE.match(stem)
    if not m:
        return None
    issue_label = m["issue"].strip()
    digits = re.match(r"^(\d+)", issue_label)
    return {
        "journal_name": m["journal"].strip(),
        "year": int(m["year"]),
        "issue_label": issue_label,
        "issue_num": int(digits.group(1)) if digits else None,
    }


def build_issue_board(key: dict, editors: list, stats: Counter) -> list[dict]:
    """Board rows for one issue, de-duplicated on (name, role, association)."""
    seen = set()
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
        seen.add((name, role, association))
        rows.append({**key, "name": name, "role": role, "association": association or None})
    return rows


def load_corpus(data_dir: Path, limit: int | None = None) -> dict:
    """Every issue JSON as issue and board rows, plus quality counters."""
    stats = Counter()
    records = []
    paths = sorted(data_dir.glob("*.json"))
    if limit:
        paths = paths[:limit]

    for path in paths:
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

        key = {k: parts[k] for k in ("journal_name", "year", "issue_label")}
        board = build_issue_board(key, payload.get("editors", []), stats)
        n_board = len({(r["name"], r["association"]) for r in board})
        if not n_board:
            stats["issues_without_board"] += 1
        if parts["issue_num"] is None:
            stats["issues_without_number"] += 1
        records.append({"issue": {**parts, "n_board": n_board, "source_file": path.name},
                        "board": board})

    stats["issues"] = len(records)
    stats["board_rows"] = sum(len(r["board"]) for r in records)
    return {"records": records, "stats": stats}


def print_report(result: dict) -> None:
    stats = result["stats"]
    issues = [r["issue"] for r in result["records"]]
    board = [row for r in result["records"] for row in r["board"]]

    print("\n=== CORPUS ===")
    print(f"  issues loaded          {stats['issues']:>7}")
    print(f"  journals               {len({i['journal_name'] for i in issues}):>7}")
    print(f"  board rows             {stats['board_rows']:>7}")
    print(f"  distinct editors       {len({r['name'] for r in board}):>7}")

    print("\n=== ANOMALIES ===")
    for key in ("bad_filename", "unreadable_json", "duplicate_editor_rows",
                "issues_without_board", "issues_without_number"):
        print(f"  {key:<30} {stats[key]:>7}")

    print("\n=== MOST FREQUENT EDITOR NAMES (eyeball for boilerplate) ===")
    for name, count in Counter(r["name"] for r in board).most_common(20):
        print(f"  {count:>7}  {name}")


def ensure_schema_and_tables(db) -> None:
    if not db.schema_exists(SCHEMA):
        db.create_schema(SCHEMA)
    db.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{TABLE_ISSUES}" (
            journal_name TEXT,
            year         INTEGER,
            issue_label  TEXT,
            issue_num    INTEGER,
            n_board      INTEGER,
            source_file  TEXT
        )
    """)
    db.execute_query(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{TABLE_BOARD}" (
            journal_name TEXT,
            year         INTEGER,
            issue_label  TEXT,
            name         TEXT,
            role         TEXT,
            association  TEXT
        )
    """)
    db.execute_query(f'CREATE INDEX IF NOT EXISTS ix_{TABLE_BOARD}_name '
                     f'ON "{SCHEMA}"."{TABLE_BOARD}" USING hash (name)')
    db.execute_query(f'CREATE INDEX IF NOT EXISTS ix_{TABLE_BOARD}_issue '
                     f'ON "{SCHEMA}"."{TABLE_BOARD}" (journal_name, year, issue_label)')


def loaded_source_files(db) -> set[str]:
    if not db.table_exists(SCHEMA, TABLE_ISSUES):
        return set()
    rows = db.execute_query_result(
        f'SELECT source_file FROM "{SCHEMA}"."{TABLE_ISSUES}" WHERE source_file IS NOT NULL')
    return {row["source_file"] for row in rows}


def iter_batches(records: list[dict], loaded: set[str], batch_size: int):
    """(issue_rows, board_rows) batches of whole issues not yet in the database."""
    batch_issues, batch_board = [], []
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


def load(db, result: dict) -> None:
    ensure_schema_and_tables(db)
    loaded = loaded_source_files(db)
    records = result["records"]
    logging.info(f"Parsed {len(records)} issues, "
                 f"{sum(1 for r in records if r['issue']['source_file'] in loaded)} "
                 f"already in the database.")
    total = 0
    for issues, board in iter_batches(records, loaded, BATCH_SIZE):
        db.insert_atomic([(SCHEMA, TABLE_ISSUES, issues), (SCHEMA, TABLE_BOARD, board)])
        total += len(issues)
        logging.info(f"  Flushed {len(issues)} issues | {len(board)} board rows "
                     f"(total {total})")
    logging.info(f"Wrote {total} issues to {SCHEMA}.{TABLE_ISSUES}.")


def selftest() -> None:
    ok = True

    def check(label, condition):
        nonlocal ok
        print(f"  [{'PASS' if condition else 'FAIL'}] {label}")
        ok = ok and condition

    parts = parse_filename("IEEE Transactions on Computers_2020_Issue_3")
    check("filename: journal/year/issue",
          parts == {"journal_name": "IEEE Transactions on Computers", "year": 2020,
                    "issue_label": "3", "issue_num": 3})
    parts = parse_filename("IEEE Antennas and Wireless Propagation Letters_2018_Issue_11_Part_1")
    check("filename: a part keeps its label and the issue number",
          parts["issue_label"] == "11_Part_1" and parts["issue_num"] == 11)
    check("filename: a label without a number has no issue number",
          parse_filename("Proceedings of the IEEE_2012_Issue_Special_Centennial_Issue")
          ["issue_num"] is None)
    check("filename: a journal name with a year in it is not split early",
          parse_filename("A_2000 Journal_2020_Issue_1") is not None
          and parse_filename("A_2000 Journal_2020_Issue_1")["year"] == 2020)
    check("filename: unparseable name returns None", parse_filename("random") is None)

    stats = Counter()
    rows = build_issue_board({"issue_label": "1"}, [
        {"name": "Ada Lovelace", "role": "Editor", "association": "X"},
        {"name": "Ada Lovelace", "role": "Editor", "association": "X"},
        {"name": " ", "role": "Editor", "association": "X"}], stats)
    check("board: a repeated entry collapses and a blank name is dropped",
          len(rows) == 1 and stats["duplicate_editor_rows"] == 1)

    records = [{"issue": {"source_file": f"{i}.json"}, "board": [{"n": i}] * (i + 1)}
               for i in range(5)]
    batches = list(iter_batches(records, {"1.json"}, 2))
    check("batching: loaded issues are skipped, boards stay with their issue",
          [len(b[0]) for b in batches] == [2, 2] and [len(b[1]) for b in batches] == [4, 9])

    print("\nSELFTEST:", "ALL PASS" if ok else "FAILURES PRESENT")
    if not ok:
        raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load the parsed IEEE front matter.")
    parser.add_argument("--dry-run", action="store_true", help="report only")
    parser.add_argument("--limit", type=int, default=None, help="only the first N issues")
    parser.add_argument("--selftest", action="store_true", help="run the tests and exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    if args.selftest:
        return selftest()
    if not DATA_DIR.exists():
        sys.exit(f"Data directory not found: {DATA_DIR}")

    result = load_corpus(DATA_DIR, args.limit)
    print_report(result)
    if args.dry_run:
        print("\nDry run: nothing written to the database.")
        return

    from database import Postgress
    config = read_config(ENV_PATH)
    db = Postgress(server=config["POSTGRES_SERVER"], database=config["POSTGRES_DB"],
                   user=config["POSTGRES_USER"], password=config["POSTGRES_PASSWORD"])
    load(db, result)


if __name__ == "__main__":
    main()
