"""
Extracts the articles of our journals (every publisher in PUBLISHERS) and their
author signatures from the DBLP N-Triples dump, and maps our journal names to
DBLP journal keys.

    python dblp_extract.py --stage extract
        Two passes over the dump. Writes under reports/extract/:
            dblp_journals_all.csv    every journals/* key in the dump + article count
            articles.csv             one row per matched article
            signatures.csv           one row per (article, author)
        Articles of the keys in an existing journal_mapping.csv are kept too.
        Cached: re-running is a no-op unless --force.

    python dblp_extract.py --stage discover
        Writes reports/journal_mapping_proposed.csv. Rows of an existing
        journal_mapping.csv are carried over as `verified`. Verify every other row
        whose confidence is not `high` and save it as journal_mapping.csv.

If the extract stage hangs without output, the database is unreachable:
Postgress.get_connection has no login timeout.
"""

from __future__ import annotations

import argparse
import configparser
import csv
import logging
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

from cs2_config import DUMP_PATH, ENV_PATH, PUBLISHERS, REPORTS_DIR

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TABLE_ISSUES = "front_matter_issues"
TABLE_BOARD = "front_matter_board"

REC_JOURNAL_PREFIX = "https://dblp.org/rec/journals/"

SCHEMA_NS = "https://dblp.org/rdf/schema#"
P_JOURNAL = SCHEMA_NS + "publishedInJournal"
P_VOLUME = SCHEMA_NS + "publishedInJournalVolume"
P_ISSUE = SCHEMA_NS + "publishedInJournalVolumeIssue"
P_PAGINATION = SCHEMA_NS + "pagination"
P_YEAR = SCHEMA_NS + "yearOfPublication"
P_BIBTEX = SCHEMA_NS + "bibtexType"
P_TITLE = SCHEMA_NS + "title"
P_SIG_NAME = SCHEMA_NS + "signatureDblpName"
P_SIG_CREATOR = SCHEMA_NS + "signatureCreator"
P_SIG_ORDINAL = SCHEMA_NS + "signatureOrdinal"
P_SIG_PUBLICATION = SCHEMA_NS + "signaturePublication"

# The closing '>' keeps this from also matching publishedInJournalVolume(Issue).
NEEDLE_JOURNAL = f"<{P_JOURNAL}>"
ARTICLE_PREDS = {P_VOLUME, P_ISSUE, P_PAGINATION, P_YEAR, P_BIBTEX, P_TITLE, P_JOURNAL}
SIG_PREDS = {P_SIG_NAME, P_SIG_CREATOR, P_SIG_ORDINAL}
PASS2_PREDS = ARTICLE_PREDS | SIG_PREDS | {P_SIG_PUBLICATION}

ARTICLE_FIELD = {
    P_JOURNAL: "journal_title", P_VOLUME: "volume", P_ISSUE: "issue",
    P_PAGINATION: "pagination", P_YEAR: "year",
    P_BIBTEX: "bibtex_type", P_TITLE: "title",
}

STOPWORDS = {"on", "the", "of", "and", "for", "in", "to", "from", "with", "a", "an"}

# One regex pass, so an escaped backslash is consumed before a following 'u'.
NT_ESCAPE_RE = re.compile(r'\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|[tbnrf"\'\\])')
NT_SIMPLE_ESCAPE = {"t": "\t", "b": "\b", "n": "\n", "r": "\r", "f": "\f",
                    '"': '"', "'": "'", "\\": "\\"}


def nt_unescape(text: str) -> str:
    r"""Decode N-Triples backslash escapes, including \uXXXX and \UXXXXXXXX."""
    if "\\" not in text:
        return text
    return NT_ESCAPE_RE.sub(
        lambda m: (chr(int(m.group(1)[1:], 16)) if m.group(1)[0] in "uU"
                   else NT_SIMPLE_ESCAPE[m.group(1)]),
        text)


PROGRESS_EVERY = 40_000_000   # lines


# ---------------------------------------------------------------------------
# Config / small helpers
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


def our_journals(db) -> dict[str, str]:
    """journal name -> publisher, for every publisher whose front matter is loaded."""
    out = {}
    for publisher in PUBLISHERS:
        if not db.schema_exists(publisher):
            logging.warning(f"No schema `{publisher}`; its journals are left out.")
            continue
        for r in db.execute_query_result(
                f'SELECT DISTINCT journal_name FROM "{publisher}"."{TABLE_ISSUES}"'):
            out[r["journal_name"]] = publisher
    return dict(sorted(out.items()))


def pct(part, whole) -> str:
    return f"{part / whole:6.1%}" if whole else "     -"


def as_bool(value) -> bool:
    """A boolean that psqlODBC or a CSV may hand over as the string '0' or '1'."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "t", "true", "y", "yes")


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


def is_cached(path: Path, force: bool) -> bool:
    """True if this stage's output is already on disk and may be left alone."""
    if path.exists() and not force:
        print(f"\n{path} already exists; nothing to do (use --force to redo).")
        return True
    return False


def read_csv(path) -> list[dict]:
    if not Path(path).exists():
        sys.exit(f"\nERROR: {path} not found; an earlier stage has not been run.\n")
    # utf-8-sig: a file re-saved in Excel carries a BOM.
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# N-Triples parsing
# ---------------------------------------------------------------------------

def parse_triple(line: str) -> tuple[str, str, str] | None:
    """(subject, predicate, object) of one N-Triples line as bare values, or None."""
    line = line.rstrip()
    if not line.endswith(" ."):
        return None
    line = line[:-2]

    # subject: <IRI> or _:blank
    if line.startswith("<"):
        end = line.find("> ")
        if end < 0:
            return None
        subject, rest = line[1:end], line[end + 2:]
    else:
        end = line.find(" ")
        if end < 0:
            return None
        subject, rest = line[:end], line[end + 1:]

    # predicate is always an IRI
    if not rest.startswith("<"):
        return None
    end = rest.find("> ")
    if end < 0:
        return None
    predicate, obj = rest[1:end], rest[end + 2:]

    return subject, predicate, unquote_object(obj)


def unquote_object(obj: str) -> str:
    """Bare value of an N-Triples object: IRI without brackets, literal without quotes."""
    if obj.startswith("<") and obj.endswith(">"):
        return obj[1:-1]
    if obj.startswith('"'):
        # find the closing quote, honouring backslash escapes
        i, n = 1, len(obj)
        while i < n:
            c = obj[i]
            if c == "\\":
                i += 2
                continue
            if c == '"':
                break
            i += 1
        return nt_unescape(obj[1:i])
    return obj


def predicate_of(line: str) -> str | None:
    """The predicate IRI of an N-Triples line, without parsing the rest.

    An IRI contains no space, so the first ' <' always opens the predicate.
    """
    i = line.find(" <")
    if i < 0:
        return None
    j = line.find(">", i + 2)
    return line[i + 2:j] if j > 0 else None


def journal_key_of(rec_uri: str) -> str | None:
    """'https://dblp.org/rec/journals/tods/Abc23' -> 'tods'."""
    if not rec_uri.startswith(REC_JOURNAL_PREFIX):
        return None
    rest = rec_uri[len(REC_JOURNAL_PREFIX):]
    slash = rest.find("/")
    return rest[:slash] if slash > 0 else None


# ---------------------------------------------------------------------------
# ISO 4 title matching
# ---------------------------------------------------------------------------

def title_tokens(title: str) -> tuple[str, ...]:
    """Lowercase significant word tokens of a journal title, without a leading 'acm'."""
    words = re.split(r"[^A-Za-z0-9]+", (title or "").lower())
    tokens = [w for w in words if w and w not in STOPWORDS]
    if len(tokens) > 1 and tokens[0] == "acm":
        tokens = tokens[1:]
    return tuple(tokens)


def tokens_compatible(ours: tuple[str, ...], theirs: tuple[str, ...]) -> bool:
    """True if two titles agree word for word, allowing ISO 4 truncation."""
    if not ours or len(ours) != len(theirs):
        return False
    return all(a.startswith(b) or b.startswith(a) for a, b in zip(ours, theirs))


class TitleMatcher:
    """Matches a DBLP abbreviated title against our journal names."""

    def __init__(self, our_journals: list[str]):
        self.ours = [(title_tokens(j), j) for j in our_journals]
        self._cache: dict[str, tuple[str, ...]] = {}

    def match(self, dblp_title: str) -> list[str]:
        """Our journal names compatible with this DBLP title."""
        hit = self._cache.get(dblp_title)
        if hit is None:
            theirs = title_tokens(dblp_title)
            hit = tuple(name for toks, name in self.ours
                        if tokens_compatible(toks, theirs))
            self._cache[dblp_title] = hit
        return list(hit)


def split_keys(value) -> list[str]:
    return [k.strip() for k in (value or "").split("|") if k.strip()]


def propose_mapping(our_journals: dict, journal_inventory, verified=None) -> list[dict]:
    """Match our journal names to DBLP journal keys.

    `our_journals` maps name -> publisher, `journal_inventory` is rows of
    journal_key / journal_title / n_articles, and `verified` maps a name to the
    keys already verified by hand, which are kept as they are.
    """
    verified = verified or {}
    matcher = TitleMatcher(list(our_journals))
    hits = defaultdict(list)
    for r in journal_inventory:
        for name in matcher.match(r["journal_title"]):
            hits[name].append(r)
    titles = defaultdict(set)
    counts = Counter()
    for r in journal_inventory:
        titles[r["journal_key"]].add(r["journal_title"])
        counts[r["journal_key"]] += int(r["n_articles"])

    rows = []
    for name, publisher in sorted(our_journals.items()):
        if name in verified:
            keys, confidence = verified[name], "verified"
        else:
            keys = sorted({m["journal_key"] for m in hits.get(name, [])})
            confidence = "high" if len(keys) == 1 else ("multiple" if keys else "none")
        rows.append({
            "journal": name,
            "publisher": publisher,
            "dblp_key": "|".join(keys),
            "dblp_journal_title": " | ".join(sorted({t for k in keys for t in titles[k]})),
            "n_articles": sum(counts[k] for k in keys),
            "confidence": confidence,
        })
    return rows


def verified_mapping(path: Path) -> dict[str, list[str]]:
    """name -> keys of an existing journal_mapping.csv, also under its old
    `acm_journal` header; {} when there is none."""
    if not path.exists():
        return {}
    return {row.get("journal") or row.get("acm_journal"): split_keys(row.get("dblp_key"))
            for row in read_csv(path)}


def load_mapping(path: Path) -> dict[str, list[str]]:
    """journal -> its DBLP journal keys, from the verified mapping."""
    if not path.exists():
        sys.exit(f"\nERROR: {path} not found.\n"
                 f"Run python dblp_extract.py --stage discover, verify "
                 f"journal_mapping_proposed.csv (check every row whose confidence is "
                 f"not `high`) and save it as {path.name}.\n")
    rows = read_csv(path)
    if rows and "publisher" not in rows[0]:
        sys.exit(f"\nERROR: {path} has no publisher column. "
                 f"Run python dblp_extract.py --stage discover and verify the result.\n")
    mapping = {row["journal"]: split_keys(row["dblp_key"]) for row in rows
               if split_keys(row["dblp_key"])}
    if not mapping:
        sys.exit(f"\nERROR: {path} has no usable dblp_key values.\n")
    return mapping


def load_publishers(path: Path) -> dict[str, str]:
    """journal -> publisher, from the verified mapping."""
    return {row["journal"]: row["publisher"] for row in read_csv(path)}


# ---------------------------------------------------------------------------
# Joining a front-matter issue to DBLP: most literal candidate first
# ---------------------------------------------------------------------------

# Per publisher: (tier, DBLP field, front-matter column, front-matter issue column).
# ACM issues are identified by volume and issue, IEEE issues by year and issue.
CANDIDATE_TIERS = {
    "acm": [
        (1, "volume", "volume_label", "issue_label"),
        (2, "volume", "volume_num",   "issue_label"),
        (3, "volume", "volume_label", "issue_first_num"),
        (4, "volume", "volume_num",   "issue_first_num"),
    ],
    "ieee": [
        (1, "year", "year", "issue_label"),
        (2, "year", "year", "issue_num"),
    ],
}


def candidate_tiers(issue: dict) -> list[tuple[int, str, str, str]]:
    """(tier, DBLP field, value, issue) candidates for one issue, most literal first."""
    out: list[tuple[int, str, str, str]] = []
    for tier, field, period_col, num_col in CANDIDATE_TIERS[issue["publisher"]]:
        period, num = issue.get(period_col), issue.get(num_col)
        if period is None or num is None:
            continue
        period, num = str(period).strip(), str(num).strip()
        if not period or not num:
            continue
        if (field, period, num) in [c[1:] for c in out]:
            continue
        out.append((tier, field, period, num))
    return out


# ---------------------------------------------------------------------------
# The issue join
# ---------------------------------------------------------------------------

def index_dblp_issues(articles: list[dict], mapping: dict[str, list[str]]) -> dict:
    """(journal key, "volume" or "year", value, issue) -> the articles in it, for
    the mapped journals."""
    wanted = {k for keys in mapping.values() for k in keys}
    index = defaultdict(list)
    for a in articles:
        if a["journal_key"] not in wanted or not a["issue"]:
            continue
        for field in ("volume", "year"):
            if a[field]:
                index[(a["journal_key"], field, a[field].strip(), a["issue"].strip())].append(a)
    return index


def match_issue(issue: dict, keys: list[str], issue_index: dict):
    """Best (tier, dblp key, field, value, issue, articles) for one front-matter issue."""
    for tier, field, period, num in candidate_tiers(issue):
        for key in keys:
            hit = issue_index.get((key, field, period, num))
            if hit:
                return tier, key, field, period, num, hit
    return None


# ---------------------------------------------------------------------------
# Stage: extract
# ---------------------------------------------------------------------------

def open_dump(path: Path):
    if not path.exists():
        sys.exit(f"\nERROR: DBLP dump not found at {path.resolve()}\n"
                 f"Expected the unzipped N-Triples file. Pass --dump to override.\n")
    return open(path, "r", encoding="utf-8", errors="replace", buffering=1 << 22)


def extract_pass1(dump: Path, our_journals: list[str],
                  key_of: dict | None = None) -> tuple[dict, list[dict]]:
    """Find our articles, and inventory every journal in the dump.

    `key_of` maps a DBLP journal key to our journal name for keys already
    verified; their articles are kept whatever their title.
    """
    key_of = key_of or {}
    matcher = TitleMatcher(our_journals)
    inventory: dict[tuple[str, str], int] = Counter()
    keep: dict[str, tuple[str, str]] = {}     # rec uri -> (journal key, our journal name)
    seen = 0
    start = time.time()

    print(f"  pass 1/2: scanning {dump} for journal articles ...")
    with open_dump(dump) as f:
        for i, line in enumerate(f, 1):
            if NEEDLE_JOURNAL not in line:
                continue
            triple = parse_triple(line)
            if triple is None:
                continue
            subject, predicate, title = triple
            if predicate != P_JOURNAL:
                continue
            key = journal_key_of(subject)
            if key is None:
                continue
            seen += 1
            inventory[(key, title)] += 1
            ours = [key_of[key]] if key in key_of else matcher.match(title)
            if ours:
                # an ambiguous title is kept under the first name
                keep[subject] = (key, ours[0])
            if i % PROGRESS_EVERY == 0:
                print(f"    {i / 1e6:.0f}M lines, {seen} journal articles, "
                      f"{len(keep)} ours, {time.time() - start:.0f}s")

    journals = [{"journal_key": k, "journal_title": t, "n_articles": n}
                for (k, t), n in sorted(inventory.items(), key=lambda kv: -kv[1])]
    print(f"  pass 1 done in {time.time() - start:.0f}s: {seen} journal articles, "
          f"{len(journals)} (key, title) pairs, "
          f"{len(keep)} articles matched to our {len(our_journals)}")
    return keep, journals


def extract_pass2(dump: Path, keep: dict) -> tuple[list[dict], list[dict]]:
    """Pull field values and author signatures for the articles pass 1 selected.

    A signature's publication comes last, so its other fields are buffered until then.
    """
    articles: dict[str, dict] = {}
    signatures: list[dict] = []
    pending: dict[str, dict] = {}
    start = time.time()

    print("  pass 2/2: pulling fields and author signatures ...")
    with open_dump(dump) as f:
        for i, line in enumerate(f, 1):
            predicate = predicate_of(line)
            if predicate not in PASS2_PREDS:
                continue
            triple = parse_triple(line)
            if triple is None:
                continue
            subject, predicate, obj = triple

            if predicate in SIG_PREDS:
                pending.setdefault(subject, {})[predicate] = obj
                continue

            if predicate == P_SIG_PUBLICATION:
                sig = pending.pop(subject, None)
                if sig is not None and obj in keep:
                    signatures.append({
                        "rec_key": obj,
                        "ordinal": sig.get(P_SIG_ORDINAL, ""),
                        "name": sig.get(P_SIG_NAME, ""),
                        "pid": sig.get(P_SIG_CREATOR, ""),
                    })
                continue

            if subject in keep and predicate in ARTICLE_PREDS:
                row = articles.get(subject)
                if row is None:
                    journal_key, our_journal = keep[subject]
                    row = articles[subject] = {
                        "rec_key": subject, "journal_key": journal_key,
                        "journal": our_journal, "journal_title": "",
                        "volume": "", "issue": "", "pagination": "",
                        "year": "", "bibtex_type": "", "title": "",
                    }
                row[ARTICLE_FIELD[predicate]] = obj

            if i % PROGRESS_EVERY == 0:
                print(f"    {i / 1e6:.0f}M lines, {len(articles)} articles, "
                      f"{len(signatures)} signatures, {len(pending)} buffered, "
                      f"{time.time() - start:.0f}s")

    print(f"  pass 2 done in {time.time() - start:.0f}s: {len(articles)} articles, "
          f"{len(signatures)} signatures")
    if pending:
        logging.warning(f"{len(pending)} signature nodes never named a publication")
    return list(articles.values()), signatures


def stage_discover(db, out_dir: Path) -> None:
    """Propose the journal -> DBLP key map, to be verified by hand."""
    journals = read_csv(out_dir / "extract" / "dblp_journals_all.csv")
    ours = our_journals(db)

    mapping = propose_mapping(ours, journals, verified_mapping(out_dir / "journal_mapping.csv"))
    print(f"\n=== JOURNAL MAPPING ({len(ours)} journals vs "
          f"{len(journals)} DBLP journal titles) ===")
    for (publisher, confidence), count in sorted(
            Counter((r["publisher"], r["confidence"]) for r in mapping).items()):
        print(f"  {publisher:<6}{confidence:<10} {count:>4}")
    for row in mapping:
        if row["confidence"] not in ("high", "verified"):
            print(f"    [{row['confidence']}] {row['journal']}"
                  f"  ->  {row['dblp_key'] or '(nothing)'}")

    proposed = out_dir / "journal_mapping_proposed.csv"
    write_csv(proposed, mapping)
    print(f"\nWrote {proposed}. Verify every row that is not `high`, then save it as "
          f"{out_dir / 'journal_mapping.csv'} -- that is the file bbn_extract.py reads.")


def stage_extract(db, out_dir: Path, dump: Path, force: bool) -> None:
    extract_dir = out_dir / "extract"
    articles_csv = extract_dir / "articles.csv"
    if is_cached(articles_csv, force):
        return

    ours = our_journals(db)
    print(f"\n=== EXTRACT: {len(ours)} journals to look for ("
          + ", ".join(f"{p} {n}" for p, n in sorted(Counter(ours.values()).items())) + ") ===")
    key_of = {k: name for name, keys in verified_mapping(out_dir / "journal_mapping.csv").items()
              if name in ours for k in keys}

    keep, journals = extract_pass1(dump, list(ours), key_of)
    write_csv(extract_dir / "dblp_journals_all.csv", journals)
    if not keep:
        sys.exit("\nERROR: no articles matched any of our journals. Check the dump path "
                 "and that the front_matter_issues tables are populated.\n")

    articles, signatures = extract_pass2(dump, keep)
    write_csv(articles_csv, articles)
    write_csv(extract_dir / "signatures.csv", signatures)
    print("\nNext: python dblp_extract.py --stage discover")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract our journals' slice of the DBLP N-Triples dump (read-only).")
    parser.add_argument("--stage", choices=["extract", "discover"], required=True,
                        help="extract: read the dump and write the CSVs; "
                             "discover: propose the journal map from them")
    parser.add_argument("--out-dir", default=str(REPORTS_DIR), help="where to write the CSVs")
    parser.add_argument("--dump", default=None,
                        help=f"path to the unzipped dblp .nt (default: {DUMP_PATH})")
    parser.add_argument("--env", default=None,
                        help=f"path to the .env with the POSTGRES_* values "
                             f"(default: {ENV_PATH})")
    parser.add_argument("--force", action="store_true",
                        help="redo the extract even if it is already cached")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    db = connect(Path(args.env) if args.env else ENV_PATH)
    if args.stage == "discover":
        stage_discover(db, Path(args.out_dir))
    else:
        stage_extract(db, Path(args.out_dir),
                      Path(args.dump) if args.dump else DUMP_PATH, args.force)


if __name__ == "__main__":
    main()
