"""
Builds the BBN corpus for the co-author-editor case study from the front matter
of every publisher in PUBLISHERS, our journals' slice of the DBLP dump
(dblp_extract.py) and further passes over the dump.

    python bbn_extract.py --stage pairs
        Suspect papers, their co-authorships, board tenure, the pre-tenure
        control and the people whose full record is needed. The only stage that
        reads the database.
    python bbn_extract.py --stage coauthors
        Dump pass: every publication of those people.
    python bbn_extract.py --stage editor_authors
        Dump pass: authors and year of every publication of the editors.
    python bbn_extract.py --stage signer_authors
        Dump pass: authors of every publication of the suspect papers' signers.
    python bbn_extract.py --stage corpus
        Writes bbn/bbn_cs2_corpus.json.
    python bbn_extract.py --stage all

The dump passes are cached; --force redoes them.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

from cs2_config import (BBN_DIR, DUMP_PATH, ENV_PATH, FIT_MIN_COLLABS, FIT_STATES,
                        JUNIOR_MAX_PUBS, MAX_AUTHORS, NETWORK_BANDS, OUTSIDE_BANDS,
                        OUTSIDE_STATES, PUBLISHERS, REPEATED_MIN, REPORTS_DIR, STATES)
from dblp_extract import (
    PROGRESS_EVERY, P_SIG_CREATOR, P_SIG_NAME, P_SIG_ORDINAL,
    P_SIG_PUBLICATION, P_YEAR, SIG_PREDS, TABLE_BOARD, TABLE_ISSUES, as_bool, connect,
    index_dblp_issues, is_cached, load_mapping, load_publishers, match_issue, open_dump,
    parse_triple, pct, predicate_of, read_csv, write_csv,
)

REC_PREFIX = "https://dblp.org/rec/"
PID_PREFIX = "https://dblp.org/pid/"
JOURNAL_KIND = "journals"


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def base_name(name: str) -> str:
    """'Yang Liu 0001' -> 'Yang Liu'."""
    parts = (name or "").rsplit(" ", 1)
    if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 4:
        return parts[0]
    return name or ""


def person_id(pid: str, name: str) -> str:
    """Identity key: the DBLP PID where DBLP has one, else the printed base name."""
    return (pid or "").strip() or base_name((name or "").strip())


def name_key(name: str) -> str:
    """A name for matching: base name, lower case, without accents or punctuation.

    'W. R. STONE' -> 'w r stone', 'Jürgen Müller-Lüdke 0001' -> 'jurgen muller ludke'.
    """
    text = unicodedata.normalize("NFKD", base_name(name or "")).encode("ascii", "ignore")
    return " ".join(re.findall(r"[a-z0-9]+", text.decode().lower()))


def initials_match(a: str, b: str) -> bool:
    """Two name keys with the same surname whose first given names agree, where
    one may be an initial: 'w r stone' and 'w ross stone'."""
    a, b = a.split(), b.split()
    if len(a) < 2 or len(b) < 2 or a[-1] != b[-1]:
        return False
    x, y = a[0], b[0]
    return x == y or (len(x) == 1 and y.startswith(x)) or (len(y) == 1 and x.startswith(y))


class PeopleIndex:
    """The people behind the signatures, to tell whether an initialled name
    such as 'c douligeris' can stand for only one of them."""

    def __init__(self, signatures=()):
        self.by_surname = defaultdict(set)
        for s in signatures:
            key = name_key(s["name"])
            if key:
                self.by_surname[key.split()[-1]].add((key, person_id(s["pid"], s["name"])))
        self._cache: dict[str, bool] = {}

    def is_unique(self, key: str) -> bool:
        """True if exactly one person has this name key or one it initials_match."""
        if key not in self._cache:
            people = {pid for k, pid in self.by_surname.get(key.split()[-1], ())
                      if k == key or initials_match(key, k)}
            self._cache[key] = len(people) == 1
        return self._cache[key]


class BoardIndex:
    """The names on a board, to look up a signer's name in."""

    def __init__(self, names=(), people: PeopleIndex | None = None):
        self.keys = {name_key(n) for n in names} - {""}
        self.people = people
        self.by_surname = defaultdict(list)
        for key in sorted(self.keys):
            self.by_surname[key.split()[-1]].append(key)

    def match(self, name) -> str | None:
        """The board key this name matches: the same key, else the one key that
        initials_match it. A match that rests on an initial also needs the board
        key to fit only one person in `people`, when given. None otherwise."""
        key = name_key(name)
        if not key or key in self.keys:
            return key or None
        hits = [k for k in self.by_surname.get(key.split()[-1], ()) if initials_match(k, key)]
        if len(hits) != 1:
            return None
        hit = hits[0]
        if self.people and hit.split()[0] != key.split()[0] and not self.people.is_unique(hit):
            return None
        return hit


def venue_of(rec_uri: str) -> tuple[str, str]:
    """'https://dblp.org/rec/conf/sigmod/Abc23' -> ('conf', 'sigmod')."""
    if not rec_uri.startswith(REC_PREFIX):
        return "", ""
    parts = rec_uri[len(REC_PREFIX):].split("/")
    if len(parts) < 2:
        return "", ""
    return parts[0], parts[1]


def is_inside(rec_uri: str, inside_keys: set[str]) -> bool:
    """True if this publication is in one of the given DBLP journal keys."""
    kind, key = venue_of(rec_uri)
    return kind == JOURNAL_KIND and key in inside_keys


def career_stage(n_pubs: int) -> str:
    """The co-author's career stage."""
    return "junior" if n_pubs <= JUNIOR_MAX_PUBS else "established"


def band_of(n: int, edges) -> int:
    """Index of the band `n` falls in, given ascending inclusive upper edges."""
    for i, hi in enumerate(edges):
        if n <= hi:
            return i
    return len(edges)


def classify(n_outside: int, n_inside: int, repeated_min: int = REPEATED_MIN) -> str:
    """The state of one co-authorship, strongest evidence first."""
    if n_outside > 0:
        return OUTSIDE_STATES[band_of(n_outside, OUTSIDE_BANDS)]
    return "repeated" if n_inside >= repeated_min else "single"


def midrank_pct(values, x) -> float:
    """Midrank percentile of x within values, ties averaged. 0 = lowest."""
    n = len(values)
    if n < 2:
        return 0.5
    below = sum(1 for v in values if v < x)
    equal = sum(1 for v in values if v == x)
    return (below + (equal - 1) / 2) / (n - 1)


def issue_year(articles: list[dict]) -> str:
    """The year of a matched DBLP issue: the earliest year its articles carry."""
    years = sorted(a["year"] for a in articles if a.get("year"))
    return years[0] if years else ""


def paper_loads(coauthor_ids: list[str]) -> dict[str, float]:
    """One paper carries load 1, split over its co-authors."""
    unique = list(dict.fromkeys(cid for cid in coauthor_ids if cid))
    if not unique:
        return {}
    share = 1.0 / len(unique)
    return {cid: share for cid in unique}


def publications_of(path) -> dict[str, set[str]]:
    """person -> the publications a dump pass found for them (pid_publications.csv)."""
    pubs_of = defaultdict(set)
    for row in read_csv(path):
        pubs_of[row["person_id"]].add(row["rec_key"])
    return pubs_of


# ---------------------------------------------------------------------------
# The fit: where the editor sits in the co-author's own network
# ---------------------------------------------------------------------------

def collaboration_networks(path, wanted: set, with_venues=False) -> tuple[dict, dict]:
    """person -> collaborator -> publications shared, and -> the venues they were in.

    Only the `wanted` people get a network. Without `with_venues` the second dict
    is empty.
    """
    authors_of = defaultdict(list)
    with open(path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            authors_of[row["rec_key"]].append(row["person_id"])

    depth = defaultdict(lambda: defaultdict(int))
    spread = defaultdict(lambda: defaultdict(set))
    for rec_key, ids in authors_of.items():
        ids = set(ids)
        if len(ids) < 2 or len(ids) > MAX_AUTHORS:
            continue
        focal = ids & wanted
        if not focal:
            continue
        venue = venue_of(rec_key) if with_venues else None
        for a in focal:
            da = depth[a]
            for b in ids:
                if b != a:
                    da[b] += 1
            if with_venues:
                sa = spread[a]
                for b in ids:
                    if b != a:
                        sa[b].add(venue)
    return depth, spread


def annotate_fit(nodes: dict, depth: dict) -> None:
    """Add `fit_pct` and `network_band` to every node, None without enough collaborators."""
    for node in nodes.values():
        cols = depth.get(node["coauthor_id"])
        if not cols or len(cols) < FIT_MIN_COLLABS:
            node["fit_pct"] = None
            node["network_band"] = None
            continue
        values = list(cols.values())
        if node["editor_id"] in cols:
            x = cols[node["editor_id"]]
        else:
            # The dump pass missed the signature; the pair shares at least one paper.
            x = 1
            values = values + [1]
        node["fit_pct"] = round(midrank_pct(values, x), 6)
        node["network_band"] = band_of(len(cols), NETWORK_BANDS)


def fit_cuts(nodes: dict) -> dict:
    """Load-weighted terciles of `fit_pct` within each state: {state: [lo, hi] or None}."""
    by_state = defaultdict(list)
    for node in nodes.values():
        if node.get("fit_pct") is not None:
            by_state[node["state"]].append((node["fit_pct"], node["load"]))
    cuts = {}
    for state, pairs in by_state.items():
        pairs.sort()
        total = sum(w for _, w in pairs)
        edges, run, i = [], 0.0, 0
        for q in (1 / 3, 2 / 3):
            target = q * total
            while i < len(pairs) - 1 and run + pairs[i][1] < target:
                run += pairs[i][1]
                i += 1
            edges.append(pairs[i][0])
        cuts[state] = None if edges[0] >= edges[1] else [round(e, 6) for e in edges]
    return cuts


def fit_label(pct, state, cuts) -> str:
    """Discretize a percentile against its own state's cuts."""
    if pct is None:
        return ""
    edge = cuts.get(state)
    if not edge:
        return "typical"
    return ("peripheral" if pct < edge[0]
            else "core" if pct >= edge[1] else "typical")


def fit_baseline(nodes: dict) -> dict:
    """Load-weighted counts[state][network_band][fit], the genuine reference."""
    counts = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for node in nodes.values():
        if not node.get("fit"):
            continue
        counts[node["state"]][str(node["network_band"])][node["fit"]] += node["load"]
    return {state: {band: {f: round(vals.get(f, 0.0), 6) for f in FIT_STATES}
                    for band, vals in bands.items()}
            for state, bands in counts.items()}


def relabel_fit(nodes: dict, cuts=None) -> dict:
    """Give every node its fit label, and return the fit baseline over them.

    The cuts default to the nodes' own terciles.
    """
    cuts = fit_cuts(nodes) if cuts is None else cuts
    for node in nodes.values():
        node["fit"] = fit_label(node.get("fit_pct"), node["state"], cuts)
    return fit_baseline(nodes)


def by_publisher(nodes: dict) -> dict:
    """publisher -> {node id: node}."""
    groups = defaultdict(dict)
    for node_id, node in nodes.items():
        groups[node.get("publisher", "")][node_id] = node
    return groups


def publisher_fit_cuts(nodes: dict) -> dict:
    """fit_cuts within each publisher: {publisher: cuts}."""
    return {p: fit_cuts(group) for p, group in by_publisher(nodes).items()}


def relabel_fit_per_publisher(nodes: dict, cuts=None) -> dict:
    """relabel_fit within each publisher, returning {publisher: fit baseline}.

    `cuts` is {publisher: cuts}; by default each publisher's own nodes' terciles.
    """
    return {p: relabel_fit(group, None if cuts is None else cuts.get(p, {}))
            for p, group in by_publisher(nodes).items()}


# ---------------------------------------------------------------------------
# Stage: pairs  (DB + cached DBLP extract; no dump reading)
# ---------------------------------------------------------------------------

FRONT_MATTER_RE = re.compile(
    r"^\s*("
    r"editorial|guest editorial|introduction|foreword|preface|prologue|"
    r"welcome|letter from|message from|note from|in this issue|from the editor|"
    r"editor's note|editors' note|special issue|special section|"
    r"erratum|corrigendum|retraction|acknowledg|in memoriam|obituary|"
    r"report on|reviewers|call for papers|corrections? to\b|correction:|"
    r"\d{4} index|table of contents|information for authors|\["
    r")", re.I)


def is_front_matter(title) -> bool:
    """True if the title looks like a front-matter piece, else False."""
    return bool(FRONT_MATTER_RE.match(title or ""))


# The column that, with the issue label, identifies an issue of each publisher.
PERIOD_COLUMN = {"acm": "volume_label", "ieee": "year"}


def read_front_matter(db) -> tuple[list, dict, dict]:
    """The front-matter issues of every publisher, the board of each, and every
    board name per journal. An issue carries its `publisher` and `period`."""
    issues, per_issue, per_journal = [], defaultdict(set), defaultdict(set)
    for publisher in PUBLISHERS:
        if not db.schema_exists(publisher):
            logging.warning(f"No schema `{publisher}`; its front matter is left out.")
            continue
        period = PERIOD_COLUMN[publisher]
        for issue in db.execute_query_result(
                f'SELECT * FROM "{publisher}"."{TABLE_ISSUES}"'):
            issues.append({**issue, "publisher": publisher, "period": str(issue[period])})
        for row in db.execute_query_result(f"""
                SELECT journal_name, {period}::text AS period, issue_label, name
                FROM "{publisher}"."{TABLE_BOARD}" WHERE name IS NOT NULL"""):
            name = (row["name"] or "").strip()
            if not name:
                continue
            per_issue[(row["journal_name"], row["period"], row["issue_label"])].add(name)
            per_journal[row["journal_name"]].add(name)
    return issues, per_issue, per_journal


def dated_issues(issues, mapping, issue_index, board_of_issue, stats=None):
    """(journal, year, board, articles) per DBLP issue that a front-matter issue
    with a board maps to. Front-matter issues mapping to the same DBLP issue (the
    parts of one issue) are merged, with their boards united. `stats` counts per
    publisher how many front-matter issues were matched."""
    stats = Counter() if stats is None else stats
    merged = {}
    for issue in issues:
        journal, publisher = issue["journal_name"], issue["publisher"]
        stats[(publisher, "issues")] += 1
        keys = mapping.get(journal, [])
        hit = match_issue(issue, keys, issue_index) if keys else None
        if not hit:
            continue
        board = board_of_issue.get((journal, issue["period"], issue["issue_label"]), set())
        if not board:
            continue
        stats[(publisher, "matched")] += 1
        entry = merged.setdefault(hit[1:5], (journal, set(), hit[-1]))
        entry[1].update(board)
    for journal, board, articles in merged.values():
        yield journal, issue_year(articles), board, articles


def coauthorship_rows(article, sigs, ed_id, ed_name, journal, group, year,
                      board: BoardIndex, front_matter: int) -> list[dict]:
    """One row per distinct co-author of `ed_id` on this article, with their share of the load."""
    coauthors = [s for s in sigs if person_id(s["pid"], s["name"]) != ed_id]
    loads = paper_loads([person_id(s["pid"], s["name"]) for s in coauthors])
    rows, seen = [], set()
    for sig in coauthors:
        cid = person_id(sig["pid"], sig["name"])
        if cid in seen or cid not in loads:
            continue
        seen.add(cid)
        co_name = base_name(sig["name"])
        rows.append({
            "rec_key": article["rec_key"], "journal": journal,
            "year": year, "group": group,
            "editor_id": ed_id, "editor_name": ed_name,
            "coauthor_id": cid, "coauthor_name": co_name,
            "load": round(loads[cid], 6),
            "front_matter": front_matter,
            "coauthor_on_board": int(board.match(sig["name"]) is not None),
        })
    return rows


class PairsCollector:
    """What the pairs stage learns, issue by issue, from the dated front matter.

    An editor is any signer of an article whose name matches a name on the
    board of that article's own issue (BoardIndex.match).
    """

    def __init__(self, board_of_journal, people: PeopleIndex | None = None):
        self.people = people
        self.board_of_journal = {j: BoardIndex(names, people)
                                 for j, names in board_of_journal.items()}
        self.papers: list[dict] = []          # one row per (suspect paper x editor)
        self.rows: list[dict] = []            # suspect co-authorship rows
        self.tenure_years = defaultdict(list)      # (journal, editor id) -> issue years
        self.tenure_names = defaultdict(Counter)   # (journal, editor id) -> DBLP names
        self.tenure_keys = defaultdict(Counter)    # (journal, editor id) -> board name keys
        self.board_years = defaultdict(list)       # (journal, board name key) -> roster years
        self.identities_of_name = defaultdict(set)
        self.n_no_pid = 0

    def add_issue(self, journal, year, board, articles, authors_of) -> None:
        index = BoardIndex(board, self.people)
        # Keyed by name: a board member who never signed anything has no PID.
        if year:
            for key in index.keys:
                self.board_years[(journal, key)].append(year)
        for article in articles:
            sigs = authors_of.get(article["rec_key"], [])
            on_board = {}
            for s in sigs:
                key = index.match(s["name"])
                if key:
                    on_board.setdefault(person_id(s["pid"], s["name"]), (s, key))
            for editor_sig, key in on_board.values():
                self.add_paper(article, sigs, editor_sig, journal, year, key)

    def add_paper(self, article, sigs, editor_sig, journal, year, board_key="") -> None:
        ed_name = base_name(editor_sig["name"])
        ed_id = person_id(editor_sig["pid"], ed_name)
        if not editor_sig["pid"]:
            self.n_no_pid += 1
        self.identities_of_name[ed_name].add(ed_id)
        self.tenure_years[(journal, ed_id)].append(year)
        self.tenure_names[(journal, ed_id)][ed_name] += 1
        self.tenure_keys[(journal, ed_id)][board_key or name_key(ed_name)] += 1

        front_matter = int(is_front_matter(article.get("title")))
        rows = coauthorship_rows(article, sigs, ed_id, ed_name, journal, "suspect",
                                 article["year"] or year,
                                 self.board_of_journal.get(journal, BoardIndex()), front_matter)
        self.rows += rows
        self.papers.append({
            "rec_key": article["rec_key"], "journal": journal,
            "dblp_key": article["journal_key"], "volume": article["volume"],
            "issue": article["issue"], "year": article["year"] or year,
            "editor_id": ed_id, "editor_name": ed_name,
            "n_coauthors": len(rows),
            "front_matter": front_matter,
        })


def board_tenure(tenure_years, tenure_names, tenure_keys, board_years) -> list[dict]:
    """A tenure row per (journal, editor), with their window on that board.

    The window comes from the roster, or from the years the editor published
    when the name is on no dated issue.
    """
    rows = []
    for (journal, ed_id), years in tenure_years.items():
        name = tenure_names[(journal, ed_id)].most_common(1)[0][0]
        key = tenure_keys[(journal, ed_id)].most_common(1)[0][0]
        published = sorted(y for y in years if y)
        roster = sorted(board_years.get((journal, key), []))
        known = roster or published
        rows.append({
            "journal": journal, "editor_id": ed_id, "editor_name": name,
            "first_board_year": known[0] if known else "",
            "last_board_year": known[-1] if known else "",
            "n_board_issues": len(roster),
            "n_board_issues_with_a_paper": len(published),
            "first_paper_year": published[0] if published else "",
            "roster_window": int(bool(roster)),
        })
    return rows


def control_coauthorships(tenure, articles_of_person, authors_of, mapping,
                          board_of_journal) -> tuple[list[dict], int]:
    """The within-editor control: same journal, before the editor joined its board."""
    rows, n_papers = [], 0
    for t in tenure:
        first = t["first_board_year"]
        if not first:
            continue
        journal, ed_id, ed_name = t["journal"], t["editor_id"], t["editor_name"]
        inside_keys = set(mapping.get(journal, []))
        for article in articles_of_person.get(ed_id, []):
            if article["journal_key"] not in inside_keys:
                continue
            if not article["year"] or article["year"] >= first:
                continue
            n_papers += 1
            rows += coauthorship_rows(
                article, authors_of.get(article["rec_key"], []), ed_id, ed_name,
                journal, "control", article["year"],
                board_of_journal.get(journal, BoardIndex()),
                int(is_front_matter(article.get("title"))))
    return rows, n_papers


def print_pairs_summary(pairs: PairsCollector, control_rows, tenure, wanted,
                        n_control_papers, issue_stats) -> None:
    n_solo = sum(1 for p in pairs.papers if p["n_coauthors"] == 0)
    editors = {p["editor_id"] for p in pairs.papers}
    ambiguous = [n for n, ids in pairs.identities_of_name.items() if len(ids) > 1]
    from_roster = sum(1 for t in tenure if t["roster_window"])
    later = sum(1 for t in tenure if t["first_paper_year"] and t["first_board_year"]
                and t["first_paper_year"] > t["first_board_year"])

    for publisher in sorted({p for p, _ in issue_stats}):
        n, hit = issue_stats[(publisher, "issues")], issue_stats[(publisher, "matched")]
        print(f"  {publisher} front-matter issues matched to DBLP "
              f"{hit:>6} of {n} ({pct(hit, n)})")
    print(f"\n  suspect papers (paper x editor)     {len(pairs.papers):>7}")
    print(f"  ...solo (no co-author, no node)     {n_solo:>7}  "
          f"({pct(n_solo, len(pairs.papers))})")
    print(f"  suspect co-authorship rows          {len(pairs.rows):>7}")
    print(f"  distinct editors                    {len(editors):>7}")
    print(f"  ...whose name maps to >1 identity   {len(ambiguous):>7}  "
          f"(the homonyms DBLP does split)")
    print(f"  ...signatures with no DBLP PID      {pairs.n_no_pid:>7}  "
          f"(identity falls back to the name)")
    print(f"  control papers (pre-tenure, same j) {n_control_papers:>7}")
    print(f"  board windows taken from the roster   {from_roster:>7}  "
          f"({pct(from_roster, len(tenure))})")
    print(f"  ...where the editor first published later than joining "
          f"{later} ({pct(later, len(tenure))})")
    print(f"  control co-authorship rows          {len(control_rows):>7}")
    print(f"  people whose full record is needed  {len(wanted):>7}")
    print("\nNext: --stage coauthors  (the first dump pass)")


def stage_pairs(db, reports_dir: Path, out_dir: Path, mapping_path: Path) -> None:
    extract_dir = reports_dir / "extract"
    if not (extract_dir / "articles.csv").exists():
        sys.exit(f"\nERROR: no cached DBLP extract under {extract_dir}.\n"
                 f"Run the extract first:\n"
                 f"    python dblp_extract.py --stage extract  --out-dir {reports_dir}\n"
                 f"    python dblp_extract.py --stage discover --out-dir {reports_dir}\n"
                 f"    (verify journal_mapping_proposed.csv, save it as journal_mapping.csv)\n")
    articles = read_csv(extract_dir / "articles.csv")
    signatures = read_csv(extract_dir / "signatures.csv")
    mapping = load_mapping(mapping_path)

    authors_of = defaultdict(list)
    for sig in signatures:
        authors_of[sig["rec_key"]].append(sig)
    issue_index = index_dblp_issues(articles, mapping)

    issues, board_of_issue, board_of_journal = read_front_matter(db)
    print(f"\n=== PAIRS: {len(issues)} front-matter issues, "
          f"{len(board_of_journal)} journals with a board ===")

    pairs = PairsCollector(board_of_journal, PeopleIndex(signatures))
    issue_stats = Counter()
    for journal, year, board, arts in dated_issues(issues, mapping, issue_index,
                                                   board_of_issue, issue_stats):
        pairs.add_issue(journal, year, board, arts, authors_of)
    tenure = board_tenure(pairs.tenure_years, pairs.tenure_names, pairs.tenure_keys,
                          pairs.board_years)

    article_of = {a["rec_key"]: a for a in articles}
    articles_of_person = defaultdict(list)
    for sig in signatures:
        article = article_of.get(sig["rec_key"])
        if article:
            articles_of_person[person_id(sig["pid"], sig["name"])].append(article)
    control_rows, n_control_papers = control_coauthorships(
        tenure, articles_of_person, authors_of, mapping, pairs.board_of_journal)

    all_rows = pairs.rows + control_rows
    wanted = sorted({r["editor_id"] for r in all_rows}
                    | {r["coauthor_id"] for r in all_rows}
                    | {p["editor_id"] for p in pairs.papers})

    write_csv(out_dir / "suspect_papers.csv", pairs.papers)
    write_csv(out_dir / "coauthorships.csv", all_rows)
    write_csv(out_dir / "editor_tenure.csv", tenure)
    # Every name on every board, also those who never published there.
    write_csv(out_dir / "board_roster.csv",
              [{"journal": j, "name": n, "first_year": min(v),
                "last_year": max(v), "n_issues": len(v)}
               for (j, n), v in sorted(pairs.board_years.items())])
    write_csv(out_dir / "wanted_pids.csv", [{"person_id": p} for p in wanted])

    print_pairs_summary(pairs, control_rows, tenure, wanted, n_control_papers, issue_stats)


# ---------------------------------------------------------------------------
# Stages: coauthors, editor_authors, signer_authors  (one dump pass each)
# ---------------------------------------------------------------------------

KEEP_PREDS = (SIG_PREDS - {P_SIG_ORDINAL}) | {P_SIG_PUBLICATION, P_YEAR}


def scan_signatures(dump: Path, looking_for: str, row_for,
                    years_for: set = frozenset()) -> list[dict]:
    """The dump's author signatures, as `row_for(person, publication)` shapes them.

    `row_for` returns the CSV row to keep, or None to drop the signature. Rows of
    the publications in `years_for` also get the publication year.
    """
    pending: dict[str, dict] = {}
    rows: list[dict] = []
    years: dict[str, str] = {}
    start = time.time()

    print(f"  scanning {dump} for {looking_for} ...")
    with open_dump(dump) as f:
        for i, line in enumerate(f, 1):
            if i % PROGRESS_EVERY == 0:
                print(f"    {i / 1e6:.0f}M lines, {len(rows)} signatures kept, "
                      f"{len(pending)} buffered, {time.time() - start:.0f}s")
            if predicate_of(line) not in KEEP_PREDS:
                continue
            triple = parse_triple(line)
            if triple is None:
                continue
            subject, predicate, obj = triple
            if predicate == P_YEAR:
                if subject in years_for:
                    years[subject] = obj
                continue
            if predicate in SIG_PREDS:
                pending.setdefault(subject, {})[predicate] = obj
                continue
            sig = pending.pop(subject, None)
            if sig is None:
                continue
            row = row_for(person_id(sig.get(P_SIG_CREATOR, ""), sig.get(P_SIG_NAME, "")),
                          obj)
            if row is not None:
                rows.append(row)

    print(f"  done in {time.time() - start:.0f}s: {len(rows)} signatures, "
          f"{len({r['person_id'] for r in rows})} people over "
          f"{len({r['rec_key'] for r in rows})} publications")
    if pending:
        logging.warning(f"{len(pending)} signature nodes never named a publication")
    if years_for:
        for row in rows:
            row["year"] = years.get(row["rec_key"], "")
    return rows


def stage_coauthors(out_dir: Path, dump: Path, force: bool) -> None:
    """Every publication of every person the corpus needs a full record for."""
    out_path = out_dir / "pid_publications.csv"
    if is_cached(out_path, force):
        return
    wanted = {r["person_id"] for r in read_csv(out_dir / "wanted_pids.csv")}
    if not wanted:
        sys.exit("\nERROR: wanted_pids.csv is empty. Run --stage pairs first.\n")
    print(f"\n=== COAUTHORS: one pass over the dump for {len(wanted)} people ===")
    write_csv(out_path, scan_signatures(
        dump, f"the signatures of {len(wanted)} people",
        lambda person, rec: {"person_id": person, "rec_key": rec}
        if person in wanted else None))
    print("\nNext: --stage editor_authors")


def scan_authors_of(out_path: Path, out_dir: Path, dump: Path, people: set,
                    banner: str, next_stage: str, with_year=False) -> None:
    """The full author list of every publication of `people`, from one dump pass."""
    pubs = {r["rec_key"] for r in read_csv(out_dir / "pid_publications.csv")
            if r["person_id"] in people}
    if not pubs:
        sys.exit("\nERROR: no publications for any of them. Run --stage coauthors first.\n")
    print(f"\n=== {banner}: one pass over the dump for the {len(pubs)} publications "
          f"of {len(people)} people ===")
    write_csv(out_path, scan_signatures(
        dump, f"the authors of {len(pubs)} publications",
        lambda person, rec: {"rec_key": rec, "person_id": person}
        if rec in pubs else None,
        years_for=pubs if with_year else frozenset()))
    print(f"\nNext: --stage {next_stage}")


def stage_editor_authors(out_dir: Path, dump: Path, force: bool) -> None:
    """The full author list and the year of every publication an editor signed."""
    out_path = out_dir / "editor_pub_authors.csv"
    if is_cached(out_path, force):
        return
    editors = {r["editor_id"] for r in read_csv(out_dir / "suspect_papers.csv")}
    if not editors:
        sys.exit("\nERROR: suspect_papers.csv is empty. Run --stage pairs first.\n")
    scan_authors_of(out_path, out_dir, dump, editors, "EDITOR AUTHORS", "signer_authors",
                    with_year=True)


def stage_signer_authors(out_dir: Path, reports_dir: Path, dump: Path, force: bool) -> None:
    """The full author list of every publication of every signer of a suspect paper."""
    out_path = out_dir / "signer_pub_authors.csv"
    if is_cached(out_path, force):
        return
    suspects = {r["rec_key"] for r in read_csv(out_dir / "suspect_papers.csv")
                if not as_bool(r.get("front_matter"))}
    signers = {person_id(s["pid"], s["name"])
               for s in read_csv(reports_dir / "extract" / "signatures.csv")
               if s["rec_key"] in suspects}
    scan_authors_of(out_path, out_dir, dump, signers, "SIGNER AUTHORS", "corpus")


# ---------------------------------------------------------------------------
# Stage: corpus
# ---------------------------------------------------------------------------

def inout_points(editor_id: str, seats: dict, pubs, mapping, authors_of_pub,
                 year_of: dict) -> list:
    """The in/out gate: one (x, y) point per board seat the editor holds.

    `seats` maps each journal to the editor's (first, last) board year there; only
    publications with a year inside that window count.
    """
    points = []
    for journal, (first, last) in seats.items():
        keys = set(mapping.get(journal, []))
        if not keys:
            continue
        inside, outside = set(), set()
        for rec in pubs:
            year = year_of.get(rec, "")
            if not year or not first <= int(year) <= last:
                continue
            target = inside if is_inside(rec, keys) else outside
            target.update(authors_of_pub.get(rec, ()))
        inside.discard(editor_id)
        outside.discard(editor_id)
        points.append({"journal": journal, "x": len(inside), "y": len(outside)})
    return points


def benign_reason(entry, coauthor_pubs=None) -> str:
    """Why this co-authorship is excluded from the evidence, or "" if it is not.

    board_member      the co-author sits on the same board.
    front_matter      every paper the pair shares is front matter.
    state_determined  the co-author's whole record is the shared paper.
    """
    if entry["on_board"]:
        return "board_member"
    if entry["n_papers"] and entry["n_front_matter"] == entry["n_papers"]:
        return "front_matter"
    if coauthor_pubs == 1:
        return "state_determined"
    return ""


def build_nodes(rows, pubs_of, mapping, controlled_of, repeated_min=REPEATED_MIN,
                publisher_of=None):
    """One node per (editor, journal, co-author), with its summed load, state and career stage."""
    publisher_of = publisher_of or {}
    grouped = defaultdict(lambda: {"load": 0.0, "n_papers": 0, "rec_keys": [],
                                   "n_front_matter": 0, "on_board": False})
    labels: dict[str, str] = {}
    for row in rows:
        key = (row["editor_id"], row["journal"], row["coauthor_id"])
        entry = grouped[key]
        entry["load"] += float(row["load"])
        entry["n_papers"] += 1
        entry["rec_keys"].append(row["rec_key"])
        entry["n_front_matter"] += as_bool(row.get("front_matter"))
        entry["on_board"] = entry["on_board"] or as_bool(row.get("coauthor_on_board"))
        labels[row["coauthor_id"]] = row["coauthor_name"]

    nodes = {}
    for (ed_id, journal, co_id), entry in grouped.items():
        inside_keys = set(mapping.get(journal, []))
        # The suspect papers are unioned in, in case the dump pass missed a signature.
        shared = (pubs_of.get(ed_id, set()) & pubs_of.get(co_id, set())) | set(entry["rec_keys"])
        inside = {r for r in shared if is_inside(r, inside_keys)}
        outside = shared - inside
        # Strict reading: venues the editor also edits do not count as outside.
        controlled = controlled_of.get(ed_id, set()) - inside_keys
        outside_strict = {r for r in outside if not is_inside(r, controlled)}
        node_id = f"{ed_id}||{journal}||{co_id}"
        n_pubs = len(pubs_of.get(co_id, set()))
        nodes[node_id] = {
            "editor_id": ed_id, "journal": journal,
            "publisher": publisher_of.get(journal, ""), "coauthor_id": co_id,
            "coauthor_name": labels.get(co_id, co_id),
            "load": round(entry["load"], 6), "n_shared_suspect": entry["n_papers"],
            "n_shared_inside": len(inside), "n_shared_outside": len(outside),
            "n_shared_outside_strict": len(outside_strict),
            "coauthor_pubs": n_pubs, "career_stage": career_stage(n_pubs),
            "state": classify(len(outside), len(inside), repeated_min),
            "n_front_matter": entry["n_front_matter"],
            "coauthor_on_board": entry["on_board"],
            "benign_reason": benign_reason(entry, n_pubs),
            # filled by annotate_fit / relabel_fit, which need the networks
            "fit_pct": None, "network_band": None, "fit": "",
        }
    return nodes


def baseline_counts(nodes) -> dict:
    """Load-weighted genuine baseline per journal: counts[journal][career_stage][state],
    with the journal's publisher."""
    counts = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    publisher = {}
    for node in nodes.values():
        counts[node["journal"]][node["career_stage"]][node["state"]] += node["load"]
        publisher[node["journal"]] = node.get("publisher", "")
    return {
        journal: {
            "publisher": publisher[journal],
            "baseline_counts": {stage: {s: round(bins.get(s, 0.0), 6) for s in STATES}
                                for stage, bins in by_stage.items()},
        }
        for journal, by_stage in counts.items()
    }


def index_by_editor(nodes) -> dict:
    """editor id -> the ids of that editor's nodes."""
    index = defaultdict(list)
    for node_id, node in nodes.items():
        index[node["editor_id"]].append(node_id)
    return index


def editor_diagnostics(editor_index, labels, papers, pubs_of, mapping,
                       authors_of_pub, year_of, windows) -> dict:
    """Per editor: what they published, where, and their in/out gate points.

    `inout_points` is None when --stage editor_authors has not been run.
    `windows` maps (journal, editor) to the (first, last) board year.
    """
    papers_by_editor = defaultdict(list)
    for paper in papers:
        papers_by_editor[paper["editor_id"]].append(paper)

    per_editor = {}
    for ed_id in sorted(set(editor_index) | set(papers_by_editor)):
        own = papers_by_editor.get(ed_id, [])
        in_journals = {p["journal"] for p in own}
        in_keys = {k for j in in_journals for k in mapping.get(j, [])}
        pubs = pubs_of.get(ed_id, set())
        seats = {j: windows[(j, ed_id)] for j in sorted(in_journals)
                 if (j, ed_id) in windows}
        per_editor[ed_id] = {
            "name": labels.get(ed_id, ed_id),
            "is_pid": ed_id.startswith(PID_PREFIX),
            "journals": sorted(in_journals),
            "n_suspect_papers": len(own),
            "n_pubs_outside_journal":
                sum(1 for r in pubs if not is_inside(r, in_keys)),
            "inout_points": (inout_points(ed_id, seats, pubs, mapping, authors_of_pub,
                                          year_of) if authors_of_pub else None),
        }
    return per_editor


def print_corpus_summary(nodes, control_nodes, benign, papers,
                         n_editors, n_control_editors) -> None:
    total_load = sum(n["load"] for n in nodes.values())
    benign_load = sum(n["load"] for n in benign.values())
    n_solo = sum(1 for p in papers if int(p["n_coauthors"]) == 0)
    by_state, load_by_state = Counter(), defaultdict(float)
    for node in nodes.values():
        by_state[node["state"]] += 1
        load_by_state[node["state"]] += node["load"]

    print(f"\n=== CORPUS: {len(nodes)} co-authorship nodes over {n_editors} editors ===")
    print(f"  total load {total_load:.1f}; with the {benign_load:.1f} benign load "
          f"added back {total_load + benign_load:.1f}, against {len(papers)} suspect "
          f"papers of which {n_solo} solo")
    print(f"  {'state':<12}{'nodes':>8}{'share':>9}{'load':>10}{'load share':>12}")
    for state in STATES:
        print(f"  {state:<12}{by_state[state]:>8}{pct(by_state[state], len(nodes)):>9}"
              f"{load_by_state[state]:>10.1f}{pct(load_by_state[state], total_load):>12}")
    print(f"  control nodes (pre-tenure)  {len(control_nodes)} over "
          f"{n_control_editors} editors")
    print(f"  excluded before scoring     {len(benign)} nodes, load {benign_load:.1f} "
          f"({pct(benign_load, benign_load + total_load)} of what was extracted); "
          f"bbn_report.py breaks this down by reason")


def warn_missing(path: Path, needed_for: str, stage: str) -> None:
    logging.warning(f"{path.name} not found -- {needed_for} will be unavailable. "
                    f"Run --stage {stage}.")


def stage_corpus(out_dir: Path, mapping_path: Path) -> None:
    mapping = load_mapping(mapping_path)
    publisher_of = load_publishers(mapping_path)
    rows = read_csv(out_dir / "coauthorships.csv")
    papers = read_csv(out_dir / "suspect_papers.csv")
    tenure = read_csv(out_dir / "editor_tenure.csv")
    pubs_of = publications_of(out_dir / "pid_publications.csv")

    authors_path = out_dir / "editor_pub_authors.csv"
    authors_of_pub, year_of = defaultdict(set), {}
    if authors_path.exists():
        for row in read_csv(authors_path):
            if "year" not in row:
                sys.exit(f"\nERROR: {authors_path.name} predates the year column. "
                         "Run --stage editor_authors --force.\n")
            authors_of_pub[row["rec_key"]].add(row["person_id"])
            year_of[row["rec_key"]] = row["year"]
    else:
        warn_missing(authors_path, "the in/out co-author gate", "editor_authors")

    windows = {(row["journal"], row["editor_id"]):
               (int(row["first_board_year"]), int(row["last_board_year"]))
               for row in tenure if row["first_board_year"] and row["last_board_year"]}

    # The journals each editor sits on a board of.
    controlled_of = defaultdict(set)
    for row in tenure:
        controlled_of[row["editor_id"]].update(mapping.get(row["journal"], []))

    nodes = build_nodes([r for r in rows if r["group"] == "suspect"],
                        pubs_of, mapping, controlled_of, publisher_of=publisher_of)
    control_nodes = build_nodes([r for r in rows if r["group"] == "control"],
                                pubs_of, mapping, controlled_of, publisher_of=publisher_of)

    signer_path = out_dir / "signer_pub_authors.csv"
    if signer_path.exists():
        wanted = {n["coauthor_id"] for group in (nodes, control_nodes)
                  for n in group.values()}
        depth, _ = collaboration_networks(signer_path, wanted)
        annotate_fit(nodes, depth)
        annotate_fit(control_nodes, depth)
    else:
        warn_missing(signer_path, "the partner-pattern (fit) node", "signer_authors")

    # Benign nodes leave the evidence and the baseline; the fit cuts come from
    # the scored nodes only, per publisher.
    benign = {k: v for k, v in nodes.items() if v["benign_reason"]}
    nodes = {k: v for k, v in nodes.items() if not v["benign_reason"]}
    control_nodes = {k: v for k, v in control_nodes.items() if not v["benign_reason"]}
    cuts = publisher_fit_cuts(nodes)
    fit_base = relabel_fit_per_publisher(nodes, cuts)
    relabel_fit_per_publisher(control_nodes, cuts)
    relabel_fit_per_publisher(benign, cuts)

    editor_index = index_by_editor(nodes)
    control_index = index_by_editor(control_nodes)

    labels: dict[str, str] = {}
    for row in tenure:
        labels.setdefault(row["editor_id"], row["editor_name"])
    for paper in papers:
        labels.setdefault(paper["editor_id"], paper["editor_name"])

    corpus = {
        "model": "cs2_v1_per_coauthorship_load_weighted",
        "config": {
            "publishers": sorted(set(publisher_of.values())),
            "states": STATES,
            "repeated_min": REPEATED_MIN,
            "split_outside": True,
            "outside_bands": OUTSIDE_BANDS,
            "junior_max_pubs": JUNIOR_MAX_PUBS,
            "career_stages": ["junior", "established"],
            "fit_states": FIT_STATES,
            "fit_min_collabs": FIT_MIN_COLLABS,
            "network_bands": NETWORK_BANDS,
            "load_rule": "one suspect paper carries load 1, split over its co-authors",
        },
        "journals": baseline_counts(nodes),
        "fit_baseline": fit_base,
        "coauthorships": nodes,
        "editor_index": {k: sorted(v) for k, v in editor_index.items()},
        "editor_labels": labels,
        "editors": editor_diagnostics(editor_index, labels, papers, pubs_of,
                                      mapping, authors_of_pub, year_of, windows),
        "control_coauthorships": control_nodes,
        "control_index": {k: sorted(v) for k, v in control_index.items()},
        "benign_coauthorships": benign,
    }

    out_path = out_dir / "bbn_cs2_corpus.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(corpus, f, ensure_ascii=False, indent=2)

    print_corpus_summary(nodes, control_nodes, benign, papers,
                         len(editor_index), len(control_index))
    print(f"\nWrote {out_path}\nNext: python bbn_infer.py")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the CS2 co-authorship BBN corpus (read-only).")
    parser.add_argument("--stage", required=True,
                        choices=["pairs", "coauthors", "editor_authors",
                                 "signer_authors", "corpus", "all"])
    parser.add_argument("--reports-dir", default=str(REPORTS_DIR),
                        help="dblp_extract.py's --out-dir (holds extract/ and the mapping)")
    parser.add_argument("--out-dir", default=str(BBN_DIR), help="where the BBN files go")
    parser.add_argument("--dump", default=None,
                        help=f"path to the unzipped dblp .nt (default: {DUMP_PATH})")
    parser.add_argument("--env", default=None,
                        help=f"path to the .env with the POSTGRES_* values (default: {ENV_PATH})")
    parser.add_argument("--mapping", default=None,
                        help="verified journal mapping CSV "
                             "(default: <reports-dir>/journal_mapping.csv)")
    parser.add_argument("--force", action="store_true",
                        help="redo a cached dump pass (coauthors, editor_authors, "
                             "signer_authors)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    reports_dir = Path(args.reports_dir)
    out_dir = Path(args.out_dir)
    mapping_path = Path(args.mapping) if args.mapping else reports_dir / "journal_mapping.csv"
    dump = Path(args.dump) if args.dump else DUMP_PATH

    if args.stage in ("pairs", "all"):
        db = connect(Path(args.env) if args.env else ENV_PATH)
        stage_pairs(db, reports_dir, out_dir, mapping_path)
    if args.stage in ("coauthors", "all"):
        stage_coauthors(out_dir, dump, args.force)
    if args.stage in ("editor_authors", "all"):
        stage_editor_authors(out_dir, dump, args.force)
    if args.stage in ("signer_authors", "all"):
        stage_signer_authors(out_dir, reports_dir, dump, args.force)
    if args.stage in ("corpus", "all"):
        stage_corpus(out_dir, mapping_path)


if __name__ == "__main__":
    main()
