"""
Tests for bbn_extract.py: the pure functions and the stage building blocks, on
hand-checked cases. No DB, no dump, no cache; the file readers get a tempfile.

    python test_bbn_extract.py
"""

from __future__ import annotations

import tempfile
from collections import Counter
from pathlib import Path

import bbn_extract as extract

_failures = 0


def check(label, condition):
    global _failures
    if not condition:
        _failures += 1
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}")


def approx(a, b, tol=1e-9):
    return abs(a - b) <= tol


def tempfile_with(text: str, suffix=".csv") -> Path:
    f = tempfile.NamedTemporaryFile("w", suffix=suffix, delete=False, encoding="utf-8",
                                    newline="")
    f.write(text)
    f.close()
    return Path(f.name)


JKEY = "https://dblp.org/rec/journals/jkey/"
CONF = "https://dblp.org/rec/conf/sigmod/"


def sig(pid, name):
    return {"pid": pid, "name": name}


# --- pure helpers -------------------------------------------------------------

def test_name_matching():
    print("\n-- matching a signer to a board name --")
    check("a name key ignores case, accents, punctuation and the DBLP number",
          extract.name_key("Jürgen MÜLLER-Lüdke 0001") == "jurgen muller ludke")
    check("an initial matches the full first name on the same surname",
          extract.initials_match("w r stone", "w ross stone"))
    check("two different full first names do not match",
          not extract.initials_match("amitava chatterjee", "anirban chatterjee"))
    check("a different surname never matches",
          not extract.initials_match("w stone", "w stine"))
    board = extract.BoardIndex({"W. R. STONE", "Paul C.-P. Chao", "J. Smith", "J. A. Smith",
                                "Jo Smith"})
    check("a board name printed in capitals is found",
          board.match("Paul C.-P. Chao") == "paul c p chao")
    check("an initialled board name is found from the DBLP name",
          board.match("W. Ross Stone 0002") == "w r stone")
    check("an exact match wins over initials",
          board.match("Jo Smith") == "jo smith")
    check("a name two board names match by initial alone is not matched",
          board.match("Jane Smith") is None)
    check("an empty board matches nobody", extract.BoardIndex().match("Ed X") is None)

    people = extract.PeopleIndex([
        sig("https://dblp.org/pid/cd", "Christos Douligeris"),
        sig("https://dblp.org/pid/lz1", "Lei Zhang 0001"),
        sig("https://dblp.org/pid/lz2", "Li Zhang"),
        sig("https://dblp.org/pid/az", "Albert Y. Zomaya"),
        sig("https://dblp.org/pid/az2", "Albert Zomaya")])
    board = extract.BoardIndex({"C. DOULIGERIS", "L. ZHANG", "Albert Zomaya"}, people)
    check("an initialled board name that fits one person is matched",
          board.match("Christos Douligeris") == "c douligeris")
    check("an initialled board name that fits several people is not",
          board.match("Lei Zhang 0001") is None)
    check("a match on the full first name needs no such check",
          board.match("Albert Y. Zomaya") == "albert zomaya")


def test_identity_and_venue():
    print("\n-- names, identity and venues --")
    check("base_name strips a 4-digit DBLP discriminator",
          extract.base_name("Yang Liu 0001") == "Yang Liu")
    check("base_name leaves a shorter trailing number alone",
          extract.base_name("Henry VIII 42") == "Henry VIII 42")
    check("base_name leaves an undiscriminated name alone",
          extract.base_name("V. S. Subrahmanian") == "V. S. Subrahmanian")

    check("identity prefers the DBLP PID",
          extract.person_id("https://dblp.org/pid/1/2", "X")
          == "https://dblp.org/pid/1/2")
    check("identity falls back to the name when there is no PID",
          extract.person_id("", "Jane Doe") == "Jane Doe")
    check("...and to the BASE name, so the two identity paths agree",
          extract.person_id("", "Jane Doe 0001") == "Jane Doe")

    check("venue_of reads a conference record",
          extract.venue_of(CONF + "Abc23") == ("conf", "sigmod"))
    check("a conference is never inside the edited journal",
          not extract.is_inside("https://dblp.org/rec/conf/jkey/Abc23", {"jkey"}))
    check("a journal record in a mapped key is inside",
          extract.is_inside(JKEY + "Abc23", {"jkey", "jacm"}))
    check("a journal record in another journal is outside",
          not extract.is_inside("https://dblp.org/rec/journals/csur/Abc23", {"jkey"}))


def test_load_rule():
    """One paper = load 1, split over the authors other than the editor, so a
    multi-author paper counts as ONE event and not as one per co-author."""
    print("\n-- the load rule --")
    loads = extract.paper_loads(["A", "B", "C"])
    check("3 co-authors get 1/3 each", all(approx(v, 1 / 3) for v in loads.values()))
    check("a solo paper creates no node at all", extract.paper_loads([]) == {})
    check("a co-author listed twice takes one share, not two",
          approx(sum(extract.paper_loads(["A", "A", "B"]).values()), 1.0)
          and approx(extract.paper_loads(["A", "A", "B"])["A"], 0.5))


def test_classify():
    print("\n-- the state of a co-authorship --")
    check("no outside tie, 2 shared in-journal papers -> `repeated`",
          extract.classify(n_outside=0, n_inside=2) == "repeated")
    check("no outside tie, 1 shared paper -> `single`",
          extract.classify(n_outside=0, n_inside=1) == "single")
    check("repeated_min=3 pushes a 2-paper pair back to `single`",
          extract.classify(0, 2, repeated_min=3) == "single")
    check("one shared paper elsewhere is the thinnest outside tie",
          extract.classify(n_outside=1, n_inside=9) == "outside_1")
    check("two to four shared papers elsewhere is the middle bin",
          extract.classify(n_outside=2, n_inside=1) == "outside_2_4"
          and extract.classify(n_outside=4, n_inside=1) == "outside_2_4")
    check("five or more is a deep collaboration",
          extract.classify(n_outside=5, n_inside=1) == "outside_5plus")
    check("a co-author at the junior cut is junior",
          extract.career_stage(extract.JUNIOR_MAX_PUBS) == "junior")
    check("one publication past the cut is established",
          extract.career_stage(extract.JUNIOR_MAX_PUBS + 1) == "established")


def test_percentiles_and_bands():
    print("\n-- percentiles and bands --")
    check("the lowest value gets percentile 0",
          extract.midrank_pct([1, 2, 3, 4, 5], 1) == 0.0)
    check("the highest gets 1", extract.midrank_pct([1, 2, 3, 4, 5], 5) == 1.0)
    check("an all-ties distribution puts everyone in the middle",
          extract.midrank_pct([1, 1, 1, 1], 1) == 0.5)
    check("a tied block is averaged, not ordered arbitrarily",
          extract.midrank_pct([1, 1, 1, 1, 9], 1) == 1.5 / 4
          and abs(extract.midrank_pct([1, 1, 1, 4], 1) - 1.0 / 3) < 1e-12)
    check("a one-element distribution is no distribution: percentile 0.5",
          extract.midrank_pct([3], 3) == 0.5)
    check("a band edge is inclusive",
          extract.band_of(20, extract.NETWORK_BANDS) == 0
          and extract.band_of(21, extract.NETWORK_BANDS) == 1)
    check("past the last edge is the last band",
          extract.band_of(10_000, extract.NETWORK_BANDS) == len(extract.NETWORK_BANDS))


# --- the fit --------------------------------------------------------------------

def fit_nodes(pcts, load=1.0, state="single"):
    return {f"n{i}": {"state": state, "fit_pct": p, "load": load, "network_band": 0}
            for i, p in enumerate(pcts)}


def test_fit_cuts_and_labels():
    print("\n-- the fit: cuts, labels and baseline --")
    even = fit_nodes([i / 10 for i in range(9)])           # 0.0 .. 0.8, load 1 each
    cuts = extract.fit_cuts(even)
    check("equal loads give plain terciles: the 3rd and 6th of 9 percentiles",
          cuts == {"single": [0.2, 0.5]})
    heavy = fit_nodes([i / 10 for i in range(8)])
    heavy["big"] = {"state": "single", "fit_pct": 0.9, "load": 100.0, "network_band": 0}
    check("one node carrying nearly all the load leaves no spread: no cuts",
          extract.fit_cuts(heavy) == {"single": None})
    check("identical percentiles leave no spread either",
          extract.fit_cuts(fit_nodes([0.5, 0.5, 0.5])) == {"single": None})
    check("a state whose nodes have no percentile gets no entry",
          extract.fit_cuts(fit_nodes([None, None], state="repeated")) == {})
    check("cuts are per state",
          set(extract.fit_cuts({**even, **fit_nodes([0.1, 0.5, 0.9], state="repeated")}))
          == {"single", "repeated"})

    check("below the low cut is peripheral",
          extract.fit_label(0.10, "single", cuts) == "peripheral")
    check("the low cut itself is typical",
          extract.fit_label(0.2, "single", cuts) == "typical")
    check("the high cut itself is core",
          extract.fit_label(0.5, "single", cuts) == "core")
    check("a state with no usable spread is uniformly typical",
          extract.fit_label(0.99, "outside_5plus", {"outside_5plus": None}) == "typical")
    check("a co-author with too small a network gets no fit at all",
          extract.fit_label(None, "single", cuts) == "")

    base = extract.relabel_fit(even)
    check("relabel_fit labels every node against its own state's cuts",
          Counter(n["fit"] for n in even.values())
          == {"peripheral": 2, "typical": 3, "core": 4})
    check("...and returns the load-weighted fit baseline per state and band",
          base == {"single": {"0": {"peripheral": 2.0, "typical": 3.0, "core": 4.0}}})
    extract.relabel_fit(even, cuts={"single": [0.45, 0.85]})
    check("cuts passed in are used instead of the nodes' own",
          Counter(n["fit"] for n in even.values())
          == {"peripheral": 5, "typical": 4})
    check("a node without a percentile stays out of the baseline",
          extract.relabel_fit(fit_nodes([None])) == {})

    acm = {k: {**n, "publisher": "acm"} for k, n in fit_nodes([i / 10 for i in range(9)]).items()}
    ieee = {f"i{k}": {**n, "publisher": "ieee"} for k, n in fit_nodes([0.5] * 3).items()}
    cuts = extract.publisher_fit_cuts({**acm, **ieee})
    check("fit cuts are taken per publisher",
          cuts == {"acm": {"single": [0.2, 0.5]}, "ieee": {"single": None}})
    base = extract.relabel_fit_per_publisher({**acm, **ieee}, cuts)
    check("...and so is the fit baseline",
          set(base) == {"acm", "ieee"} and base["ieee"]["single"]["0"]["typical"] == 3.0)


def test_annotate_fit():
    print("\n-- annotate_fit --")
    nodes = {"a": {"coauthor_id": "A", "editor_id": "E", "load": 1.0},
             "b": {"coauthor_id": "B", "editor_id": "E", "load": 1.0},
             "c": {"coauthor_id": "C", "editor_id": "E", "load": 1.0}}
    extract.annotate_fit(nodes, {"A": {"E": 1, "x": 3, "y": 3, "z": 3, "w": 3},
                                 "B": {"E": 1, "x": 3},
                                 "C": {"x": 3, "y": 3, "z": 3, "w": 3, "v": 3}})
    check("a big enough network is scored: E is A's one-off, percentile 0",
          nodes["a"]["fit_pct"] == 0.0 and nodes["a"]["network_band"] == 0)
    check("a network below the minimum is refused",
          nodes["b"]["fit_pct"] is None and nodes["b"]["network_band"] is None)
    check("an editor the dump pass missed enters the partner's network at depth 1",
          nodes["c"]["fit_pct"] == 0.0 and nodes["c"]["network_band"] == 0)


def test_networks_from_file():
    print("\n-- collaboration networks and publication lists from a CSV --")
    big = "".join(f"{CONF}Big,P{i}\n" for i in range(extract.MAX_AUTHORS + 1))
    path = tempfile_with("rec_key,person_id\n"
                         f"{JKEY}P1,A\n{JKEY}P1,B\n{JKEY}P1,C\n"
                         f"{CONF}P2,A\n{CONF}P2,B\n"
                         f"{JKEY}Solo,A\n"
                         + big + f"{CONF}Big,A\n")
    depth, spread = extract.collaboration_networks(path, {"A"}, with_venues=True)
    check("only the wanted person gets a network", set(depth) == {"A"})
    check("depth counts the papers a pair shares",
          dict(depth["A"]) == {"B": 2, "C": 1})
    check("spread counts the distinct venues they shared them in",
          len(spread["A"]["B"]) == 2 and len(spread["A"]["C"]) == 1)
    check("a solo paper and a paper above MAX_AUTHORS contribute nothing",
          "P0" not in depth["A"])
    depth, spread = extract.collaboration_networks(path, {"A"})
    check("without venues the second dict stays empty", spread == {})

    pubs = extract.publications_of(tempfile_with("person_id,rec_key\nA,r1\nA,r2\nB,r1\n"))
    check("publications_of groups a person's records into a set",
          pubs["A"] == {"r1", "r2"} and pubs["B"] == {"r1"})
    check("...and an unknown person has none", pubs["Z"] == set())


# --- stage pairs: the building blocks --------------------------------------------

E, A, B, F = (sig("https://dblp.org/pid/e", "Ed X"), sig("https://dblp.org/pid/a", "Al A"),
              sig("", "Bea Board 0001"), sig("https://dblp.org/pid/f", "Fay F"))
ARTICLE = {"rec_key": JKEY + "P1", "journal_key": "jkey", "volume": "1", "issue": "2",
           "year": "2020", "title": "A research title"}


def test_coauthorship_rows():
    print("\n-- co-authorship rows for one article --")
    rows = extract.coauthorship_rows(ARTICLE, [E, A, A, B], "https://dblp.org/pid/e",
                                     "Ed X", "J", "suspect", "2020",
                                     extract.BoardIndex({"Bea Board"}), front_matter=1)
    by_id = {r["coauthor_id"]: r for r in rows}
    check("one row per distinct co-author; the editor is not their own co-author",
          set(by_id) == {"https://dblp.org/pid/a", "Bea Board"})
    check("a co-author who signed twice takes one share",
          all(approx(r["load"], 0.5) for r in rows))
    check("a PID-less signer is keyed and named by the base name",
          by_id["Bea Board"]["coauthor_name"] == "Bea Board")
    check("the board flag marks the co-author who sits on the same board",
          by_id["Bea Board"]["coauthor_on_board"] == 1
          and by_id["https://dblp.org/pid/a"]["coauthor_on_board"] == 0)
    check("group, year and the front-matter flag are carried on every row",
          all((r["group"], r["year"], r["front_matter"]) == ("suspect", "2020", 1)
              for r in rows))
    check("an article the editor signed alone yields no rows",
          extract.coauthorship_rows(ARTICLE, [E], "https://dblp.org/pid/e", "Ed X",
                                    "J", "suspect", "2020", extract.BoardIndex(), 0) == [])


def test_front_matter():
    print("\n-- front matter --")
    check("an editorial is front matter",
          extract.is_front_matter("Editorial: the Year Ahead."))
    check("a special-issue introduction is front matter",
          extract.is_front_matter(
              "Introduction to the Special Issue on Incident Response."))
    check("leading whitespace does not hide front matter",
          extract.is_front_matter("  Guest Editorial: Data Ethics"))
    check("a survey that merely starts with `An Introduction` is NOT front matter",
          not extract.is_front_matter(
              "An Introduction to Type Theory for Programmers"))
    check("a research title is not front matter",
          not extract.is_front_matter("Memory Forensics on Embedded Devices"))
    check("an IEEE correction notice is front matter",
          extract.is_front_matter("Corrections to \u201cA Fast Solver\u201d"))
    check("a research title starting with `Correction of` is not",
          not extract.is_front_matter("Correction of Motion Artifacts in MRI"))
    check("a missing title is not front matter", not extract.is_front_matter(None))


def test_dated_issues_and_collector():
    print("\n-- dated issues and the pairs collector --")
    issue = {"publisher": "acm", "journal_name": "J", "volume_label": "1", "volume_num": 1,
             "issue_label": "2", "issue_first_num": 2, "period": "1"}
    mapping = {"J": ["jkey"], "I": ["ikey"]}
    ieee_article = {**ARTICLE, "rec_key": JKEY + "I1", "journal_key": "ikey", "volume": "70"}
    index = extract.index_dblp_issues([ARTICLE, ieee_article], mapping)
    board_of_issue = {("J", "1", "2"): {"Ed X"}}
    stats = Counter()
    hits = list(extract.dated_issues([issue, {**issue, "journal_name": "Q"},
                                      {**issue, "issue_label": "9"}],
                                     mapping, index, board_of_issue, stats))
    check("an issue DBLP knows, with a board, is dated from its articles",
          hits == [("J", "2020", {"Ed X"}, [ARTICLE])])
    check("an unmapped journal and an issue without a board are skipped, and counted",
          len(hits) == 1 and stats[("acm", "issues")] == 3 and stats[("acm", "matched")] == 1)

    part = {"publisher": "ieee", "journal_name": "I", "year": 2020, "issue_label": "2_Part_1",
            "issue_num": 2, "period": "2020"}
    parts = [part, {**part, "issue_label": "2_Part_2"}]
    boards = {("I", "2020", "2_Part_1"): {"Ann A"}, ("I", "2020", "2_Part_2"): {"Bob B"}}
    hits = list(extract.dated_issues(parts, mapping, index, boards))
    check("an IEEE issue is matched on year and issue number",
          [h[3] for h in hits] == [[ieee_article]])
    check("the parts of one issue yield its papers once, with the boards united",
          len(hits) == 1 and hits[0][2] == {"Ann A", "Bob B"})

    pairs = extract.PairsCollector(board_of_journal={"J": {"Ed X", "Bea Board"}})
    pairs.add_issue("J", "2020", {"Ed X"}, [ARTICLE],
                    {ARTICLE["rec_key"]: [E, E, A, B]})
    check("an editor signing the same article twice yields it once",
          len(pairs.papers) == 1 and pairs.papers[0]["n_coauthors"] == 2)
    check("the paper row carries the editor, the year and the DBLP key",
          pairs.papers[0]["editor_id"] == "https://dblp.org/pid/e"
          and pairs.papers[0]["year"] == "2020" and pairs.papers[0]["dblp_key"] == "jkey")
    check("every board name of a dated issue gets that roster year, under its key",
          pairs.board_years == {("J", "ed x"): ["2020"]})
    check("the editor's tenure and printed name are recorded per journal",
          pairs.tenure_years == {("J", "https://dblp.org/pid/e"): ["2020"]}
          and pairs.tenure_names[("J", "https://dblp.org/pid/e")] == Counter({"Ed X": 1}))

    pairs.add_issue("J", "2021", {"Ed Y"}, [{**ARTICLE, "rec_key": JKEY + "P2"}],
                    {JKEY + "P2": [sig("", "Ed Y"), A]})
    check("an editor without a PID is identified by name and counted",
          pairs.n_no_pid == 1 and ("J", "Ed Y") in pairs.tenure_years)


def test_board_tenure_and_control():
    print("\n-- board tenure and the pre-tenure control --")
    tenure_years = {("J", "pE"): ["2019", "2020"], ("J", "pF"): ["2021"]}
    tenure_names = {("J", "pE"): Counter({"Ed Xavier": 2}), ("J", "pF"): Counter({"Fay": 1})}
    tenure_keys = {("J", "pE"): Counter({"e x": 2}), ("J", "pF"): Counter({"fay": 1})}
    board_years = {("J", "e x"): ["2018", "2019", "2020", "2021"]}
    tenure = extract.board_tenure(tenure_years, tenure_names, tenure_keys, board_years)
    e, f = tenure
    check("the window comes from the roster under the matched board name",
          (e["first_board_year"], e["last_board_year"], e["roster_window"])
          == ("2018", "2021", 1) and e["editor_name"] == "Ed Xavier")
    check("issues on the board and issues with a paper are both counted",
          e["n_board_issues"] == 4 and e["n_board_issues_with_a_paper"] == 2
          and e["first_paper_year"] == "2019")
    check("a name on no dated issue falls back to the years they published",
          (f["first_board_year"], f["last_board_year"], f["roster_window"])
          == ("2021", "2021", 0))

    tenure = [{"journal": "J", "editor_id": "pE", "editor_name": "Ed X",
               "first_board_year": "2018"},
              {"journal": "K", "editor_id": "pG", "editor_name": "G",
               "first_board_year": ""}]
    art = lambda key, year, rec: {"rec_key": rec, "journal_key": key, "year": year,
                                  "title": "t"}
    before, during, elsewhere, undated = (art("jkey", "2016", "r1"), art("jkey", "2018", "r2"),
                                          art("other", "2015", "r3"), art("jkey", "", "r4"))
    authors_of = {a["rec_key"]: [sig("pE", "Ed X"), A] for a in
                  (before, during, elsewhere, undated)}
    rows, n_papers = extract.control_coauthorships(
        tenure, {"pE": [before, during, elsewhere, undated], "pG": [before]},
        authors_of, {"J": ["jkey"]}, {"J": extract.BoardIndex()})
    check("only the editor's earlier papers IN the edited journal are controls",
          n_papers == 1 and [r["rec_key"] for r in rows] == ["r1"])
    check("a control row is marked as such, with the paper's own year",
          rows[0]["group"] == "control" and rows[0]["year"] == "2016")


# --- stage coauthors: the dump scan -------------------------------------------------

def test_scan_signatures():
    print("\n-- scanning the dump for signatures --")
    ns = "https://dblp.org/rdf/schema#"
    dump = tempfile_with(
        f'_:Sig_1 <{ns}signatureDblpName> "Yang Liu 0001" .\n'
        f'_:Sig_1 <{ns}signatureCreator> <https://dblp.org/pid/1/2> .\n'
        f'_:Sig_1 <{ns}signatureOrdinal> "1" .\n'
        f'_:Sig_1 <{ns}signaturePublication> <{JKEY}A1> .\n'
        f'_:Sig_2 <{ns}signatureDblpName> "No Pid 0002" .\n'
        f'_:Sig_2 <{ns}signaturePublication> <{CONF}B1> .\n'
        f'<{JKEY}A1> <{ns}title> "T" .\n'
        f'<{JKEY}A1> <{ns}yearOfPublication> '
        f'"2019"^^<http://www.w3.org/2001/XMLSchema#gYear> .\n'
        f'_:Sig_3 <{ns}signaturePublication> <{CONF}Orphan> .\n', suffix=".nt")
    rows = extract.scan_signatures(dump, "a test",
                                   lambda person, rec: {"person_id": person, "rec_key": rec})
    check("a signature is resolved when its publication line arrives",
          {"person_id": "https://dblp.org/pid/1/2", "rec_key": JKEY + "A1"} in rows)
    check("a signature without a creator falls back to the base name",
          {"person_id": "No Pid", "rec_key": CONF + "B1"} in rows)
    check("a publication line with no buffered signature is dropped", len(rows) == 2)
    kept = extract.scan_signatures(dump, "a test",
                                   lambda person, rec: {"rec_key": rec, "person_id": person}
                                   if rec == JKEY + "A1" else None)
    check("row_for decides what is kept", len(kept) == 1)
    dated = extract.scan_signatures(dump, "a test",
                                    lambda person, rec: {"rec_key": rec, "person_id": person},
                                    years_for={JKEY + "A1", CONF + "B1"})
    year = {r["rec_key"]: r["year"] for r in dated}
    check("years_for adds the year, even when it follows the signatures in the dump",
          year[JKEY + "A1"] == "2019")
    check("a publication the dump gives no year gets an empty one",
          year[CONF + "B1"] == "")
    check("without years_for the rows carry no year at all",
          all("year" not in r for r in rows))


# --- stage corpus -----------------------------------------------------------------

def suspect_rows():
    """E wrote two papers in journal J: P1 with A and B, P2 with A alone."""
    row = lambda co, rec, load: {
        "editor_id": "E", "journal": "J", "coauthor_id": co, "coauthor_name": co,
        "rec_key": JKEY + rec, "load": load, "group": "suspect"}
    return [row("A", "P1", "0.5"), row("B", "P1", "0.5"), row("A", "P2", "1.0")]


def test_build_nodes():
    """The whole per-node pipeline on a hand-checked case. A also co-published
    with E at a conference; B never did."""
    print("\n-- build_nodes --")
    mapping = {"J": ["jkey"]}
    rows = suspect_rows()
    # A has a long record, so `established`; B has published only the one shared
    # paper, so `junior` -- the two land in different career stages of the baseline.
    pubs = {
        "E": {JKEY + "P1", JKEY + "P2", CONF + "X", "https://dblp.org/rec/journals/other/Y"},
        "A": {JKEY + "P1", JKEY + "P2", CONF + "X"}
             | {f"https://dblp.org/rec/conf/other/A{i}"
                for i in range(extract.JUNIOR_MAX_PUBS)},
        "B": {JKEY + "P1"},
    }
    nodes = extract.build_nodes(rows, pubs, mapping, controlled_of={},
                                publisher_of={"J": "acm"})
    a_node, b_node = nodes["E||J||A"], nodes["E||J||B"]
    check("a node carries its journal's publisher", a_node["publisher"] == "acm")
    check("A's load is 0.5 + 1.0 = 1.5", approx(a_node["load"], 1.5))
    check("B's load is 0.5", approx(b_node["load"], 0.5))
    check("A is `outside_1` (one shared SIGMOD paper, no more)",
          a_node["state"] == "outside_1")
    check("B is `single` (one shared paper, nothing else)",
          b_node["state"] == "single")
    check("A's 2 shared in-journal papers are counted",
          a_node["n_shared_inside"] == 2)
    check("B's short record makes B a junior co-author",
          b_node["career_stage"] == "junior")
    check("A's long record makes A an established co-author",
          a_node["career_stage"] == "established")
    check("B, whose whole record is the shared paper, is state-determined",
          b_node["benign_reason"] == "state_determined" and a_node["benign_reason"] == "")

    # A pair whose only outside tie is in ANOTHER journal E edits.
    strict_pubs = dict(pubs)
    strict_pubs["B"] = {JKEY + "P1", "https://dblp.org/rec/journals/mine/Z"}
    strict_pubs["E"] = pubs["E"] | {"https://dblp.org/rec/journals/mine/Z"}
    strict = extract.build_nodes(rows, strict_pubs, mapping,
                                 controlled_of={"E": {"jkey", "mine"}})
    check("a tie in another venue the editor controls is outside by default",
          strict["E||J||B"]["state"] == "outside_1")
    check("...but the strict count, which bbn_report re-states from, excludes it",
          strict["E||J||B"]["n_shared_outside_strict"] == 0
          and extract.classify(0, strict["E||J||B"]["n_shared_inside"]) == "single")

    thin = extract.build_nodes(rows, {"E": set(), "A": set(), "B": set()}, mapping, {})
    check("suspect papers count as shared even if the dump pass missed them",
          thin["E||J||A"]["n_shared_inside"] == 2
          and thin["E||J||A"]["state"] == "repeated")

    check("the baseline records the journal's publisher",
          extract.baseline_counts(nodes)["J"]["publisher"] == "acm")
    base = extract.baseline_counts(nodes)["J"]["baseline_counts"]
    check("the baseline is weighted by load (A contributes 1.5 to `outside_1`)",
          approx(base["established"]["outside_1"], 1.5))
    check("the baseline puts B's 0.5 on `single` in the junior career stage",
          approx(base["junior"]["single"], 0.5))
    check("every state is present in every career stage, at 0 where nothing landed",
          set(base["junior"]) == set(extract.STATES) and base["junior"]["repeated"] == 0.0)
    check("index_by_editor lists an editor's node ids",
          dict(extract.index_by_editor(nodes)) == {"E": ["E||J||A", "E||J||B"]})


def test_benign():
    print("\n-- benign-by-construction nodes --")
    check("a pair whose every shared paper is front matter is benign",
          extract.benign_reason({"on_board": False, "n_papers": 2,
                                 "n_front_matter": 2}) == "front_matter")
    check("one real paper alongside an editorial keeps the pair as evidence",
          extract.benign_reason({"on_board": False, "n_papers": 2,
                                 "n_front_matter": 1}) == "")
    check("a co-author on the same board is benign whatever they wrote",
          extract.benign_reason({"on_board": True, "n_papers": 3,
                                 "n_front_matter": 0}) == "board_member")
    check("board membership is reported ahead of front matter",
          extract.benign_reason({"on_board": True, "n_papers": 1,
                                 "n_front_matter": 1}) == "board_member")
    plain = {"on_board": False, "n_papers": 1, "n_front_matter": 0}
    check("an ordinary pair is not benign", extract.benign_reason(plain, 12) == "")
    check("a co-author with no record beyond the shared paper is state-determined",
          extract.benign_reason(plain, 1) == "state_determined")
    check("...but two publications leave room for an outside tie",
          extract.benign_reason(plain, 2) == "")
    check("an unknown record size is not a reason to exclude",
          extract.benign_reason(plain) == "")


def test_editor_diagnostics_and_inout():
    """E sits on the boards of J1 (key jkey) and J2 (key jtwo), 2010-2015. Inside
    J1: A, B. Everywhere else: Z (a conference) and A again (another journal).
    Outside the window: W (2005, before the seat) and V (no year)."""
    print("\n-- per-editor diagnostics and the in/out gate points (figure 8.7) --")
    other = "https://dblp.org/rec/journals/other/"
    authors_of_pub = {
        JKEY + "P1": {"E", "A", "B"},
        CONF + "X": {"E", "Z"},
        other + "Y": {"E", "A"},
        CONF + "Old": {"E", "W"},
        CONF + "Undated": {"E", "V"},
    }
    year_of = {JKEY + "P1": "2012", CONF + "X": "2010", other + "Y": "2015",
               CONF + "Old": "2005", CONF + "Undated": ""}
    mapping = {"J1": ["jkey"], "J2": ["jtwo"], "J3": []}
    seats = {"J1": (2010, 2015), "J2": (2010, 2015), "J3": (2010, 2015)}
    pts = {p["journal"]: p for p in
           extract.inout_points("E", seats, set(authors_of_pub), mapping,
                                authors_of_pub, year_of)}
    check("one point per mapped journal; an unmapped one is not plotted",
          sorted(pts) == ["J1", "J2"])
    check("x counts distinct in-journal co-authors, editor excluded",
          pts["J1"]["x"] == 2)
    check("a co-author seen both inside and outside is on both axes",
          pts["J1"]["y"] == 2)
    check("the same record read against another board seat moves to y",
          pts["J2"] == {"journal": "J2", "x": 0, "y": 3})
    check("a publication with no author list contributes nobody",
          extract.inout_points("E", {"J1": (2010, 2015)}, {CONF + "Q"}, mapping, {},
                               {CONF + "Q": "2012"})
          == [{"journal": "J1", "x": 0, "y": 0}])
    late = extract.inout_points("E", {"J1": (2013, 2015)}, set(authors_of_pub), mapping,
                                authors_of_pub, year_of)
    check("only the seat's own window counts, on both axes (P1 and X fall before it)",
          late == [{"journal": "J1", "x": 0, "y": 1}])
    check("the window bounds are inclusive",
          extract.inout_points("E", {"J1": (2012, 2012)}, set(authors_of_pub), mapping,
                               authors_of_pub, year_of)
          == [{"journal": "J1", "x": 2, "y": 0}])

    nodes = {"E||J1||A": {"editor_id": "E", "coauthor_id": "A"},
             "E||J1||B": {"editor_id": "E", "coauthor_id": "B"}}
    papers = [{"editor_id": "E", "journal": "J1"},
              {"editor_id": "https://dblp.org/pid/z", "journal": "J1"}]
    windows = {("J1", "E"): (2010, 2015)}
    diag = extract.editor_diagnostics({"E": list(nodes)}, {"E": "Ed"}, papers,
                                      {"E": set(authors_of_pub)}, mapping, authors_of_pub,
                                      year_of, windows)
    check("every editor with a suspect paper is described, in a fixed order",
          list(diag) == ["E", "https://dblp.org/pid/z"])
    check("the editor's suspect papers and whole-career outside publications are counted",
          diag["E"]["n_pubs_outside_journal"] == 4
          and diag["E"]["n_suspect_papers"] == 1)
    check("the gate point of a seat is read within that seat's window",
          diag["E"]["inout_points"] == [{"journal": "J1", "x": 2, "y": 2}])
    check("a seat without a window gets no gate point",
          extract.editor_diagnostics({"E": list(nodes)}, {}, papers,
                                     {"E": set(authors_of_pub)}, mapping, authors_of_pub,
                                     year_of, {})["E"]["inout_points"] == [])
    check("a PID-identified editor is flagged as such, a named one is not",
          diag["https://dblp.org/pid/z"]["is_pid"] and not diag["E"]["is_pid"])
    check("an editor with no node still keeps their paper count",
          diag["https://dblp.org/pid/z"]["n_suspect_papers"] == 1)
    check("the gate points are None when no author lists were collected",
          extract.editor_diagnostics({"E": list(nodes)}, {}, papers, {},
                                     mapping, {}, {}, windows)["E"]["inout_points"] is None)


def test_issue_year():
    print("\n-- issue year --")
    check("issue year is the earliest year its articles carry",
          extract.issue_year([{"year": "2013"}, {"year": "2012"},
                              {"year": ""}]) == "2012")
    check("an issue with no dated article has no year",
          extract.issue_year([{"year": ""}]) == "")


def main():
    test_name_matching()
    test_identity_and_venue()
    test_load_rule()
    test_classify()
    test_percentiles_and_bands()
    test_fit_cuts_and_labels()
    test_annotate_fit()
    test_networks_from_file()
    test_coauthorship_rows()
    test_front_matter()
    test_dated_issues_and_collector()
    test_board_tenure_and_control()
    test_scan_signatures()
    test_build_nodes()
    test_benign()
    test_editor_diagnostics_and_inout()
    test_issue_year()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
