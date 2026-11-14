"""
Tests for bbn_extract.py: the well-known, seat, off-power and corpus functions on
hand-checked cases.
No DB; the file readers get a tempfile.

    python test_bbn_extract.py
"""

from __future__ import annotations

import csv
import tempfile
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


def member(edition, role, pid=None, name_key="", dblp_name=None):
    return {"edition_key": edition, "role_category": role, "dblp_pid": pid,
            "name_key": name_key, "dblp_name": dblp_name}


def edition(stream, year, papers, core_rank="B", source="lncs"):
    return {"stream": stream, "year": year, "papers": papers, "core_rank": core_rank,
            "source": source}


# Stream A has editions 2010-2013 and 2016, stream B one in 2011. A/2013 has an
# incomplete PC. p sits on A/2012 and reviewed for A/2011; r sits on every edition of A.
EDITIONS = {
    "A/2010": edition("A", 2010, 20), "A/2011": edition("A", 2011, 30),
    "A/2012": edition("A", 2012, 25), "A/2013": edition("A", 2013, 10),
    "A/2016": edition("A", 2016, 40), "B/2011": edition("B", 2011, 100),
}
COMPLETE = {"A/2010", "A/2011", "A/2012", "A/2016", "B/2011"}
ON_POWER = {"p": {"A/2011", "A/2012"},
            "r": {"A/2010", "A/2011", "A/2012", "A/2013", "A/2016"}}
CITES = {"p": {"A/2010": 2, "A/2011": 5, "A/2012": 6, "A/2013": 3, "A/2016": 4,
               "B/2011": 1}}
CITING = {"p": {"A/2010": 1, "A/2011": 3, "A/2012": 3, "A/2013": 2, "A/2016": 2,
                "B/2011": 1}}
OWN = {"p": {"A/2010": 1, "A/2012": 1, "B/2011": 2}}


def off_power(window=2):
    return extract.OffPower(EDITIONS, COMPLETE, ON_POWER, CITES, OWN, window)


# --- the committee -------------------------------------------------------------

def test_pc_sizes():
    print("\n-- PC size --")
    sizes = extract.pc_sizes([
        member("E", "pc", "a"), member("E", "senior_pc", "b"),
        member("E", "pc", None, "c c"), member("E", "pc", "a"),
        member("E", "pc_chair", "d"), member("E", "reviewer", "e"),
        member("F", "other", "a")])
    check("pc and senior_pc count, matched or not, each person once", sizes["E"] == 3)
    check("an edition without pc rows has no PC size", "F" not in sizes)


def test_complete_editions():
    print("\n-- plausibly complete committees --")
    editions = {"S/2010": edition("S", 2010, 1), "S/2011": edition("S", 2011, 1),
                "S/2012": edition("S", 2012, 1), "S/2020": edition("S", 2020, 1),
                "T/2010": edition("T", 2010, 1)}
    sizes = {"S/2010": 40, "S/2011": 40, "S/2012": 15, "S/2020": 15, "T/2010": 8,
             "U/2010": 50}
    complete = extract.complete_editions(editions, sizes, min_size=10, min_share=0.5,
                                         window=2)
    check("a PC of the usual size is complete", {"S/2010", "S/2011"} <= complete)
    check("a PC below half the median nearby is not", "S/2012" not in complete)
    check("the median only takes editions within the window", "S/2020" in complete)
    check("a PC below the absolute minimum is not, whatever the median",
          "T/2010" not in complete)
    check("an edition without volumes is left out", "U/2010" not in complete)
    check("a lower share lets the small PC through",
          "S/2012" in extract.complete_editions(editions, sizes, 10, 0.3, 2))


def test_is_main():
    print("\n-- main and satellite editions --")
    check("a bare year is a main edition",
          extract.is_main("conf/cpm/2007") and extract.is_main("conf/sigir/82"))
    check("a suffix after the year is a satellite",
          not any(extract.is_main(e) for e in
                  ("conf/eccv/2020w", "conf/eccv/2024-w8", "conf/miccai/2014stia")))
    check("the year is the conference's, from the key",
          (extract.conference_year("conf/caise/2006"), extract.conference_year("conf/sigir/82"),
           extract.conference_year("conf/eccv/2024-w8"),
           extract.conference_year("conf/caise/2004umics")) == (2006, 1982, 2024, 2004))
    check("a key without a year has none", extract.conference_year("conf/x/abc") is None)
    off = extract.OffPower({**EDITIONS, "A/2012w": edition("A", 2012, 500)},
                           COMPLETE | {"A/2012w"}, ON_POWER, CITES, OWN)
    check("a satellite is never an off-power edition, also not in the same year",
          off.conference("p", "A", 2012) == off_power().conference("p", "A", 2012))


def test_same_committee():
    print("\n-- editions with the same committee --")
    pc = [f"p{i}" for i in range(10)]
    rows = ([{"edition_key": e, "dblp_pid": p} for e in ("conf/g/2010", "conf/g/2010c")
             for p in pc]
            + [{"edition_key": "conf/g/2010w", "dblp_pid": p} for p in pc[:5]]
            + [{"edition_key": f"conf/e/2024-w{n}", "dblp_pid": p} for n in (1, 2, 10)
               for p in pc]
            + [{"edition_key": "conf/g/2011", "dblp_pid": p} for p in pc])
    volume_editions = ["conf/g/2010", "conf/g/2010c", "conf/g/2010w", "conf/g/2011",
                       "conf/e/2024-w1", "conf/e/2024-w2", "conf/e/2024-w10", "conf/g/2012"]
    mapping = extract.same_committee_editions(volume_editions, rows, 0.8)
    check("a companion with the main PC goes into the main edition",
          mapping["conf/g/2010c"] == "conf/g/2010")
    check("a committee that shares half keeps its own edition",
          mapping["conf/g/2010w"] == "conf/g/2010w")
    check("the same PC in another year keeps its own edition",
          mapping["conf/g/2011"] == "conf/g/2011")
    check("volumes without a main edition go under the shortest key",
          {mapping[f"conf/e/2024-w{n}"] for n in (1, 2, 10)} == {"conf/e/2024-w1"})
    check("an edition without a committee maps to itself",
          mapping["conf/g/2012"] == "conf/g/2012")


def test_on_power():
    print("\n-- on-power editions --")
    on = extract.on_power_editions([
        member("E1", "pc", "p", "pat q"), member("E2", "reviewer", "p", "pat q"),
        member("E3", "other", "p", "p quinn"), member("E4", "pc", None, "pat q"),
        member("E5", "pc", None, "p quinn"), member("E6", "pc", None, "someone else"),
        member("E7", "pc", "r", "rob s")])
    check("any role makes an edition on-power",
          {"E1", "E2", "E3"} <= on["p"])
    check("an unmatched row under one of the member's names too",
          {"E4", "E5"} <= on["p"])
    check("an unmatched row under another name does not", "E6" not in on["p"])
    check("another member's seat does not", "E7" not in on["p"])


def test_scored_seats():
    print("\n-- scored seats --")
    seats = extract.scored_seats([
        member("E", "pc", "p", dblp_name="Pat Quinn"), member("E", "senior_pc", "p"),
        member("E", "pc_chair", "c"), member("E", "reviewer", "r"),
        member("E", "pc", None, "un matched"), member("F", "pc", "p")])
    check("pc and senior_pc of one edition make one senior_pc seat",
          seats[("E", "p")]["role"] == "senior_pc")
    check("the seat keeps the dblp name", seats[("E", "p")]["name"] == "Pat Quinn")
    check("chairs, reviewers and unmatched members hold no scored seat",
          set(seats) == {("E", "p"), ("F", "p")})


# --- the off-power rate ----------------------------------------------------------

def test_off_power_conference():
    print("\n-- off-power, same stream --")
    off = off_power()
    check("A/2010 is off-power for p in 2012, A/2011-A/2013 are on-power or incomplete",
          off.conference("p", "A", 2012, min_papers=10) == (2, 19, 2))
    check("the member's own papers are left out of the papers",
          off.conference("p", "A", 2012, min_papers=10)[1] == 20 - 1)
    check("too few papers within the window reaches further out, to A/2016",
          off.conference("p", "A", 2012, min_papers=30) == (2 + 4, 19 + 40, 4))
    check("every off-power edition within the window is taken, also past min_papers",
          off.conference("q", "A", 2011, min_papers=10) == (0, 30 + 20 + 25, 1))
    off = off_power(window=0)
    check("beyond the window, editions equally far are taken together",
          off.conference("q", "A", 2011, min_papers=40) == (0, 30 + 20 + 25, 1))
    check("...and the search stops once min_papers is reached",
          off.conference("q", "A", 2011, min_papers=10) == (0, 30, 0))
    check("a member on every edition of the stream has no off-power papers",
          off.conference("r", "A", 2012) == (0, 0, 0))
    off = extract.OffPower(EDITIONS, COMPLETE, ON_POWER, CITES, OWN, window=2, max_span=3)
    check("never further out than max_span, also with too few papers",
          off.conference("p", "A", 2012, min_papers=30) == (2, 19, 2))
    check("another count sums over the same editions",
          off_power().conference("p", "A", 2012, min_papers=30, counts=CITING) == (1 + 2, 59, 4))


def test_off_power_rate():
    print("\n-- choosing the rate --")
    levels = [("conference", (2, 19)), ("peers", (3, 117))]
    src, cites, papers, rate = extract.off_power_rate(levels, min_papers=30, pseudo=0.5)
    check("too few off-power papers falls back to the peers",
          (src, cites, papers) == ("peers", 3, 117))
    check("the rate carries the pseudo-count", approx(rate, 3.5 / 117))
    src, _, _, rate = extract.off_power_rate(levels, min_papers=10, pseudo=0.5)
    check("enough off-power papers keeps the same stream",
          src == "conference" and approx(rate, 2.5 / 19))
    check("zero citations still gives a positive rate",
          extract.off_power_rate([("conference", (0, 50))], 30, 0.5)[3] > 0)
    check("no level with enough papers gives no rate",
          extract.off_power_rate([("conference", (1, 5)), ("peers", (0, 0))], 30, 0.5)
          == ("", 0, 0, None))


def test_seat_rows():
    print("\n-- seat rows --")
    seats = {("A/2012", "p"): {"role": "pc", "name": "Pat Quinn"},
             ("A/2010", "q"): {"role": "senior_pc", "name": "Q"},
             ("A/2012", "r"): {"role": "pc", "name": "R"},
             ("A/2012", "s"): {"role": "pc", "name": "S"}}

    def rows(min_peers):
        return {(r["edition_key"], r["dblp_pid"]): r for r in extract.seat_rows(
            seats, EDITIONS, CITES, CITING, {("A/2012", "p"): 9}, OWN, {"A/2012": 12},
            off_power(), min_peers, per_paper={("A/2012", "p"): [4, 1, 1]})}

    p = rows(2)[("A/2012", "p")]
    check("cites and cites_gate come from the citations",
          (p["cites"], p["cites_gate"]) == (6, 9))
    check("papers leaves out the member's own papers", p["papers"] == 25 - 1)
    check("the seat carries the source of its edition", p["source"] == "lncs")
    check("pc_size is the edition's, 0 without a committee",
          (p["pc_size"], rows(2)[("A/2010", "q")]["pc_size"]) == (12, 0))
    check("the rate comes from the same stream, four years out, and expected = rate * papers",
          (p["src"], p["span"], p["off_cites"], p["off_papers"]) == ("conference", 4, 6, 59)
          and approx(p["expected"], round(6.5 / 59 * 24, 4)))
    check("surplus = cites - expected", approx(p["surplus"], round(6 - 6.5 / 59 * 24, 4)))
    check("the surplus of 3.4 needs the paper that cites the member most", p["fit_papers"] == 1)
    check("the citing papers are expected from the same editions",
          p["citing"] == 3 and approx(p["expected_citing"], round(3.5 / 59 * 24, 4)))
    check("depth = (cites / citing) / (expected / expected_citing)",
          approx(p["depth"], round((6 / 3) / (6.5 / 3.5), 4)))
    check("the other seats, with a lower rate, leave expected alone",
          (p["peer_cites"], p["peer_papers"]) == (0, 50) and p["peer_rate"] < p["rate"])
    s = rows(2)[("A/2012", "s")]
    check("with a higher rate of the other seats, expected is theirs",
          s["src"] == "conference" and approx(s["expected"], round(6.5 / 49 * 25, 4))
          and approx(s["surplus"], -s["expected"]))
    check("...and so are the expected citing papers, while without a citing paper there is "
          "no depth", approx(s["expected_citing"], round(3.5 / 49 * 25, 4)) and s["depth"] == "")
    q = rows(2)[("A/2010", "q")]
    check("a member nobody cites has cites = 0 and cites_gate = 0",
          (q["cites"], q["cites_gate"]) == (0, 0))
    check("...and still a rate", q["rate"] != "" and q["expected"] != "")
    r = rows(2)[("A/2012", "r")]
    check("a member on every edition of the stream falls back to the other seats, p and s",
          (r["src"], r["span"], r["off_cites"], r["off_papers"]) == ("peers", "", 6 + 0, 24 + 25)
          and approx(r["expected"], round(6.5 / 49 * 25, 4)))
    no_rate = rows(3)[("A/2012", "r")]
    check("...but not with fewer peers than min_peers, and then no surplus either",
          no_rate["expected"] == "" and no_rate["surplus"] == "" and no_rate["fit_papers"] == "")
    check("fit_papers: the fewest papers, most citing first, that hold the surplus",
          (extract.fit_papers([4, 1, 1], 4), extract.fit_papers([4, 1, 1], 4.5),
           extract.fit_papers([4, 1, 1], 6), extract.fit_papers([4, 1, 1], -1),
           extract.fit_papers([], 0)) == (1, 2, 3, 0, 0))


def test_gate():
    print("\n-- the gate --")
    rows = [{"dblp_pid": "a", "cites_gate": "11"}, {"dblp_pid": "a", "cites_gate": 3},
            {"dblp_pid": "b", "cites_gate": 10}]
    gated = extract.gated_members(rows, gate=10)
    check("more than 10 references passes, 10 does not", set(gated) == {"a"})
    check("only the seats over the gate are kept", len(gated["a"]) == 1)


def test_build_corpus():
    print("\n-- the corpus --")
    path = tempfile_with(
        "edition_key,stream,year,core_rank,source,dblp_pid,name,role,cites,citing,cites_gate,"
        "papers,pc_size,src,span,off_cites,off_papers,rate,peer_cites,peer_papers,peer_rate,"
        "expected,surplus,fit_papers,expected_citing,depth\n"
        "A/2010,A,2010,B,acm,p,Pat Quinn,pc,3,2,4,20,12,conference,1,2,40,0.0625,0,0,,1.25,1.75,1,1.0,1.2\n"
        "A/2011,A,2011,B,acm,p,Pat Quinn,pc,0,0,15,0,12,conference,1,2,40,0.0625,0,0,,0.0,0.0,0,0.0,\n"
        "A/2012,A,2012,B,lncs,p,,senior_pc,0,0,0,30,12,,,0,0,,0,0,,,,,,\n"
        "B/2011,B,2011,A,lncs,q,Q,pc,0,0,12,30,10,,,0,0,,0,0,,,,,,\n")
    with open(path, encoding="utf-8", newline="") as f:
        corpus = extract.build_corpus(csv.DictReader(f), {"p": {2008: 3, 2009: 4, 2010: 5}})
    p = corpus["members"]["p"]
    check("a seat carries the member's publications before its year", p["seats"][0]["pubs"] == 7)
    check("seats without a rate and without papers are left out", len(p["seats"]) == 1)
    check("the gate count is over all seats", p["cites_gate_max"] == 15)
    check("the name is the first one given", p["name"] == "Pat Quinn")
    s = p["seats"][0]
    check("counts are ints and the expected counts floats",
          (s["cites"], s["citing"], s["papers"], s["pc_size"], s["year"], s["expected"],
           s["expected_citing"]) == (3, 2, 20, 12, 2010, 1.25, 1.0) and isinstance(s["expected_citing"], float))
    check("a seat keeps its source", s["source"] == "acm")
    check("a member without a kept seat is left out", "q" not in corpus["members"])
    check("the funnel counts every step",
          corpus["funnel"] == {"seats": 4, "seats without a rate": 2, "seats without papers": 1,
                               "seats kept": 1, "members": 2,
                               "members with a seat kept": 1})


# --- publications --------------------------------------------------------------

def triple(s, p, o):
    return f"<https://dblp.org/rec/{s}> {p} {o} .\n"


def record(key, rdf_type, year, *pids):
    lines = [triple(key, extract.P_TYPE, f"<https://dblp.org/rdf/schema#{rdf_type}>"),
             triple(key, extract.P_TYPE, "<https://dblp.org/rdf/schema#Publication>")]
    lines += [triple(key, extract.P_AUTHORED_BY, f"<https://dblp.org/pid/{p}>") for p in pids]
    lines.append("_:Sig_1 <https://dblp.org/rdf/schema#signatureCreator> "
                 f"<https://dblp.org/pid/{pids[0]}> .\n")
    if year:
        lines.append(triple(key, extract.P_YEAR,
                            f'"{year}"^^<http://www.w3.org/2001/XMLSchema#gYear>'))
    return lines + ["\n"]


def test_publication_counts():
    print("\n-- publications --")
    lines = (record("conf/a/P1", "Inproceedings", 2010, "11/1", "n/GonzaloNavarro", "99/9")
             + record("journals/b/P2", "Article", 2010, "11/1")
             + record("journals/corr/P3", "Informal", 2010, "11/1")
             + record("reference/x/P4", "Reference", 2011, "11/1")
             + record("conf/a/P5", "Inproceedings", 2012, "11/1")
             + record("conf/a/P6", "Inproceedings", None, "11/1")
             + ["<https://dblp.org/pid/11/1> <https://dblp.org/rdf/schema#creatorOf> "
                "<https://dblp.org/rec/conf/a/P1> .\n"])
    counts = extract.publication_counts(lines, {"11/1", "n/GonzaloNavarro"},
                                        types=["Article", "Inproceedings"])
    check("one count per record per author and year",
          counts[("11/1", 2010)] == 2 and counts[("n/GonzaloNavarro", 2010)] == 1)
    check("informal and reference records do not count", ("11/1", 2011) not in counts)
    check("the last record counts too", counts[("11/1", 2012)] == 1)
    check("a record without a year does not count", sum(counts.values()) == 4)
    check("only the wanted pids are counted", all(pid != "99/9" for pid, _ in counts))
    check("publications before the year only", extract.pubs_before({2009: 2, 2010: 3}, 2010) == 2)


# --- well-known works ----------------------------------------------------------

def test_index_citations():
    print("\n-- citations in the OpenCitations Index --")
    lines = ["oci,citing,cited,creation,timespan,journal_sc,author_sc\n",
             "1-2,omid:br/1,omid:br/9,2010-05-01,P1Y,no,no\n",
             "1-3,omid:br/2,omid:br/9,2010,P1Y,yes,no\n",
             "1-4,omid:br/3,omid:br/9,2012,P3Y,no,yes\n",
             "1-5,omid:br/3,omid:br/8,,,no,no\n",
             "1-6,omid:br/3,omid:br/7,2011,P2Y,no,no\n"]
    counts = extract.index_citations(lines, {"br/9": 0, "br/8": 1})
    check("one count per citation to a wanted work, by the year of the citing work",
          counts == {(0, 2010): 2})
    check("a citation by an author of the cited work does not count", (0, 2012) not in counts)

    totals = extract.CitationCounts(2, 2010, 2012)
    totals.add({(0, 2005): 1, (0, 2010): 2, (0, 2012): 1, (1, 2011): 3, (1, 2015): 9})
    totals.accumulate()
    check("citations up to and including the year",
          (totals.up_to(0, 2010), totals.up_to(0, 2012)) == (3, 4))
    check("citations after the last year do not count",
          (totals.up_to(1, 2010), totals.up_to(1, 2012)) == (0, 3))

    path = Path(tempfile.mkdtemp()) / "index_counts.pkl"
    extract.save_counts(path, ["br/8", "br/9"], totals)
    loaded = extract.load_counts(path, ["br/8", "br/9"], 2010, 2012)
    check("saved counts read back", loaded is not None and loaded.totals == totals.totals
          and loaded.up_to(1, 2012) == 3)
    check("saved counts of other works or years are not used",
          extract.load_counts(path, ["br/8", "br/7"], 2010, 2012) is None
          and extract.load_counts(path, ["br/8", "br/9"], 2010, 2013) is None
          and extract.load_counts(path.with_name("none.pkl"), ["br/8"], 2010, 2012) is None)


def test_well_known_works():
    print("\n-- well-known works --")
    check("the threshold is the fewest citations in the top share",
          (extract.top_threshold(list(range(1, 101)), 0.01),
           extract.top_threshold([3, 1, 4, 2], 0.5), extract.top_threshold([5], 0.01))
          == (100, 3, 5))
    editions = {"S/2010": edition("S", 2010, 1), "S/2012": edition("S", 2012, 1),
                "T/2012": edition("T", 2012, 1)}
    counts = extract.CitationCounts(11, 2010, 2012)
    counts.add({**{(work, 2010): work + 1 for work in range(10)}, (10, 2012): 50})
    counts.accumulate()
    cited = ([("S/2010", work) for work in range(10)]
             + [("S/2010", 10), ("S/2012", 8), ("S/2012", 9), ("T/2012", 0), ("T/2012", 1)])
    known = extract.well_known_works(cited, editions, counts, top=0.1, min_works=5)
    check("the top of the works the stream cites is well known",
          ("S/2010", 9) in known and ("S/2010", 8) not in known)
    check("a work without citations by then never is", ("S/2010", 10) not in known)
    check("the threshold is that of the edition's year",
          ("S/2012", 9) in known and ("S/2012", 8) not in known)
    check("a stream with too few cited works takes the works of all streams",
          not {("T/2012", 0), ("T/2012", 1)} & known)
    check("...which a lower min_works does not",
          ("T/2012", 1) in extract.well_known_works(cited, editions, counts, top=0.1,
                                                    min_works=2))


# --- files ---------------------------------------------------------------------

def test_files():
    print("\n-- files --")
    env = extract.read_config(tempfile_with("POSTGRES_SERVER=localhost\nPOSTGRES_DB=db\n",
                                            suffix=".env"))
    check("the .env reads without a section header",
          (env["POSTGRES_SERVER"], env["POSTGRES_DB"]) == ("localhost", "db"))
    path = Path(tempfile.mkdtemp()) / "out" / "seats.csv"
    extract.write_csv(path, [{"a": 1, "b": ""}, {"a": 2, "b": 0.5}])
    with open(path, encoding="utf-8", newline="") as f:
        back = list(csv.DictReader(f))
    check("write_csv makes the folder and writes every row",
          [r["a"] for r in back] == ["1", "2"] and back[1]["b"] == "0.5")


def main():
    test_pc_sizes()
    test_complete_editions()
    test_is_main()
    test_same_committee()
    test_on_power()
    test_scored_seats()
    test_off_power_conference()
    test_off_power_rate()
    test_seat_rows()
    test_gate()
    test_build_corpus()
    test_publication_counts()
    test_index_citations()
    test_well_known_works()
    test_files()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
