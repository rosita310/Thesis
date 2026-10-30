"""
Tests for bbn_extract.py: the seat and off-power functions on hand-checked cases.
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


def edition(stream, year, n, core_rank="B"):
    return {"stream": stream, "year": year, "n": n, "core_rank": core_rank}


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
    check("the member's own papers are left out of n",
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


def test_off_power_rate():
    print("\n-- choosing the rate --")
    levels = [("conference", (2, 19)), ("peers", (3, 117))]
    src, k, n, rate = extract.off_power_rate(levels, min_papers=30, pseudo=0.5)
    check("too few off-power papers falls back to the peers",
          (src, k, n) == ("peers", 3, 117))
    check("the rate carries the pseudo-count", approx(rate, 3.5 / 117))
    src, _, _, rate = extract.off_power_rate(levels, min_papers=10, pseudo=0.5)
    check("enough off-power papers keeps the same stream",
          src == "conference" and approx(rate, 2.5 / 19))
    check("zero citations still gives a positive rate",
          extract.off_power_rate([("conference", (0, 50))], 30, 0.5)[3] > 0)
    check("no level with enough papers gives no rate",
          extract.off_power_rate([("conference", (1, 5)), ("peers", (0, 0))], 30, 0.5)
          == ("", 0, 0, None))


def test_citation_ratio():
    print("\n-- citation ratio --")
    check("as expected is 1", approx(extract.citation_ratio(3, 3.0), 1.0))
    check("one citation where 0.2 are expected is not much", approx(extract.citation_ratio(1, 0.2), 2 / 1.2))
    check("the same ratio weighs more with more citations",
          extract.citation_ratio(20, 4.0) > extract.citation_ratio(5, 1.0))
    check("no citations stays below 1", extract.citation_ratio(0, 0.5) < 1)
    check("the slack is a parameter", approx(extract.citation_ratio(1, 0.2, pseudo=0), 5.0))


def test_seat_rows():
    print("\n-- seat rows --")
    seats = {("A/2012", "p"): {"role": "pc", "name": "Pat Quinn"},
             ("A/2010", "q"): {"role": "senior_pc", "name": "Q"},
             ("A/2012", "r"): {"role": "pc", "name": "R"},
             ("A/2012", "s"): {"role": "pc", "name": "S"}}

    def rows(min_peers):
        return {(r["edition_key"], r["dblp_pid"]): r for r in extract.seat_rows(
            seats, EDITIONS, CITES, {("A/2012", "p"): 9}, OWN, off_power(), min_peers)}

    p = rows(2)[("A/2012", "p")]
    check("x and x_westerbaan come from the citations", (p["x"], p["x_westerbaan"]) == (6, 9))
    check("n leaves out the member's own papers", p["n"] == 25 - 1)
    check("the rate comes from the same stream, four years out, and E = rate * n",
          (p["src"], p["span"], p["k_off"], p["n_off"]) == ("conference", 4, 6, 59)
          and approx(p["E"], round(6.5 / 59 * 24, 4)))
    check("ratio = (x + 1) / (E + 1)", approx(p["ratio"], round(7 / (6.5 / 59 * 24 + 1), 4)))
    q = rows(2)[("A/2010", "q")]
    check("a member nobody cites has x = 0 and x_westerbaan = 0",
          (q["x"], q["x_westerbaan"]) == (0, 0))
    check("...and still a rate", q["rate"] != "" and q["E"] != "")
    r = rows(2)[("A/2012", "r")]
    check("a member on every edition of the stream falls back to the other seats, p and s",
          (r["src"], r["span"], r["k_off"], r["n_off"]) == ("peers", "", 6 + 0, 24 + 25)
          and approx(r["E"], round(6.5 / 49 * 25, 4)))
    no_rate = rows(3)[("A/2012", "r")]
    check("...but not with fewer peers than min_peers, and then no ratio either",
          no_rate["E"] == "" and no_rate["ratio"] == "")


def test_gate():
    print("\n-- the gate --")
    rows = [{"dblp_pid": "a", "x_westerbaan": "11"}, {"dblp_pid": "a", "x_westerbaan": 3},
            {"dblp_pid": "b", "x_westerbaan": 10}]
    gated = extract.gated_members(rows, gate=10)
    check("more than 10 references passes, 10 does not", set(gated) == {"a"})
    check("only the seats over the gate are kept", len(gated["a"]) == 1)


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
    test_on_power()
    test_scored_seats()
    test_off_power_conference()
    test_off_power_rate()
    test_citation_ratio()
    test_seat_rows()
    test_gate()
    test_files()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
