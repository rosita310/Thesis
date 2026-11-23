"""
Tests for bbn_report.py (the sensitivity analysis), on a synthetic corpus. No data file needed.

    python test_bbn_report.py
"""

from __future__ import annotations

import contextlib
import io

import bbn_report as report
from cs3_config import SENSITIVITY_VARIANTS
from test_bbn_infer import corpus, seat

_failures = 0


def check(label, condition):
    global _failures
    if not condition:
        _failures += 1
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}")


def stability_corpus():
    """a: five extreme seats, gated. b: five typical seats, gated. e: five seats with a
    surplus of 4, gated: typical under the run's edges 5/10/20, mild under 4/8/16.
    z: 40 typical seats, never over the gate, the background of the conference."""
    return corpus({
        "a": [seat(60, 1.0, edition=f"conf/a/a{i}") for i in range(5)],
        "b": [seat(1, 1.0, edition=f"conf/a/b{i}", cites_gate=12) for i in range(5)],
        "e": [seat(5, 1.0, edition=f"conf/a/e{i}", cites_gate=12) for i in range(5)],
        "z": [seat(0, 1.0, edition=f"conf/a/z{i}") for i in range(40)],
    })


VARIANTS = {
    "surplus edges": [("edges 4/8/16", {"edges": [4, 8, 16]})],
    "n_min": [("n_min 6", {"min_seats": 6})],
}


def test_shortlist_and_compare():
    print("\n-- shortlist and comparison with the run --")
    rows = [{"pid": "p", "passes_gate": True, "p_genuine": 0.1},
            {"pid": "q", "passes_gate": False, "p_genuine": 0.2},
            {"pid": "r", "passes_gate": True, "p_genuine": 0.3},
            {"pid": "s", "passes_gate": True, "p_genuine": 0.7}]
    check("the shortlist is the gated members below the threshold, in order",
          report.shortlist(rows) == ["p", "r"])
    result = report.compare(rows, base=["r", "t"], tracked=["t", "r"])
    check("kept counts the members also on the run's shortlist, new the rest",
          (result["size"], result["kept"], result["new"]) == (2, 1, 1))
    check("the tracked members get their rank on the variant's shortlist, or None",
          result["ranks"] == [None, 2])
    check("scored and gated are counted over the whole ranking",
          (result["scored"], result["gated"]) == (4, 3))


def test_run_variants():
    print("\n-- the variants on a synthetic corpus --")
    run, results = report.run_variants(stability_corpus(), VARIANTS)
    by = {r["label"]: r for r in results}
    check("the run shortlists the member with extreme seats only", run["shortlist"] == ["a"])
    check("the run's first member is tracked at rank 1", run["ranks"] == [1])
    edges = by["edges 4/8/16"]
    check("lower surplus edges make e's seats mild and add e as new",
          "e" in edges["shortlist"] and edges["kept"] == 1 and edges["new"] == 1)
    n_min = by["n_min 6"]
    check("a higher minimum record scores fewer members and drops a",
          n_min["scored"] < run["scored"] and n_min["size"] == 0 and n_min["ranks"] == [None])
    check("nobody stays shortlisted under every variant when one shortlists no one",
          report.always_shortlisted(run, results) == [])
    check("...and a stays under the edges variant alone",
          report.always_shortlisted(run, results[:1]) == ["a"])


def test_config_variants():
    print("\n-- the configured variants --")
    run, results = report.run_variants(stability_corpus())
    labels = [label for entries in SENSITIVITY_VARIANTS.values() for label, _ in entries]
    check("every configured variant runs through rank_corpus and is reported once",
          [r["label"] for r in results] == labels)
    check("a higher prior never adds members to the shortlist",
          all(r["new"] == 0 for r in results if r["group"] == "prior"))
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        report.print_results(run, results)
    text = out.getvalue()
    check("the report prints the run and every variant",
          all(label in text for label in ["run"] + labels))
    check("...and how many stay shortlisted under every variant",
          "on the shortlist under every variant" in text)


def main():
    test_shortlist_and_compare()
    test_run_variants()
    test_config_variants()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
