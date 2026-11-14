"""
Tests for bbn_inject.py, on a synthetic corpus. No data file needed.

    python test_bbn_inject.py
"""

from __future__ import annotations

import bbn_inject as inject

_failures = 0


def check(label, condition):
    global _failures
    if not condition:
        _failures += 1
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}")


def seat(cites, citing, papers=40, pc_size=20, stream="conf/a", edition="conf/a/2010"):
    return {"edition_key": edition, "stream": stream, "core_rank": "A", "source": "lncs",
            "role": "pc",
            "src": "conference", "year": 2010, "cites": cites, "citing": citing,
            "cites_gate": cites + 1, "papers": papers, "pc_size": pc_size, "off_cites": 0,
            "off_papers": 50, "rate": 0.02, "expected": 1.0, "expected_citing": 1.0,
            "pubs": 30}


def corpus(members):
    return {"model": "test", "funnel": {}, "members": {
        pid: {"name": pid.upper(), "seats": seats,
              "cites_gate_max": max(s["cites_gate"] for s in seats)}
        for pid, seats in members.items()}}


def test_coerce():
    print("\n-- a coerced seat --")
    check("the review load is reviews * papers / pc_size, rounded up",
          inject.review_load(seat(0, 0, papers=40, pc_size=30), reviews=3) == 4)
    check("the review load is at most the papers",
          inject.review_load(seat(0, 0, papers=5, pc_size=2), reviews=3) == 5)
    s = inject.coerce(seat(4, 10, papers=40, pc_size=20), extra=2, reviews=3)
    check("every reviewed paper cites `extra` more", (s["cites"], s["cites_gate"]) == (4 + 12, 5 + 12))
    check("the reviewed papers not citing yet, in proportion, become citing papers",
          s["citing"] == 10 + round(6 * (1 - 10 / 40)))
    full = inject.coerce(seat(40, 40, papers=40, pc_size=20), extra=1, reviews=3)
    check("no citing papers beyond the papers", full["citing"] == 40)


def test_fits_review_load():
    print("\n-- surplus within the review load --")
    seats = [{**seat(16, 4), "fit_papers": 3}, {**seat(16, 4), "fit_papers": 7},
             {**seat(31, 20), "fit_papers": 12}, {**seat(3, 3), "fit_papers": 1}]
    fits = inject.fits_review_load(corpus({"p": seats}), reviews=3)
    check("per bin: the share of seats whose fit_papers is within k = 6",
          fits == {"mild": (2, 0.5), "high": (1, 0.0)})


def test_inject():
    print("\n-- injecting a corpus --")
    data = corpus({"p": [seat(0, 0, edition=f"conf/a/{y}") for y in (2010, 2011, 2012)],
                   "q": [seat(1, 1, edition=f"conf/a/{y}") for y in (2010, 2011, 2012)]})
    injected = inject.inject(data, {"p": [2, 0, 1]}, extra=1, n_seats=2, reviews=3)
    p = injected["members"]["p"]["seats"]
    check("the first n seats in the order are coerced", [s["cites"] for s in p] == [6, 0, 6])
    check("the gate count follows", injected["members"]["p"]["cites_gate_max"] == 7)
    check("the other members and the corpus itself are untouched",
          injected["members"]["q"] is data["members"]["q"]
          and [s["cites"] for s in data["members"]["p"]["seats"]] == [0, 0, 0])


def test_draw():
    print("\n-- drawing the members --")
    rows = {"a": [{"pid": "p", "p_genuine": 0.9, "passes_gate": False},
                  {"pid": "q", "p_genuine": 0.1, "passes_gate": False},
                  {"pid": "r", "p_genuine": 0.9, "passes_gate": True}],
            "b": [{"pid": "p", "p_genuine": 0.9, "passes_gate": False},
                  {"pid": "q", "p_genuine": 0.9, "passes_gate": False},
                  {"pid": "r", "p_genuine": 0.2, "passes_gate": True}]}
    seats = {"p": [1, 2, 3], "q": [1, 2, 3], "r": [1, 2, 3]}
    order = inject.draw(rows, seats, n_members=10, seed=0)
    check("members shortlisted by any variant, gated or not, are left out", set(order) == {"p"})
    check("the order holds every seat once", sorted(order["p"]) == [0, 1, 2])
    check("the same seed draws the same", inject.draw(rows, seats, 10, 0) == order)


def test_run():
    print("\n-- the test --")
    members = {f"m{i}": [seat(0, 0, edition=f"conf/a/{y}") for y in (2010, 2011, 2012)]
               for i in range(200)}
    base, results = inject.run(corpus(members), extras=[1, 10], seat_counts=[1, 3],
                               n_members=5, seed=0, reviews=3)
    check("nobody is shortlisted without injection", all(not s for s in base.values()))
    check("one row per variant, extra and number of seats",
          len(results) == len(inject.VARIANTS) * 4)
    weak = [r for r in results if (r["extra"], r["seats"]) == (1, 1)]
    strong = [r for r in results if (r["extra"], r["seats"]) == (10, 3)]
    check("six planted citations in one seat stay typical",
          all(r["shortlisted_ungated"] == 0 and r["median_injected"] == 6 for r in weak))
    check("sixty in every seat are shortlisted",
          all(r["shortlisted"] == r["members"] == 5 for r in strong))


def main():
    test_coerce()
    test_fits_review_load()
    test_inject()
    test_draw()
    test_run()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
