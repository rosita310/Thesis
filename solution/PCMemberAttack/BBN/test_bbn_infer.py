"""
Tests for bbn_infer.py, on a synthetic corpus. No data file needed.

    python test_bbn_infer.py
"""

from __future__ import annotations

import csv
import math
import tempfile
from pathlib import Path

import bbn_infer as infer

_failures = 0


def check(label, condition):
    global _failures
    if not condition:
        _failures += 1
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}")


def approx(a, b, tol=1e-9):
    return abs(a - b) <= tol


ALPHA = 0.5
MANIP = {"typical": 0.0, "mild": 0.2, "high": 0.4, "extreme": 0.4}


def seat(x, E, stream="conf/a", core_rank="A", src="conference", edition=None,
         x_westerbaan=None, pubs=30):
    return {"edition_key": edition or f"{stream}/2010", "stream": stream,
            "core_rank": core_rank, "role": "pc", "src": src, "year": 2010,
            "x": x, "x_westerbaan": x if x_westerbaan is None else x_westerbaan,
            "n": 50, "k_off": 0, "n_off": 50, "rate": E / 50, "E": E, "pubs": pubs}


def classified(*args, **kwargs):
    return infer.classify(seat(*args, **kwargs))


def corpus(members):
    """members: pid -> list of seats."""
    return {"model": "test", "funnel": {}, "members": {
        pid: {"name": pid.upper(), "seats": seats,
              "x_westerbaan_max": max(s["x_westerbaan"] for s in seats)}
        for pid, seats in members.items()}}


# --- the evidence ------------------------------------------------------------

def test_bins():
    print("\n-- surplus bins and bands --")
    edges = [10, 20, 40]
    check("below the first edge is typical",
          infer.surplus_bin(9.99, edges) == "typical" and infer.surplus_bin(-3, edges) == "typical")
    check("an edge belongs to the bin above it",
          [infer.surplus_bin(s, edges) for s in (10, 20, 40)] == ["mild", "high", "extreme"])
    check("the edges are a parameter", infer.surplus_bin(12, [15, 30, 60]) == "typical")
    bands = [0.5, 2, 5]
    check("band labels",
          [infer.band_label(e, bands) for e in (0.1, 0.5, 1.9, 2, 5, 30)]
          == ["<0.5", "0.5-2", "0.5-2", "2-5", ">=5", ">=5"])
    s = classified(26, 1.0)
    check("classify takes the surplus x - E", approx(s["surplus"], 25.0))
    check("...and bins it and E", (s["state"], s["band"]) == ("high", "E 0.5-2"))
    check("the same surplus is the same bin whatever E",
          classified(35, 10.0)["state"] == s["state"])
    check("classify leaves the seat itself alone", "state" not in seat(26, 1.0))
    peer = infer.classify(seat(7, 1.0, src="peers", pubs=60), pubs_bands=[15, 60, 180])
    check("a seat with the peer rate takes the band of the member's publications",
          peer["band"] == "pubs 60-180")
    check("...whatever its E",
          infer.classify(seat(7, 9.0, src="peers", pubs=60), pubs_bands=[15, 60, 180])["band"]
          == peer["band"])


# --- the genuine column -----------------------------------------------------

def test_genuine_dist():
    print("\n-- the genuine column and its fallback --")
    others = ([classified(0, 1.0, edition=f"conf/a/{i}") for i in range(8)]
              + [classified(0, 1.0, stream="conf/b", edition=f"conf/b/{i}") for i in range(8)]
              + [classified(0, 1.0, stream="conf/c", core_rank="B", edition=f"conf/c/{i}")
                 for i in range(8)])
    own = [classified(60, 1.0, edition="conf/a/own1"), classified(60, 1.0, edition="conf/a/own2")]
    counts = infer.bin_counts(others + own)
    own_counts = infer.bin_counts(own)
    target = own[0]

    g, level = infer.genuine_dist(counts, target, own_counts, min_n=8)
    check("enough seats in the conference after leaving out the member's own",
          level == "conference")
    check("...and the member's own extreme seats are not in it",
          approx(g["extreme"], 0.5 / (8 + 4 * 0.5)))
    check("the column is smoothed into a distribution", approx(sum(g.values()), 1.0))
    _, level = infer.genuine_dist(counts, target, own_counts, min_n=9)
    check("too few in the conference falls back to the CORE rank", level == "core_rank")
    _, level = infer.genuine_dist(counts, target, own_counts, min_n=17)
    check("too few in the rank falls back to all conferences", level == "pooled")
    g, level = infer.genuine_dist(counts, target, own_counts, min_n=1000)
    check("the pooled level is used regardless",
          level == "pooled" and approx(g["typical"], 24.5 / 26))
    _, level = infer.genuine_dist(counts, target, {}, min_n=10)
    check("without leaving out the own seats the conference would have been enough",
          level == "conference")

    peer = classified(60, 1.0, src="peers", edition="conf/a/peer")
    g, level = infer.genuine_dist(infer.bin_counts(others + own + [peer]), peer, {}, min_n=1)
    check("a seat with a peer rate is judged only against peer-rate seats",
          level == "conference" and approx(g["extreme"], 1.5 / 3))
    big = classified(12, 10.0, edition="conf/a/big")
    g, level = infer.genuine_dist(infer.bin_counts(others + [big]), big, {}, min_n=1)
    check("...and a seat in another prom band only against that band",
          level == "conference" and approx(g["typical"], 1.5 / 3))
    prolific = classified(60, 1.0, src="peers", edition="conf/a/prolific", pubs=500)
    g, _ = infer.genuine_dist(infer.bin_counts(others + own + [peer, prolific]), prolific, {},
                              min_n=1)
    check("a peer seat is judged only against peer seats with as many publications",
          approx(g["extreme"], 1.5 / 3))
    g, _ = infer.genuine_dist(infer.bin_counts(others), big, {}, min_n=1)
    check("an empty cell gives the uniform column", approx(g["typical"], 0.25))


# --- the likelihood ratio and the score ---------------------------------------

def test_likelihood_ratios():
    print("\n-- the likelihood ratio of one seat --")
    g = {"typical": 0.9, "mild": 0.06, "high": 0.03, "extreme": 0.01}
    lr = {s: infer.seat_lr(s, g, ALPHA, MANIP) for s in g}
    check(f"LR_typical == 1/(1-alpha) = {1 / (1 - ALPHA):g}", approx(lr["typical"], 2.0))
    check("every bin with coerced mass lowers genuineness",
          all(lr[s] < 1 for s in ("mild", "high", "extreme")))
    check("the rarer the genuine bin, the lower the LR",
          lr["extreme"] < lr["high"] < lr["mild"])
    check("LR = g / (alpha m + (1 - alpha) g)",
          approx(lr["extreme"], 0.01 / (0.5 * 0.4 + 0.5 * 0.01)))


def test_score_member():
    print("\n-- the score of a member --")
    others = [classified(0, 1.0, edition=f"conf/a/{i}") for i in range(40)]
    prior_odds = math.log(0.95 / 0.05)
    p, log_odds, detail = infer.score_member([], infer.bin_counts(others), ALPHA, 0.95, MANIP)
    check("no seats leaves the prior", approx(p, 0.95) and approx(log_odds, prior_odds))
    typical = [classified(0, 1.0, edition="conf/a/t")]
    p_typ, _, _ = infer.score_member(typical, infer.bin_counts(others + typical),
                                     ALPHA, 0.95, MANIP)
    check("a typical seat raises P(genuine)", p_typ > 0.95)
    extreme = [classified(60, 1.0, edition=f"conf/a/e{i}") for i in range(3)]
    p_ext, log_odds, detail = infer.score_member(extreme, infer.bin_counts(others + extreme),
                                                 ALPHA, 0.95, MANIP)
    check("three extreme seats bring a member below 0.5", p_ext < 0.5)
    check("the log-odds are the prior plus the sum of log LR",
          approx(log_odds / math.log(10),
                 math.log10(0.95 / 0.05) + sum(d["woe"] for d in detail), 1e-5))
    check("one detail record per seat, with its bin and level",
          len(detail) == 3 and detail[0]["state"] == "extreme"
          and detail[0]["fallback_level"] == "conference")
    check("the posterior is overflow-safe",
          infer.log_odds_to_p(-1000) == 0.0 and infer.log_odds_to_p(1000) == 1.0)


# --- ranking ---------------------------------------------------------------------

def ranking_corpus():
    """a: three extreme seats, gated. b: three typical seats, gated. c: three typical
    seats, never over the gate. d: two seats, below MIN_SEATS."""
    background = [seat(0, 1.0, edition=f"conf/a/{i}") for i in range(40)]
    return corpus({
        "a": [seat(60, 1.0, edition=f"conf/a/a{i}") for i in range(3)],
        "b": [seat(1, 1.0, edition=f"conf/a/b{i}", x_westerbaan=12) for i in range(3)],
        "c": [seat(0, 1.0, edition=f"conf/a/c{i}") for i in range(3)],
        "d": [seat(60, 1.0, edition=f"conf/a/d{i}") for i in range(2)],
        "z": background,
    })


def test_rank_corpus():
    print("\n-- ranking --")
    rows = infer.rank_corpus(ranking_corpus(), alpha=ALPHA, prior=0.95, manip=MANIP,
                             min_seats=3, min_n=30, gate=10)
    pids = [r["pid"] for r in rows]
    check("members below min_seats are not scored", "d" not in pids)
    check("the most suspicious member comes first", pids[0] == "a")
    check("the ranking is ordered by woe", [r["woe"] for r in rows] == sorted(r["woe"] for r in rows))
    by = {r["pid"]: r for r in rows}
    check("the gate is Westerbaan's: more than 10 references on one seat",
          by["a"]["passes_gate"] and by["b"]["passes_gate"] and not by["c"]["passes_gate"])
    check("the bin counts are per member", (by["a"]["n_extreme"], by["a"]["n_typical"]) == (3, 0))
    check("woe is in log10: three typical seats add 3 log10 2 to the prior",
          approx(by["c"]["woe"], math.log10(0.95 / 0.05) + 3 * math.log10(2), 1e-6))
    unscored = infer.rank_corpus(ranking_corpus(), alpha=ALPHA, prior=0.95, manip=MANIP,
                                 min_seats=2, min_n=30)
    d = {r["pid"]: r for r in unscored}["d"]
    a_with_d = {r["pid"]: r for r in unscored}["a"]
    check("a lower min_seats scores d", d["n_seats"] == 2)
    check("the baseline counts d's seats whether d is scored or not",
          approx(a_with_d["woe"], by["a"]["woe"]))


def test_output():
    print("\n-- output --")
    rows = infer.rank_corpus(ranking_corpus(), alpha=ALPHA, prior=0.95, manip=MANIP,
                             min_seats=3, min_n=30)
    path = Path(tempfile.mkdtemp()) / "ranking.csv"
    infer.write_ranking_csv(path, rows)
    with open(path, encoding="utf-8", newline="") as f:
        back = list(csv.DictReader(f))
    check("one row per member, with its rank", [r["rank"] for r in back]
          == [str(i) for i in range(1, len(rows) + 1)])
    check("the columns are the ranking columns", list(back[0]) == infer.RANKING_COLUMNS)
    records = infer.member_records(rows, threshold=0.5)
    check("the shortlist flag follows the threshold",
          [r["shortlisted"] for r in records] == [r["p_genuine"] < 0.5 for r in rows])
    check("the seats of a record are most incriminating first",
          [d["woe"] for d in records[-1]["seats"]]
          == sorted(d["woe"] for d in records[-1]["seats"]))
    check("fmt_p never shows a non-zero posterior as zero", infer.fmt_p(1e-9) != "0.0000")


def main():
    test_bins()
    test_genuine_dist()
    test_likelihood_ratios()
    test_score_member()
    test_rank_corpus()
    test_output()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
