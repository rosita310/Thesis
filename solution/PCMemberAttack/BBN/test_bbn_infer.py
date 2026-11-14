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
N_STATES = len(infer.EVIDENCE_STATES)


def seat(cites, expected, stream="conf/a", core_rank="A", src="conference", edition=None,
         cites_gate=None, pubs=30, citing=None, expected_citing=None, source="lncs"):
    """Without citing and expected_citing, depth is 1."""
    return {"edition_key": edition or f"{stream}/2010", "stream": stream,
            "core_rank": core_rank, "source": source, "role": "pc", "src": src, "year": 2010,
            "cites": cites, "citing": cites if citing is None else citing,
            "cites_gate": cites if cites_gate is None else cites_gate,
            "papers": 50, "off_cites": 0, "off_papers": 50, "rate": expected / 50,
            "expected": expected,
            "expected_citing": expected if expected_citing is None else expected_citing,
            "pubs": pubs}


def classified(*args, **kwargs):
    return infer.classify(seat(*args, **kwargs))


def corpus(members):
    """members: pid -> list of seats."""
    return {"model": "test", "funnel": {}, "members": {
        pid: {"name": pid.upper(), "seats": seats,
              "cites_gate_max": max(s["cites_gate"] for s in seats)}
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
          [infer.band_label(value, bands) for value in (0.1, 0.5, 1.9, 2, 5, 30)]
          == ["<0.5", "0.5-2", "0.5-2", "2-5", ">=5", ">=5"])
    s = classified(26, 1.0)
    check("classify takes the surplus cites - expected", approx(s["surplus"], 25.0))
    check("...and bins it and expected", (s["state"], s["band"]) == ("high", "expected 0.5-2"))
    check("the same surplus is the same bin whatever expected",
          classified(35, 10.0)["state"] == s["state"])
    check("classify leaves the seat itself alone", "state" not in seat(26, 1.0))
    peer = infer.classify(seat(7, 1.0, src="peers", pubs=60), pubs_bands=[15, 60, 180])
    check("a seat with the peer rate takes the band of the member's publications",
          peer["band"] == "pubs 60-180")
    check("...whatever its expected",
          infer.classify(seat(7, 9.0, src="peers", pubs=60), pubs_bands=[15, 60, 180])["band"]
          == peer["band"])


def test_depth():
    print("\n-- depth --")
    s = seat(30, 6.0, citing=5, expected_citing=4.0)
    check("depth = (cites / citing) / (expected / expected_citing)",
          approx(infer.seat_depth(s), (30 / 5) / (6.0 / 4.0)))
    check("no citing paper has no depth", infer.seat_depth(seat(0, 1.0)) is None)
    check("a bin after typical with depth at the edge is deep",
          infer.seat_state(25, 1.5, depth_edge=1.5) == "high deep"
          and infer.seat_state(25, 1.49, depth_edge=1.5) == "high")
    check("typical is never deep", infer.seat_state(5, 3.0, depth_edge=1.5) == "typical")
    check("without a depth a seat is not deep", infer.seat_state(25, None) == "high")
    c = infer.classify(s, depth_edge=1.5)
    check("classify keeps the bin and the state apart",
          (c["bin"], c["state"], c["depth"]) == ("high", "high deep", 4.0))
    check("the evidence states are typical and every other bin, as is and deep",
          infer.evidence_states(["t", "m", "h"]) == ["t", "m", "m deep", "h", "h deep"])


# --- the genuine column -----------------------------------------------------

def test_genuine_dist():
    print("\n-- the genuine column and its fallback --")
    others = ([classified(0, 1.0, edition=f"conf/a/{i}") for i in range(8)]
              + [classified(0, 1.0, stream="conf/b", edition=f"conf/b/{i}") for i in range(8)]
              + [classified(0, 1.0, stream="conf/c", core_rank="B", edition=f"conf/c/{i}")
                 for i in range(8)])
    own = [classified(60, 1.0, edition="conf/a/own1"), classified(60, 1.0, edition="conf/a/own2")]
    counts = infer.state_counts(others + own)
    own_counts = infer.state_counts(own)
    target = own[0]

    baseline, level = infer.genuine_dist(counts, target, own_counts, min_n=8)
    check("enough seats in the conference after leaving out the member's own",
          level == "conference")
    check("...and the member's own extreme seats are not in it",
          approx(baseline["extreme"], 0.5 / (8 + N_STATES * 0.5)))
    check("the column is smoothed into a distribution", approx(sum(baseline.values()), 1.0))
    _, level = infer.genuine_dist(counts, target, own_counts, min_n=9)
    check("too few in the conference falls back to the CORE rank", level == "core_rank")
    _, level = infer.genuine_dist(counts, target, own_counts, min_n=17)
    check("too few in the rank falls back to all conferences", level == "pooled")
    baseline, level = infer.genuine_dist(counts, target, own_counts, min_n=1000)
    check("the pooled level is used regardless",
          level == "pooled" and approx(baseline["typical"], 24.5 / (24 + N_STATES * 0.5)))
    _, level = infer.genuine_dist(counts, target, {}, min_n=10)
    check("without leaving out the own seats the conference would have been enough",
          level == "conference")

    peer = classified(60, 1.0, src="peers", edition="conf/a/peer")
    baseline, level = infer.genuine_dist(infer.state_counts(others + own + [peer]), peer, {},
                                         min_n=1)
    check("a seat with a peer rate is judged only against peer-rate seats",
          level == "conference" and approx(baseline["extreme"], 1.5 / (1 + N_STATES * 0.5)))
    big = classified(12, 10.0, edition="conf/a/big")
    baseline, level = infer.genuine_dist(infer.state_counts(others + [big]), big, {}, min_n=1)
    check("...and a seat in another prom band only against that band",
          level == "conference" and approx(baseline["typical"], 1.5 / (1 + N_STATES * 0.5)))
    prolific = classified(60, 1.0, src="peers", edition="conf/a/prolific", pubs=500)
    baseline, _ = infer.genuine_dist(infer.state_counts(others + own + [peer, prolific]),
                                     prolific, {}, min_n=1)
    check("a peer seat is judged only against peer seats with as many publications",
          approx(baseline["extreme"], 1.5 / (1 + N_STATES * 0.5)))
    baseline, _ = infer.genuine_dist(infer.state_counts(others), big, {}, min_n=1)
    check("an empty cell gives the uniform column", approx(baseline["typical"], 1 / N_STATES))


# --- the likelihood ratio and the score ---------------------------------------

def test_likelihood_ratios():
    print("\n-- the likelihood ratio of one seat --")
    baseline = {"typical": 0.9, "mild": 0.06, "high": 0.03, "extreme": 0.01}
    lr = {s: infer.seat_lr(s, baseline, ALPHA, MANIP) for s in baseline}
    check(f"LR_typical == 1/(1-alpha) = {1 / (1 - ALPHA):g}", approx(lr["typical"], 2.0))
    check("every bin with coerced mass lowers genuineness",
          all(lr[s] < 1 for s in ("mild", "high", "extreme")))
    check("the rarer the genuine bin, the lower the LR",
          lr["extreme"] < lr["high"] < lr["mild"])
    check("LR = genuine / (alpha manip + (1 - alpha) genuine)",
          approx(lr["extreme"], 0.01 / (0.5 * 0.4 + 0.5 * 0.01)))
    manip = infer.manip_dist(MANIP, deep=0.7)
    check("manip_dist splits every bin after typical into shallow and deep",
          approx(manip["high deep"], 0.4 * 0.7) and approx(manip["high"], 0.4 * 0.3)
          and manip["typical"] == 0.0 and approx(sum(manip.values()), 1.0))


def test_score_member():
    print("\n-- the score of a member --")
    others = [classified(0, 1.0, edition=f"conf/a/{i}") for i in range(40)]
    prior_odds = math.log(0.95 / 0.05)
    p, log_odds, detail = infer.score_member([], infer.state_counts(others), ALPHA, 0.95, MANIP)
    check("no seats leaves the prior", approx(p, 0.95) and approx(log_odds, prior_odds))
    typical = [classified(0, 1.0, edition="conf/a/t")]
    p_typ, _, _ = infer.score_member(typical, infer.state_counts(others + typical),
                                     ALPHA, 0.95, MANIP)
    check("a typical seat raises P(genuine)", p_typ > 0.95)
    extreme = [classified(60, 1.0, edition=f"conf/a/e{i}") for i in range(3)]
    p_ext, log_odds, detail = infer.score_member(extreme, infer.state_counts(others + extreme),
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
        "a": [seat(60, 1.0, edition=f"conf/a/a{i}", source="acm" if i else "lncs")
              for i in range(3)],
        "b": [seat(1, 1.0, edition=f"conf/a/b{i}", cites_gate=12) for i in range(3)],
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
    check("the seats of both sources count, and the row names them",
          by["a"]["n_seats"] == 3 and by["a"]["sources"] == "acm|lncs")
    check("woe is in log10: three typical seats add 3 log10 2 to the prior",
          approx(by["c"]["woe"], math.log10(0.95 / 0.05) + 3 * math.log10(2), 1e-6))
    unscored = infer.rank_corpus(ranking_corpus(), alpha=ALPHA, prior=0.95, manip=MANIP,
                                 min_seats=2, min_n=30)
    d = {r["pid"]: r for r in unscored}["d"]
    a_with_d = {r["pid"]: r for r in unscored}["a"]
    check("a lower min_seats scores d", d["n_seats"] == 2)
    check("the baseline counts d's seats whether d is scored or not",
          approx(a_with_d["woe"], by["a"]["woe"]))


def test_depth_in_ranking():
    print("\n-- depth in the ranking --")
    background = [seat(0, 1.0, edition=f"conf/a/{i}") for i in range(40)]
    data = corpus({
        "shallow": [seat(60, 1.0, edition=f"conf/a/s{i}") for i in range(3)],
        "deep": [seat(60, 1.0, citing=15, edition=f"conf/a/d{i}") for i in range(3)],
        "z": background})
    by = {r["pid"]: r for r in infer.rank_corpus(data, alpha=ALPHA, prior=0.95, manip=MANIP,
                                                 manip_deep=0.7, depth_edge=1.5, min_seats=3,
                                                 min_n=30)}
    check("deep seats count as deep, in the same bin",
          (by["deep"]["n_deep"], by["deep"]["n_extreme"], by["shallow"]["n_deep"]) == (3, 3, 0))
    check("with deep seats rare among genuine ones, the same surplus weighs more when deep",
          by["deep"]["woe"] < by["shallow"]["woe"])
    by = {r["pid"]: r for r in infer.rank_corpus(data, alpha=ALPHA, prior=0.95, manip=MANIP,
                                                 manip_deep=0.5, depth_edge=1.5, min_seats=3,
                                                 min_n=30)}
    check("...and as much when coerced seats are as often deep as not",
          approx(by["deep"]["woe"], by["shallow"]["woe"]))


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
    test_depth()
    test_genuine_dist()
    test_likelihood_ratios()
    test_score_member()
    test_rank_corpus()
    test_depth_in_ranking()
    test_output()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
