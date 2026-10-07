"""
Tests for bbn_placebo.py. No data directory needed.

    python test_bbn_placebo.py
"""

from __future__ import annotations

import bbn_placebo as placebo

_failures = 0


def check(label, condition):
    global _failures
    if not condition:
        _failures += 1
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}")


def approx(a, b, tol=1e-9):
    return abs(a - b) <= tol


HERE = "https://dblp.org/rec/journals/jk/p1"
ALSO = "https://dblp.org/rec/journals/jk/p2"
AWAY = "https://dblp.org/rec/conf/c/x1"


def test_venue_locking():
    print("\n-- venue locking --")
    pubs = {"E": {HERE, ALSO, AWAY}, "A": {HERE, ALSO}, "B": {HERE, AWAY}}
    check("a pair sharing only in-journal papers is venue-locked",
          placebo.venue_locked("E", "A", {"jk"}, HERE, pubs) == 1)
    check("a pair sharing an outside paper is not",
          placebo.venue_locked("E", "B", {"jk"}, HERE, pubs) == 0)
    check("the paper at hand counts as shared even if a record misses it",
          placebo.venue_locked("E", "Z", {"jk"}, HERE, {}) == 1)

    # A three-person paper where the editor (index 0) is the odd one out: this
    # journal is all E shares with either of them, while A and B also share a
    # conference paper.
    signers = ["E", "A", "B"]
    pubs = {"E": {HERE}, "A": {HERE, AWAY}, "B": {HERE, AWAY}}
    lock = placebo.locked_pairs(signers, {"jk"}, HERE, pubs)
    check("A and B are not locked with each other, the editor is with both",
          lock == {(0, 1): 1, (0, 2): 1, (1, 2): 0})
    check("the editor's fraction is 1.0 and each other's is 0.5",
          placebo.locked_fraction(lock, 0, 3) == 1.0
          and placebo.locked_fraction(lock, 1, 3) == 0.5
          and placebo.locked_fraction(lock, 2, 3) == 0.5)

    unit, = placebo.venue_units([placebo.Pool("E", HERE, "J", {"jk"}, signers)], pubs)
    check("a unit holds the editor's fraction beside each stand-in's",
          unit["observed"] == 1.0 and unit["alternatives"] == [0.5, 0.5])
    check("the most prolific stand-in is the one with the longest record",
          unit["senior"] == 0.5 and unit["editor_is_most_prolific"] is False)
    check("stand-ins in the editor's record band are averaged into `matched`",
          unit["matched"] == 0.5)
    extra = {f"x{i}" for i in range(20)}
    lone = {"E": {HERE}, "A": {HERE, AWAY} | extra, "B": {HERE, AWAY} | extra}
    unit, = placebo.venue_units([placebo.Pool("E", HERE, "J", {"jk"}, signers)], lone)
    check("...and `matched` is None when no stand-in shares the editor's band",
          unit["matched"] is None)


def test_clustering():
    """One editor with many papers must not count as many editors."""
    print("\n-- clustering on the editor --")
    editors = ["E1", "E1", "E1", "E2"]
    observed = [1.0, 1.0, 1.0, 0.0]
    kept, per_editor = placebo.cluster_by_editor(editors, observed,
                                                 [0.0, 0.0, 0.0, 0.0])
    check("every unit with a comparator is kept", len(kept) == 4)
    check("...but the t is taken over one value per editor", len(per_editor) == 2)
    check("an editor's units are averaged, not repeated", per_editor == [1.0, 0.0])
    kept, per_editor = placebo.cluster_by_editor(editors, observed,
                                                 [0.0, None, 0.0, 0.0])
    check("a unit with no comparator drops out of both",
          len(kept) == 3 and len(per_editor) == 2)


def test_permutation_null():
    print("\n-- the per-editor permutation null --")
    extreme = [{"editor": "E", "observed": 1.0, "alternatives": [0.5, 0.5]}] * 4
    flat = [{"editor": "F", "observed": 0.5, "alternatives": [0.5, 0.5]}] * 4
    check("an editor extreme on 4 papers gets a small p",
          placebo.exact_test(extreme, samples=4000)[0]["p"] < 0.05)
    check("an editor identical to their stand-ins gets p = 1",
          approx(placebo.exact_test(flat, samples=2000)[0]["p"], 1.0))
    check("the same input gives the same p-values every run",
          placebo.exact_test(extreme + flat, samples=2000)
          == placebo.exact_test(extreme + flat, samples=2000))
    alone = placebo.exact_test(extreme, samples=2000)[0]["p"]
    with_f = [r["p"] for r in placebo.exact_test(flat + extreme, samples=2000)
              if r["editor"] == "E"][0]
    check("an editor's p does not depend on who else is in the run, or in what order",
          alone == with_f)
    results = placebo.exact_test(flat + extreme, samples=2000)
    check("results are sorted most significant first, which the BH ranks rely on",
          [r["editor"] for r in results] == ["E", "F"]
          and results[0]["n_papers"] == 4)
    check("observed and expected are summed over the editor's papers",
          approx(results[0]["observed"], 4.0) and approx(results[0]["expected"], 4 * 2 / 3))

    q = placebo.bh_q([0.01, 0.02] + [0.5] * 8)
    check("Benjamini-Hochberg scales a p by n over its rank",
          approx(q[0], 0.1) and approx(q[1], 0.1))
    check("...and never exceeds 1", placebo.bh_q([0.9, 0.95]) == [0.95, 0.95])
    q = placebo.bh_q([0.02, 0.03])
    check("...and is monotone: a later rank can lower an earlier q",
          approx(q[0], 0.03) and approx(q[1], 0.03))


def test_partner_patterns():
    print("\n-- the partner's own collaboration pattern --")
    depth = {"A": {"E": 1, "B": 4, "X": 4, "Y": 4, "Z": 4}}
    spread = {"A": {k: {"j"} for k in depth["A"]}}
    pattern = placebo.describe(depth, spread)
    check("a partner with too few collaborators yields no pattern",
          placebo.describe({"A": {"E": 1, "X": 1}},
                           {"A": {"E": {"j"}, "X": {"j"}}}) == {})
    check("a pattern is the partner's depth and spread distributions",
          pattern["A"]["depth"] == [1, 4, 4, 4, 4] and pattern["A"]["min_depth"] == 1
          and pattern["A"]["n"] == 5)

    signers = ["E", "A", "B"]
    editor = placebo.signer_pattern(signers, 0, depth, spread, pattern)
    check("the editor is A's one-off: bottom percentile, at A's minimum depth, one venue",
          editor == {"pct_depth": 0.0, "pct_spread": 0.5, "is_min_depth": 1.0,
                     "lone_venue": 1.0})
    stand_in = placebo.signer_pattern(signers, 2, depth, spread, pattern)
    check("B is one of A's four deep partners: percentile 0.625, not the minimum",
          approx(stand_in["pct_depth"], 0.625) and stand_in["is_min_depth"] == 0.0)
    check("a signer none of whose partners has a pattern gets None",
          placebo.signer_pattern(signers, 1, depth, spread, pattern) is None)

    unit, = placebo.pattern_units([placebo.Pool("E", HERE, "J", {"jk"}, signers)],
                                  depth, spread, pattern)
    check("a unit compares the editor with the stand-ins that could be placed",
          unit["observed"] == editor and approx(unit["alternatives"]["pct_depth"], 0.625))
    check("stand-ins of similar network size form the matched comparator",
          unit["matched"] is not None and approx(unit["matched"]["pct_depth"], 0.625))
    check("a pool whose editor cannot be placed yields no unit",
          placebo.pattern_units([placebo.Pool("Q", HERE, "J", {"jk"}, ["Q", "A", "B"])],
                                depth, spread, pattern) == [])


def main():
    test_venue_locking()
    test_clustering()
    test_permutation_null()
    test_partner_patterns()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
