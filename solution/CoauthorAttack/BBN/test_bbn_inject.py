"""
Tests for bbn_inject.py, on a synthetic corpus. No data file needed.

    python test_bbn_inject.py
"""

from __future__ import annotations

import copy
import random

import bbn_infer as infer
import bbn_inject as inject

_failures = 0


def check(label, condition):
    global _failures
    if not condition:
        _failures += 1
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}")


def approx(a, b, tol=1e-9):
    return abs(a - b) <= tol


# --- the synthetic corpus --------------------------------------------------
# One ACM journal J1 whose baseline includes the host's real nodes, as the extract
# emits it. The host E has one outside co-authorship and a seat below the diagonal.
# Every co-author has 40 collaborators; those of the `single` co-authorships have
# more one-offs than the rest, so a planted `single` gets a personal adjustment.

STATES = ["outside_5plus", "outside_2_4", "outside_1", "repeated", "single"]


def node(state, load, coauthor, stage="established", pool_n=40, pool_oneoffs=None):
    oneoffs = (30 if state == "single" else 15) if pool_oneoffs is None else pool_oneoffs
    return {"editor_id": "E", "journal": "J1", "publisher": "acm", "coauthor_id": coauthor,
            "coauthor_name": coauthor, "state": state, "load": load, "career_stage": stage,
            "pool_n": pool_n, "pool_oneoffs": oneoffs}


REAL = [node("outside_5plus", 2.0, "a")]
OTHERS = {f"O||J1||{s}{i}": {**node(s, 1.0, f"{s}{i}"), "editor_id": "O"}
          for s, n in [("outside_5plus", 40), ("outside_2_4", 15), ("outside_1", 10),
                       ("repeated", 2), ("single", 8)] for i in range(n)}


def corpus(real=REAL, points=None):
    nodes = {**OTHERS, **{f"E||J1||{n['coauthor_id']}": n for n in real}}
    counts = {}
    for n in nodes.values():
        counts[n["state"]] = counts.get(n["state"], 0.0) + n["load"]
    return {"config": {"states": STATES, "repeated_min": 2, "junior_max_pubs": 10},
            "journals": {"J1": {"publisher": "acm",
                                "baseline_counts": {"established": counts}}},
            "coauthorships": nodes,
            "editor_index": {"E": [f"E||J1||{n['coauthor_id']}" for n in real],
                             "O": list(OTHERS)},
            "editors": {"E": {"n_suspect_papers": 2, "inout_points":
                              points or [{"journal": "J1", "x": 2, "y": 20}]},
                        "O": {"n_suspect_papers": 75, "inout_points":
                              [{"journal": "J1", "x": 75, "y": 300}]}}}


def with_in_baseline(data, planted):
    """The corpus as it would be had the planted nodes been extracted with it: in
    the journal's baseline and among the co-authorships r_bar is taken over."""
    data = copy.deepcopy(data)
    counts = data["journals"]["J1"]["baseline_counts"]["established"]
    for i, n in enumerate(planted):
        counts[n["state"]] = counts.get(n["state"], 0.0) + n["load"]
        data["coauthorships"][f"E||J1||planted{i}"] = {**n, "editor_id": "E"}
    return data


# --- planting ---------------------------------------------------------------

def test_planted_states():
    print("\n-- the planted co-authorships --")
    rng = random.Random(1)
    one_off = inject.planted_states("one-off", 2, rng, 2, coauthors=3)
    check("one-off: every paper brings three new `single` co-authorships",
          len(one_off) == 6 and all(s == "single" for s, _ in one_off))
    check("one-off: each paper weighs 1, split over its co-authors",
          approx(sum(load for _, load in one_off), 2.0))
    check("standing, one paper: the co-authors are still `single`",
          {s for s, _ in inject.planted_states("standing", 1, rng, 2, coauthors=3)}
          == {"single"})
    standing = inject.planted_states("standing", 5, rng, 2, coauthors=3)
    check("standing, R_min papers or more: three `repeated` co-authors carrying all the load",
          len(standing) == 3 and {s for s, _ in standing} == {"repeated"}
          and approx(sum(load for _, load in standing), 5.0))
    drawn = [s for _ in range(300)
             for s, _ in inject.planted_states("mix", 1, rng, 2, coauthors=3)]
    check("mix: states drawn from m(s), never one it gives no mass",
          "outside_5plus" not in drawn and {"single", "repeated"} <= set(drawn))
    check("follow-up: every co-authorship has one tie outside the journal",
          {s for s, _ in inject.planted_states("follow-up", 3, rng, 2)} == {"outside_1"})
    check("no papers, nothing planted",
          inject.planted_states("one-off", 0, rng, 2) == [])


def test_donors():
    print("\n-- the donor of career stage and pool stats --")
    real = {"x": node("single", 1.0, "x", stage="junior", pool_n=77, pool_oneoffs=11),
            "y": {**node("single", 1.0, "y", pool_n=50, pool_oneoffs=5), "journal": "J2"}}
    donors = inject.Donors(real, {"J1": "acm", "J2": "acm", "J3": "acm", "K1": "ieee"})
    rng = random.Random(1)
    check("a planted node copies a real one of its state in the same journal",
          donors.draw("J1", "single", rng)["pool_n"] == 77)
    check("a journal without one falls back to the publisher",
          donors.draw("J3", "single", rng)["pool_n"] in {77, 50})
    check("no real node of that state in the publisher: no donor",
          donors.draw("K1", "single", rng) == {})
    planted = inject.planted_nodes("J1", "one-off", 1, rng, donors, 2)
    check("planted nodes take the donor's pool stats and career stage",
          all(n["pool_n"] == 77 and n["pool_oneoffs"] == 11 and n["career_stage"] == "junior"
              for n in planted))
    bare = inject.planted_nodes("K1", "one-off", 1, rng, donors, 2)
    check("without a donor a planted node has no pool stats, so no adjustment",
          all(n["pool_n"] is None and n["career_stage"] == "established" for n in bare))


def test_seat():
    print("\n-- the in/out gate after planting --")
    points = [{"journal": "J1", "x": 2, "y": 8}, {"journal": "J2", "x": 0, "y": 5}]
    after = inject.with_planted_seat(points, "J1", 6)
    check("only the seat of the planted journal gains the new co-authors",
          after[0]["x"] == 8 and after[1]["x"] == 0)
    check("enough new co-authors push a failing editor through the gate",
          not infer.passes_inout_gate(points) and infer.passes_inout_gate(after))
    check("papers are split over seats as evenly as possible, the larger part first",
          inject.split(5, 2) == [3, 2] and inject.split(4, 2) == [2, 2]
          and inject.split(0, 2) == [0, 0] and inject.split(3, 1) == [3])


# --- scoring ------------------------------------------------------------------

def test_leave_out_is_exact():
    print("\n-- the planted score equals a corpus extracted with the planted papers --")
    data = corpus()
    ref = infer.reference(data)
    rng = random.Random(3)
    donors = inject.Donors(data["coauthorships"], ref.publisher_of)
    planted = inject.planted_nodes("J1", "mix", 4, rng, donors, 2)
    check("the planted co-authorships get the personal adjustment of their donors",
          all(d["adjusted"] for d in infer.score_editor(planted, ref, own=REAL)[2]))
    _, real, _ = infer.score_editor(REAL, ref)
    quick = real + inject.planted_log_odds(planted, REAL, ref)
    full_ref = infer.reference(with_in_baseline(data, planted))
    _, full, _ = infer.score_editor(REAL + planted, full_ref)
    check(f"real + planted log-odds == scoring the full record ({quick:.6f} vs {full:.6f})",
          approx(quick, full))
    check("merging planted nodes with the same scoring fields changes nothing",
          approx(infer.score_editor(planted, ref, own=REAL)[1],
                 infer.score_editor(inject.merged(planted), ref, own=REAL)[1]))
    check("merging sums the load", approx(sum(n["load"] for n in inject.merged(planted)), 4.0))


def two_board_corpus():
    """The synthetic corpus with E also on the board of J2, a copy of J1 with the same
    seat below the diagonal."""
    data = corpus(points=[{"journal": "J1", "x": 2, "y": 20},
                          {"journal": "J2", "x": 2, "y": 20}])
    data["journals"]["J2"] = copy.deepcopy(data["journals"]["J1"])
    return data


def test_run():
    print("\n-- the run over every editor --")
    trials = inject.run(corpus())
    def host(profile, seats=1, ts=trials):
        return {t["papers"]: t for t in ts if t["profile"] == profile
                and t["seats"] == seats and t["band"] == "<=2"}

    by_papers = host("one-off")
    real = {r["ident"]: r for r in infer.rank_corpus(corpus(), inout_gate=False)}
    check("no planted papers reproduces the real score",
          approx(by_papers[0]["p"], real["E"]["p_genuine"]))
    ps = [by_papers[k]["p"] for k in sorted(by_papers)]
    check("every extra one-off paper lowers P(genuine)",
          all(a > b for a, b in zip(ps, ps[1:])))
    check("one coerced paper does not clear the gate, twenty do",
          not by_papers[1]["gate"] and by_papers[20]["gate"])
    follow_up = host("follow-up")
    check("follow-up co-authorships (one tie outside) never lower P(genuine)",
          all(follow_up[k]["p"] >= follow_up[0]["p"] for k in follow_up))
    check("an editor on one board gets no run spread over two seats",
          not any(t["seats"] == 2 for t in trials))
    rows = inject.summarise(trials)
    check("the summary has a row over all hosts for every one-seat run and count",
          {(r["profile"], r["seats"], r["papers"]) for r in rows if r["hosts"] == "all"}
          == {run + (k,) for run in inject.runs() if run[1] == 1
              for k in inject.INJECT_PAPERS})
    check("a gated share never exceeds the ungated one",
          all(r["shortlisted_gated"] <= r["shortlisted"] for r in rows))

    two = inject.run(two_board_corpus())
    one_seat, spread = host("one-off", 1, two), host("one-off", 2, two)
    check("an editor on two boards gets the one-off run spread over two seats",
          set(spread) == set(inject.INJECT_PAPERS))
    check("spread over two seats, no planted papers still reproduces the real score",
          approx(spread[0]["p"], one_seat[0]["p"]))
    check("ten papers in one seat clear the gate, five in each of two seats do not",
          one_seat[10]["gate"] and not spread[10]["gate"])
    check("the editor on two boards is counted in the multi-board host group",
          any(r["hosts"] == inject.MULTI_BOARD and r["seats"] == 2
              for r in inject.summarise(two)))


def test_host_band():
    print("\n-- host bands --")
    check("1, 2 | 3, 10 | 11 papers fall in <=2 | 3-10 | >10",
          [inject.host_band(n) for n in (1, 2, 3, 10, 11)]
          == ["<=2", "<=2", "3-10", "3-10", ">10"])
    check("the band names come in order", inject.host_band_names() == ["<=2", "3-10", ">10"])


def main():
    test_planted_states()
    test_donors()
    test_seat()
    test_leave_out_is_exact()
    test_run()
    test_host_band()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
