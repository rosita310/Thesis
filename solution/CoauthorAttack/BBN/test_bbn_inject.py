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
# One ACM journal J1 with 77 of load, as the extract emits it: editor E with one
# outside co-authorship, editor O with the rest. Every co-author has 40 collaborators;
# those of the `single` co-authorships have more one-offs than the rest, so a coerced
# co-author can get a personal adjustment.

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


def corpus(real=REAL):
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
                              [{"journal": "J1", "x": 2, "y": 20}]},
                        "O": {"n_suspect_papers": 75, "inout_points":
                              [{"journal": "J1", "x": 75, "y": 300}]}}}


def two_journal_corpus():
    """The synthetic corpus with J2, a copy of J1 with its own co-authorships."""
    data = corpus()
    data["journals"]["J2"] = copy.deepcopy(data["journals"]["J1"])
    for key, n in list(data["coauthorships"].items()):
        data["coauthorships"][key.replace("J1", "J2")] = {**n, "journal": "J2"}
    return data


def with_in_baseline(data, coerced):
    """The corpus as it would be had the coerced nodes been extracted with it: in
    the journal's baseline and among the co-authorships the typical habit is taken over."""
    data = copy.deepcopy(data)
    counts = data["journals"]["J1"]["baseline_counts"]["established"]
    for i, n in enumerate(coerced):
        counts[n["state"]] = counts.get(n["state"], 0.0) + n["load"]
        data["coauthorships"][f"F||J1||coerced{i}"] = {**n, "editor_id": "F"}
    return data


# --- the fake editor -----------------------------------------------------------

def test_counts():
    print("\n-- coerced papers and splits --")
    check("a third, half, and two-thirds of 15 papers are 5, 8, and 10 coerced",
          [inject.coerced_count(15, s) for s in (1 / 3, 1 / 2, 2 / 3)] == [5, 8, 10])
    check("a third, half, and two-thirds of 30 papers are 10, 15, and 20 coerced",
          [inject.coerced_count(30, s) for s in (1 / 3, 1 / 2, 2 / 3)] == [10, 15, 20])
    check("a record is split as evenly as possible, the larger part first",
          inject.split(5, 2) == [3, 2] and inject.split(4, 2) == [2, 2]
          and inject.split(3, 1) == [3])


def test_coerced_states():
    print("\n-- the coerced co-authorships --")
    one_off = inject.coerced_states("one-off", 2, 2, coauthors=3)
    check("one-off: every paper brings three new `single` co-authorships",
          len(one_off) == 6 and all(s == "single" for s, _ in one_off))
    check("one-off: each paper weighs 1, split over its co-authors",
          approx(sum(load for _, load in one_off), 2.0))
    standing = inject.coerced_states("standing", 5, 2, coauthors=3)
    check("standing: three `repeated` co-authors carrying all the load",
          len(standing) == 3 and {s for s, _ in standing} == {"repeated"}
          and approx(sum(load for _, load in standing), 5.0))
    check("standing, one paper: the co-authors are still `single`",
          {s for s, _ in inject.coerced_states("standing", 1, 2)} == {"single"})
    both = inject.coerced_states("both", 5, 2, coauthors=3)
    check("both, five papers: three one-off papers and two standing ones",
          [s for s, _ in both].count("single") == 9
          and [s for s, _ in both].count("repeated") == 3
          and approx(sum(load for _, load in both), 5.0))
    check("no coerced papers, no co-authorships", inject.coerced_states("one-off", 0, 2) == [])


def test_journals():
    print("\n-- drawing from the real journal --")
    data = two_journal_corpus()
    journals = inject.Journals(data["coauthorships"], {"J1": "acm", "J2": "acm"})
    check("a journal with 77 of load can supply a record of 30 and keep 30 for itself",
          journals.eligible(records=[30]) == ["J1", "J2"])
    check("...but not a record of 50", journals.eligible(records=[50]) == [])
    rng = random.Random(4)
    nodes, own = journals.genuine("J1", 7, rng)
    check("the genuine co-authorships make up exactly the genuine papers",
          approx(sum(n["load"] for n in nodes), 7.0))
    check("they are drawn without replacement, from the journal itself",
          len({n["coauthor_id"] for n in own}) == len(own)
          and all(n["journal"] == "J1" for n in own))
    check("the baselines leave out the whole originals",
          [n["coauthor_id"] for n in own] == [n["coauthor_id"] for n in nodes]
          and sum(n["load"] for n in own) >= 7.0)
    cut = journals.genuine("J1", 1.5, random.Random(1))
    check("the last co-authorship counts only with the load still needed",
          approx(sum(n["load"] for n in cut[0]), 1.5))


def test_fake_editor():
    print("\n-- a fake editor --")
    data = two_journal_corpus()
    journals = inject.Journals(data["coauthorships"], {"J1": "acm", "J2": "acm"})
    nodes, own = inject.fake_editor(["J1"], 15, 1 / 2, "one-off", random.Random(2),
                                    journals, 2)
    coerced = [n for n in nodes if "coauthor_id" not in n]
    check("half of 15 papers: 8 coerced and 7 genuine",
          approx(sum(n["load"] for n in coerced), 8.0)
          and approx(sum(n["load"] for n in nodes) - 8.0, 7.0))
    check("the coerced co-authors share nothing outside the journal",
          {n["state"] for n in coerced} == {"single"})
    check("a coerced co-author takes the habit of a real co-author of the journal",
          all(n["pool_n"] == 40 for n in coerced))
    two, _ = inject.fake_editor(["J1", "J2"], 30, 2 / 3, "one-off", random.Random(2),
                                journals, 2)
    load = {j: sum(n["load"] for n in two if n["journal"] == j) for j in ("J1", "J2")}
    check("on two boards, the record is split evenly over both journals",
          approx(load["J1"], 15.0) and approx(load["J2"], 15.0))


def test_leave_out_is_exact():
    print("\n-- the fake score equals a corpus extracted with the fake editor --")
    data = corpus()
    ref = infer.reference(data)
    journals = inject.Journals(data["coauthorships"], ref.publisher_of)
    nodes, own = inject.fake_editor(["J1"], 15, 2 / 3, "both", random.Random(3),
                                    journals, 2)
    coerced = [n for n in nodes if "coauthor_id" not in n]
    _, quick, _ = infer.score_editor(nodes, ref, own=own)
    full_ref = infer.reference(with_in_baseline(data, coerced))
    _, full, _ = infer.score_editor(nodes, full_ref, own=own + coerced)
    check(f"scored against the real corpus == extracted with it ({quick:.6f} vs {full:.6f})",
          approx(quick, full))


# --- the test --------------------------------------------------------------------

def test_run():
    print("\n-- the run --")
    trials, n_real, eligible = inject.run(two_journal_corpus())
    check("every kind of fake editor is made in every eligible journal",
          {(t["profile"], t["boards"], t["record"], round(t["share"], 4), t["journal"])
           for t in trials}
          == {s[:3] + (round(s[3], 4), j) for s in inject.settings() for j in eligible})
    check("the real ranking the fakes are placed in has every real editor", n_real == 2)
    by = {}
    for t in trials:
        if t["profile"] == "one-off" and t["boards"] == 1 and t["record"] == 30:
            by.setdefault(round(t["share"], 4), []).append(t["p"])
    medians = [sorted(by[s])[len(by[s]) // 2] for s in sorted(by)]
    check("a larger coerced share lowers P(genuine)",
          all(a > b for a, b in zip(medians, medians[1:])))
    check("a career two-thirds built on one-off coercion is found",
          all(p < 0.5 for p in by[round(2 / 3, 4)]))
    rows = inject.summarise(trials)
    check("the summary has a row for every kind of fake editor",
          len(rows) == len(inject.settings()))
    check("rank 1 is below every real editor, and ties rank before",
          inject.rank_among(-5.0, [-1.0, 0.5]) == 1 and inject.rank_among(0.5, [-1.0, 0.5]) == 2
          and inject.rank_among(9.0, [-1.0, 0.5]) == 3)
    one = inject.run(corpus())[0]
    check("with one eligible journal, no fake editor can sit on two boards",
          not any(t["boards"] == 2 for t in one))


def main():
    test_counts()
    test_coerced_states()
    test_journals()
    test_fake_editor()
    test_leave_out_is_exact()
    test_run()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
