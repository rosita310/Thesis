"""
Tests for bbn_infer.py, on a synthetic corpus. No data file needed.

    python test_bbn_infer.py
"""

from __future__ import annotations

import math

import bbn_infer as infer

_failures = 0


def check(label, condition):
    global _failures
    if not condition:
        _failures += 1
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}")


def approx(a, b, tol=1e-6):
    return abs(a - b) <= tol


# --- the synthetic corpus --------------------------------------------------
# A three-state model is enough to exercise the machinery, which is generic over
# the states. Journal J1's baseline INCLUDES the scored editor's own nodes, as
# the extract emits them; leave-one-editor exclusion takes them back out again.

STATES = ["outside", "repeated", "single"]
JOURNALS = {"J1": {"publisher": "acm", "baseline_counts": {
    "established": {"outside": 300.0, "repeated": 60.0, "single": 40.0},
    "junior": {"outside": 40.0, "repeated": 20.0, "single": 60.0},
}}}
ALPHA = 0.20
MANIP = {"outside": 0.0, "repeated": 0.15, "single": 0.85}


def corpus(journals=JOURNALS, **extra):
    return {"config": {"states": STATES, "repeated_min": 2, "junior_max_pubs": 10},
            "journals": journals, **extra}


def ref(journals=JOURNALS):
    return infer.reference(corpus(journals))


REF = ref()


def node(state, load, record="established", coauthor="c", inside=1, outside=0,
         journal="J1", **extra):
    return {"journal": journal, "state": state, "load": load, "career_stage": record,
            "coauthor_id": coauthor, "coauthor_name": coauthor, "editor_id": "E",
            "n_shared_inside": inside, "n_shared_outside": outside,
            "n_shared_outside_strict": outside,
            "n_shared_suspect": 1, "coauthor_pubs": 50, **extra}


def posterior(nodes, alpha=ALPHA, prior=infer.PRIOR, r=REF):
    return infer.score_editor(nodes, r, alpha, prior, manip=MANIP)[0]


def ranking_corpus():
    """Three editors in J1: E1 with one locked node below the diagonal, E2 with
    one outside node and a red board seat, and E3 without gate points."""
    nodes = {
        "E1||J1||a": {**node("single", 3.0, coauthor="a"), "editor_id": "E1"},
        "E2||J1||b": {**node("outside", 3.0, coauthor="b", outside=2), "editor_id": "E2"},
        "E3||J1||c": {**node("repeated", 1.0, coauthor="c", inside=2), "editor_id": "E3"},
    }
    return corpus(
        coauthorships=nodes,
        editor_index={"E1": ["E1||J1||a"], "E2": ["E2||J1||b"], "E3": ["E3||J1||c"]},
        editor_labels={"E1": "Ed One", "E2": "Ed Two", "E3": "Ed Three"},
        editors={
            "E1": {"n_suspect_papers": 3, "is_pid": False,
                   "inout_points": [{"year": 2010, "x": 1, "y": 5}]},
            "E2": {"n_suspect_papers": 3, "is_pid": True,
                   "inout_points": [{"year": 2010, "x": 0, "y": 3},
                                    {"year": 2011, "x": 4, "y": 2}]},
            "E3": {"n_suspect_papers": 1, "is_pid": False},
        })


# --- the likelihood ratio ---------------------------------------------------

def test_likelihood_ratios():
    print("\n-- the likelihood ratio of one co-authorship --")
    gdist, _ = infer.genuine_dist(REF, "J1", "established", [])
    check(f"LR_outside == 1/(1-alpha) = {1 / (1 - ALPHA):.5f} "
          f"(got {infer.node_lr('outside', gdist, ALPHA, MANIP):.8f})",
          approx(infer.node_lr("outside", gdist, ALPHA, MANIP), 1 / (1 - ALPHA), 1e-9))
    check("LR_single < 1 (a locked co-authorship lowers genuineness)",
          infer.node_lr("single", gdist, ALPHA, MANIP) < 1)
    check("LR_repeated sits between the two (partial dampening)",
          infer.node_lr("single", gdist, ALPHA, MANIP)
          < infer.node_lr("repeated", gdist, ALPHA, MANIP)
          < infer.node_lr("outside", gdist, ALPHA, MANIP))
    check("the reference smooths every baseline into a distribution",
          approx(sum(gdist.values()), 1.0))


# --- the personal one-off baseline ---------------------------------------------

N_MIN = int(infer.FALLBACK_MIN_N)


def pooled_node(editor, journal, load, n, k, state="single"):
    """A co-authorship whose co-author has n collaborators, k of them one-offs."""
    return {**node(state, load, coauthor=f"{editor}{journal}{n}-{k}", journal=journal),
            "editor_id": editor, "pool_n": n, "pool_oneoffs": k}


def share(n, k):
    return (k + 0.5) / (n + 1)


def personal_corpus():
    """ACM journals J1 (well populated) and J2 (thin), IEEE journal I1. E sits on
    J1 and J2; O1..O4 are other editors. Two co-authors are below N_MIN."""
    others = [pooled_node("O1", "J1", 20.0, 39, 19), pooled_node("O2", "J1", 20.0, 39, 29),
              pooled_node("O3", "J2", 10.0, 39, 14), pooled_node("O4", "I1", 40.0, 39, 39),
              pooled_node("O1", "J1", 5.0, N_MIN - 1, 0), pooled_node("O3", "J2", 5.0, 3, 3)]
    mine = [pooled_node("E", "J1", 10.0, 39, 9), pooled_node("E", "J2", 4.0, 59, 50)]
    nodes = {f"{n['editor_id']}||{n['journal']}||{n['coauthor_id']}": n
             for n in others + mine}
    journals = {**JOURNALS,
                "J2": {"publisher": "acm", "baseline_counts": {"established": {
                    "outside": 10.0, "repeated": 2.0, "single": 7.0}}},
                "I1": {"publisher": "ieee", "baseline_counts": {"established": {
                    "outside": 30.0, "repeated": 0.0, "single": 10.0}}}}
    index = {}
    for key, n in nodes.items():
        index.setdefault(n["editor_id"], []).append(key)
    return corpus(journals, coauthorships=nodes, editor_index=index,
                  editors={e: {"n_suspect_papers": 1} for e in index}), mine


def test_oneoff_habit():
    print("\n-- the co-author's one-off habit --")
    check("the habit is (k + 0.5) / (n + 1)",
          approx(infer.oneoff_habit({"pool_n": 39, "pool_oneoffs": 9}), 9.5 / 40))
    zero = infer.oneoff_habit({"pool_n": 40, "pool_oneoffs": 0})
    check(f"the pseudo-count keeps the habit above 0 when k = 0 (got {zero:.4f})",
          zero > 0 and approx(zero, 0.5 / 41))
    check("...and below 1 when every collaborator is a one-off",
          infer.oneoff_habit({"pool_n": 40, "pool_oneoffs": 40}) < 1)
    check("N_min collaborators qualify, one fewer do not, no network never",
          infer.qualifies({"pool_n": N_MIN}, N_MIN)
          and not infer.qualifies({"pool_n": N_MIN - 1}, N_MIN)
          and not infer.qualifies({"pool_n": None}, 0))


def test_typical_habit():
    print("\n-- the typical habit: leave-one-editor-out and the publisher fallback --")
    data, mine = personal_corpus()
    r = infer.reference(data)
    others_acm = 20 * share(39, 19) + 20 * share(39, 29) + 10 * share(39, 14)
    with_mine = others_acm + 10 * share(39, 9) + 4 * share(59, 50)
    whole, level = infer.typical_habit(r, "J1", [])
    expected = (20 * share(39, 19) + 20 * share(39, 29) + 10 * share(39, 9)) / 50
    check(f"J1's typical habit is the load-weighted mean habit of its qualifying "
          f"co-authorships ({whole:.4f})", level == "journal" and approx(whole, expected))
    loo, level = infer.typical_habit(r, "J1", mine)
    check(f"leaving the editor out removes their own co-authorships ({loo:.4f})",
          level == "journal"
          and approx(loo, (20 * share(39, 19) + 20 * share(39, 29)) / 40))
    pooled, level = infer.typical_habit(r, "J2", [])
    thin, level_mine = infer.typical_habit(r, "J2", mine)
    check(f"a journal below N_min falls back to its publisher (got {level}, "
          f"{level_mine} with the editor left out)", level == level_mine == "pooled")
    check("...whose pool is the ACM journals only, without the editor's own "
          "co-authorships", approx(thin, others_acm / 50)
          and approx(pooled, with_mine / 64))
    check("a publisher with no qualifying load gives no typical habit",
          infer.typical_habit(infer.reference(corpus()), "J1", []) == (None, ""))
    off = infer.reference(data, pool_min=math.inf)
    check("pool_min = inf leaves the sums empty",
          off.oneoff_journal == {} and off.oneoff_pooled == {})


def test_personal_column():
    print("\n-- the personal genuine column --")
    gdist, _ = infer.genuine_dist(REF, "J1", "established", [])
    for habit_ratio in (0.1, 0.8, 1.0, 1.7):
        g = infer.personal_column(gdist, habit_ratio)
        check(f"habit ratio {habit_ratio}: g_i(single) = g(single|j) * ratio and the "
              f"column sums to 1",
              approx(g["single"], gdist["single"] * habit_ratio) and approx(sum(g.values()), 1.0))
    g = infer.personal_column(gdist, 1.7)
    check("the other ties are rescaled by one common factor",
          approx(g["outside"] / gdist["outside"], g["repeated"] / gdist["repeated"]))
    capped = infer.personal_column(gdist, 1e6)
    check(f"a huge habit ratio is capped at {infer.PERSONAL_SINGLE_CAP}, still summing to 1",
          approx(capped["single"], infer.PERSONAL_SINGLE_CAP)
          and approx(sum(capped.values()), 1.0))

    data, _ = personal_corpus()
    r = infer.reference(data)
    j1 = [n for n in data["coauthorships"].values()
          if n["journal"] == "J1" and infer.qualifies(n, N_MIN)]
    gj, _ = infer.genuine_dist(r, "J1", "established", [])
    singles = [infer.personal_dist(r, n, gj, [])[0]["single"] for n in j1]
    mean = sum(g * n["load"] for g, n in zip(singles, j1)) / sum(n["load"] for n in j1)
    check(f"over the journal's own qualifying load, g_i(single) averages back to "
          f"g(single|j) ({mean:.5f} vs {gj['single']:.5f})", approx(mean, gj["single"]))

    below = pooled_node("E", "J1", 1.0, N_MIN - 1, 0)
    check("a co-author below N_min keeps the journal's column",
          infer.personal_dist(r, below, gj, [below]) == (gj, None, ""))
    low = pooled_node("E", "J1", 1.0, 99, 5)
    high = pooled_node("E", "J1", 1.0, 99, 90)
    lr_low = infer.node_lr("single", infer.personal_dist(r, low, gj, [])[0], ALPHA, MANIP)
    lr_high = infer.node_lr("single", infer.personal_dist(r, high, gj, [])[0], ALPHA, MANIP)
    check("a one-off with a co-author who rarely one-offs is more incriminating "
          "than with one who often does", lr_low < lr_high)
    check("an outside tie keeps LR = 1/(1-alpha) whatever the co-author's habit",
          approx(infer.node_lr("outside", infer.personal_dist(r, low, gj, [])[0],
                               ALPHA, MANIP), 1 / (1 - ALPHA)))


def test_no_qualifying_node_is_the_tie_model():
    print("\n-- with no co-author over the boundary, the plain tie model --")
    data, _ = personal_corpus()
    small = {k: {**n, "pool_n": min(n["pool_n"], N_MIN - 1)}
             for k, n in data["coauthorships"].items()}
    data = {**data, "coauthorships": small}
    rows = infer.rank_corpus(data, ALPHA, manip=MANIP, inout_gate=False)
    check("no node is adjusted", not any(d["adjusted"] for r in rows for d in r["detail"]))
    plain = infer.rank_corpus(data, ALPHA, manip=MANIP, inout_gate=False,
                              pool_min=math.inf)
    check("every score equals the scoring with the adjustment switched off",
          [(r["ident"], r["woe"]) for r in rows] == [(r["ident"], r["woe"]) for r in plain])
    r = infer.reference(data)
    by_hand = {}
    for e, ids in data["editor_index"].items():
        own = [small[i] for i in ids]
        by_hand[e] = math.log(infer.PRIOR / (1 - infer.PRIOR)) + sum(
            n["load"] * math.log(infer.node_lr(
                n["state"], infer.genuine_dist(r, n["journal"], "established", own)[0],
                ALPHA, MANIP)) for n in own)
    check("...and the tie model by hand: log prior odds + SUM load * log LR(tie)",
          all(approx(row["woe"] * math.log(10), by_hand[row["ident"]]) for row in rows))

    full, _ = personal_corpus()
    adjusted = {r["ident"]: r for r in infer.rank_corpus(full, ALPHA, manip=MANIP,
                                                         inout_gate=False)}
    off = {r["ident"]: r for r in infer.rank_corpus(full, ALPHA, manip=MANIP,
                                                    inout_gate=False, pool_min=math.inf)}
    check("with qualifying co-authors the score does move",
          not approx(adjusted["E"]["woe"], off["E"]["woe"]))
    d = {x["journal"]: x for x in adjusted["E"]["detail"]}
    check("the detail records the habit ratio, the journal's and the adjusted g, and the "
          "typical habit's level",
          d["J1"]["adjusted"] and d["J1"]["oneoff_level"] == "journal"
          and d["J2"]["oneoff_level"] == "pooled"
          and approx(d["J2"]["single_p"],
                     min(d["J2"]["journal_single_p"] * d["J2"]["habit_ratio"],
                         infer.PERSONAL_SINGLE_CAP), 1e-5))


def test_load_accumulation():
    """One 4-author paper (4 nodes of load 1/4) must move the score by exactly as
    much as one 1-co-author paper in the same state."""
    print("\n-- the load rule in the score --")
    big_paper = [node("single", 0.25, coauthor=f"c{i}") for i in range(4)]
    small_paper = [node("single", 1.0)]
    check(f"a 4-co-author paper scores exactly like a 1-co-author paper "
          f"({posterior(big_paper):.6f} vs {posterior(small_paper):.6f})",
          approx(posterior(big_paper), posterior(small_paper)))
    check("...and both are more suspicious than the prior",
          posterior(big_paper) < infer.PRIOR)
    check("two locked papers beat one",
          posterior([node("single", 2.0)]) < posterior(small_paper))
    check("an outside tie raises genuineness above the prior",
          posterior([node("outside", 1.0)]) > infer.PRIOR)
    check("a locked paper plus an outside tie is milder than the locked paper alone",
          posterior([node("single", 1.0), node("outside", 1.0)])
          > posterior(small_paper))
    _, log_odds, detail = infer.score_editor(big_paper, REF, ALPHA, manip=MANIP)
    check("the detail carries one record per node, with its contribution",
          len(detail) == 4 and approx(sum(d["weighted_log_lr"] for d in detail),
                                      log_odds - infer.score_editor([], REF, ALPHA)[1],
                                      1e-4))


# --- the baseline ------------------------------------------------------------

def test_baseline_fallback():
    print("\n-- leave-one-editor-out and the fallback levels --")
    own = [node("single", 30.0)]
    excluded, _ = infer.genuine_dist(REF, "J1", "established", own)
    included, level = infer.genuine_dist(REF, "J1", "established", [])
    check("exclusion shrinks the editor's own state in the baseline",
          excluded["single"] < included["single"])
    check(f"a well-populated journal is the first level (got {level})",
          level == "journal")

    # The rule the docstring promises: FALLBACK_MIN_N is checked AFTER exclusion.
    own_journal = {"J3": {"publisher": "acm", "baseline_counts": {"established": {
        "outside": 5.0, "repeated": 0.0, "single": 30.0}}}}
    r = ref({**JOURNALS, **own_journal})
    mine = [node("single", 30.0, journal="J3")]
    _, level_alone = infer.genuine_dist(r, "J3", "established", [])
    _, level_mine = infer.genuine_dist(r, "J3", "established", mine)
    check(f"a 35-load journal passes FALLBACK_MIN_N on its own (got {level_alone})",
          level_alone == "journal")
    check(f"...but not once the editor who IS 30 of it is taken out (got {level_mine})",
          level_mine == "pooled")
    pooled_dist, _ = infer.genuine_dist(r, "J3", "established", mine)
    check("the pooled fallback also leaves the editor out",
          pooled_dist["single"]
          < infer.genuine_dist(r, "J3", "established", [],
                               condition_on_career_stage=False)[0]["single"])

    thin = {"J2": {"publisher": "acm", "baseline_counts": {
        "junior": {"outside": 1.0, "repeated": 0.0, "single": 2.0}}}}
    r = ref({**JOURNALS, **thin})
    _, level = infer.genuine_dist(r, "J2", "junior", [])
    check(f"a thin journal falls back to pooled (got {level})", level == "pooled")
    _, level = infer.genuine_dist(r, "J2", "junior", [], condition_on_career_stage=True)
    check(f"conditioned on career stage, a thin journal falls back to "
          f"pooled:career_stage first (got {level})", level == "pooled:career_stage")
    ieee = {"I1": {"publisher": "ieee", "baseline_counts": {
        "junior": {"outside": 0.0, "repeated": 0.0, "single": 5.0}}}}
    dist, level = infer.genuine_dist(ref({**JOURNALS, **ieee}), "I1", "junior", [])
    check(f"a thin journal falls back to its own publisher's pool, not another's "
          f"(got {level}, single={dist['single']:.3f})",
          level == "pooled" and approx(dist["single"], 5.5 / 6.5))
    _, level = infer.genuine_dist(infer.reference(corpus({})), "JX", "junior", [])
    check(f"an unknown journal with no pooled data still returns a distribution "
          f"(got {level})", level == "pooled")


def test_publishers_kept_apart():
    print("\n-- publishers are scored apart --")
    thin = {"J2": {"publisher": "acm", "baseline_counts": {"established": {
        "outside": 1.0, "repeated": 0.0, "single": 2.0}}}}
    ieee = {"I1": {"publisher": "ieee", "baseline_counts": {"established": {
        "outside": 0.0, "repeated": 0.0, "single": 500.0}}}}
    acm_node = [node("single", 1.0, journal="J2")]
    alone = posterior(acm_node, r=ref({**JOURNALS, **thin}))
    together = posterior(acm_node, r=ref({**JOURNALS, **thin, **ieee}))
    check("an ACM editor's posterior is unchanged when IEEE journals are added",
          approx(alone, together, 1e-12))
    both = posterior(acm_node + [node("single", 1.0, journal="I1")],
                     r=ref({**JOURNALS, **thin, **ieee}))
    check("an editor on both publishers' boards adds up the evidence of both",
          not approx(both, together))


def test_career_stage_is_not_a_parent():
    """The count is measured as of the dump rather than as of the paper, so the
    default marginalizes over career stage; the switch exists for the diagnostic."""
    print("\n-- the co-author's career stage --")
    lr_est = infer.node_lr("single", infer.genuine_dist(
        REF, "J1", "established", [])[0], ALPHA, MANIP)
    lr_jun = infer.node_lr("single", infer.genuine_dist(
        REF, "J1", "junior", [])[0], ALPHA, MANIP)
    check(f"career stage does not change the LR by default "
          f"(junior {lr_jun:.4f} == established {lr_est:.4f})", approx(lr_jun, lr_est))

    lr_est_s = infer.node_lr("single", infer.genuine_dist(
        REF, "J1", "established", [], condition_on_career_stage=True)[0], ALPHA, MANIP)
    lr_jun_s = infer.node_lr("single", infer.genuine_dist(
        REF, "J1", "junior", [], condition_on_career_stage=True)[0], ALPHA, MANIP)
    check(f"with condition_on_career_stage=True, `single` is milder for a junior co-author "
          f"(LR {lr_jun_s:.3f} > {lr_est_s:.3f})", lr_jun_s > lr_est_s)
    _, level = infer.genuine_dist(REF, "J1", "established", [], condition_on_career_stage=True)
    check(f"condition_on_career_stage=True reaches the journal:career_stage level (got {level})",
          level == "journal:career_stage")


# --- ranking, gates and output ------------------------------------------------

def test_ranking():
    print("\n-- ranking, shares and the in/out gate --")
    rows = infer.rank_corpus(ranking_corpus(), ALPHA, infer.PRIOR, manip=MANIP,
                             inout_gate=False)
    check("ranking puts the locked editor first and the outside tie last",
          [r["name"] for r in rows] == ["Ed One", "Ed Three", "Ed Two"])
    check("ranking is sorted ascending by weight of evidence",
          [r["woe"] for r in rows] == sorted(r["woe"] for r in rows))
    check("every ranking column is on every row",
          all(set(infer.RANKING_COLUMNS) <= set(r) for r in rows))
    check("a row carries its nodes and one detail record per node",
          all(len(r["detail"]) == r["n_nodes"] == len(r["nodes"]) for r in rows))
    check("the pid column is filled only for a DBLP-identified editor",
          [r["pid"] for r in rows] == ["", "", "E2"])
    check("each row lists the publishers of the editor's journals",
          all(r["publishers"] == "acm" for r in rows))

    by_name = {r["name"]: r for r in rows}
    check("the in/out gate flag follows the reddest year on a board",
          by_name["Ed Two"]["passes_inout_gate"] and not by_name["Ed One"]["passes_inout_gate"]
          and by_name["Ed Two"]["inout_x"] == 4 and by_name["Ed Two"]["inout_year"] == 2011)
    check("an editor without gate points does not pass",
          not by_name["Ed Three"]["passes_inout_gate"])

    gated = infer.rank_corpus(ranking_corpus(), ALPHA, infer.PRIOR, manip=MANIP,
                              inout_gate=True)
    check("applying the in/out gate keeps only the editors with a red seat",
          [r["name"] for r in gated] == ["Ed Two"])
    check("...without changing their posterior",
          approx(gated[0]["p_genuine"], by_name["Ed Two"]["p_genuine"], 1e-12))

    pointless = ranking_corpus()
    for info in pointless["editors"].values():
        info.pop("inout_points", None)
    try:
        infer.rank_corpus(pointless, manip=MANIP, inout_gate=True)
        stopped = False
    except SystemExit:
        stopped = True
    check("the gate refuses a corpus without in/out points instead of ranking nobody",
          stopped)

    shares, load = infer.state_shares([node("single", 3.0), node("outside", 0.2),
                                       node("outside", 0.2), node("outside", 0.2)])
    check(f"state shares are load-weighted (single={shares['single']:.3f})",
          approx(shares["single"], 3.0 / 3.6) and approx(load, 3.6))


def test_records():
    print("\n-- the scored records --")
    rows = infer.rank_corpus(ranking_corpus(), ALPHA, infer.PRIOR, manip=MANIP,
                             inout_gate=False)
    records = infer.editor_records(rows, threshold=0.9)
    check("one record per scored editor, in ranking order",
          [e["name"] for e in records] == [r["name"] for r in rows])
    check("the shortlist flag is P(genuine) below the threshold",
          [e["shortlisted"] for e in records]
          == [r["p_genuine"] < 0.9 for r in rows])
    many = [node("single", 1.0, coauthor="a"), node("outside", 1.0, coauthor="b"),
            node("repeated", 1.0, coauthor="c")]
    row = {**rows[0], "nodes": many,
           "detail": infer.score_editor(many, REF, ALPHA, manip=MANIP)[2]}
    contributions = [d["weighted_log_lr"] for d in
                     infer.editor_records([row])[0]["coauthorships"]]
    check("a record's co-authorships are listed most incriminating first",
          contributions == sorted(contributions) and contributions[0] < 0)


def test_inout_gate():
    print("\n-- the in/out boundary (figure 8.7) --")
    check("a year exactly on the diagonal passes",
          infer.passes_inout_gate([{"year": 2010, "x": 3, "y": 3}]))
    check("one red year carries an editor whose other years are blameless",
          infer.passes_inout_gate([{"year": 2010, "x": 1, "y": 9},
                                   {"year": 2011, "x": 4, "y": 2}]))
    check("more outside than inside in every year fails",
          not infer.passes_inout_gate([{"year": 2010, "x": 2, "y": 3}]))
    check("a year with no co-author at all (only solo papers) does not pass",
          not infer.passes_inout_gate([{"year": 2010, "x": 0, "y": 0}]))
    check("an editor who was never plotted is never a pass",
          not infer.passes_inout_gate(None) and not infer.passes_inout_gate([]))
    check("the reddest point is the largest x - y, not the largest ratio",
          infer.reddest_point([{"year": 2010, "x": 2, "y": 1},
                               {"year": 2011, "x": 30, "y": 25}])["year"] == 2011)
    check("no points means no reddest point", infer.reddest_point([]) is None)


def test_formatting():
    print("\n-- formatting --")
    check("a tiny posterior is never printed as 0.0000",
          infer.fmt_p(1e-7) == "1.00e-07" and infer.fmt_p(0.5) == "0.5000")
    check("extreme log-odds do not overflow, and 0 log-odds is P = 0.5",
          infer.log_odds_to_p(-800) == 0.0 and infer.log_odds_to_p(800) == 1.0
          and approx(infer.log_odds_to_p(0), 0.5))


def main():
    test_likelihood_ratios()
    test_oneoff_habit()
    test_typical_habit()
    test_personal_column()
    test_no_qualifying_node_is_the_tie_model()
    test_load_accumulation()
    test_baseline_fallback()
    test_publishers_kept_apart()
    test_career_stage_is_not_a_parent()
    test_ranking()
    test_records()
    test_inout_gate()
    test_formatting()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
