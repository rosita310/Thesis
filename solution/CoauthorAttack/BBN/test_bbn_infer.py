"""
Tests for bbn_infer.py, on a synthetic corpus. No data file needed.

    python test_bbn_infer.py
"""

from __future__ import annotations

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
MANIP_FIT = {"single": {"peripheral": 0.60, "typical": 0.30, "core": 0.10},
             "outside": {"peripheral": 1 / 3, "typical": 1 / 3, "core": 1 / 3},
             "repeated": {"peripheral": 1 / 3, "typical": 1 / 3, "core": 1 / 3}}


def corpus(journals=JOURNALS, fit_base=None, **extra):
    return {"config": {"states": STATES, "repeated_min": 2, "junior_max_pubs": 10},
            "journals": journals, "fit_baseline": fit_base, **extra}


def ref(journals=JOURNALS, fit_base=None):
    return infer.reference(corpus(journals, fit_base))


REF = ref()


def node(state, load, record="established", coauthor="c", inside=1, outside=0,
         journal="J1", **extra):
    return {"journal": journal, "state": state, "load": load, "career_stage": record,
            "coauthor_id": coauthor, "coauthor_name": coauthor, "editor_id": "E",
            "n_shared_inside": inside, "n_shared_outside": outside,
            "n_shared_outside_strict": outside,
            "n_shared_suspect": 1, "coauthor_pubs": 50, **extra}


def posterior(nodes, alpha=ALPHA, prior=infer.PRIOR, r=REF):
    return infer.score_editor(nodes, r, alpha, prior, manip=MANIP,
                              manip_fit=MANIP_FIT)[0]


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
                   "inout_points": [{"journal": "J1", "x": 1, "y": 5}]},
            "E2": {"n_suspect_papers": 3, "is_pid": True,
                   "inout_points": [{"journal": "J1", "x": 4, "y": 2}]},
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


def test_fit_factor():
    print("\n-- the fit factor, the child of the state --")
    fit_base = {"acm": {
        "single": {"0": {"peripheral": 40.0, "typical": 40.0, "core": 40.0},
                   "1": {"peripheral": 5.0, "typical": 5.0, "core": 5.0}},
        "outside": {"0": {"peripheral": 40.0, "typical": 40.0, "core": 40.0}}}}
    r = ref(fit_base=fit_base)
    gdist, _ = infer.genuine_dist(r, "J1", "established", [])
    fdist, flevel = infer.fit_dist(r, "J1", "single", 0, [])
    check("a well-populated (state, band) level is used first",
          flevel == "state:band")
    check("the fit baseline smooths to a distribution", approx(sum(fdist.values()), 1.0))
    check("a thin band falls back to the state, pooled over bands",
          infer.fit_dist(r, "J1", "single", 1, [])[1] == "state")
    check("a state with no fit data at all falls back to uniform",
          infer.fit_dist(r, "J1", "repeated", 0, [])[1] == "uniform")
    r_ieee = ref({**JOURNALS, "I1": {"publisher": "ieee", "baseline_counts": {}}}, fit_base)
    check("another publisher's journal does not use this publisher's fit baseline",
          infer.fit_dist(r_ieee, "I1", "single", 0, [])[1] == "uniform")

    lr_per = infer.node_lr("single", gdist, ALPHA, MANIP, "peripheral", fdist, MANIP_FIT)
    lr_typ = infer.node_lr("single", gdist, ALPHA, MANIP, "typical", fdist, MANIP_FIT)
    lr_core = infer.node_lr("single", gdist, ALPHA, MANIP, "core", fdist, MANIP_FIT)
    check("a one-off with someone who rarely one-offs is the most incriminating",
          lr_per < lr_typ < lr_core)
    check("an unknown fit leaves the node on its state alone",
          approx(infer.node_lr("single", gdist, ALPHA, MANIP, "", fdist, MANIP_FIT),
                 infer.node_lr("single", gdist, ALPHA, MANIP)))
    check("m(state)=0 keeps LR at 1/(1-alpha) for EVERY fit value",
          all(approx(infer.node_lr("outside", gdist, ALPHA, MANIP, f, fdist, MANIP_FIT),
                     1 / (1 - ALPHA), 1e-9) for f in infer.FIT_STATES))

    own = [node("single", 30.0, fit="peripheral", network_band=0)]
    check("an editor is removed from their own fit baseline",
          infer.fit_dist(r, "J1", "single", 0, own)[0]["peripheral"]
          < infer.fit_dist(r, "J1", "single", 0, [])[0]["peripheral"])

    fit_nodes = [node("single", 1.0, fit="peripheral", network_band=0)]
    with_fit = posterior(fit_nodes, r=r)
    without = posterior(fit_nodes)
    check(f"a corpus with a fit baseline scores a peripheral one-off lower "
          f"({with_fit:.4f} vs {without:.4f} without one)", with_fit < without)
    check("a node without a fit is scored on its state even when the corpus has fits",
          approx(posterior([node("single", 1.0)], r=r), without))

    for state, shape in infer.MANIP_FIT.items():
        check(f"m(f|{state}) sums to 1", approx(sum(shape.values()), 1.0)
              and set(shape) == set(infer.FIT_STATES))


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
    check("the in/out gate flag follows the reddest board seat",
          by_name["Ed Two"]["passes_inout_gate"] and not by_name["Ed One"]["passes_inout_gate"]
          and by_name["Ed Two"]["inout_x"] == 4)
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
    check("a seat exactly on the diagonal passes",
          infer.passes_inout_gate([{"journal": "J", "x": 3, "y": 3}]))
    check("one red seat carries an editor whose other seat is blameless",
          infer.passes_inout_gate([{"journal": "A", "x": 1, "y": 9},
                                   {"journal": "B", "x": 4, "y": 2}]))
    check("more outside than inside on every seat fails",
          not infer.passes_inout_gate([{"journal": "A", "x": 2, "y": 3}]))
    check("an editor who was never plotted is never a pass",
          not infer.passes_inout_gate(None) and not infer.passes_inout_gate([]))
    check("the reddest point is the largest x - y, not the largest ratio",
          infer.reddest_point([{"journal": "A", "x": 2, "y": 1},
                               {"journal": "B", "x": 30, "y": 25}])["journal"] == "B")
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
    test_fit_factor()
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
