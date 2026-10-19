"""
Tests for bbn_report.py: the sweep machinery, the stability line and the
reports on the personal one-off baseline. No data file needed.

    python test_bbn_report.py
"""

from __future__ import annotations

import io
import math
from contextlib import redirect_stdout

import bbn_report as report
import cs2_config as cfg

_failures = 0


def check(label, condition):
    global _failures
    if not condition:
        _failures += 1
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}")


def approx(a, b, tol=1e-9):
    return abs(a - b) <= tol


def captured(fn, *args, **kwargs) -> str:
    buf = io.StringIO()
    with redirect_stdout(buf):
        fn(*args, **kwargs)
    return buf.getvalue()


def node(editor, coauthor, state, load, inside=1, outside=0, outside_strict=None,
         pool_n=None, pool_oneoffs=0):
    return {"editor_id": editor, "journal": "J", "publisher": "acm", "coauthor_id": coauthor,
            "coauthor_name": coauthor, "state": state, "load": load,
            "career_stage": "established", "n_shared_inside": inside,
            "n_shared_outside": outside,
            "n_shared_outside_strict": outside if outside_strict is None else outside_strict,
            "n_shared_suspect": 1, "coauthor_pubs": 50,
            "pool_n": pool_n, "pool_oneoffs": pool_oneoffs}


def corpus():
    """Three editors in one journal, on the production states. E1 is venue-
    locked, E2 repeats with one group, E3 has an outside tie in a venue E3 also
    edits (so it is `outside_1` by default and `single` under the strict reading).
    E1 and E2 pass the in/out gate; E3's board seat lies below the diagonal."""
    seats = {"E1": (3, 1), "E2": (2, 2), "E3": (1, 4)}
    nodes = {
        "E1||J||a": node("E1", "a", "single", 1.0),
        "E2||J||b": node("E2", "b", "repeated", 2.0, inside=2),
        "E3||J||c": node("E3", "c", "outside_1", 2.0, outside=1, outside_strict=0),
    }
    journals = {"J": {"publisher": "acm", "baseline_counts": {"established": {
        "outside_5plus": 30.0, "outside_2_4": 20.0, "outside_1": 22.0,
        "repeated": 12.0, "single": 13.0}}}}
    return {"config": {"states": cfg.STATES, "repeated_min": 2, "junior_max_pubs": 10},
            "journals": journals,
            "coauthorships": nodes,
            "editor_index": {e: [k for k in nodes if k.startswith(e)] for e in
                             ("E1", "E2", "E3")},
            "editor_labels": {"E1": "Ed One", "E2": "Ed Two", "E3": "Ed Three"},
            "editors": {e: {"n_suspect_papers": 2, "is_pid": False,
                            "inout_points": [{"year": 2010, "x": x, "y": y}]}
                        for e, (x, y) in seats.items()}}


def test_sweep_settings():
    print("\n-- the m(s) shapes --")
    for name, shape in cfg.MANIP_SWEEP.items():
        check(f"m(s) shape `{name}` sums to 1 over the right states",
              set(shape) == set(cfg.MANIP_DIST) and approx(sum(shape.values()), 1.0))


def test_reclassify():
    print("\n-- re-stating the corpus for the definitional sweeps --")
    data = corpus()
    swept = report.reclassify_corpus(data, repeated_min=3)
    check("R>=3 pushes a 2-paper in-journal pair from `repeated` to `single`",
          swept["coauthorships"]["E2||J||b"]["state"] == "single")
    check("...and leaves the original corpus untouched",
          data["coauthorships"]["E2||J||b"]["state"] == "repeated")
    check("the outside states are unaffected by the repeated threshold",
          swept["coauthorships"]["E3||J||c"]["state"] == "outside_1")
    strict = report.reclassify_corpus(data, repeated_min=2, outside_reading="strict")
    check("the strict `outside` reading demotes a tie in a venue the editor controls",
          strict["coauthorships"]["E3||J||c"]["state"] == "single")
    base = swept["journals"]["J"]["baseline_counts"]["established"]
    check("the baseline is rebuilt from the re-stated nodes, load-weighted",
          approx(base["single"], 3.0) and approx(base["outside_1"], 2.0)
          and approx(base["repeated"], 0.0))
    check("...over all five states, so a state with no node is present at 0",
          set(base) == set(cfg.STATES))
    check("the pool stats travel with the re-stated nodes",
          all("pool_n" in n for n in swept["coauthorships"].values()))
    check("a corpus without control nodes reclassifies without them",
          report.reclassify_corpus({**data, "control_coauthorships": {}}, 2)
          ["control_coauthorships"] == {})


def test_stability_line():
    print("\n-- the ranking-stability line --")
    base_pos = {"A": 0, "B": 1, "C": 2}
    variant = [{"ident": "C", "p_genuine": 0.1}, {"ident": "A", "p_genuine": 0.2},
               {"ident": "D", "p_genuine": 0.9}]
    line = captured(report.report_stability, "x", variant, base_pos)
    check("the variant's shortlist is everyone below the threshold", "size=2" in line)
    check("in-common counts the editors both shortlists hold",
          "in-common=   2/3" in line)
    check("the rank shifts are |1| for A and |2| for C, so median 2 and max 2",
          "median|Drank|=2" in line and "max|Drank|=2" in line)
    line = captured(report.report_stability, "x", [], base_pos)
    check("an empty variant shortlist reports zeros rather than failing",
          "size=0" in line and "in-common=   0/3" in line and "median|Drank|=0" in line)


def test_gate_report():
    print("\n-- the in/out gate report --")
    data = corpus()
    out = captured(report.print_gate, report.rank_corpus(data, inout_gate=False))
    check("the gate is reported over every scored editor, not only the passing ones",
          "editors plotted 3 of 3" in out)
    check("the seats on or above the diagonal are counted as red",
          "points in the red region x >= y       2" in out)
    check("an editor with one red seat passes",
          "editors with >= 1 red point           2" in out)


def test_publishers():
    print("\n-- per publisher --")
    rows = report.rank_corpus(corpus(), inout_gate=False)
    rows[0] = {**rows[0], "publishers": "acm|ieee"}
    out = captured(report.print_publishers, rows, rows[:1])
    check("an editor on both publishers' boards is counted under each",
          "acm              3            1" in out and "ieee             1            1" in out)
    check("...and reported once as such", "more than one publisher: 1" in out)


def test_sweeps_run():
    print("\n-- the sweeps on a corpus with no shortlist --")
    data = corpus()
    rows = report.rank_corpus(data)
    shortlist = [r for r in rows if r["p_genuine"] < cfg.SHORTLIST_THRESHOLD]
    out = captured(report.print_sweeps, data, rows, shortlist,
                   {r["ident"]: i for i, r in enumerate(shortlist)})
    if shortlist:
        check("(this corpus was expected to have an empty shortlist)", False)
    check("an empty shortlist falls back to the head of the ranking",
          "(no editor is below" in out)
    check("every sweep section is printed",
          all(h in out for h in ("ALPHA SENSITIVITY", "PRIOR SENSITIVITY",
                                 "MANIP-SHAPE", "RANKING STABILITY",
                                 "ADJUSTMENT BOUNDARY", "DEFINITION SENSITIVITY")))
    check("the boundary table has a row without the adjustment and one per N_min",
          "  none  " in out and all(f"  {n:<6}" in out for n in cfg.POOL_MIN_SWEEP))
    check("the alpha table has one row per swept alpha",
          all(f"  {a:<6}" in out for a in cfg.ALPHA_SWEEP))


def pooled_corpus():
    """corpus() with pool stats: E1's co-author has few one-offs, E2's many; and
    other editors' co-authors fill journal J well past N_min."""
    data = corpus()
    nodes = dict(data["coauthorships"])
    nodes["E1||J||a"] = {**nodes["E1||J||a"], "pool_n": 99, "pool_oneoffs": 9}
    nodes["E2||J||b"] = {**nodes["E2||J||b"], "pool_n": 99, "pool_oneoffs": 89}
    for i in range(40):
        nodes[f"O||J||o{i}"] = {**node("O", f"o{i}", "outside_5plus", 1.0, outside=6,
                                       pool_n=49, pool_oneoffs=24 + i % 3)}
    nodes["O||J||k0"] = node("O", "k0", "single", 0.5, pool_n=60, pool_oneoffs=0)
    nodes["O||J||small"] = node("O", "small", "single", 1.0, pool_n=5, pool_oneoffs=5)
    index = {e: [k for k in nodes if k.startswith(e + "|")] for e in ("E1", "E2", "E3", "O")}
    editors = {**data["editors"], "O": {"n_suspect_papers": 40, "is_pid": False,
                                        "inout_points": [{"year": 2010, "x": 1, "y": 9}]}}
    return {**data, "coauthorships": nodes, "editor_index": index, "editors": editors}


def test_rho_bins():
    print("\n-- the bins of rho --")
    check("an edge belongs to the bin above it",
          [report.rho_bin(r, [0.5, 1.0]) for r in (0.1, 0.5, 0.99, 1.0, 7)] == [0, 1, 1, 2, 2])
    check("the labels name every bin, the open ends included",
          report.rho_labels([0.25, 0.5, 2.0]) == ["<0.25", "0.25-0.5", "0.5-2", ">2"])
    details = [{"rho": 0.4, "load": 1.0, "state": "single", "single_p": 0.04,
                "journal_single_p": 0.1},
               {"rho": 0.6, "load": 3.0, "state": "outside_1", "single_p": 0.06,
                "journal_single_p": 0.1},
               {"rho": 1.5, "load": 2.0, "state": "single", "single_p": 0.15,
                "journal_single_p": 0.1}]
    rows = report.calibration(details, edges=[1.0])
    check("the calibration has one row per non-empty bin",
          [r["bin"] for r in rows] == ["<1", ">1"])
    low = rows[0]
    check("...with load-weighted means and the observed `single` load share",
          approx(low["load"], 4.0) and approx(low["rho"], 0.55)
          and approx(low["observed"], 0.25) and approx(low["predicted"], 0.055)
          and approx(low["journal"], 0.1))


def test_personal_reports():
    print("\n-- the reports on the personal one-off baseline --")
    data = pooled_corpus()
    ungated = report.rank_corpus(data, inout_gate=False)
    out = captured(report.print_coverage, data)
    check("coverage: the share of the load whose co-author reaches N_min",
          f"{'acm':<10}{45:>8}{46.5:>10.1f}" in out
          and f"{report.pct(43.5, 46.5):>15}" in out)
    out = captured(report.print_typical_share, report.reference(data), ungated)
    check("r_bar: J has its own value, no journal falls back",
          "1 journals: 1 with their own r_bar" in out and "0 fall back" in out)
    out = captured(report.print_zero_oneoffs, ungated)
    check("k = 0: the one co-author without a one-off collaborator is reported",
          "1 co-authorships, load 0.50" in out and "collaborators 60" in out)
    out = captured(report.print_calibration, ungated, report.reference(data))
    check("calibration: a table over all publishers and one per publisher",
          "all publishers: 43 adjusted" in out and "acm: 43 adjusted" in out)
    base = {r["ident"]: i for i, r in enumerate(report.shortlisted(ungated))}
    out = captured(report.print_adjustment_sweep, data, base, base)
    check("the ablation and one line per boundary, gated and not",
          out.count(", gate ") == 1 + len(cfg.POOL_MIN_SWEEP)
          and out.count(", no gate ") == 1 + len(cfg.POOL_MIN_SWEEP)
          and "adjusted load 0.0%" in out)
    by_id = {r["ident"]: r for r in ungated}
    off = {r["ident"]: r for r in report.rank_corpus(data, inout_gate=False,
                                                     pool_min=math.inf)}
    check("a co-author with few one-offs makes a `single` tie more incriminating; "
          "one without pool stats leaves the score as it was",
          by_id["E1"]["woe"] < off["E1"]["woe"] and by_id["E3"]["woe"] == off["E3"]["woe"])
    out = captured(report.print_count_ablation, data, ungated)
    check("the count comparison names the network without the personal adjustment",
          "network without the personal adjustment" in out)


def test_ranking_head():
    print("\n-- the head of the ranking --")
    rows = [{"p_genuine": p} for p in (0.1, 0.3, 0.6, 0.92, 0.97)]
    out = captured(report.print_ranking_head, rows, "x")
    check("#1, the range of the next ones, and the counts below each level",
          "#1 P=0.1000" in out and "#2-#5 P 0.3000-0.9700" in out
          and "below 0.5: 2" in out and "below 0.9: 3" in out
          and f"below the prior {cfg.PRIOR}: 4" in out)


def main():
    test_sweep_settings()
    test_reclassify()
    test_stability_line()
    test_gate_report()
    test_publishers()
    test_sweeps_run()
    test_rho_bins()
    test_personal_reports()
    test_ranking_head()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
