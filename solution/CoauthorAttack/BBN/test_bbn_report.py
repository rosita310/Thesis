"""
Tests for bbn_report.py: the sweep machinery and the stability line. No data
file needed.

    python test_bbn_report.py
"""

from __future__ import annotations

import io
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
         fit_pct=None):
    return {"editor_id": editor, "journal": "J", "publisher": "acm", "coauthor_id": coauthor,
            "coauthor_name": coauthor, "state": state, "load": load,
            "career_stage": "established", "n_shared_inside": inside,
            "n_shared_outside": outside,
            "n_shared_outside_strict": outside if outside_strict is None else outside_strict,
            "n_shared_suspect": 1, "coauthor_pubs": 50, "fit_pct": fit_pct,
            "network_band": 0 if fit_pct is not None else None, "fit": ""}


def corpus():
    """Three editors in one journal, on the production states. E1 is venue-
    locked, E2 repeats with one group, E3 has an outside tie in a venue E3 also
    edits (so it is `outside_1` by default and `single` under the strict reading).
    E1 and E2 pass the in/out gate; E3's board seat lies below the diagonal."""
    seats = {"E1": (3, 1), "E2": (2, 2), "E3": (1, 4)}
    nodes = {
        "E1||J||a": node("E1", "a", "single", 1.0, fit_pct=0.1),
        "E2||J||b": node("E2", "b", "repeated", 2.0, inside=2, fit_pct=0.5),
        "E3||J||c": node("E3", "c", "outside_1", 2.0, outside=1, outside_strict=0,
                         fit_pct=0.9),
    }
    journals = {"J": {"publisher": "acm", "baseline_counts": {"established": {
        "outside_5plus": 30.0, "outside_2_4": 20.0, "outside_1": 22.0,
        "repeated": 12.0, "single": 13.0}}}}
    return {"config": {"states": cfg.STATES, "repeated_min": 2, "junior_max_pubs": 10},
            "journals": journals, "fit_baseline": None,
            "coauthorships": nodes,
            "editor_index": {e: [k for k in nodes if k.startswith(e)] for e in
                             ("E1", "E2", "E3")},
            "editor_labels": {"E1": "Ed One", "E2": "Ed Two", "E3": "Ed Three"},
            "editors": {e: {"n_suspect_papers": 2, "is_pid": False,
                            "inout_points": [{"journal": "J", "x": x, "y": y}]}
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
    check("the fit is re-cut and its baseline rebuilt with the new states",
          set(swept["fit_baseline"]) == {"acm"}
          and set(swept["fit_baseline"]["acm"]) <= set(cfg.STATES)
          and all(n["fit"] for n in swept["coauthorships"].values()))
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
                                 "FIT-FACTOR ABLATION", "DEFINITION SENSITIVITY")))
    check("the alpha table has one row per swept alpha",
          all(f"  {a:<6}" in out for a in cfg.ALPHA_SWEEP))


def main():
    test_sweep_settings()
    test_reclassify()
    test_stability_line()
    test_gate_report()
    test_publishers()
    test_sweeps_run()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
