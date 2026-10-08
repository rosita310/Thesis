"""
Diagnostics and sensitivity sweeps over the scored corpus, printed to the console.

Reports: the shortlist per publisher, benign exclusions, the in/out gate, the
journals of the shortlist,
co-author career stage and the within-editor tenure control.
Sweeps: alpha, the prior, m(s), the fit factor, R_min and the `outside` reading.

    python bbn_report.py [--in bbn/bbn_cs2_corpus.json]
"""

from __future__ import annotations

from collections import Counter, defaultdict

from bbn_extract import baseline_counts, classify, relabel_fit_per_publisher
from bbn_infer import (corpus_path_argument, fmt_p, load_corpus, rank_corpus,
                       reference, score_editor, state_shares)
from cs2_config import (ALPHA, ALPHA_SWEEP, INOUT_GATE, MANIP_DIST, MANIP_SWEEP,
                        MIN_CONTROL_NODES, OUTSIDE_READINGS, PRIOR,
                        PRIOR_SWEEP, REPEATED_SWEEP, SHORTLIST_THRESHOLD)
from dblp_extract import pct

# Console layout only.
SWEEP_PRINT_CAP = 12
JOURNAL_PRINT_CAP = 25
MIN_SHORTLISTED_PER_JOURNAL = 5


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

def print_publishers(rows, shortlist):
    """Ranked and shortlisted editors per publisher; an editor on boards of
    several publishers is counted under each."""
    ranked = Counter(p for r in rows for p in r["publishers"].split("|") if p)
    short = Counter(p for r in shortlist for p in r["publishers"].split("|") if p)
    both = sum(1 for r in rows if "|" in r["publishers"])
    print("\n=== PER PUBLISHER ===")
    print(f"  {'publisher':<10}{'ranked':>8}{'shortlisted':>13}")
    for publisher in sorted(ranked):
        print(f"  {publisher:<10}{ranked[publisher]:>8}{short[publisher]:>13}")
    print(f"  editors on boards of more than one publisher: {both}")


def print_journals(shortlist):
    """The journals the shortlist concentrates in."""
    per_journal = Counter(journal for r in shortlist
                          for journal in {n["journal"] for n in r["nodes"]})
    ranked = sorted(per_journal.items(), key=lambda kv: (-kv[1], kv[0]))
    print(f"\n=== JOURNALS BY #SHORTLISTED EDITORS (>= {MIN_SHORTLISTED_PER_JOURNAL}) ===")
    for journal, count in ranked[:JOURNAL_PRINT_CAP]:
        if count < MIN_SHORTLISTED_PER_JOURNAL:
            break
        print(f"  {count:>5}  {journal[:60]}")


def print_gate(rows):
    """The in/out gate over every scored editor; `rows` must be the ungated ranking."""
    print("\n=== IN/OUT CO-AUTHOR GATE (fig. 8.7: x = in-journal co-authors, y = outside) ===")
    plotted = [r for r in rows if r.get("inout_points")]
    if not plotted:
        print("  unavailable: run `python bbn_extract.py --stage editor_authors`,")
        print("  then rebuild the corpus with --stage corpus.")
        return
    points = [p for r in plotted for p in r["inout_points"]]
    red = [p for p in points if p["x"] >= p["y"]]
    passing = [r for r in plotted if r["passes_inout_gate"]]
    print(f"  editors plotted {len(plotted)} of {len(rows)}; "
          f"points (editor x board seat) {len(points)}")
    print(f"  points in the red region x >= y  {len(red):>6}"
          f"  ({pct(len(red), len(points))})")
    print(f"  editors with >= 1 red point      {len(passing):>6}"
          f"  ({pct(len(passing), len(plotted))})")
    margins = sorted((p["x"] - p["y"] for p in points), reverse=True)
    n = len(margins)
    print("  x - y at the top of the distribution: "
          + ", ".join(f"p{p}={margins[min(n - 1, int(n * (100 - p) / 100))]}"
                      for p in (99, 95, 90, 75, 50)))
    print(f"  (INOUT_GATE={INOUT_GATE}: "
          + ("only the passing editors are ranked below)" if INOUT_GATE
             else "every scored editor is ranked below)"))


def print_benign(data):
    """What the corpus excluded before scoring, and why."""
    benign = data.get("benign_coauthorships")
    if not benign:
        return
    kept = sum(n["load"] for n in data["coauthorships"].values())
    dropped = sum(n["load"] for n in benign.values())
    print("\n=== EXCLUDED BEFORE SCORING ===")
    print(f"  nodes {len(benign)}, load {dropped:.1f} "
          f"({pct(dropped, dropped + kept)} of the extracted evidence)")
    by_reason = defaultdict(lambda: [0, 0.0])
    for n in benign.values():
        tally = by_reason[n["benign_reason"]]
        tally[0] += 1
        tally[1] += n["load"]
    for reason, (count, load) in sorted(by_reason.items(), key=lambda kv: -kv[1][1]):
        print(f"    {reason:<14}{count:>6} nodes{load:>10.1f} load")
    states = Counter(n["state"] for n in benign.values())
    print("  the states they would have contributed: "
          + ", ".join(f"{s}={states.get(s, 0)}" for s in data["config"]["states"]))


def print_record_diagnostic(rows, data, base_pos):
    """The state mix per co-author career stage, and the ranking conditioned on it."""
    nodes = [n for r in rows for n in r["nodes"]]
    stages = sorted({n["career_stage"] for n in nodes})
    print("\n=== CO-AUTHOR CAREER STAGE (diagnostic; NOT a parent of the state node) ===")
    print("  (the co-author's record as of the dump, not as of the paper)")
    print(f"  {'career stage':<14}{'nodes':>8}{'load':>10}"
          + "".join(f"{s:>11}" for s in data["config"]["states"]))
    for stage in stages:
        subset = [n for n in nodes if n["career_stage"] == stage]
        shares, load = state_shares(subset)
        print(f"  {stage:<14}{len(subset):>8}{load:>10.1f}"
              + "".join(f"{shares.get(s, 0.0):>10.1%} " for s in data["config"]["states"]))

    print("\n  what conditioning on it would do to the ranking:")
    report_stability("conditioned on career stage",
                     rank_corpus(data, condition_on_career_stage=True), base_pos)


def print_tenure(rows, data):
    """The state mix before and during tenure, pooled and per editor."""
    controls = data.get("control_coauthorships", {})
    if not controls:
        print("\n=== WITHIN-EDITOR TENURE CONTROL: no pre-tenure co-authorships extracted ===")
        return
    suspect_nodes = [n for r in rows for n in r["nodes"]]
    scored = {r["ident"] for r in rows}
    control_nodes = [n for n in controls.values() if n["editor_id"] in scored]
    s_shares, s_load = state_shares(suspect_nodes)
    c_shares, c_load = state_shares(control_nodes)

    print("\n=== WITHIN-EDITOR TENURE CONTROL (same editor, same journal, before the board) ===")
    print("  (a diagnostic, NOT evidence in the network)")
    print(f"  {'state':<12}{'during tenure':>15}{'before tenure':>15}{'shift':>10}")
    for state in data["config"]["states"]:
        during, before = s_shares.get(state, 0.0), c_shares.get(state, 0.0)
        print(f"  {state:<12}{during:>14.1%}{before:>15.1%}{during - before:>+10.1%}")
    print(f"  load          {s_load:>14.1f}{c_load:>15.1f}"
          f"   ({len({n['editor_id'] for n in control_nodes})} editors have a control)")

    have = [r for r in rows if r["n_control_nodes"] >= MIN_CONTROL_NODES]
    print(f"\n  editors with >= {MIN_CONTROL_NODES} control co-authorships: {len(have)}"
          f" of {len(rows)}  <- the within-editor test is only available for these")
    if have:
        rose = sum(1 for r in have if r["share_single"] > r["control_share_single"])
        print(f"  ...whose `single` share ROSE on joining the board: {rose} "
              f"({pct(rose, len(have))})")


# ---------------------------------------------------------------------------
# Sweeps
# ---------------------------------------------------------------------------

def reclassify_corpus(data, repeated_min, outside_reading="any"):
    """A copy of `data` with every node re-stated and the baselines and fit rebuilt."""
    count = "n_shared_outside_strict" if outside_reading == "strict" else "n_shared_outside"

    def restate(nodes):
        return {nid: {**n, "state": classify(n[count], n["n_shared_inside"], repeated_min)}
                for nid, n in nodes.items()}

    nodes = restate(data["coauthorships"])
    return {**data, "coauthorships": nodes,
            "control_coauthorships": restate(data.get("control_coauthorships", {})),
            "journals": baseline_counts(nodes),
            "fit_baseline": relabel_fit_per_publisher(nodes)}


def report_stability(label, rows, base_pos, extra=""):
    """One line: how a variant's shortlist compares with the baseline's {editor: rank}."""
    shortlist = [r for r in rows if r["p_genuine"] < SHORTLIST_THRESHOLD]
    pos = {r["ident"]: i for i, r in enumerate(shortlist)}
    common = set(base_pos) & set(pos)
    shifts = sorted(abs(base_pos[i] - pos[i]) for i in common)
    median = shifts[len(shifts) // 2] if shifts else 0
    print(f"  {label:<28} size={len(shortlist):<5} "
          f"in-common={len(common):>4}/{len(base_pos):<4} "
          f"median|Drank|={median:<4} max|Drank|={shifts[-1] if shifts else 0:<4} {extra}")


def print_posterior_sweep(title, header, settings, label_width, sweep, score) -> None:
    """A P(genuine) table: one row per setting, one column per editor."""
    print(f"\n=== {title} ===")
    print(header + "  ".join(f"{r['name'].split()[-1][:11]:>11}" for r in sweep))
    for label, setting in settings:
        print(f"  {label:<{label_width}}"
              + "".join(f"  {fmt_p(score(r, setting)):>11}" for r in sweep))


def print_sweeps(data, rows, shortlist, base_pos):
    """Sensitivity of the posterior and the ranking to each setting."""
    sweep = (shortlist or rows)[:SWEEP_PRINT_CAP]
    if not sweep:
        return
    if not shortlist:
        print(f"\n(no editor is below {SHORTLIST_THRESHOLD}; the sweeps below run on "
              f"the {len(sweep)} lowest-scoring instead)")

    ref = reference(data)

    def posterior(row, alpha=ALPHA, prior=PRIOR, manip=MANIP_DIST):
        return score_editor(row["nodes"], ref, alpha, prior, manip=manip)[0]

    print_posterior_sweep(
        f"ALPHA SENSITIVITY  (P(genuine), prior={PRIOR}; top {len(sweep)})",
        "  alpha  ", [(a, a) for a in ALPHA_SWEEP], 6, sweep,
        lambda r, a: posterior(r, alpha=a))
    print_posterior_sweep(
        f"PRIOR SENSITIVITY  (P(genuine), alpha={ALPHA}; top {len(sweep)})",
        "  P(gen)0 ", [(p, p) for p in PRIOR_SWEEP], 6, sweep,
        lambda r, p: posterior(r, prior=p))
    print_posterior_sweep(
        f"MANIP-SHAPE m(s) SENSITIVITY  (P(genuine); top {len(sweep)})",
        "  shape       ", list(MANIP_SWEEP.items()), 11, sweep,
        lambda r, m: posterior(r, manip=m))

    print(f"\n=== m(s) RANKING STABILITY vs baseline (shortlist n={len(shortlist)}) ===")
    for name, manip in MANIP_SWEEP.items():
        report_stability(name, rank_corpus(data, manip=manip), base_pos)

    print("\n=== FIT-FACTOR ABLATION: every node scored on its state alone ===")
    report_stability("no fit factor", rank_corpus({**data, "fit_baseline": None}), base_pos)

    print("\n=== DEFINITION SENSITIVITY: `repeated` threshold and `outside` reading ===")
    print("  (re-derived from the corpus's own counts; the extract's setting is "
          f"repeated_min={data['config']['repeated_min']}, outside=any)")
    for reading in OUTSIDE_READINGS:
        for repeated_min in REPEATED_SWEEP:
            v_rows = rank_corpus(reclassify_corpus(data, repeated_min, reading))
            shares, _ = state_shares([n for r in v_rows for n in r["nodes"]])
            report_stability(f"outside={reading}, R>={repeated_min}", v_rows, base_pos,
                             extra=f"single={shares.get('single', 0):.1%} "
                                   f"out1={shares.get('outside_1', 0):.1%}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    in_path = corpus_path_argument("CS2 BBN diagnostics and sensitivity sweeps.")
    data = load_corpus(in_path)
    rows = rank_corpus(data)
    shortlist = [r for r in rows if r["p_genuine"] < SHORTLIST_THRESHOLD]
    base_pos = {r["ident"]: i for i, r in enumerate(shortlist)}
    print(f"Ranked {len(rows)} editors; {len(shortlist)} below threshold "
          f"{SHORTLIST_THRESHOLD}.")

    print_publishers(rows, shortlist)
    print_benign(data)
    print_gate(rank_corpus(data, inout_gate=False))
    print_journals(shortlist)
    print_record_diagnostic(rows, data, base_pos)
    print_tenure(rows, data)
    print_sweeps(data, rows, shortlist, base_pos)


if __name__ == "__main__":
    main()
