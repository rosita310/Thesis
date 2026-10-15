"""
Diagnostics and sensitivity sweeps over the scored corpus, printed to the console.

Reports: the shortlist per publisher, benign exclusions, the in/out gate, the
journals of the shortlist,
co-author career stage and the within-editor tenure control.
Sweeps: alpha, the prior, m(s), the fit factor, R_min and the `outside` reading,
the entry conditions (gate and minimum record), and the network against a count.

    python bbn_report.py [--in bbn/bbn_cs2_corpus.json]
"""

from __future__ import annotations

import math
import statistics
from collections import Counter, defaultdict

from bbn_extract import baseline_counts, classify, relabel_fit_per_publisher
from bbn_infer import (corpus_path_argument, fmt_p, load_corpus, merge, node_lr,
                       rank_corpus, reference, score_editor, smooth, state_shares)
from cs2_config import (ALPHA, ALPHA_SWEEP, INOUT_GATE, MANIP_DIST, MANIP_SWEEP,
                        MIN_CONTROL_NODES, MIN_RECORD_SWEEP, OUTSIDE_READINGS, PRIOR,
                        PRIOR_SWEEP, REPEATED_SWEEP, SHORTLIST_THRESHOLD)
from dblp_extract import pct

# Console layout only.
SWEEP_PRINT_CAP = 12
LOWEST_PRINT_CAP = 3        # ungated editors described without names
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
    strict = [r for r in plotted if any(p["x"] > p["y"] for p in r["inout_points"])]
    print(f"  ...with Westerbaan's strict x > y: {sum(p['x'] > p['y'] for p in points)} "
          f"points, {len(strict)} editors")
    print(f"  passing editors: median "
          f"{statistics.median(r['n_suspect_papers'] for r in passing)} suspect papers, "
          f"{sum(r['n_suspect_papers'] >= 5 for r in passing)} with >= 5")
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


def shortlisted(rows):
    return [r for r in rows if r["p_genuine"] < SHORTLIST_THRESHOLD]


def print_entry_conditions(ungated, base_pos):
    """Who is ranked, not how they are scored: the gate and a minimum record.
    `ungated` is the ranking of every scored editor."""
    gated = [r for r in ungated if r["passes_inout_gate"]]
    strict = [r for r in ungated if any(p["x"] > p["y"] for p in r["inout_points"] or [])]
    print("\n=== ENTRY CONDITIONS: who is ranked, not how they are scored ===")
    report_stability("gate x >= y (the run)", gated, base_pos)
    report_stability("gate x > y (Westerbaan)", strict, base_pos)
    report_stability("no gate", ungated, base_pos)
    for n_min in MIN_RECORD_SWEEP:
        report_stability(f"gate, >= {n_min} suspect papers",
                         [r for r in gated if r["n_suspect_papers"] >= n_min], base_pos)
        report_stability(f"no gate, >= {n_min} suspect papers",
                         [r for r in ungated if r["n_suspect_papers"] >= n_min], base_pos)

    short = shortlisted(ungated)
    if not short:
        return
    papers = [r["n_suspect_papers"] for r in short]
    publishers = Counter(p for r in short for p in r["publishers"].split("|") if p)
    print(f"\n  without the gate: {len(short)} below {SHORTLIST_THRESHOLD}, "
          f"{dict(publishers)}; median {statistics.median(papers)} suspect papers, "
          f"{sum(p >= 5 for p in papers)} with >= 5; "
          f"{sum(r['passes_inout_gate'] for r in short)} pass the gate; "
          f"the gated shortlist sits at "
          f"{[i + 1 for i, r in enumerate(short) if r['passes_inout_gate']]}")
    print("  the lowest, without names:")
    for i, r in enumerate(short[:LOWEST_PRINT_CAP], 1):
        repeated = sum(n["load"] for n in r["nodes"] if n["state"] == "repeated") / r["load"]
        print(f"    {i}. P={fmt_p(r['p_genuine'])} papers={r['n_suspect_papers']} "
              f"co-authorships={r['n_nodes']} single={r['share_single']:.0%} "
              f"repeated={repeated:.0%} outside={r['share_outside']:.0%} "
              f"journals={len(r['journals'].split('|'))} "
              f"x={r['inout_x']} y={r['inout_y']}")


def print_count_ablation(data, ungated):
    """The network against a count: one fixed log LR per tie class and publisher,
    from the pooled baseline, with no per-journal baseline and no fit."""
    ref = reference(data)
    states = ref.states
    log_lr = {(publisher, s): math.log(node_lr(s, smooth(merge(stages.values(), states),
                                                         states), ALPHA))
              for publisher, stages in ref.pooled.items() for s in states}
    prior = math.log(PRIOR / (1 - PRIOR))

    def counted(r):
        return prior + sum(n["load"] * log_lr[(ref.publisher_of.get(n["journal"], ""),
                                               n["state"])] for n in r["nodes"])

    def locked_share(journal):
        counts = merge(ref.journals[journal]["baseline_counts"].values(), states)
        total = sum(counts.values())
        return (counts["single"] + counts["repeated"]) / total if total else None

    def editor_locked(r):
        shares = [s for s in (locked_share(n["journal"]) for n in r["nodes"]) if s is not None]
        return statistics.fmean(shares) if shares else None

    def median_locked(rows):
        values = [v for v in map(editor_locked, rows) if v is not None]
        return f"{statistics.median(values):.1%}" if values else "-"

    no_fit = {r["ident"]: r for r in rank_corpus({**data, "fit_baseline": None},
                                                  inout_gate=False)}
    print("\n=== THE NETWORK AGAINST A COUNT (fixed log LR per tie class and publisher) ===")
    for label, rows in (("gate", [r for r in ungated if r["passes_inout_gate"]]),
                        ("no gate", ungated)):
        net = {r["ident"] for r in shortlisted(rows)}
        count = {r["ident"] for r in rows if counted(r) < 0}
        nofit = {r["ident"] for r in rows if no_fit[r["ident"]]["p_genuine"] < SHORTLIST_THRESHOLD}
        print(f"  {label:<8} network {len(net)} | count {len(count)}, {len(net & count)} in common"
              f" | network without the fit {len(nofit)}, {len(net & nofit)} in common")
        if label == "no gate":
            print(f"  share of venue-locked load in the editors' journals (median): "
                  f"all {median_locked(rows)}, count only "
                  f"{median_locked([r for r in rows if r['ident'] in count - net])}, "
                  f"network only {median_locked([r for r in rows if r['ident'] in net - count])}")


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

    ungated = rank_corpus(data, inout_gate=False)
    print_publishers(rows, shortlist)
    print_benign(data)
    print_gate(ungated)
    print_journals(shortlist)
    print_record_diagnostic(rows, data, base_pos)
    print_tenure(rows, data)
    print_sweeps(data, rows, shortlist, base_pos)
    print_entry_conditions(ungated, base_pos)
    print_count_ablation(data, ungated)


if __name__ == "__main__":
    main()
