"""
Diagnostics and sensitivity sweeps over the scored corpus, printed to the console.

Reports: the shortlist per publisher, benign exclusions, the in/out gate, the
journals of the shortlist, co-author career stage, the within-editor tenure
control, the head of the ranking, and the personal one-off baseline (coverage,
the spread of r_bar, co-authors without a one-off, and a calibration table).
Sweeps: alpha, the prior, m(s), the personal adjustment (off, and its boundary
N_min), R_min and the `outside` reading, the entry conditions (gate and minimum
record), and the network against a count.

    python bbn_report.py [--in bbn/bbn_cs2_corpus.json]
"""

from __future__ import annotations

import math
import statistics
from collections import Counter, defaultdict

from bbn_extract import baseline_counts, classify
from bbn_infer import (corpus_path_argument, fmt_p, load_corpus, merge, node_lr,
                       passes_inout_gate, qualifies, rank_corpus, reference,
                       score_editor, smooth, state_shares, typical_oneoff_share)
from cs2_config import (ALPHA, ALPHA_SWEEP, FALLBACK_MIN_N, HEAD_P_LEVELS, INOUT_GATE,
                        MANIP_DIST, MANIP_SWEEP, MIN_CONTROL_NODES, MIN_RECORD_SWEEP,
                        OUTSIDE_READINGS, PERSONAL_SINGLE_CAP, POOL_MIN_SWEEP, PRIOR,
                        PRIOR_SWEEP, REPEATED_SWEEP, RHO_BINS, SHORTLIST_THRESHOLD)
from dblp_extract import pct

# Console layout only.
SWEEP_PRINT_CAP = 12
LOWEST_PRINT_CAP = 3        # ungated editors described without names
JOURNAL_PRINT_CAP = 25
MIN_SHORTLISTED_PER_JOURNAL = 5
HEAD_SIZE = 10              # the head of the ranking: #1 and the next nine

NO_ADJUSTMENT = math.inf    # a boundary no co-author reaches: the plain tie model


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
    red = [p for p in points if passes_inout_gate([p])]
    passing = [r for r in plotted if r["passes_inout_gate"]]
    print(f"  editors plotted {len(plotted)} of {len(rows)}; "
          f"points (editor x year on a board) {len(points)}")
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
    quiet = [r for r in passing
             if all(p["y"] == 0 for p in r["inout_points"] if passes_inout_gate([p]))]
    print(f"  ...of which {len(quiet)} pass only in years with nothing published outside "
          f"({sum(r['p_genuine'] < SHORTLIST_THRESHOLD for r in quiet)} shortlisted)")
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
    print("  (the co-author's publications before their first one with the editor)")
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


def print_ranking_head(rows, label):
    """P(genuine) of #1, the range over the next nine, and how many editors fall
    below each level of HEAD_P_LEVELS and below the prior."""
    if not rows:
        return
    ps = [r["p_genuine"] for r in rows]
    nxt = ps[1:HEAD_SIZE]
    print(f"  {label:<8} ranked {len(rows)}; #1 P={fmt_p(ps[0])}"
          + (f", #2-#{len(nxt) + 1} P {fmt_p(min(nxt))}-{fmt_p(max(nxt))}" if nxt else "")
          + "".join(f"; below {q}: {sum(p < q for p in ps)}" for q in HEAD_P_LEVELS)
          + f"; below the prior {PRIOR}: {sum(p < PRIOR for p in ps)}")


def print_head(rows, ungated):
    print("\n=== THE HEAD OF THE RANKING ===")
    print_ranking_head(rows, "gate" if INOUT_GATE else "run")
    print_ranking_head(ungated, "no gate")


# ---------------------------------------------------------------------------
# The personal one-off baseline
# ---------------------------------------------------------------------------

def load_share(nodes, keep) -> str:
    """The load share of `nodes` for which `keep` holds, as a percentage."""
    total = sum(n["load"] for n in nodes)
    return pct(sum(n["load"] for n in nodes if keep(n)), total) if total else "-"


def print_coverage(data):
    """How much of the scored load has a co-author with N_min collaborators of their
    own, per publisher and per tie."""
    nodes = list(data["coauthorships"].values())
    publishers = sorted({n.get("publisher", "") for n in nodes})

    def adjusted(n):
        return qualifies(n, FALLBACK_MIN_N)

    print(f"\n=== PERSONAL ONE-OFF BASELINE: COVERAGE "
          f"(co-authors with >= {FALLBACK_MIN_N:g} collaborators) ===")
    print(f"  {'publisher':<10}{'nodes':>8}{'load':>10}{'adjusted nodes':>16}"
          f"{'adjusted load':>15}{'no network':>12}")
    for label in publishers + ["all"]:
        group = [n for n in nodes if label == "all" or n.get("publisher", "") == label]
        print(f"  {label:<10}{len(group):>8}{sum(n['load'] for n in group):>10.1f}"
              f"{pct(sum(map(adjusted, group)), len(group)):>16}"
              f"{load_share(group, adjusted):>15}"
              f"{load_share(group, lambda n: not n.get('pool_n')):>12}")
    print("  adjusted load share per tie: "
          + ", ".join(f"{s} {load_share([n for n in nodes if n['state'] == s], adjusted)}"
                      for s in data["config"]["states"]))


def print_typical_share(ref, ungated):
    """The spread of r_bar_j over the journals (each journal whole, no editor left
    out), and how often the scored co-authorships fall back to the publisher."""
    print("\n=== THE JOURNAL'S TYPICAL ONE-OFF SHARE r_bar_j ===")
    for publisher in sorted(set(ref.publisher_of.values())):
        journals = [j for j, p in ref.publisher_of.items() if p == publisher]
        values = [typical_oneoff_share(ref, j, []) for j in journals]
        own = sorted(r for r, level in values if level == "journal")
        weighted, load = ref.oneoff_pooled.get(publisher, (0.0, 0.0))
        pooled = f"{weighted / load:.3f}" if load else "-"
        spread = (f"min {own[0]:.3f}, median {statistics.median(own):.3f}, "
                  f"max {own[-1]:.3f}" if own else "none")
        print(f"  {publisher:<6} {len(journals)} journals: {len(own)} with their own r_bar "
              f"({spread}); {len(journals) - len(own)} fall back to the publisher's "
              f"{pooled}")
    details = adjusted_details(ungated)
    fallback = [d for d in details if d["oneoff_level"] == "pooled"]
    print(f"  scored, with the editor left out: {len(fallback)} of {len(details)} adjusted "
          f"co-authorships use the publisher's r_bar "
          f"({load_share(details, lambda d: d['oneoff_level'] == 'pooled')} of their load)")


def adjusted_details(rows):
    """The detail records of every adjusted co-authorship in `rows`."""
    return [d for r in rows for d in r["detail"] if d["adjusted"]]


def rho_bin(rho, edges=RHO_BINS) -> int:
    """Index of the bin of rho, given ascending exclusive upper edges."""
    for i, edge in enumerate(edges):
        if rho < edge:
            return i
    return len(edges)


def rho_labels(edges=RHO_BINS) -> list[str]:
    """'<0.25', '0.25-0.5', ..., '>2' for the bins of rho_bin."""
    return ([f"<{edges[0]:g}"] + [f"{a:g}-{b:g}" for a, b in zip(edges, edges[1:])]
            + [f">{edges[-1]:g}"])


def calibration(details, edges=RHO_BINS) -> list[dict]:
    """Per bin of rho, load-weighted over the adjusted co-authorships: mean rho, the
    observed `single` share, the predicted g_i(single) and the journal's g(single)."""
    bins = defaultdict(list)
    for d in details:
        bins[rho_bin(d["rho"], edges)].append(d)
    out = []
    for i, label in enumerate(rho_labels(edges)):
        group = bins.get(i, [])
        load = sum(d["load"] for d in group)
        if not load:
            continue

        def mean(key):
            return sum(d[key] * d["load"] for d in group) / load

        out.append({"bin": label, "nodes": len(group), "load": load,
                    "rho": mean("rho"),
                    "observed": sum(d["load"] for d in group if d["state"] == "single") / load,
                    "predicted": mean("single_p"), "journal": mean("journal_single_p")})
    return out


def print_calibration_table(details, label):
    rows = calibration(details)
    print(f"\n  {label}: {len(details)} adjusted co-authorships, "
          f"load {sum(d['load'] for d in details):.1f}")
    print(f"  {'rho':>9}{'nodes':>8}{'load':>9}{'mean rho':>10}{'obs single':>12}"
          f"{'g_i(single)':>13}{'journal g':>11}{'obs/pred':>10}")
    for r in rows:
        print(f"  {r['bin']:>9}{r['nodes']:>8}{r['load']:>9.1f}{r['rho']:>10.2f}"
              f"{r['observed']:>12.3f}{r['predicted']:>13.3f}{r['journal']:>11.3f}"
              f"{r['observed'] / r['predicted']:>10.2f}")
    total = sum(r["load"] for r in rows)
    if total:
        print(f"  {'all':>9}{len(details):>8}{total:>9.1f}"
              f"{sum(r['rho'] * r['load'] for r in rows) / total:>10.2f}"
              + "".join(f"{sum(r[k] * r['load'] for r in rows) / total:>{w}.3f}"
                        for k, w in (("observed", 12), ("predicted", 13), ("journal", 11))))


def print_calibration(ungated, ref):
    """Is the scaling g(single|j) * rho monotone, roughly proportional, and
    conservative? Observed against predicted, over every scored editor."""
    details = adjusted_details(ungated)
    capped = [d for d in details if d["journal_single_p"] * d["rho"] > PERSONAL_SINGLE_CAP]
    print("\n=== CALIBRATION OF THE PERSONAL ONE-OFF BASELINE (bins of rho) ===")
    print("  (obs single = the load share of `single` among the adjusted co-authorships;")
    print("   g_i(single) = the model's g(single|j) * rho, journal g = g(single|j) unadjusted;")
    print("   both with the editor left out, as scored; the evidence is not genuine-only)")
    print(f"  the cap {PERSONAL_SINGLE_CAP} binds for {len(capped)} co-authorships")
    print_calibration_table(details, "all publishers")
    for publisher in sorted(set(ref.publisher_of.values())):
        print_calibration_table([d for d in details
                                 if ref.publisher_of.get(d["journal"]) == publisher],
                                publisher)


def print_zero_oneoffs(ungated):
    """Adjusted co-authors who never published once only with anyone: share_A rests
    on the pseudo-count alone."""
    zero = [d for d in adjusted_details(ungated) if d["pool_oneoffs"] == 0]
    print("\n=== ADJUSTED CO-AUTHORS WITH NO ONE-OFF COLLABORATOR (k_A = 0) ===")
    if not zero:
        print("  none")
        return
    rhos = sorted(d["rho"] for d in zero)
    print(f"  {len(zero)} co-authorships, load {sum(d['load'] for d in zero):.2f}; "
          f"rho {', '.join(f'{r:.3f}' for r in rhos)}; collaborators "
          f"{', '.join(str(d['pool_n']) for d in sorted(zero, key=lambda d: d['rho']))}; "
          f"ties {dict(Counter(d['state'] for d in zero))}")


def print_adjustment_sweep(data, base_pos, ungated_pos):
    """The shortlists without the personal adjustment and at other boundaries N_min,
    against the run's (gated and ungated)."""
    nodes = list(data["coauthorships"].values())
    print(f"\n=== PERSONAL ADJUSTMENT: ABLATION AND BOUNDARY "
          f"(the run: N_min = {FALLBACK_MIN_N:g}) ===")
    variants = [("no personal adjustment", NO_ADJUSTMENT)] + [
        (f"N_min = {n:g}", n) for n in POOL_MIN_SWEEP]
    for label, pool_min in variants:
        ungated = rank_corpus(data, inout_gate=False, pool_min=pool_min)
        gated = [r for r in ungated if r["passes_inout_gate"]]
        share = load_share(nodes, lambda n: qualifies(n, pool_min))
        report_stability(f"{label}, gate", gated, base_pos,
                         extra=f"adjusted load {share.strip()}")
        report_stability(f"{label}, no gate", ungated, ungated_pos)


# ---------------------------------------------------------------------------
# Sweeps
# ---------------------------------------------------------------------------

def reclassify_corpus(data, repeated_min, outside_reading="any"):
    """A copy of `data` with every node re-stated and the baselines rebuilt."""
    count = "n_shared_outside_strict" if outside_reading == "strict" else "n_shared_outside"

    def restate(nodes):
        return {nid: {**n, "state": classify(n[count], n["n_shared_inside"], repeated_min)}
                for nid, n in nodes.items()}

    nodes = restate(data["coauthorships"])
    return {**data, "coauthorships": nodes,
            "control_coauthorships": restate(data.get("control_coauthorships", {})),
            "journals": baseline_counts(nodes)}


def report_stability(label, rows, base_pos, extra=""):
    """One line: how a variant's shortlist compares with the baseline's {editor: rank}."""
    shortlist = [r for r in rows if r["p_genuine"] < SHORTLIST_THRESHOLD]
    pos = {r["ident"]: i for i, r in enumerate(shortlist)}
    common = set(base_pos) & set(pos)
    shifts = sorted(abs(base_pos[i] - pos[i]) for i in common)
    median = shifts[len(shifts) // 2] if shifts else 0
    print(f"  {label:<34} size={len(shortlist):<5} "
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
    boundaries = [("none", NO_ADJUSTMENT)] + [(n, n) for n in POOL_MIN_SWEEP]
    refs = {pool_min: reference(data, pool_min) for _, pool_min in boundaries}

    def posterior(row, alpha=ALPHA, prior=PRIOR, manip=MANIP_DIST, r=ref):
        return score_editor(row["nodes"], r, alpha, prior, manip=manip)[0]

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

    print_posterior_sweep(
        f"PERSONAL ADJUSTMENT BOUNDARY N_min  (P(genuine); top {len(sweep)})",
        "  N_min  ", boundaries, 6, sweep,
        lambda r, pool_min: posterior(r, r=refs[pool_min]))

    print(f"\n=== m(s) RANKING STABILITY vs baseline (shortlist n={len(shortlist)}) ===")
    for name, manip in MANIP_SWEEP.items():
        report_stability(name, rank_corpus(data, manip=manip), base_pos)

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
    from the pooled baseline, with no per-journal baseline and no personal adjustment."""
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

    plain = {r["ident"]: r for r in rank_corpus(data, inout_gate=False,
                                                 pool_min=NO_ADJUSTMENT)}
    print("\n=== THE NETWORK AGAINST A COUNT (fixed log LR per tie class and publisher) ===")
    for label, rows in (("gate", [r for r in ungated if r["passes_inout_gate"]]),
                        ("no gate", ungated)):
        net = {r["ident"] for r in shortlisted(rows)}
        count = {r["ident"] for r in rows if counted(r) < 0}
        tie = {r["ident"] for r in rows if plain[r["ident"]]["p_genuine"] < SHORTLIST_THRESHOLD}
        print(f"  {label:<8} network {len(net)} | count {len(count)}, {len(net & count)} in common"
              f" | network without the personal adjustment {len(tie)}, "
              f"{len(net & tie)} in common")
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
    ungated_pos = {r["ident"]: i for i, r in enumerate(shortlisted(ungated))}
    print_publishers(rows, shortlist)
    print_benign(data)
    print_gate(ungated)
    print_journals(shortlist)
    print_head(rows, ungated)
    print_record_diagnostic(rows, data, base_pos)
    print_tenure(rows, data)
    print_coverage(data)
    print_typical_share(reference(data), ungated)
    print_zero_oneoffs(ungated)
    print_calibration(ungated, reference(data))
    print_sweeps(data, rows, shortlist, base_pos)
    print_adjustment_sweep(data, base_pos, ungated_pos)
    print_entry_conditions(ungated, base_pos)
    print_count_ablation(data, ungated)


if __name__ == "__main__":
    main()
