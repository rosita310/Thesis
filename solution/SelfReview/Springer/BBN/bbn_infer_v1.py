"""
BBN inference for the self-review case study (SQ1.1).

Reads bbn_baseline_<journal>.json (produced by bbn_extract_baseline.py),
discretizes the peer-group distributions into the CPT `Is fraudster = false`
rows, combines them with the elicited `Is fraudster = true` rows, and computes
the posterior P(fraudster | evidence) for each investigated author.

Network (v1):

        [article_type] [pages]        [Is fraudster]
                \        |         /         |       \
                 v       v        v          v        v
                   [Min log-z]        [Median log-z]   [Consistency]
                                                   ( N gaps -> confidence flag )

Inference is exact and done by hand (the net is small and we want it auditable):

    P(F=t | M, A, Md, C) proportional to
        P(F=t) * P(M | F=t, A) * P(Md | F=t)^[obs] * P(C | F=t)^[obs]

article_type A is observed and has no fraud parent, so its prior cancels; we
just read the P(M | F, A=a) column. Median and Consistency are GATED: a single
gap makes median == min (double counting) and consistency trivial, so they are
only conditioned on when there is enough data (see EVIDENCE GATING below).

Everything tunable lives in the CONFIG block. The fraud-conditional rows are
expert estimates; revise them and re-run. A prior sweep is printed so you can
report ranking stability rather than absolute posteriors.

Run from this directory with the project venv:
    python bbn_infer.py
"""

from __future__ import annotations

import json
import os

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

JOURNAL_ID = "10623"
IN_PATH = os.path.join(os.path.dirname(__file__), "bbn_baselines", f"bbn_baseline_{JOURNAL_ID}.json")

PRIOR_FRAUD = 0.05                       # base rate P(fraudster); swept below
PRIOR_SWEEP = [0.01, 0.02, 0.05, 0.10, 0.20]
LAPLACE = 0.5                            # additive smoothing for empty baseline cells

# --- evidence gating (prevents double-counting the single gap) -------------
MIN_NGAPS_FOR_MEDIAN = 2                 # condition on Median only if n_gaps >= this
MIN_YEARS_FOR_CONSISTENCY = 2            # condition on Consistency only if active years >= this

# --- discretization bins (tail-anchored; see thesis rationale) -------------
MIN_STATES = ["not_extreme", "extreme", "very_extreme"]
def bin_min(z):
    if z <= -4.0:   return "very_extreme"
    if z <= -2.0:   return "extreme"
    return "not_extreme"

MEDIAN_STATES = ["typical", "fast", "very_fast"]
def bin_median(z):
    if z <= -2.0:   return "very_fast"
    if z <= -1.0:   return "fast"
    return "typical"

CONSISTENCY_STATES = ["none", "incidental", "structural"]
def bin_consistency(n_periods):
    if n_periods <= 0:  return "none"
    if n_periods == 1:  return "incidental"
    return "structural"

def confidence_band(n_gaps):
    if n_gaps <= 2:   return "low"
    if n_gaps <= 9:   return "medium"
    return "high"

# --- ELICITED fraud-conditional rows  P(node | F=true) ---------------------
# Expert estimates. Reasoning is in the comments; revise and re-run.
# A fraudster who manipulates the review of (some of) their papers skews the
# WORST gap strongly fast; the MEDIAN only mildly (they don't manipulate every
# paper); consistency tends to recur but not every period.
ELICIT_MIN_GIVEN_FRAUD = {
    # normal article reviewed implausibly fast is the core signature
    "normal_type": {"not_extreme": 0.15, "extreme": 0.35, "very_extreme": 0.50},
    # editorials/letters are legitimately fast, so a fast gap is less diagnostic
    "fast_type":   {"not_extreme": 0.25, "extreme": 0.35, "very_extreme": 0.40},
}
ELICIT_MEDIAN_GIVEN_FRAUD = {"typical": 0.40, "fast": 0.35, "very_fast": 0.25}
ELICIT_CONSISTENCY_GIVEN_FRAUD = {"none": 0.10, "incidental": 0.50, "structural": 0.40}


# ---------------------------------------------------------------------------
# Build the F=false (baseline) rows from the peer distribution
# ---------------------------------------------------------------------------

def normalize_counts(counts, states):
    total = sum(counts.get(s, 0) for s in states) + LAPLACE * len(states)
    return {s: (counts.get(s, 0) + LAPLACE) / total for s in states}


def build_baseline(peers):
    # Min log-z, stratified by the worst paper's article-type bin
    min_counts = {"normal_type": {}, "fast_type": {}}
    for s in peers:
        a = s["min_paper_type_bin"]
        if a not in min_counts:
            a = "normal_type"
        b = bin_min(s["min_log_z"])
        min_counts[a][b] = min_counts[a].get(b, 0) + 1
    min_base = {a: normalize_counts(min_counts[a], MIN_STATES) for a in min_counts}
    min_n = {a: sum(min_counts[a].values()) for a in min_counts}

    # Median log-z, only over peers where the median is meaningful
    med_counts, med_n = {}, 0
    for s in peers:
        if s["n_gaps"] >= MIN_NGAPS_FOR_MEDIAN:
            med_counts[bin_median(s["median_log_z"])] = med_counts.get(bin_median(s["median_log_z"]), 0) + 1
            med_n += 1
    med_base = normalize_counts(med_counts, MEDIAN_STATES)

    # Consistency, only over peers active >= MIN_YEARS_FOR_CONSISTENCY years
    con_counts, con_n = {}, 0
    for s in peers:
        if s["years_active"] >= MIN_YEARS_FOR_CONSISTENCY:
            con_counts[bin_consistency(s["n_outlying_periods"])] = \
                con_counts.get(bin_consistency(s["n_outlying_periods"]), 0) + 1
            con_n += 1
    con_base = normalize_counts(con_counts, CONSISTENCY_STATES)

    return {"min": min_base, "min_n": min_n,
            "median": med_base, "median_n": med_n,
            "consistency": con_base, "consistency_n": con_n}


# ---------------------------------------------------------------------------
# Inference
# ---------------------------------------------------------------------------

def posterior(evidence, base, prior):
    """evidence: dict with min_z, type_bin, n_gaps, median_z, years_active, n_periods."""
    a = evidence["type_bin"] if evidence["type_bin"] in base["min"] else "normal_type"
    m = bin_min(evidence["min_z"])

    # likelihoods under F=true / F=false, starting from the always-observed Min
    lt = ELICIT_MIN_GIVEN_FRAUD[a][m]
    lf = base["min"][a][m]
    used = [f"Min={m}|type={a}"]

    if evidence["n_gaps"] >= MIN_NGAPS_FOR_MEDIAN:
        md = bin_median(evidence["median_z"])
        lt *= ELICIT_MEDIAN_GIVEN_FRAUD[md]
        lf *= base["median"][md]
        used.append(f"Median={md}")
    else:
        used.append("Median=GATED(n_gaps<2)")

    if evidence["years_active"] >= MIN_YEARS_FOR_CONSISTENCY:
        c = bin_consistency(evidence["n_periods"])
        lt *= ELICIT_CONSISTENCY_GIVEN_FRAUD[c]
        lf *= base["consistency"][c]
        used.append(f"Consistency={c}")
    else:
        used.append("Consistency=GATED(years<2)")

    num = prior * lt
    post = num / (num + (1 - prior) * lf)
    return post, used


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not os.path.exists(IN_PATH):
        raise SystemExit(f"Run bbn_extract_baseline.py first; {IN_PATH} not found.")
    with open(IN_PATH, encoding="utf-8") as f:
        data = json.load(f)

    peers = data["peer_stats"]
    base = build_baseline(peers)

    print(f"=== CPT  P(node | Is fraudster = FALSE)  -- empirical peer baseline ===")
    print(f"Min log-z | type=normal_type (n={base['min_n']['normal_type']}): "
          + ", ".join(f"{k}={v:.4f}" for k, v in base["min"]["normal_type"].items()))
    print(f"Min log-z | type=fast_type   (n={base['min_n']['fast_type']}, THIN -> smoothed): "
          + ", ".join(f"{k}={v:.4f}" for k, v in base["min"]["fast_type"].items()))
    print(f"Median log-z (n={base['median_n']}): "
          + ", ".join(f"{k}={v:.4f}" for k, v in base["median"].items()))
    print(f"Consistency  (n={base['consistency_n']}): "
          + ", ".join(f"{k}={v:.4f}" for k, v in base["consistency"].items()))

    print(f"\n=== CPT  P(node | Is fraudster = TRUE)  -- elicited (revise in CONFIG) ===")
    print(f"Min log-z | normal_type: {ELICIT_MIN_GIVEN_FRAUD['normal_type']}")
    print(f"Median log-z:            {ELICIT_MEDIAN_GIVEN_FRAUD}")
    print(f"Consistency:             {ELICIT_CONSISTENCY_GIVEN_FRAUD}")

    # mechanism-based alpha back-out (sanity check on the Min fraud row)
    p_base = base["min"]["normal_type"]["very_extreme"]
    p_fraud = ELICIT_MIN_GIVEN_FRAUD["normal_type"]["very_extreme"]
    # p_fraud ~= alpha + (1-alpha) p_base  =>  alpha = (p_fraud - p_base)/(1 - p_base)
    alpha = (p_fraud - p_base) / (1 - p_base)
    print(f"\nMechanism check (Min very_extreme): baseline p={p_base:.4f}, elicited under fraud={p_fraud:.2f} "
          f"=> implied fraction of manipulated papers alpha ~= {alpha:.2f} "
          f"({'plausible' if 0.05 <= alpha <= 0.8 else 'CHECK — implausible'})")

    print(f"\n=== POSTERIORS (prior P(fraud)={PRIOR_FRAUD}) ===")
    suspects = data["suspects"]
    rows = []
    for name, s in suspects.items():
        if s is None:
            print(f"  {name}: no usable data.")
            continue
        ev = {"min_z": s["min_log_z"], "type_bin": s["min_paper_type_bin"],
              "n_gaps": s["n_gaps"], "median_z": s["median_log_z"],
              "years_active": s["years_active"], "n_periods": s["n_outlying_periods"]}
        post, used = posterior(ev, base, PRIOR_FRAUD)
        conf = confidence_band(s["n_gaps"])
        rows.append((name, post, conf, s["n_gaps"]))
        print(f"  {name:<22} P(fraud)={post:.3f}  confidence={conf:<6} (n_gaps={s['n_gaps']})")
        print(f"       evidence used: {', '.join(used)}")

    print(f"\n=== PRIOR SENSITIVITY (ranking stability) ===")
    print("  prior   " + "  ".join(f"{n.split()[-1]:>10}" for n, *_ in rows))
    for pr in PRIOR_SWEEP:
        line = f"  {pr:<6}"
        for name, s in [(n, suspects[n]) for n, *_ in rows]:
            ev = {"min_z": s["min_log_z"], "type_bin": s["min_paper_type_bin"],
                  "n_gaps": s["n_gaps"], "median_z": s["median_log_z"],
                  "years_active": s["years_active"], "n_periods": s["n_outlying_periods"]}
            p, _ = posterior(ev, base, pr)
            line += f"  {p:>10.3f}"
        print(line)


if __name__ == "__main__":
    main()
