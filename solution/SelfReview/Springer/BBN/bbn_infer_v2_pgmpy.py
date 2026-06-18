"""
pgmpy cross-check of the hand-rolled V2 inference.

Builds the SAME per-author Bayesian network in pgmpy -- F = is_fraudster as the
latent root, one observed gap node per gap, each gap's CPT being the journal-
specific honest column for F=false and the alpha-mixture for F=true -- runs exact
VariableElimination, and prints the pgmpy posterior beside the hand-rolled one.
They should agree to ~1e-9.

This validates the claim that bbn_infer_v2.py's closed form
    posterior_odds = prior_odds * PRODUCT_gaps LR(gap)
IS exact Bayesian-network inference on this structure (a naive-Bayes polytree),
not an approximation.

CPTs and helpers are imported from bbn_infer_v2.py, so BOTH sides use byte-for-byte
identical numbers; any discrepancy would therefore be inference-only.

pgmpy pulls numpy/scipy/pandas (and, in recent versions, torch). Install it in a
SEPARATE analysis venv -- NOT the scraper venv:
    python -m venv .venv_analysis
    .venv_analysis\\Scripts\\activate
    pip install pgmpy
    python bbn_infer_v2_pgmpy.py            # full cross-check (needs the extract JSON)
    python bbn_infer_v2_pgmpy.py --selftest # API/agreement check, no DB/JSON needed
"""

from __future__ import annotations

import json
import os
import sys

import bbn_infer_v2 as v2

try:
    try:
        from pgmpy.models import DiscreteBayesianNetwork as BN   # pgmpy >= 1.0
    except ImportError:
        from pgmpy.models import BayesianNetwork as BN           # pgmpy < 1.0
    from pgmpy.factors.discrete import TabularCPD
    from pgmpy.inference import VariableElimination
except ImportError:
    sys.exit("pgmpy not installed. In a separate analysis venv:  pip install pgmpy")


# ---------------------------------------------------------------------------
# Build the per-author network and query P(F=true | all gaps)
# ---------------------------------------------------------------------------

def fraud_column(honest, alpha):
    """alpha-mixture fraud column for one gap: alpha*manip + (1-alpha)*honest."""
    return {b: alpha * v2.MANIP_DIST[b] + (1 - alpha) * honest[b] for b in honest}


def pgmpy_query(honest_columns, observed_bins, alpha, prior, bins):
    """honest_columns: list of honest dicts (one per gap); observed_bins: their z-bins."""
    model = BN()
    model.add_node("F")
    cpds = [TabularCPD("F", 2, [[1 - prior], [prior]],
                       state_names={"F": ["false", "true"]})]
    for i, honest in enumerate(honest_columns):
        fraud = fraud_column(honest, alpha)
        node = f"g{i}"
        model.add_edge("F", node)
        # rows = gap bins (in `bins` order); columns = F states [false, true]
        values = [[honest[b], fraud[b]] for b in bins]
        cpds.append(TabularCPD(node, len(bins), values,
                               evidence=["F"], evidence_card=[2],
                               state_names={node: bins, "F": ["false", "true"]}))
    model.add_cpds(*cpds)
    model.check_model()

    infer = VariableElimination(model)
    evidence = {f"g{i}": b for i, b in enumerate(observed_bins)}
    try:
        q = infer.query(["F"], evidence=evidence, show_progress=False)
    except TypeError:                       # older pgmpy without show_progress
        q = infer.query(["F"], evidence=evidence)
    return float(q.values[q.state_names["F"].index("true")])


def hand_one_author(honest_columns, observed_bins, alpha, prior, bins):
    """Hand-rolled posterior using the same LR formula as bbn_infer_v2."""
    odds = prior / (1 - prior)
    for honest, b in zip(honest_columns, observed_bins):
        odds *= (1 - alpha) + alpha * v2.MANIP_DIST[b] / honest[b]
    return odds / (1 + odds)


# ---------------------------------------------------------------------------
# Self-test (no DB / JSON needed) -- proves pgmpy API + agreement on a toy case
# ---------------------------------------------------------------------------

def selftest():
    bins = ["typical", "mild_fast", "extreme", "very_extreme"]
    honest = {"typical": 0.90, "mild_fast": 0.06, "extreme": 0.03, "very_extreme": 0.01}
    alpha, prior = 0.10, 0.05
    cols = [honest, honest]                 # two gaps
    obs = ["very_extreme", "typical"]       # one extreme, one normal
    hand = hand_one_author(cols, obs, alpha, prior, bins)
    pg = pgmpy_query(cols, obs, alpha, prior, bins)
    print(f"SELFTEST  hand={hand:.9f}  pgmpy={pg:.9f}  |diff|={abs(hand-pg):.2e}  "
          f"-> {'MATCH' if abs(hand - pg) < 1e-6 else 'MISMATCH'}")


# ---------------------------------------------------------------------------
# Main: cross-check the three suspects against the hand-rolled inference
# ---------------------------------------------------------------------------

def main():
    if "--selftest" in sys.argv:
        selftest()
        return

    if not os.path.exists(v2.IN_PATH):
        raise SystemExit(f"Run bbn_extract_v2.py first; {v2.IN_PATH} not found "
                         f"(or use --selftest).")
    with open(v2.IN_PATH, encoding="utf-8") as f:
        data = json.load(f)
    bins = data["config"]["z_bins"]
    journals = data["journals"]
    suspects = data["suspects"]
    pooled = v2.build_pooled(journals, bins)
    alpha, prior = v2.ALPHA, v2.PRIOR_FRAUD

    print(f"Cross-check vs pgmpy ({BN.__name__}) | alpha={alpha} prior={prior}\n")
    print(f"  {'author':<22} {'hand':>11} {'pgmpy':>11} {'|diff|':>10}")
    maxdiff = 0.0
    for name, s in suspects.items():
        if not s["gaps"]:
            continue
        cols = [v2.honest_dist(journals, pooled, bins, g["journal_id"],
                               g["type_bin"], g["pages_bin"])[0] for g in s["gaps"]]
        obs = [g["z_bin"] for g in s["gaps"]]
        hand = hand_one_author(cols, obs, alpha, prior, bins)
        pg = pgmpy_query(cols, obs, alpha, prior, bins)
        maxdiff = max(maxdiff, abs(hand - pg))
        print(f"  {name:<22} {hand:>11.6f} {pg:>11.6f} {abs(hand - pg):>10.2e}")
    print(f"\n  max |diff| = {maxdiff:.2e}  -> "
          f"{'MATCH (exact inference confirmed)' if maxdiff < 1e-6 else 'MISMATCH - investigate'}")


if __name__ == "__main__":
    main()
